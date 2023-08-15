// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/rs/xid"
)

const (
	minGranularityMin = 5
	maxGranularityMin = 60
	dateStringLength  = 10
)

var loggingBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, util.BlockSize)
	},
}

type VolumeGetter interface {
	getVol(name string) (*Volume, error)
}

type LoggingManager interface {
	Send(ctx context.Context, data string) error
	Close() error
}

type LoggingMgr struct {
	once    sync.Once
	closeCh chan struct{}
	writers map[string]LoggingWriter

	sync.RWMutex
	VolumeGetter
	LoggingConfig
}

type LoggingConfig struct {
	Async          bool `json:"async"`
	GranularityMin int  `json:"granularity_min"`
}

func (cfg *LoggingConfig) checkAndFix() error {
	if cfg.GranularityMin < minGranularityMin {
		cfg.GranularityMin = minGranularityMin
	}
	if cfg.GranularityMin > maxGranularityMin {
		cfg.GranularityMin = maxGranularityMin
	}

	return nil
}

func NewLoggingManager(getter VolumeGetter, conf LoggingConfig) (LoggingManager, error) {
	if err := conf.checkAndFix(); err != nil {
		return nil, err
	}

	mgr := &LoggingMgr{
		writers:       make(map[string]LoggingWriter),
		closeCh:       make(chan struct{}),
		VolumeGetter:  getter,
		LoggingConfig: conf,
	}
	go mgr.staleWriterCheckLoop()

	return mgr, nil
}

func (l *LoggingMgr) Send(ctx context.Context, data string) (err error) {
	span := trace.SpanFromContext(ctx)
	start := time.Now()
	defer func() {
		span.AppendTrackLog("loggings", start, err)
	}()

	if l.Async {
		go l.send(data)
	} else {
		err = l.send(data)
	}

	return
}

func (l *LoggingMgr) Close() error {
	l.once.Do(func() {
		close(l.closeCh)
	})

	for _, w := range l.writers {
		if w != nil {
			_ = w.Close()
		}
	}

	return nil
}

func (l *LoggingMgr) staleWriterCheckLoop() {
	const writerCheck = 60 * time.Second

	t := time.NewTimer(writerCheck)
	defer t.Stop()
	for {
		select {
		case <-l.closeCh:
			return
		case <-t.C:
			var deletes []string
			for key, writer := range l.loadAllWriters() {
				if len(key) > dateStringLength && writer != nil {
					dt, err := time.Parse("2006-01-02", key[len(key)-dateStringLength:])
					if err != nil {
						log.LogErrorf("invalid writer key: %s", key)
						continue
					}
					now := time.Now().UTC()
					if now.Sub(dt) > 24*time.Hour && now.Sub(writer.ModTime()) > time.Hour {
						deletes = append(deletes, key)
					}
					log.LogInfof("writer check key: %s, date: %v, now: %v, mtime: %v", key, dt, now, writer.ModTime())
				} else {
					log.LogWarnf("invalid writer key: %s", key)
				}
			}
			if len(deletes) > 0 {
				l.removeStaleWriters(deletes)
			}
			t.Reset(writerCheck)
		}
	}
}

func (l *LoggingMgr) removeStaleWriters(keys []string) {
	l.Lock()
	defer l.Unlock()

	for _, k := range keys {
		if writer := l.writers[k]; writer != nil {
			if err := writer.Close(); err != nil {
				log.LogWarnf("writer close failed: %v", err)
			}
		}
		delete(l.writers, k)
	}
}

func (l *LoggingMgr) send(line string) error {
	name, data, err := l.parseLine(line)
	if err != nil {
		log.LogErrorf("parse line(%s) failed: %v", line, err)
		return err
	}
	if len(data) <= 0 {
		return nil
	}

	var writer LoggingWriter
	key := filepath.Dir(name)
	if writer = l.getWriter(key); writer == nil {
		if writer, err = l.loadOrSetWriter(key); err != nil {
			log.LogErrorf("load or set writer(%s) failed: %v", key, err)
			return err
		}
	}
	if err = writer.Write(filepath.Base(name), data); err != nil {
		log.LogErrorf("write data to file(%v) failed: %v", name, err)
	}

	return err
}

func (l *LoggingMgr) parseLine(line string) (name string, data []byte, err error) {
	entry, err := auditlog.ParseReqlog(line)
	if err != nil || entry.Bucket() == "" {
		return
	}

	var vol *Volume
	if vol, err = l.getVol(entry.Bucket()); err != nil {
		return
	}
	logging, err := vol.metaLoader.loadLogging()
	if err != nil || logging == nil || logging.LoggingEnabled == nil {
		return
	}
	bucket := logging.LoggingEnabled.TargetBucket
	prefix := logging.LoggingEnabled.TargetPrefix

	return makeLoggingNameData(bucket, prefix, makeLoggingData(entry), l.GranularityMin)
}

func (l *LoggingMgr) getWriter(name string) LoggingWriter {
	l.RLock()
	defer l.RUnlock()

	return l.writers[name]
}

func (l *LoggingMgr) loadOrSetWriter(key string) (writer LoggingWriter, err error) {
	l.Lock()
	defer l.Unlock()

	if writer = l.writers[key]; writer == nil {
		keys := strings.SplitN(key, "/", 2)
		if len(keys) != 2 {
			err = fmt.Errorf("invalid key: %s", key)
			return
		}
		bucket, dir := keys[0], keys[1]
		var vol *Volume
		if vol, err = l.getVol(bucket); err != nil {
			return
		}
		if writer, err = newLoggingWriter(vol, dir+"/"); err == nil {
			l.writers[key] = writer
		}
	}

	return
}

func (l *LoggingMgr) loadAllWriters() map[string]LoggingWriter {
	l.RLock()
	defer l.RUnlock()

	a := make(map[string]LoggingWriter, len(l.writers))
	for k, v := range l.writers {
		a[k] = v
	}

	return a
}

type LoggingWriter interface {
	ModTime() time.Time
	Write(name string, data []byte) error
	Close() error
}

type loggingWrite struct {
	parentIno uint64
	vol       *Volume
	files     map[string]LoggingFile

	once    sync.Once
	closeCh chan struct{}
	modTime time.Time

	sync.RWMutex
}

func newLoggingWriter(vol *Volume, dir string) (LoggingWriter, error) {
	w := &loggingWrite{
		vol:     vol,
		files:   make(map[string]LoggingFile),
		closeCh: make(chan struct{}),
	}
	var err error
	if w.parentIno, err = w.makeParentDir(dir); err != nil {
		return nil, err
	}

	go w.staleFilesCheckLoop()

	return w, nil
}

func (w *loggingWrite) ModTime() time.Time {
	return w.modTime
}

func (w *loggingWrite) Write(name string, data []byte) error {
	file, err := w.openFile(name)
	if err != nil {
		return err
	}

	if err = file.Write(bytes.NewReader(data)); err != nil {
		if err == syscall.ENOENT {
			log.LogWarnf("file %v has been deleted elsewhere", file.Inode())
			w.removeStaleFiles([]string{name})
		}
		return err
	}
	w.modTime = time.Now().UTC()

	return nil
}

func (w *loggingWrite) Close() error {
	w.once.Do(func() {
		close(w.closeCh)
	})

	for _, file := range w.files {
		if file != nil {
			_ = file.Close()
		}
	}

	return nil
}

func (w *loggingWrite) staleFilesCheckLoop() {
	const fileCheck = 60 * time.Second

	t := time.NewTimer(fileCheck)
	defer t.Stop()
	for {
		select {
		case <-w.closeCh:
			return
		case <-t.C:
			var files []string
			for key, file := range w.loadAllFiles() {
				if file != nil && time.Since(file.ModTime()) > time.Hour {
					files = append(files, key)
				}
				log.LogInfof("file check key: %s, now: %v, mtime: %v", key, time.Now(), file.ModTime())
			}
			if len(files) > 0 {
				w.removeStaleFiles(files)
			}
			t.Reset(fileCheck)
		}
	}
}

func (w *loggingWrite) loadAllFiles() map[string]LoggingFile {
	w.RLock()
	defer w.RUnlock()

	a := make(map[string]LoggingFile, len(w.files))
	for k, v := range w.files {
		a[k] = v
	}

	return a
}

func (w *loggingWrite) removeStaleFiles(keys []string) {
	w.Lock()
	defer w.Unlock()

	for _, key := range keys {
		if file := w.files[key]; file != nil {
			if err := file.Close(); err != nil {
				log.LogErrorf("file %+v close failed: %v", file, err)
			}
		}
		delete(w.files, key)
	}
}

func (w *loggingWrite) makeParentDir(dir string) (uint64, error) {
	return w.vol.recursiveMakeDirectory(dir)
}

func (w *loggingWrite) getFile(name string) LoggingFile {
	w.RLock()
	defer w.RUnlock()

	return w.files[name]
}

func (w *loggingWrite) openFile(key string) (LoggingFile, error) {
	var file LoggingFile
	if file = w.getFile(key); file == nil {
		w.Lock()
		if file = w.files[key]; file == nil {
			info, err := w.vol.mw.InodeCreate_ll(w.parentIno, DefaultFileMode, 0, 0, nil, nil)
			if err != nil {
				log.LogErrorf("create sub inode of %d failed: %v", w.parentIno, err)
				w.Unlock()
				return nil, err
			}
			name := fmt.Sprintf("%s-%s", key, xid.New().String())
			if err = w.vol.applyInodeToDEntry(w.parentIno, name, info.Inode); err != nil {
				log.LogErrorf("apply ino(%d/%s) to dentry failed: %v", info.Inode, name, err)
				_, _ = w.vol.mw.InodeUnlink_ll(info.Inode)
				w.Unlock()
				return nil, err
			}
			file = newLoggingFile(info.Inode, info.Size, w.vol)
			w.files[key] = file
		}
		w.Unlock()
	}

	return file, w.vol.ec.OpenStream(file.Inode())
}

type LoggingFile interface {
	Inode() uint64
	ModTime() time.Time
	Write(reader io.Reader) error
	Close() error
}

type loggingFile struct {
	sync.RWMutex

	vol     *Volume
	inode   uint64
	size    uint64
	modTime time.Time
}

func newLoggingFile(inode, size uint64, vol *Volume) LoggingFile {
	return &loggingFile{inode: inode, size: size, vol: vol}
}

func (f *loggingFile) Inode() uint64 {
	return f.inode
}

func (f *loggingFile) ModTime() time.Time {
	return f.modTime
}

func (f *loggingFile) Write(reader io.Reader) error {
	f.Lock()
	defer f.Unlock()

	off := int(f.size)
	wn, err := f.write(off, reader)
	if err != nil {
		log.LogErrorf("write file %d failed: %v", f.inode, err)
		return err
	}
	if err = f.vol.ec.Flush(f.inode); err != nil {
		log.LogErrorf("flush file %d failed: %v", f.inode, err)
		return err
	}
	f.size += uint64(wn)
	f.modTime = time.Now()
	log.LogInfof("write file inode: %d, size: %d/%d, write: %d", f.inode, off, f.size, wn)

	return nil
}

func (f *loggingFile) Close() error {
	return f.vol.ec.CloseStream(f.inode)
}

func (f *loggingFile) write(off int, reader io.Reader) (write int, err error) {
	buf := loggingBufPool.Get().([]byte)
	defer loggingBufPool.Put(buf)

	checkFunc := func() error {
		if !f.vol.mw.EnableQuota {
			return nil
		}
		if ok := f.vol.ec.UidIsLimited(0); ok {
			return syscall.ENOSPC
		}
		if f.vol.mw.IsQuotaLimitedById(f.inode, true, false) {
			return syscall.ENOSPC
		}
		return nil
	}

	var rn, wn int
	for {
		if rn, err = reader.Read(buf); err != nil && err != io.EOF {
			break
		}
		if rn > 0 {
			wn, err = f.vol.ec.Write(f.inode, off, buf[:rn], proto.FlagsAppend, checkFunc)
			if err != nil {
				break
			}
			off += wn
			write += wn
		}
		if err == io.EOF {
			err = nil
			break
		}
	}

	return
}
