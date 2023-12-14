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

package lcnode

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/cubefs/cubefs/objectnode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

type CRRScanner struct {
	taskId      string
	Volume      string
	mw          MetaWrapper
	lcnode      *LcNode
	adminTask   *proto.AdminTask
	rule        *proto.CRRRule
	srcS3Client *s3.S3
	dstUploader *s3manager.Uploader
	marker      string
	lock        sync.RWMutex
	crrStat     *proto.CRRTaskStatistic
	limiter     *rate.Limiter
}

func NewCRRScanner(adminTask *proto.AdminTask, l *LcNode) (*CRRScanner, error) {
	request := adminTask.Request.(*proto.CRRTaskRequest)
	scanTask := request.Task
	var err error

	var metaConfig = &meta.MetaConfig{
		Volume:        scanTask.VolName,
		Masters:       l.masters,
		Authenticate:  false,
		ValidateOwner: false,
	}

	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return nil, err
	}

	scanner := &CRRScanner{
		taskId:    scanTask.Id,
		Volume:    scanTask.VolName,
		lcnode:    l,
		mw:        metaWrapper,
		adminTask: adminTask,
		marker:    request.Task.Rule.Marker,
		rule:      scanTask.Rule,
		crrStat:   &proto.CRRTaskStatistic{},
		limiter:   rate.NewLimiter(lcScanLimitPerSecond, defaultLcScanLimitBurst),
	}

	srcS3Cfg := &proto.S3ClientConfig{
		Region:           region,
		EndPoint:         scanTask.Rule.SrcS3Cfg.S3Addr,
		Ak:               scanTask.SrcAuth.AK,
		Sk:               scanTask.SrcAuth.SK,
		MaxRetries:       maxRetries,
		S3ForcePathStyle: true,
	}
	dstS3Cfg := &proto.S3ClientConfig{
		Region:           region,
		EndPoint:         scanTask.Rule.DstS3Cfg.S3Addr,
		Ak:               scanTask.DstAuth.AK,
		Sk:               scanTask.DstAuth.SK,
		MaxRetries:       maxRetries,
		S3ForcePathStyle: true,
	}
	scanner.srcS3Client = proto.NewS3Client(srcS3Cfg)
	scanner.dstUploader = proto.NewS3Manager(dstS3Cfg)

	return scanner, nil
}

func (l *LcNode) startCRRScan(adminTask *proto.AdminTask) (err error) {
	request := adminTask.Request.(*proto.CRRTaskRequest)
	log.LogInfof("startCRRScan: scan task(%v) received!", request.Task)
	resp := &proto.CRRTaskResponse{}
	adminTask.Response = resp

	l.scannerMutex.Lock()
	defer l.scannerMutex.Unlock()
	if _, ok := l.CRRScanners[request.Task.Id]; ok {
		log.LogInfof("startCRRScan: scan task(%v) is already running!", request.Task)
		return
	}
	scanner, err := NewCRRScanner(adminTask, l)
	if err != nil {
		log.LogErrorf("startCRRScan: NewCRRScanner err(%v)", err)
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	l.CRRScanners[scanner.taskId] = scanner

	if err = scanner.Start(); err != nil {
		log.LogErrorf("startCRRScan: scanner Start err(%v)", err)
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}

	return
}

func (s *CRRScanner) Start() (err error) {

	log.LogInfof("Start scan %+v", s.rule)
	defer func() {
		log.LogInfof("Exit scan %+v", s.rule)
	}()

	marker := s.getMarker()
	objects, _, err := s.listFiles(s.rule.Prefix, marker)
	if err != nil && err != syscall.ENOENT {
		return err
	}
	if len(objects) == 0 {
		return nil
	}

	go s.replicate()

	return nil

}

func (s *CRRScanner) replicate() {
	for {
		marker := s.getMarker()
		objects, nextMarker, err := s.listFiles(s.rule.Prefix, marker)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("replicate: listFiles err(%v)", err)
			time.Sleep(time.Second)
			continue
		}
		if len(objects) == 0 {
			log.LogWarnf("replicate: The crr task(%v) has no objects to be replicated ", s.taskId)
			return
		}
		// replicate object
		for _, object := range objects {
			err := replicateByS3Client(s, object)
			if err != nil {
				log.LogWarnf("replicate: crr err(%v), task(%v), object(%v) ", err, s.taskId, object.Path)
			}
		}
		// update marker
		if len(nextMarker) > 0 {
			s.setMarker(objects[len(objects)-1].Path)
			continue
		}

		log.LogWarnf("replicate: The crr task(%v) is done ", s.taskId)
		t := time.Now()
		response := &proto.CRRTaskResponse{
			EndTime: &t,
			Status:  proto.TaskSucceeds,
			Done:    true,
			Id:      s.taskId,
			LcNode:  s.lcnode.localServerAddr,
			CRRTaskStatistic: proto.CRRTaskStatistic{
				SuccessNum: s.crrStat.SuccessNum,
				FailNum:    s.crrStat.FailNum,
				SkipNum:    s.crrStat.SkipNum,
			},
		}
		s.adminTask.Response = response
		s.lcnode.scannerMutex.Lock()
		delete(s.lcnode.CRRScanners, s.taskId)
		s.lcnode.scannerMutex.Unlock()
		s.lcnode.respondToMaster(s.adminTask)
		s.Stop()
		return
	}
}

func replicateByS3Client(s *CRRScanner, object *objectnode.FSFileInfo) error {
	srcGetInput := &s3.GetObjectInput{
		Bucket: aws.String(s.rule.SrcS3Cfg.VolName),
		Key:    aws.String(object.Path),
	}
	srcReq, srcObject := s.srcS3Client.GetObjectRequest(srcGetInput)
	err := srcReq.Send()
	if srcObject.Body != nil {
		// body should be closed when replicate is done
		defer srcObject.Body.Close()
	}
	if err != nil {
		log.LogErrorf("replicateByS3Client: GetObjectRequest err(%v), task(%v), object(%v)", err, s.taskId, object.Path)
		atomic.StoreInt64(&s.crrStat.FailNum, 1)
		return err
	}
	upParams := &s3manager.UploadInput{
		Bucket:   aws.String(s.rule.DstS3Cfg.VolName),
		Key:      aws.String(object.Path),
		Body:     aws.ReadSeekCloser(srcObject.Body),
		Metadata: aws.StringMap(object.Metadata),
	}
	_, err = s.dstUploader.Upload(upParams, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = uploadPartConcurrency
	})
	if err != nil {
		log.LogErrorf("replicateByS3Client: PutObjectRequest err(%v), task(%v), object(%v)", err, s.taskId, object.Path)
		atomic.StoreInt64(&s.crrStat.FailNum, 1)
		return err
	}
	log.LogInfof("replicateByS3Client: replicate success, task(%v), object(%v)", s.taskId, object.Path)
	atomic.StoreInt64(&s.crrStat.SuccessNum, 1)
	return nil
}

func (s *CRRScanner) setMarker(marker string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.marker = marker
}

func (s *CRRScanner) getMarker() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.marker
}

func (s *CRRScanner) listFiles(prefix, marker string) (infos []*objectnode.FSFileInfo, nextMarker string, err error) {
	parentId, dirs, err := s.FindPrefixInode()
	if err != nil {
		log.LogErrorf("listFiles: find parentInode err(%v), prefix(%v) marker(%v) ", err, prefix, marker)
		return nil, "", err
	}

	var rc uint64
	// recursion scan
	infos, nextMarker, _, err = s.recursiveScan(infos, parentId, maxKeys, rc, dirs, prefix, marker, true, true)
	if err != nil {
		log.LogErrorf("listFiles: volume list dir fail: Volume(%v) err(%v)", s.Volume, err)
		return
	}
	return
}

func (s *CRRScanner) FindPrefixInode() (inode uint64, prefixDirs []string, err error) {
	var dirs []string
	prefixDirs = make([]string, 0)
	prefix := s.rule.Prefix
	if prefix != "" {
		dirs = strings.Split(prefix, "/")
		log.LogInfof("FindPrefixInode: volume(%v), prefix(%v), dirs(%v), len(%v)", s.Volume, prefix, dirs, len(dirs))
	}
	if len(dirs) <= 1 {
		return proto.RootIno, prefixDirs, nil
	}

	var parentId = proto.RootIno
	for index, dir := range dirs {

		// Because lookup can only retrieve dentry whose name exactly matches,
		// so do not lookup the last part.
		if index+1 == len(dirs) {
			break
		}

		curIno, curMode, err := s.mw.Lookup_ll(parentId, dir)

		// If the part except the last part does not match exactly the same dentry, there is
		// no path matching the path prefix. An ENOENT error is returned to the caller.
		if err == syscall.ENOENT {
			log.LogErrorf("FindPrefixInode: find directories fail ENOENT: parentId(%v) dir(%v)", parentId, dir)
			return 0, nil, syscall.ENOENT
		}

		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("FindPrefixInode: find directories fail: prefix(%v) err(%v)", prefix, err)
			return 0, nil, err
		}

		// Because the file cannot have the next level members,
		// if there is a directory in the middle of the prefix,
		// it means that there is no file matching the prefix.
		if !os.FileMode(curMode).IsDir() {
			return 0, nil, syscall.ENOENT
		}

		prefixDirs = append(prefixDirs, dir)
		parentId = curIno
	}
	inode = parentId

	return
}

func (s *CRRScanner) recursiveScan(fileInfos []*objectnode.FSFileInfo, parentId, maxKeys, rc uint64, dirs []string,
	prefix, marker string, onlyObject, firstEnter bool) ([]*objectnode.FSFileInfo, string, uint64, error) {
	var err error
	var nextMarker string
	var lastKey string

	var currentPath = strings.Join(dirs, pathSep) + pathSep
	if strings.HasPrefix(currentPath, pathSep) {
		currentPath = strings.TrimPrefix(currentPath, pathSep)
	}
	log.LogDebugf("recursiveScan enter: currentPath(/%v) fileInfos(%v) parentId(%v) prefix(%v) marker(%v) rc(%v)",
		currentPath, fileInfos, parentId, prefix, marker, rc)
	defer func() {
		log.LogDebugf("recursiveScan exit: currentPath(/%v) fileInfos(%v) parentId(%v)  prefix(%v) nextMarker(%v) rc(%v)",
			currentPath, fileInfos, parentId, prefix, nextMarker, rc)
	}()

	// The "prefix" needs to be extracted as marker when it is larger than "marker".
	prefixMarker := ""
	if prefix != "" {
		if len(dirs) == 0 {
			prefixMarker = prefix
		} else if strings.HasPrefix(prefix, currentPath) {
			prefixMarker = strings.TrimPrefix(prefix, currentPath)
		}
	}

	// To be sent in the readdirlimit request as a search start point.
	fromName := ""
	// Marker in this layer, shall be compared with prefixMarker to
	// determine which one should be used as the search start point.
	currentMarker := ""
	if marker != "" {
		markerNames := strings.Split(marker, pathSep)
		if len(markerNames) > len(dirs) {
			currentMarker = markerNames[len(dirs)]
		}
		if prefixMarker > currentMarker {
			fromName = prefixMarker
		} else {
			fromName = currentMarker
		}
	} else if prefixMarker != "" {
		fromName = prefixMarker
	}

	// During the process of scanning the child nodes of the current directory, there may be other
	// parallel operations that may delete the current directory.
	// If got the syscall.ENOENT error when invoke readdir, it means that the above situation has occurred.
	// At this time, stops process and returns success.
	var children []proto.Dentry

readDir:
	children, err = s.mw.ReadDirLimit_ll(parentId, fromName, maxKeys+1) // one more for nextMarker
	if err != nil && err != syscall.ENOENT {
		return fileInfos, "", 0, err
	}
	if err == syscall.ENOENT {
		return fileInfos, "", 0, nil
	}

	log.LogDebugf("recursiveScan read: currentPath(%v) parentId(%v) fromName(%v) maxKey(%v) children(%v)",
		currentPath, parentId, fromName, maxKeys, children)

	for _, child := range children {
		if child.Name == lastKey {
			continue
		}
		var path = strings.Join(append(dirs, child.Name), pathSep)
		if os.FileMode(child.Type).IsDir() {
			path += pathSep
		}
		if prefix != "" && !strings.HasPrefix(path, prefix) {
			continue
		}

		if marker != "" {
			if !os.FileMode(child.Type).IsDir() && path < marker {
				continue
			}
			if os.FileMode(child.Type).IsDir() && strings.HasPrefix(marker, path) {
				fileInfos, nextMarker, rc, err = s.recursiveScan(fileInfos, child.Inode, maxKeys, rc, append(dirs, child.Name), prefix, marker, false, false)
				if err != nil {
					return fileInfos, nextMarker, rc, err
				}
				if rc >= maxKeys && nextMarker != "" {
					return fileInfos, nextMarker, rc, err
				}
				continue
			}
		}

		if onlyObject && os.FileMode(child.Type).IsRegular() || !onlyObject {
			if rc >= maxKeys {
				nextMarker = path
				return fileInfos, nextMarker, rc, nil
			}

			fileInfo := &objectnode.FSFileInfo{
				Inode: child.Inode,
				Path:  path,
			}
			if marker == "" || marker != "" && fileInfo.Path != marker {
				fileInfos = append(fileInfos, fileInfo)
				rc++
			}
		}

		if os.FileMode(child.Type).IsDir() {
			nextMarker = fmt.Sprintf("%v%v%v", currentPath, child.Name, pathSep)
			fileInfos, nextMarker, rc, err = s.recursiveScan(fileInfos, child.Inode, maxKeys, rc, append(dirs, child.Name), prefix, nextMarker, onlyObject, false)
			if err != nil {
				return fileInfos, nextMarker, rc, err
			}
			if rc >= maxKeys && nextMarker != "" {
				return fileInfos, nextMarker, rc, err
			}
		}
	}

	if firstEnter && len(children) > 1 && rc <= maxKeys {
		lastKey = children[len(children)-1].Name
		if strings.HasPrefix(strings.Join(append(dirs, lastKey), pathSep), prefix) {
			fromName = lastKey
			maxKeys = maxKeys - rc + 1
			log.LogDebugf("recursiveScan continue: currentPath(%v) parentId(%v) prefix(%v) marker(%v) lastKey(%v) rc(%v)",
				currentPath, parentId, prefix, marker, lastKey, rc)
			goto readDir
		}
	}

	return fileInfos, nextMarker, rc, nil
}

func (s *CRRScanner) Stop() {
	s.mw.Close()
	log.LogInfof("crr scanner(%v) stopped", s.taskId)
}
