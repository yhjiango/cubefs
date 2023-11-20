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
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/cubefs/cubefs/util/log"
)

const (
	ReplicationARNFormat = "arn:cfs:replication:%v::%v"
)

type ReplicationStatus string

const (
	ReplEnabled  ReplicationStatus = "Enabled"
	ReplDisabled ReplicationStatus = "Disabled"
)

type ReplicationType int

const (
	InitReplicationType ReplicationType = 0 + iota
	PutReplicationType
	DeleteReplicationType
	UpdateReplicationType
	MultipartReplicationType
)

func (t ReplicationType) IsValid() bool {
	return t > InitReplicationType && t <= MultipartReplicationType
}

type ReplicationDestination struct {
	Bucket       string `xml:"Bucket" json:"Bucket"`
	StorageClass string `xml:"StorageClass,omitempty" json:"StorageClass,omitempty"`

	AccessKey  string
	SecretKey  string
	Region     string
	ObjectAddr string
}

type ReplicationDeleteMarker struct {
	Status ReplicationStatus `xml:"Status" json:"Status"`
}

type ReplicationRule struct {
	ID           string                  `xml:"ID,omitempty" json:"ID,omitempty"`
	Status       ReplicationStatus       `xml:"Status" json:"Status"`
	Prefix       string                  `xml:"Prefix" json:"Prefix"`
	DeleteMarker ReplicationDeleteMarker `xml:"DeleteMarkerReplication" json:"DeleteMarker"`
	Destination  ReplicationDestination  `xml:"Destination" json:"Destination"`
}

type ReplicationConfig struct {
	XMLName xml.Name          `xml:"ReplicationConfiguration" json:"-"`
	Role    string            `xml:"Role" json:"Role"`
	Rules   []ReplicationRule `xml:"Rule" json:"Rules"`
}

func (c ReplicationConfig) Filter(key string, op ReplicationType) map[string]ReplicationDestination {
	replDstMap := make(map[string]ReplicationDestination)
	for _, rule := range c.Rules {
		if rule.Status == ReplDisabled {
			continue
		}
		if op == DeleteReplicationType && rule.DeleteMarker.Status == ReplDisabled {
			continue
		}
		if !strings.HasPrefix(key, rule.Prefix) {
			continue
		}
		arn := fmt.Sprintf(ReplicationARNFormat, rule.Destination.Region, rule.Destination.Bucket)
		if _, ok := replDstMap[arn]; !ok {
			replDstMap[arn] = rule.Destination
		}
	}

	return replDstMap
}

type ReplicationOptions struct {
	OpType             ReplicationType
	RequestID          string
	ReplicationRequest bool
}

type ReplicationInfo struct {
	ARN      string
	Bucket   string
	FileInfo *FSFileInfo
	Reader   io.Reader

	ReplicationOptions
}

type Replicator struct {
	vol     *Volume
	clients map[string]ObjectLayer

	sync.RWMutex
}

func NewReplicator(vol *Volume) *Replicator {
	return &Replicator{
		vol:     vol,
		clients: make(map[string]ObjectLayer),
	}
}

func (r *Replicator) Clients() map[string]ObjectLayer {
	r.RLock()
	defer r.RUnlock()
	cls := make(map[string]ObjectLayer)
	for k, v := range r.clients {
		cls[k] = v
	}

	return cls
}

func (r *Replicator) SetClients(cls map[string]ObjectLayer) {
	r.Lock()
	defer r.Unlock()

	r.clients = cls
}

func (r *Replicator) Client(arn string) ObjectLayer {
	r.RLock()
	defer r.RUnlock()

	return r.clients[arn]
}

func (r *Replicator) Schedule(info ReplicationInfo) error {
	if info.FileInfo != nil {
		client := r.Client(info.ARN)
		if client == nil {
			return fmt.Errorf("object client of %s not found", info.ARN)
		}

		commonOpts := CommonOptions{
			RequestID:   info.RequestID,
			RequestFrom: ValueReplication,
		}
		fsInfo := info.FileInfo

		var err error
		defer func() {
			if aerr, ok := err.(awserr.Error); ok {
				log.LogErrorf("Replicator: aws error: %v", aerr)
				switch aerr.Code() {
				case "ObjectWithNewerPutTime", "AccessDenied":
					err = nil
				case "BadDigest":
					if info.OpType == DeleteReplicationType {
						err = nil
					}
				}
			}
		}()

		switch info.OpType {
		case PutReplicationType:
			if info.Reader == nil {
				// reade file by inode
				reader, writer := io.Pipe()
				go func() {
					size := uint64(fsInfo.Size)
					err = r.vol.readFile(fsInfo.Inode, size, fsInfo.Path, writer, 0, size)
					if err != nil {
						log.LogErrorf("Replicator: schedule read file failed: %v", err)
					}
					writer.CloseWithError(err)
				}()
				info.Reader = reader
			}

			opts := PutObjectOptions{
				ContentMD5:         DecodeETagHex(fsInfo.ETag),
				ContentType:        fsInfo.MIMEType,
				ContentDisposition: fsInfo.Disposition,
				CacheControl:       fsInfo.CacheControl,
				ETag:               fsInfo.ETag,
				Expires:            fsInfo.Expires,
				Tagging:            fsInfo.Tagging,
				Size:               fsInfo.Size,
				PutTime:            fsInfo.PutTime,
				Metadata:           fsInfo.Metadata,
				ACLs:               make(map[string]string),
				CommonOptions:      commonOpts,
			}
			if fsInfo.ACL != "" {
				opts.ACLs[XCfsSourceAcl] = base64.StdEncoding.EncodeToString([]byte(fsInfo.ACL))
			}
			err = client.PutObject(info.Bucket, fsInfo.Path, io.LimitReader(info.Reader, opts.Size), opts)

			return err

		case MultipartReplicationType:
			if len(info.FileInfo.Parts) == 0 {
				return errors.New("no parts of multipart found in file info")
			}

			// init multipart
			opts := PutObjectOptions{
				ContentType:        fsInfo.MIMEType,
				ContentDisposition: fsInfo.Disposition,
				CacheControl:       fsInfo.CacheControl,
				Expires:            fsInfo.Expires,
				Tagging:            fsInfo.Tagging,
				Metadata:           fsInfo.Metadata,
				ACLs:               make(map[string]string),
				CommonOptions:      commonOpts,
			}
			if fsInfo.ACL != "" {
				opts.ACLs[XCfsSourceAcl] = base64.StdEncoding.EncodeToString([]byte(fsInfo.ACL))
			}

			var uploadID string
			for try := 1; try <= 3; try++ {
				if uploadID, err = client.CreateMultipartUpload(info.Bucket, fsInfo.Path, opts); err == nil {
					break
				}
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
			}
			if err != nil {
				return err
			}

			defer func() {
				if err != nil {
					_ = client.AbortMultipartUpload(info.Bucket, fsInfo.Path, uploadID, opts.CommonOptions)
				}
			}()

			// reade file by inode
			reader, writer := io.Pipe()
			errChan := make(chan error, 1)
			go func() {
				size := uint64(fsInfo.Size)
				rerr := r.vol.readFile(fsInfo.Inode, size, fsInfo.Path, writer, 0, size)
				if rerr != nil {
					errChan <- rerr
				}
				writer.CloseWithError(rerr)
			}()

			// upload part
			var parts []*ObjectPart
			for _, part := range fsInfo.Parts {
				select {
				case err = <-errChan:
					if err != nil {
						log.LogErrorf("Replicator: schedule read file failed: %v", err)
						return err
					}
				default:
				}

				var op *ObjectPart
				upOpts := UploadPartOptions{
					Number:        part.PartNumber,
					Size:          part.Size,
					ContentMD5:    DecodeETagHex(part.ETag),
					Reader:        io.LimitReader(reader, part.Size),
					CommonOptions: commonOpts,
				}
				if op, err = client.UploadPart(info.Bucket, fsInfo.Path, uploadID, upOpts); err != nil {
					return err
				}

				parts = append(parts, op)
			}

			// complete part
			copts := CompletePartOptions{
				Parts: parts,
				PutObjectOptions: PutObjectOptions{
					ETag:          fsInfo.ETag,
					PutTime:       fsInfo.PutTime,
					CommonOptions: commonOpts,
				},
			}
			_, err = client.CompleteMultipartUpload(info.Bucket, fsInfo.Path, uploadID, copts)

			return err

		case DeleteReplicationType:
			opts := DeleteObjectOptions{
				ETag:          fsInfo.ETag,
				PutTime:       fsInfo.PutTime,
				CommonOptions: commonOpts,
			}
			err = client.DeleteObject(info.Bucket, fsInfo.Path, opts)

			return err

		case UpdateReplicationType:
			opts := PutObjectOptions{
				ContentType:        fsInfo.MIMEType,
				ContentDisposition: fsInfo.Disposition,
				CacheControl:       fsInfo.CacheControl,
				Expires:            fsInfo.Expires,
				Tagging:            fsInfo.Tagging,
				Metadata:           fsInfo.Metadata,
				MetadataDirective:  MetadataDirectiveReplace,
				ACLs:               make(map[string]string),
				CommonOptions:      commonOpts,
			}
			if fsInfo.ACL != "" {
				opts.ACLs[XCfsSourceAcl] = base64.StdEncoding.EncodeToString([]byte(fsInfo.ACL))
			}
			err = client.CopyObject(info.Bucket, fsInfo.Path, info.Bucket, fsInfo.Path, opts)

			return err

		default:
			return fmt.Errorf("unknown replication type: %v", info.OpType)
		}
	}

	return errors.New("no file info in replication info")
}
