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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ObjectLayer interface {
	GetObject(bucket, key string, off, limit int64, opts CommonOptions) (io.ReadCloser, error)
	PutObject(bucket, key string, reader io.Reader, opts PutObjectOptions) error
	CopyObject(srcBkt, srcKey, bucket, key string, opts PutObjectOptions) error
	DeleteObject(bucket, key string, opts DeleteObjectOptions) error
	CreateMultipartUpload(bucket, key string, opts PutObjectOptions) (string, error)
	UploadPart(bucket, key, id string, opts UploadPartOptions) (*ObjectPart, error)
	CompleteMultipartUpload(bucket, key, id string, opts CompletePartOptions) (*MultipartInfo, error)
	AbortMultipartUpload(bucket, key, id string, opts CommonOptions) error
}

type CommonOptions struct {
	RequestID   string
	RequestFrom string
}

func (o *CommonOptions) Apply(r *request.Request) {
	if o != nil && r != nil {
		if o.RequestID != "" {
			r.HTTPRequest.Header.Set(XReqestID, o.RequestID)
		}

		if o.RequestFrom != "" {
			r.HTTPRequest.Header.Set(XCfsSourceFrom, o.RequestFrom)
		}
	}
}

type PutObjectOptions struct {
	CommonOptions

	ETag    string
	PutTime int64

	ContentMD5         []byte
	ContentType        string
	ContentDisposition string
	CacheControl       string
	Expires            string
	Tagging            string
	Size               int64
	MetadataDirective  string
	ACLs               map[string]string
	Metadata           map[string]string
}

func (o *PutObjectOptions) Apply(r *request.Request) {
	if o != nil && r != nil {
		o.CommonOptions.Apply(r)

		if len(o.ContentMD5) > 0 {
			r.HTTPRequest.Header.Set(ContentMD5, base64.StdEncoding.EncodeToString(o.ContentMD5))
		}

		if o.ContentType != "" {
			r.HTTPRequest.Header.Set(ContentType, o.ContentType)
		}

		if o.ContentDisposition != "" {
			r.HTTPRequest.Header.Set(ContentDisposition, o.ContentDisposition)
		}

		if o.CacheControl != "" {
			r.HTTPRequest.Header.Set(CacheControl, o.CacheControl)
		}

		if o.ETag != "" {
			r.HTTPRequest.Header.Set(XCfsSourceETag, o.ETag)
		}

		if o.Expires != "" {
			r.HTTPRequest.Header.Set(Expires, o.Expires)
		}

		if o.Tagging != "" {
			r.HTTPRequest.Header.Set(XAmzTagging, o.Tagging)
		}

		if o.MetadataDirective != "" {
			r.HTTPRequest.Header.Set(XAmzMetadataDirective, o.MetadataDirective)
		}

		if o.PutTime > 0 {
			r.HTTPRequest.Header.Set(XCfsSourcePutTime, fmt.Sprintf("%d", o.PutTime))
		}

		if o.Size > 0 {
			r.HTTPRequest.Header.Set(ContentLength, fmt.Sprintf("%d", o.Size))
		}

		for k, v := range o.ACLs {
			r.HTTPRequest.Header.Set(k, v)
		}

		for k, v := range o.Metadata {
			if strings.HasPrefix(k, XAmzMetaPrefix) {
				r.HTTPRequest.Header.Set(k, v)
			} else {
				r.HTTPRequest.Header.Set(XAmzMetaPrefix+k, v)
			}
		}
	}
}

type DeleteObjectOptions struct {
	CommonOptions

	ETag    string
	PutTime int64
}

func (o *DeleteObjectOptions) Apply(r *request.Request) {
	if o != nil && r != nil {
		o.CommonOptions.Apply(r)

		if o.ETag != "" {
			r.HTTPRequest.Header.Set(XCfsSourceETag, o.ETag)
		}

		if o.PutTime > 0 {
			r.HTTPRequest.Header.Set(XCfsSourcePutTime, fmt.Sprintf("%d", o.PutTime))
		}
	}
}

type ObjectPart struct {
	ETag   string
	Number int
	Size   int64
}

type MultipartInfo struct {
	Bucket string
	Key    string
	ETag   string
	Size   int64
}

type UploadPartOptions struct {
	CommonOptions

	Number     int
	Size       int64
	ContentMD5 []byte
	Reader     io.Reader
}

func (o *UploadPartOptions) Apply(r *request.Request) {
	if o != nil && r != nil {
		o.CommonOptions.Apply(r)

		if o.Size > 0 {
			r.HTTPRequest.Header.Set(ContentLength, fmt.Sprintf("%d", o.Size))
		}
		if len(o.ContentMD5) > 0 {
			r.HTTPRequest.Header.Set(ContentMD5, base64.StdEncoding.EncodeToString(o.ContentMD5))
		}
	}
}

type CompletePartOptions struct {
	Parts []*ObjectPart

	PutObjectOptions
}

type S3Config struct {
	AccessKey        string
	SecretKey        string
	Region           string
	Endpoint         string
	EnableDebug      bool
	DisablePathStyle bool
	MaxRetries       int
	HTTPClient       *http.Client
}

type S3Client struct {
	s3 *s3.S3
}

func NewS3Client(cfg S3Config) (ObjectLayer, error) {
	endpoint := cfg.Endpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = fmt.Sprintf("http://%s", endpoint)
	}
	if _, err := url.Parse(endpoint); err != nil {
		return nil, fmt.Errorf("invalid endpoint %s: %v", cfg.Endpoint, err)
	}

	conf := aws.Config{
		Endpoint:                       aws.String(cfg.Endpoint),
		Region:                         aws.String(cfg.Region),
		S3ForcePathStyle:               aws.Bool(true),
		DisableSSL:                     aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
		Credentials:                    credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
	}
	if cfg.MaxRetries > 0 {
		conf.MaxRetries = aws.Int(cfg.MaxRetries)
	}
	if cfg.EnableDebug {
		conf.LogLevel = aws.LogLevel(aws.LogDebug)
	}
	if cfg.DisablePathStyle {
		conf.S3ForcePathStyle = aws.Bool(false)
	}
	if strings.HasPrefix(strings.ToLower(endpoint), "https") {
		conf.DisableSSL = aws.Bool(false)
	}
	if cfg.HTTPClient != nil {
		conf.HTTPClient = cfg.HTTPClient
	} else {
		dialer := &net.Dialer{
			KeepAlive: 30 * time.Second,
			Timeout:   30 * time.Second,
		}
		conf.HTTPClient = &http.Client{
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				DialContext:         dialer.DialContext,
				TLSHandshakeTimeout: 20 * time.Second,
				IdleConnTimeout:     300 * time.Second,
				DisableCompression:  true,
				ForceAttemptHTTP2:   true,
				MaxIdleConnsPerHost: 500,
				ReadBufferSize:      32 << 10,
				WriteBufferSize:     32 << 10,
			},
		}
	}

	sess, err := session.NewSessionWithOptions(session.Options{Config: conf})
	if err != nil {
		return nil, fmt.Errorf("fail to create s3 session: %v", err)
	}

	return &S3Client{
		s3: s3.New(sess),
	}, nil
}

func (s *S3Client) GetObject(bucket, key string, off, limit int64, opts CommonOptions) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("bytes=%d-", off)
		}
		input.Range = aws.String(r)
	}
	req, resp := s.s3.GetObjectRequest(input)
	opts.Apply(req)
	if err := req.Send(); err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *S3Client) PutObject(bucket, key string, reader io.Reader, opts PutObjectOptions) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, _ := s.s3.PutObjectRequest(input)
	req.SetStreamingBody(ioutil.NopCloser(reader))
	opts.Apply(req)

	return req.Send()
}

func (s *S3Client) CopyObject(srcBkt, srcKey, bucket, key string, opts PutObjectOptions) error {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		CopySource: aws.String(srcBkt + "/" + srcKey),
	}
	req, _ := s.s3.CopyObjectRequest(input)
	opts.Apply(req)

	return req.Send()
}

func (s *S3Client) DeleteObject(bucket, key string, opts DeleteObjectOptions) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, _ := s.s3.DeleteObjectRequest(input)
	opts.Apply(req)

	return req.Send()
}

func (s *S3Client) CreateMultipartUpload(bucket, key string, opts PutObjectOptions) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, resp := s.s3.CreateMultipartUploadRequest(input)
	opts.Apply(req)
	if err := req.Send(); err != nil {
		return "", err
	}

	return *resp.UploadId, nil
}

func (s *S3Client) UploadPart(bucket, key, id string, opts UploadPartOptions) (*ObjectPart, error) {
	input := &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(id),
		PartNumber: aws.Int64(int64(opts.Number)),
	}
	req, resp := s.s3.UploadPartRequest(input)
	req.SetStreamingBody(ioutil.NopCloser(opts.Reader))
	opts.Apply(req)
	if err := req.Send(); err != nil {
		return nil, err
	}

	return &ObjectPart{ETag: *resp.ETag, Number: opts.Number, Size: opts.Size}, nil
}

func (s *S3Client) CompleteMultipartUpload(bucket, key, id string, opts CompletePartOptions) (*MultipartInfo, error) {
	var s3Parts []*s3.CompletedPart
	for _, op := range opts.Parts {
		if op != nil {
			part := &s3.CompletedPart{
				ETag:       aws.String(op.ETag),
				PartNumber: aws.Int64(int64(op.Number)),
			}
			s3Parts = append(s3Parts, part)
		}
	}
	sort.SliceStable(s3Parts, func(i, j int) bool { return *s3Parts[i].PartNumber < *s3Parts[j].PartNumber })

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(id),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: s3Parts},
	}
	req, resp := s.s3.CompleteMultipartUploadRequest(input)
	opts.Apply(req)
	if err := req.Send(); err != nil {
		return nil, err
	}

	return &MultipartInfo{
		Bucket: bucket,
		Key:    key,
		ETag:   *resp.ETag,
	}, nil
}

func (s *S3Client) AbortMultipartUpload(bucket, key, id string, opts CommonOptions) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(id),
	}
	req, _ := s.s3.AbortMultipartUploadRequest(input)
	opts.Apply(req)

	return req.Send()
}
