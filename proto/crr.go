package proto

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/cubefs/cubefs/util/log"
)

type CRRConfiguration struct {
	VolName string    `json:"vol_name"`
	Rules   []CRRRule `json:"rules"`
}

func (conf *CRRConfiguration) GetCRRTasks() []*CRRTask {
	tasks := make([]*CRRTask, 0)
	for _, r := range conf.Rules {
		task := &CRRTask{
			Id:      fmt.Sprintf("%s:%s:%s", conf.VolName, r.DstS3Cfg.VolName, r.DstS3Cfg.Region),
			VolName: conf.VolName,
			Rule:    &r,
		}
		tasks = append(tasks, task)
		log.LogDebugf("GetCRRTasks: CRRTask(%v) generated from rule(%v) in volume(%v)", *task, r, conf.VolName)
	}
	return tasks
}

type CRRTask struct {
	Id      string
	VolName string
	Rule    *CRRRule
	SrcAuth Auth
	DstAuth Auth
}

type CRRRule struct {
	SrcS3Cfg      S3Config `json:"src_s3cfg"`
	DstS3Cfg      S3Config `json:"dst_s3cfg"`
	DstMasterAddr string   `json:"dst_master_addr,omitempty"`
	Prefix        string   `json:"prefix,omitempty"`
	Marker        string   `json:"marker,omitempty"`
	SyncDelete    bool     `json:"sync_delete,omitempty"`
}

type S3Config struct {
	VolName string `json:"vol_name"`
	S3Addr  string `json:"s3addr"`
	Region  string `json:"region"`
	Auth    *Auth  `json:"auth,omitempty"`
}

type Auth struct {
	AK string
	SK string
}

type CRRTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *CRRTask
}

type CRRTaskResponse struct {
	Id         string
	LcNode     string
	StartTime  *time.Time
	EndTime    *time.Time
	UpdateTime *time.Time
	Done       bool
	Status     uint8
	Result     string
	CRRTaskStatistic
}

type CRRTaskStatistic struct {
	Marker     string `json:"marker"`
	SuccessNum int64  `json:"success_num"`
	FailNum    int64  `json:"fail_num"`
	SkipNum    int64  `json:"skip_num"`
}

type S3ClientConfig struct {
	Region           string
	EndPoint         string
	Ak               string
	Sk               string
	MaxRetries       int
	EnableDebug      bool
	S3ForcePathStyle bool
}

func NewS3Client(s3Cfg *S3ClientConfig) (s3Client *s3.S3) {
	conf := &aws.Config{
		Region:           aws.String(s3Cfg.Region),
		Endpoint:         aws.String(s3Cfg.EndPoint),
		S3ForcePathStyle: aws.Bool(s3Cfg.S3ForcePathStyle),
		Credentials:      credentials.NewStaticCredentials(s3Cfg.Ak, s3Cfg.Sk, ""),
		MaxRetries:       aws.Int(s3Cfg.MaxRetries),
	}
	if s3Cfg.EnableDebug {
		conf.LogLevel = aws.LogLevel(aws.LogDebug)
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	s3client := s3.New(sess)
	return s3client
}

func NewS3Manager(s3Cfg *S3ClientConfig) (uploader *s3manager.Uploader) {
	conf := &aws.Config{
		Region:           aws.String(s3Cfg.Region),
		Endpoint:         aws.String(s3Cfg.EndPoint),
		S3ForcePathStyle: aws.Bool(s3Cfg.S3ForcePathStyle),
		Credentials:      credentials.NewStaticCredentials(s3Cfg.Ak, s3Cfg.Sk, ""),
		MaxRetries:       aws.Int(s3Cfg.MaxRetries),
	}
	if s3Cfg.EnableDebug {
		conf.LogLevel = aws.LogLevel(aws.LogDebug)
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	uploader = s3manager.NewUploader(sess)
	return uploader
}

func HeadBucket(s3cfg *S3Config) error {
	cfg := &S3ClientConfig{
		Region:           s3cfg.Region,
		EndPoint:         s3cfg.S3Addr,
		Ak:               s3cfg.Auth.AK,
		Sk:               s3cfg.Auth.SK,
		MaxRetries:       3,
		S3ForcePathStyle: true,
	}
	s3Cli := NewS3Client(cfg)
	headBucketInput := &s3.HeadBucketInput{
		Bucket: aws.String(s3cfg.VolName),
	}
	_, err := s3Cli.HeadBucket(headBucketInput)
	return err
}
