package proto

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	CRRStatusEnabled  = "Enabled"
	CRRStatusDisabled = "Disabled"
)

type CRRConfiguration struct {
	VolName string     `json:"vol_name"`
	Rules   []*CRRRule `json:"rules"`
}

func (conf *CRRConfiguration) GetEnabledRuleTasks() []*CRRTask {
	tasks := make([]*CRRTask, 0)
	for _, r := range conf.Rules {
		if r.Status != RuleEnabled {
			log.LogDebugf("GetEnabledRuleTasks: skip disabled rule(%v) in volume(%v)", r.RuleID, conf.VolName)
			continue
		}
		task := &CRRTask{
			Id:      fmt.Sprintf("%s:%s", conf.VolName, r.RuleID),
			VolName: conf.VolName,
			Rule:    r,
		}
		tasks = append(tasks, task)
		log.LogDebugf("GetEnabledRuleTasks: RuleTask(%v) generated from rule(%v) in volume(%v)", *task, r.RuleID, conf.VolName)
	}
	return tasks
}

type CRRTask struct {
	Id      string
	VolName string
	Rule    *CRRRule
}

type CRRRule struct {
	RuleID     string `json:"rule_id"`
	DstVolName string `json:"dst_vol_name"`
	MasterAddr string `json:"master_addr"`
	ObjectAddr string `json:"object_addr"`
	Status     string `json:"status"`
	Prefix     string `json:"prefix"`
	SyncDelete bool   `json:"sync_delete"`
}

type LcNodeCRRTaskResponse struct {
	Id        string
	StartTime *time.Time
	EndTime   *time.Time
	Done      bool
	Status    uint8
	Result    string
	LcNodeCRRTaskStatistic
}

type LcNodeCRRTaskStatistic struct {
	Volume               string
	RuleId               string
	TotalInodeScannedNum int64
	FileScannedNum       int64
	DirScannedNum        int64
	ExpiredNum           int64
	ErrorSkippedNum      int64
}

type CRRRuleTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *CRRTask
}
