package master

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const saveMarkerDur = 30 * time.Second

type CRRMgr struct {
	sync.RWMutex
	cluster       *Cluster
	CRRConfig     map[string]*proto.CRRConfiguration
	lcNodeStatus  *lcNodeStatus
	CRRTaskStatus *CRRTaskStatus
	idleLcNodeCh  chan struct{}
	exitCh        chan struct{}
}

func newCRRMgr(cluster *Cluster) *CRRMgr {
	log.LogInfof("action[newCRRMgr] construct")
	mgr := &CRRMgr{
		cluster:       cluster,
		CRRConfig:     make(map[string]*proto.CRRConfiguration),
		lcNodeStatus:  newLcNodeStatus(),
		CRRTaskStatus: newCRRTaskStatus(),
		idleLcNodeCh:  make(chan struct{}),
		exitCh:        make(chan struct{}),
	}
	go mgr.saveCRRStatus()
	return mgr
}

func (mgr *CRRMgr) SetCRR(conf *proto.CRRConfiguration) *proto.CRRConfiguration {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.CRRConfig[conf.VolName] = conf
	return conf
}

func (mgr *CRRMgr) GetCRR(VolName string) (conf *proto.CRRConfiguration) {
	mgr.RLock()
	defer mgr.RUnlock()

	var ok bool
	conf, ok = mgr.CRRConfig[VolName]
	if !ok {
		return nil
	}
	return conf
}

func (mgr *CRRMgr) DeleteCRR(VolName string) {
	mgr.Lock()
	defer mgr.Unlock()

	delete(mgr.CRRConfig, VolName)
}

func (mgr *CRRMgr) startCRRScan() {
	// stop if already scanning
	if mgr.isScanning() {
		log.LogInfof("startCRRScan: scanning is not completed, CRRTaskStatus(%v)", mgr.CRRTaskStatus)
		return
	}

	tasks := mgr.genCRRTasks()
	if len(tasks) <= 0 {
		log.LogDebugf("startCRRScan: no crr rule task to schedule!")
		return
	}
	log.LogDebugf("startCRRScan: %v crr rule tasks to schedule!", len(tasks))

	for _, r := range tasks {
		mgr.CRRTaskStatus.ToBeScanned[r.Id] = r
		r.Rule.Marker = mgr.CRRTaskStatus.GetCRRStatus(r.Id).Marker
	}

	go mgr.process()
}

func (mgr *CRRMgr) saveCRRStatus() {
	ticker := time.NewTicker(saveMarkerDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			mgr.recordCRRStatusToDB()
		case <-mgr.exitCh:
			mgr.recordCRRStatusToDB()
			return
		}
	}
}

func (mgr *CRRMgr) recordCRRStatusToDB() {
	mgr.CRRTaskStatus.RLock()
	defer mgr.CRRTaskStatus.RUnlock()

	for taskId, taskResp := range mgr.CRRTaskStatus.Results {
		strs := strings.SplitN(taskId, ":", 3)
		if time.Now().After(taskResp.UpdateTime.Add(2*saveMarkerDur)) || len(strs) < 3 {
			continue
		}
		exist := false
		CRRConf := mgr.GetCRR(strs[0])
		if CRRConf != nil {
			for _, rule := range CRRConf.Rules {
				if rule.DstS3Cfg.VolName == strs[1] && rule.DstS3Cfg.Region == strs[2] {
					exist = true
				}
			}
		}
		if !exist {
			metadata := new(RaftCmd)
			metadata.Op = opCRRStatusDelete
			metadata.K = CRRStatusPrefix + taskId
			if err := mgr.cluster.submit(metadata); err != nil {
				log.LogErrorf("recordCRRStatusToDB: err(%v) ", err)
				return
			}
		}
		v, _ := json.Marshal(taskResp.CRRTaskStatistic)
		metadata := new(RaftCmd)
		metadata.Op = opCRRStatusSet
		metadata.K = CRRStatusPrefix + taskId
		metadata.V = v
		if err := mgr.cluster.submit(metadata); err != nil {
			log.LogErrorf("recordCRRStatusToDB: err(%v) ", err)
			return
		}
	}

}

func (mgr *CRRMgr) genCRRTasks() []*proto.CRRTask {
	mgr.RLock()
	defer mgr.RUnlock()
	tasks := make([]*proto.CRRTask, 0)
	for _, v := range mgr.CRRConfig {
		ts := v.GetCRRTasks()
		tasks = append(tasks, ts...)
	}
	return tasks
}

func (mgr *CRRMgr) isScanning() bool {
	log.LogInfof("isScanning lcNodeStatus: %v", mgr.lcNodeStatus)
	if len(mgr.CRRTaskStatus.ToBeScanned) > 0 {
		return true
	}

	for _, v := range mgr.CRRTaskStatus.Results {
		if v.Done != true && time.Now().Before(v.UpdateTime.Add(time.Minute*10)) {
			return true
		}
	}

	for _, c := range mgr.lcNodeStatus.WorkingCount {
		if c > 0 {
			return true
		}
	}
	return false
}

func (mgr *CRRMgr) process() {
	log.LogInfof("CRRMgr process start, rule num(%v)", len(mgr.CRRTaskStatus.ToBeScanned))
	now := time.Now()
	mgr.CRRTaskStatus.StartTime = &now
	for mgr.isScanning() {
		log.LogDebugf("wait idleLcNodeCh... ToBeScanned num(%v)", len(mgr.CRRTaskStatus.ToBeScanned))
		select {
		case <-mgr.exitCh:
			log.LogInfo("exitCh notified, CRRMgr process exit")
			return
		case <-mgr.idleLcNodeCh:
			log.LogDebug("idleLcNodeCh notified")
			// ToBeScanned -> Scanning
			task := mgr.CRRTaskStatus.GetOneTask()
			if task == nil {
				log.LogWarn("CRRTaskStatus.GetOneTask, no task to do")
				continue
			}

			nodeAddr := mgr.lcNodeStatus.GetIdleNode()
			if nodeAddr == "" {
				log.LogWarn("no idle lcnode, redo task")
				mgr.CRRTaskStatus.RedoTask(task)
				continue
			}

			val, ok := mgr.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("lcNodes.Load, nodeAddr(%v) is not available, redo task", nodeAddr)
				mgr.lcNodeStatus.RemoveNode(nodeAddr)
				mgr.CRRTaskStatus.RedoTask(task)
				continue
			}

			node := val.(*LcNode)
			adminTask := node.createCRRTask(mgr.cluster.masterAddr(), task)
			mgr.cluster.addLcNodeTasks([]*proto.AdminTask{adminTask})
			log.LogDebugf("add CRR scan task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
	now = time.Now()
	mgr.CRRTaskStatus.EndTime = &now
	log.LogInfof("CRRMgr process finish, lcRuleTaskStatus results(%v)", mgr.CRRTaskStatus.Results)
}

func (mgr *CRRMgr) notifyIdleLcNode() {
	select {
	case mgr.idleLcNodeCh <- struct{}{}:
		log.LogDebug("action[handleLcNodeHeartbeatResp], CRRMgr scan routine notified!")
	default:
		log.LogDebug("action[handleLcNodeHeartbeatResp], CRRMgr skipping notify!")
	}
}

type CRRTaskStatus struct {
	sync.RWMutex
	ToBeScanned map[string]*proto.CRRTask         // task to be scanned
	Results     map[string]*proto.CRRTaskResponse // task scan results
	StartTime   *time.Time
	EndTime     *time.Time
}

func newCRRTaskStatus() *CRRTaskStatus {
	return &CRRTaskStatus{
		ToBeScanned: make(map[string]*proto.CRRTask),
		Results:     make(map[string]*proto.CRRTaskResponse),
	}
}

func (crs *CRRTaskStatus) GetOneTask() (task *proto.CRRTask) {
	crs.Lock()
	defer crs.Unlock()
	if len(crs.ToBeScanned) == 0 {
		return
	}
	for _, t := range crs.ToBeScanned {
		task = t
		break
	}
	delete(crs.ToBeScanned, task.Id)
	return
}

func (crs *CRRTaskStatus) RedoTask(task *proto.CRRTask) {
	crs.Lock()
	defer crs.Unlock()
	if task == nil {
		return
	}
	crs.ToBeScanned[task.Id] = task
}

func (crs *CRRTaskStatus) AddCRRStatus(resp *proto.CRRTaskResponse) {
	crs.Lock()
	defer crs.Unlock()
	crs.Results[resp.Id] = resp
}

func (crs *CRRTaskStatus) GetCRRStatus(taskId string) proto.CRRTaskStatistic {
	crs.RLock()
	defer crs.RUnlock()
	status := proto.CRRTaskStatistic{}
	res := crs.Results[taskId]
	if res == nil {
		return status
	}
	return res.CRRTaskStatistic
}
