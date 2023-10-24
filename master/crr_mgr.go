package master

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type CRRMgr struct {
	sync.RWMutex
	cluster           *Cluster
	CRRConfig         map[string]*proto.CRRConfiguration
	lcNodeStatus      *lcNodeStatus
	CRRRuleTaskStatus *CRRRuleTaskStatus
	idleLcNodeCh      chan struct{}
	exitCh            chan struct{}
}

func newCRRMgr(cluster *Cluster) *CRRMgr {
	log.LogInfof("action[newCRRMgr] construct")
	mgr := &CRRMgr{
		cluster:           cluster,
		CRRConfig:         make(map[string]*proto.CRRConfiguration),
		lcNodeStatus:      NewLcNodeStatus(),
		CRRRuleTaskStatus: NewCRRRuleTaskStatus(),
		idleLcNodeCh:      make(chan struct{}),
		exitCh:            make(chan struct{}),
	}
	return mgr
}

func (mgr *CRRMgr) SetCRR(CRRConf *proto.CRRConfiguration) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.CRRConfig[CRRConf.VolName] = CRRConf
	return
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
	if mgr.scanning() {
		log.LogWarnf("rescheduleScanRoutine: scanning is not completed, CRRRuleTaskStatus(%v)", mgr.CRRRuleTaskStatus)
		return
	}

	tasks := mgr.genEnabledRuleTasks()
	if len(tasks) <= 0 {
		log.LogDebugf("startCRRScan: no crr rule task to schedule!")
		return
	}
	log.LogDebugf("startCRRScan: %v crr rule tasks to schedule!", len(tasks))

	// start scan init
	mgr.lcNodeStatus.WorkingNodes = make(map[string]string)
	mgr.CRRRuleTaskStatus = NewCRRRuleTaskStatus()
	for _, r := range tasks {
		mgr.CRRRuleTaskStatus.ToBeScanned[r.Id] = r
	}

	go mgr.process()
}

func (mgr *CRRMgr) genEnabledRuleTasks() []*proto.CRRTask {
	mgr.RLock()
	defer mgr.RUnlock()
	tasks := make([]*proto.CRRTask, 0)
	for _, v := range mgr.CRRConfig {
		ts := v.GetEnabledRuleTasks()
		if len(ts) > 0 {
			tasks = append(tasks, ts...)
		}
	}
	return tasks
}

func (mgr *CRRMgr) scanning() bool {
	log.LogInfof("scanning lcNodeStatus: %v", mgr.lcNodeStatus)
	if len(mgr.CRRRuleTaskStatus.ToBeScanned) > 0 {
		return true
	}

	if len(mgr.CRRRuleTaskStatus.Scanning) > 0 {
		scanning := mgr.CRRRuleTaskStatus.Scanning
		var nodes []string
		mgr.cluster.lcNodes.Range(func(addr, value interface{}) bool {
			nodes = append(nodes, addr.(string))
			return true
		})
		for _, task := range scanning {
			workingNodes := mgr.lcNodeStatus.WorkingNodes
			var node string
			for nodeAddr, t := range workingNodes {
				if task.Id == t {
					node = nodeAddr
				}
			}
			if exist(node, nodes) {
				log.LogInfof("scanning task: %v, exist node: %v, all nodes: %v", task, node, nodes)
				return true
			}
			log.LogInfof("scanning task: %v, but not exist node: %v, all nodes: %v", task, node, nodes)
		}
	}
	return false
}

func (mgr *CRRMgr) process() {
	log.LogInfof("CRRMgr process start, rule num(%v)", len(mgr.CRRRuleTaskStatus.ToBeScanned))
	now := time.Now()
	mgr.CRRRuleTaskStatus.StartTime = &now
	for mgr.scanning() {
		log.LogDebugf("wait idleLcNodeCh... ToBeScanned num(%v), Scanning num(%v)",
			len(mgr.CRRRuleTaskStatus.ToBeScanned), len(mgr.CRRRuleTaskStatus.Scanning))
		select {
		case <-mgr.exitCh:
			log.LogInfo("exitCh notified, lifecycleManager process exit")
			return
		case <-mgr.idleLcNodeCh:
			log.LogDebug("idleLcNodeCh notified")

			// ToBeScanned -> Scanning
			taskId := mgr.CRRRuleTaskStatus.GetOneTask()
			if taskId == "" {
				log.LogWarn("CRRRuleTaskStatus.GetOneTask, no task")
				continue
			}

			// idleNodes -> workingNodes
			nodeAddr := mgr.lcNodeStatus.GetIdleNode(taskId)
			if nodeAddr == "" {
				log.LogWarn("no idle lcnode, redo task")
				mgr.CRRRuleTaskStatus.RedoTask(taskId)
				continue
			}

			val, ok := mgr.cluster.lcNodes.Load(nodeAddr)
			if !ok {
				log.LogErrorf("lcNodes.Load, nodeAddr(%v) is not available, redo task", nodeAddr)
				mgr.CRRRuleTaskStatus.RedoTask(mgr.lcNodeStatus.RemoveNode(nodeAddr))
				continue
			}

			node := val.(*LcNode)
			task := mgr.CRRRuleTaskStatus.GetTaskFromScanning(taskId) // task can not be nil
			if task == nil {
				log.LogErrorf("task is nil, release node(%v)", nodeAddr)
				mgr.lcNodeStatus.ReleaseNode(nodeAddr)
				continue
			}
			adminTask := node.createCRRTask(mgr.cluster.masterAddr(), task)
			mgr.cluster.addLcNodeTasks([]*proto.AdminTask{adminTask})
			log.LogDebugf("add CRR scan task(%v) to lcnode(%v)", *task, nodeAddr)
		}
	}
	now = time.Now()
	mgr.CRRRuleTaskStatus.EndTime = &now
	log.LogInfof("CRRMgr process finish, lcRuleTaskStatus results(%v)", mgr.CRRRuleTaskStatus.Results)
}

func (mgr *CRRMgr) notifyIdleLcNode() {
	select {
	case mgr.idleLcNodeCh <- struct{}{}:
		log.LogDebug("action[handleLcNodeHeartbeatResp], CRRMgr scan routine notified!")
	default:
		log.LogDebug("action[handleLcNodeHeartbeatResp], CRRMgr skipping notify!")
	}
}

type CRRRuleTaskStatus struct {
	sync.RWMutex
	ToBeScanned map[string]*proto.CRRTask               // task to be scanned
	Scanning    map[string]*proto.CRRTask               // task being scanned
	Results     map[string]*proto.LcNodeCRRTaskResponse // task scan results
	StartTime   *time.Time
	EndTime     *time.Time
}

func NewCRRRuleTaskStatus() *CRRRuleTaskStatus {
	return &CRRRuleTaskStatus{
		ToBeScanned: make(map[string]*proto.CRRTask),
		Scanning:    make(map[string]*proto.CRRTask),
		Results:     make(map[string]*proto.LcNodeCRRTaskResponse),
	}
}

func (crs *CRRRuleTaskStatus) GetOneTask() (taskId string) {
	crs.Lock()
	defer crs.Unlock()
	if len(crs.ToBeScanned) == 0 {
		return
	}

	for k, v := range crs.ToBeScanned {
		taskId = k
		crs.Scanning[k] = v
		delete(crs.ToBeScanned, k)
		return
	}
	return
}

func (crs *CRRRuleTaskStatus) RedoTask(taskId string) {
	crs.Lock()
	defer crs.Unlock()
	if taskId == "" {
		return
	}

	if task, ok := crs.Scanning[taskId]; ok {
		crs.ToBeScanned[taskId] = task
		delete(crs.Scanning, taskId)
	}
}

func (crs *CRRRuleTaskStatus) DeleteScanningTask(taskId string) {
	crs.Lock()
	defer crs.Unlock()
	if taskId == "" {
		return
	}
	delete(crs.Scanning, taskId)
}

func (crs *CRRRuleTaskStatus) GetTaskFromScanning(taskId string) *proto.CRRTask {
	crs.Lock()
	defer crs.Unlock()
	return crs.Scanning[taskId]
}

func (crs *CRRRuleTaskStatus) AddResult(resp *proto.LcNodeCRRTaskResponse) {
	crs.Lock()
	defer crs.Unlock()
	crs.Results[resp.Id] = resp
}
