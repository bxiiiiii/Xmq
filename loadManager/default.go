package loadmanager

import (
	ct "Xmq/collect"
	"Xmq/config"
	"Xmq/logger"
	rc "Xmq/registrationCenter"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	Follower = iota
	Leader
)

var cbch chan bool

type LoadManager struct {
	mu    sync.Mutex
	State int
	bNode *rc.BrokerNode

	preBrokers  map[string]*rc.BrokerNode
	curBrokers  map[string]*rc.BrokerNode
	LoadRanking []*rc.BrokerNode
}

type LoadReport struct {
}

func NewLoadManager(bNode *rc.BrokerNode) *LoadManager{
	lm := &LoadManager{
		State: Follower,
		bNode: bNode,
		preBrokers: make(map[string]*rc.BrokerNode),
		curBrokers: make(map[string]*rc.BrokerNode),
	}
	return lm
}

func (lm *LoadManager) Run() {
	go lm.startCollectLoadData()
	go lm.pushLoadReport2registry()
	go lm.startLeaderElection()
}

// func callback(e zk.Event) {
// 	cbch <- true
// 	logger.Debugf("leader watcher is notified to restart LeaderElection")
// }

func (lm *LoadManager) startLeaderElection() {
	// zkcli, err := rc.NewClientWithCallback(callback)
	// if err != nil {
	// 	logger.Errorf("NewClientWithCallback failed: %v", err)
	// }

	for {
		isExists, err := rc.ZkCli.IsLeaderExist()
		if err != nil {
			logger.Errorf("IsLeaderExist failed: %v", err)
		}

		if isExists {
			_, ch, err := rc.ZkCli.RegisterLeadBrokerWatch()
			if err != nil {
				logger.Errorf("RegisterWatcher failed: %v", err)
			}
			<-ch
			lm.State = Follower

		} else {
			url := fmt.Sprintf("%v:%v", lm.bNode.Host, lm.bNode.Port)
			if err := rc.ZkCli.RegisterLeadBrokernode(url); err != nil {
				logger.Errorf("RegisterLnode failed: %v", err)
			} else {
				lm.State = Leader
				go lm.startLeaderTask()
			}
		}
	}
}

func (lm *LoadManager) startLeaderTask() {
	if config.SrvConf.IsLoadBalancerEnabled {
		lm.startWatchAllBrokers()
		lm.pullAllBrokersLoad()
	}
}

func (lm *LoadManager) startWatchAllBrokers() {
	path := rc.ZkCli.ZkBrokerRoot
	_, ch, err := rc.ZkCli.RegisterChildrenWatcher(path)
	if err != nil {
		logger.Errorf("RegisterChildrenWatcher failed: %v", err)
	}
	<-ch

	lm.mu.Lock()
	lm.pullAllBrokersLoad()
	for name, bnode := range lm.preBrokers {
		if _, ok := lm.curBrokers[name]; !ok {
			if err := lm.reallocateBundle(bnode); err != nil {
				logger.Errorf("reallocateBundle failed: %v", err)
			}
		}
	}
	lm.preBrokers = lm.curBrokers
	lm.calculateLoad()
	lm.mu.Unlock()
}

func (lm *LoadManager) reallocateBundle(broker *rc.BrokerNode) error {
	return nil
}

func (lm *LoadManager) AllocateBundle() (*rc.BrokerNode, error) {
	if len(lm.LoadRanking) <= 0 {
		return nil, errors.New("there is no used broker ?")
	}
	return lm.LoadRanking[0], nil
}

func (lm *LoadManager) startCollectLoadData() {
	for {
		usage, err := ct.CollectLoadData()
		if err != nil {
			logger.Errorf("CollectLoadData failed: %v", err)
		} else {
			lm.bNode.Load = usage
		}
		time.Sleep(time.Second * time.Duration(config.SrvConf.CollectLoadDataInterval))
	}
}

func (lm *LoadManager) pushLoadReport2registry() {
	for {
		if err := rc.ZkCli.UpdateBroker(lm.bNode); err != nil {
			logger.Infoln(*lm.bNode)
			logger.Errorf("UpdateBroker failed: %v", err)
		}
		time.Sleep(time.Second * time.Duration(config.SrvConf.PushLoadDataInterval))
	}
}

func (lm *LoadManager) pullAllBrokersLoad() error {
	brokers, err := rc.ZkCli.GetAllBrokers()
	if err != nil {
		return err
	}
	lm.LoadRanking = brokers

	lm.curBrokers = make(map[string]*rc.BrokerNode)
	for _, broker := range brokers {
		lm.curBrokers[broker.Name] = broker
	}
	return nil
}

func (lm *LoadManager) calculateLoad() {
	lm.mu.Lock()
	for _, en := range lm.LoadRanking {
		en.LoadIndex = lm.calculateMethod(en.Load)
	}

	sort.SliceStable(lm.LoadRanking, func(i, j int) bool {
		return lm.LoadRanking[i].LoadIndex < lm.LoadRanking[j].LoadIndex
	})
	lm.mu.Unlock()
}

func (lm *LoadManager) calculateMethod(b ct.BrokerUsage) float64 {
	result := b.Cpu.Usage*config.SrvConf.CpuWeight +
		b.SwapMemory.Usage*config.SrvConf.SwapMemoryWeight +
		b.VirtualMemory.Usage*config.SrvConf.VirtualMemoryWeight +
		b.BandwidthIn.Usage*config.SrvConf.BandwidthInWeight +
		b.BandwidthOut.Usage*config.SrvConf.BandwidthOutWeight
	return result
}