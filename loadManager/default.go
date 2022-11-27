package loadmanager

import (
	ct "Xmq/collect"
	"Xmq/config"
	"Xmq/logger"
	rc "Xmq/registrationCenter"
	"sort"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	Follower = iota
	Leader
)

var cbch chan bool

type LoadManager struct {
	Mu    sync.Mutex
	State int
	BNode rc.BrokerNode

	LoadRanking []*rc.BrokerNode
}

type LoadReport struct {
}

//func NewLoadManager()

func (lm *LoadManager) Run() {
	go lm.startCollectLoadData()
	go lm.pushLoadReport2registry()
	go lm.startLeaderElection()
}

func callback(e zk.Event) {
	cbch <- true
	logger.Debugf("leader watcher is notified to restart LeaderElection")
}

func (lm *LoadManager) startLeaderElection() {
	zkcli, err := rc.NewClientWithCallback(callback)
	if err != nil {
		logger.Errorf("NewClientWithCallback failed: %v", err)
	}

	for {
		isExists, err := zkcli.IsLeaderExist()
		if err != nil {
			logger.Errorf("IsLeaderExist failed: %v", err)
		}

		if isExists {
			if err := zkcli.RegisterWatcher(zkcli.ZkLeaderRoot); err != nil {
				logger.Errorf("RegisterWatcher failed: %v", err)
			}
			lm.State = Follower

		} else {
			if err := rc.ZkCli.RegisterLnode(); err != nil {
				logger.Errorf("RegisterLnode failed: %v", err)
				lm.startLeaderElection()
			} else {
				lm.State = Leader
			}
		}

		<-cbch
	}
}

func (lm *LoadManager) brokerIsTheLeaderNow() {
	if config.SrvConf.IsLoadBalancerEnabled {
		lm.startWatchAllBrokers()
		lm.pullAllBrokersLoad()
	}
}

func (lm *LoadManager) startWatchAllBrokers() {
	path := rc.ZkCli.ZkBrokerRoot + "/"
	curBrokers, ch, err := rc.ZkCli.RegisterChildrenWatcher(path)
	if err != nil {
		logger.Errorf("RegisterChildrenWatcher failed: %v", err)
	}                                                                             
}

func (lm *LoadManager) brokerIsAFollowerNow() {

}

func (lm *LoadManager) startCollectLoadData() {
	for {
		usage, err := ct.CollectLoadData()
		if err != nil {
			logger.Errorf("CollectLoadData failed: %v", err)
		} else {
			lm.BNode.Load = usage
		}
		time.Sleep(time.Second * time.Duration(config.SrvConf.CollectLoadDataInterval))
	}
}

func (lm *LoadManager) generateLoadReport() {

}

func (lm *LoadManager) pushLoadReport2registry() {
	for {
		if err := rc.ZkCli.UpdateBroker(lm.BNode); err != nil {
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
	return nil
}

func (lm *LoadManager) CalculateLoad() {
	for _, en := range lm.LoadRanking {
		en.LoadIndex = lm.calculateMethod(en.Load)
	}

	sort.SliceStable(lm.LoadRanking, func(i, j int) bool {
		return lm.LoadRanking[i].LoadIndex < lm.LoadRanking[j].LoadIndex
	})
}

func (lm *LoadManager) calculateMethod(b ct.BrokerUsage) float64 {
	result := b.Cpu.Usage*config.SrvConf.Lw.Cpu +
		b.SwapMemory.Usage*config.SrvConf.Lw.SwapMemory +
		b.VirtualMemory.Usage*config.SrvConf.Lw.VirtualMemory +
		b.BandwidthIn.Usage*config.SrvConf.Lw.BandwidthIn +
		b.BandwidthOut.Usage*config.SrvConf.Lw.BandwidthOut
	return result
}
