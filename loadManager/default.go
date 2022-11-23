package loadmanager

import (
	ct "Xmq/collect"
	"Xmq/config"
	"Xmq/logger"
	rc "Xmq/registrationCenter"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	Follower = iota
	Leader
)

var cbch chan bool

type loadManager struct {
	mu    sync.Mutex
	state int
	load  ct.BrokerUsage
}

type LoadReport struct {
}

func (lm *loadManager) Run() {
	
}

func callback(e zk.Event) {
	cbch <- true
	logger.Debugf("leader watcher is notified to restart LeaderElection")
}

func (lm *loadManager) startLeaderElection() {
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
			lm.state = Follower

		} else {
			if err := rc.ZkCli.RegisterLnode(); err != nil {
				logger.Errorf("RegisterLnode failed: %v", err)
				lm.startLeaderElection()
			} else {
				lm.state = Leader
			}
		}

		<-cbch
	}
}

func (lm *loadManager) brokerIsTheLeaderNow() {
	if config.SrvConf.IsLoadBalancerEnabled {

	}
}

func (lm *loadManager) brokerIsAFollowerNow() {

}

func (lm *loadManager) startCollectLoadData() {
	for {
		usage, err := ct.CollectLoadData()
		if err != nil {
			logger.Errorf("CollectLoadData failed: %v", err)
		} else {
			lm.load = usage
		}
		time.Sleep(time.Second * time.Duration(config.SrvConf.CollectLoadDataInterval))
	}
}

func (lm *loadManager) generateLoadReport() {

}

func (lm *loadManager) pushLoadReport2registry() {

}
