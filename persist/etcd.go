package persist

import (
	"Xmq/config"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var EtcdCli *clientv3.Client

func PersistInit() {
	config := clientv3.Config{
		Endpoints:   config.EConf.Endpoints,
		DialTimeout: time.Duration(config.EConf.DialTimeout) * time.Second,
	}
	var err error
	EtcdCli, err = clientv3.New(config)
	if err != nil {
		panic("connect etcd failed.")
	}
}

// operations include put/get/... are implemented in broker.
