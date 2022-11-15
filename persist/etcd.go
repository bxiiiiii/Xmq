package persist

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var EtcdCli *clientv3.Client

func init() {
	config := clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
	var err error
	EtcdCli, err = clientv3.New(config)
	if err != nil {
		panic("connect etcd failed.")
	}
}

// operations include put/get/... are implemented in broker.
