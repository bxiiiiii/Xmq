package zookeeper

import (
	"Xmq/logger"
	"time"

	"github.com/dubbogo/go-zookeeper/zk"
)

var (
	hosts       = []string{"127.0.0.1:2181"}
	path        = "/gg"
	flags int32 = zk.FlagEphemeral
	data        = []byte("zk usedata1")
	acls        = zk.WorldACL(zk.PermAll)
)

var zkc *zk.Conn

func init() {
	zkc, _, err := zk.Connect(hosts, time.Second * 5)
	if err != nil {
		logger.Errorf("connect zk failed: %v", err)
	}
	create(zkc, "/ids", data, 0)
	create(zkc, "/topic", data, 0)
}

func create(conn *zk.Conn, path string, data []byte, flags int32) {
	_, err := conn.Create(path, data, flags, acls)
	if err != nil {
		logger.Errorf("create znode '%v' failed: %v", path, err)
		return
	}
}

func del(conn *zk.Conn, path string) {
	_, stat, _ := conn.Get(path)
	err := conn.Delete(path, stat.Version)
	if err != nil {
		logger.Errorf("delete znode '%v' failed: %v", path, err)
		return
	}
}
