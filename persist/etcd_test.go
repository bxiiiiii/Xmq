package persist

import (
	"context"
	"testing"

	"Xmq/config"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestPut(t *testing.T) {
	config.GetConfig("../config/config.yaml")
	PersistInit()
	kv := clientv3.NewKV(EtcdCli)

	key := "/test/p1/t"
	val := "helloworld"
	_, err := kv.Put(context.TODO(), key, val)
	assert.Nil(t, err)

	resp, err := kv.Get(context.TODO(), key)
	assert.Nil(t, err)
	assert.Equal(t, val, string(resp.Kvs[0].Value))
}