package RegistraionCenter

import (
	"Xmq/config"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newClient() (*ZkClient, error) {
	config.GetConfig("../config/config.yaml")
	cli, err := NewClient()
	if err != nil {
		return nil, err
	}
	cli.preRoot()

	return cli, nil
}

func TestRegisterTemNode(t *testing.T) {
	cli, err := newClient()
	assert.Nil(t, err)

	path := "/testTem"
	err = cli.registerTemNode(path, []byte{1})
	assert.Nil(t, err)

	isExist, err := cli.isZnodeExists(path)
	assert.Nil(t, err)
	assert.Equal(t, true, isExist)

	cli.Close()

	cliNew, err := newClient()
	assert.Nil(t, err)
	isExist, err = cliNew.isZnodeExists(path)

	assert.Nil(t, err)
	assert.Equal(t, false, isExist)
}

func TestRegisterNode(t *testing.T) {
	cli, err := newClient()
	assert.Nil(t, err)

	path := "/test"
	err = cli.RegisterNode(path, []byte{1})
	assert.Nil(t, err)

	isExist, err := cli.isZnodeExists(path)
	assert.Nil(t, err)
	assert.Equal(t, true, isExist)

	cli.Close()

	cliNew, err := newClient()
	assert.Nil(t, err)
	isExist, err = cliNew.isZnodeExists(path)
	assert.Nil(t, err)
	assert.Equal(t, true, isExist)

	err = cliNew.Conn.Delete(path, 0)
	assert.Nil(t, err)
}

func NewWatchClient() (*ZkClient, *ZkClient, *ZkClient, error) {
	cli1, err := newClient()
	if err != nil {
		return nil, nil, nil, err
	}

	cli2, err := newClient()
	if err != nil {
		return nil, nil, nil, err
	}

	cli3, err := newClient()
	if err != nil {
		return nil, nil, nil, err
	}

	return cli1, cli2, cli3, nil
}

func TestLeadBrokerWatch(t *testing.T) {
	cli1, cli2, cli3, err := NewWatchClient()
	assert.Nil(t, err)

	lnode := &LeaderNode{
		LeaderUrl: "test1:1111",
	}
	err = cli1.RegisterLeadBrokernode(lnode)
	assert.Nil(t, err)

	mch := make(chan bool)
	go func() {
		isExists, ch, err := cli2.RegisterLeadBrokerWatch()
		assert.Nil(t, err)
		assert.True(t, isExists)
		<-ch

		lnode.LeaderUrl = "test2:2222"
		err = cli2.RegisterLeadBrokernode(lnode)
		assert.Nil(t, err)
		cli2.Close()
		mch <- true
	}()
	time.Sleep(time.Second)
	cli1.Close()
	<-mch

	time.Sleep(time.Second)
	lnode.LeaderUrl = "test3:3333"
	err = cli3.RegisterLeadBrokernode(lnode)
	assert.Nil(t, err)
}
