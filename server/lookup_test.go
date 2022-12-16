package server

import (
	"Xmq/config"
	pb "Xmq/proto"
	"context"
	"fmt"
	"time"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLookUp(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "TestLookUp"
	partition := 1
	args := &pb.LookUpArgs{
		Topic:     topic,
		Partition: int32(partition),
	}
	reply, err := s.LookUp(context.TODO(), args)
	if err != nil {
		if err.Error() != "need to connect leader to alloc" {
			assert.Nil(t, err)
		}
	}
	srvUrl := fmt.Sprintf("%v:%v", config.SrvConf.Host, config.SrvConf.Port)
	assert.Equal(t, srvUrl, reply.Url)
}

func TestRequestAlloc(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)
	time.Sleep(time.Second*3)
	topic := "TestRequestAlloc"
	partition := 1
	allocArgs := &pb.RequestAllocArgs{
		Topic:     topic,
		Partition: int32(partition),
		Redo:      0,
	}
	allocReply, err := s.RequestAlloc(context.TODO(), allocArgs)
	assert.Nil(t, err)
	srvUrl := fmt.Sprintf("%v:%v", config.SrvConf.Host, config.SrvConf.Port)
	assert.Equal(t, srvUrl, allocReply.Url)
}
