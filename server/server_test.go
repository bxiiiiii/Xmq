package server

import (
	"Xmq/config"
	"Xmq/msg"
	"Xmq/persist"
	pb "Xmq/proto"
	rc "Xmq/registrationCenter"
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn

	pb.UnimplementedClientServer
}

func (c *Client) connect() error {
	srvUrl := fmt.Sprintf("%v:%v", config.SrvConf.Host, config.SrvConf.Port)
	conn, err := grpc.Dial(srvUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = conn

	lis, err := net.Listen("tcp", "127.0.0.1:7777")
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	pb.RegisterClientServer(s, &Client{})
	go s.Serve(lis)

	return nil
}

func (c *Client) ProcessMsg(ctx context.Context, args *pb.MsgArgs) (*pb.MsgReply, error) {
	reply := &pb.MsgReply{}
	fmt.Println(args)

	return reply, nil
}

func RunServer() (*Server, error) {
	config.GetConfig("../config/config.yaml")
	persist.PersistInit()
	rc.RcInit()

	server := NewServerFromConfig()
	err := server.Online()
	if err != nil {
		return nil, err
	}
	// server.RunWithGrpc()

	return server, nil
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func TestPut(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "testTopic"
	payload := "testPayload"
	partition := 1
	msid := 1
	pa := &msg.PubArg{
		Topic:     topic,
		Partition: partition,
		Mid:       123465789,
	}
	mData := msg.MsgData{
		Msid:    uint64(msid),
		Payload: payload,
	}
	_, err = s.PutMsg(pa, mData)
	assert.Nil(t, err)

	pua := &msg.PullArg{
		Topic:     topic,
		Partition: partition,
	}
	data, err := s.GetMsg(pua, uint64(msid))
	assert.Nil(t, err)
	assert.Equal(t, payload, data.Payload)
}

func TestConnect(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	args := &pb.ConnectArgs{
		Name:         "testconnect",
		Topic:        "testtopic",
		Partition:    1,
		Type:         Puber,
		PartitionNum: 1,
		PubMode:      int32(PMode_Shared),
		Id:           nrand(),
	}
	_, err = s.Connect(context.TODO(), args)
	assert.Nil(t, err)
}

func TestPubulish(t *testing.T) {
	// time.Sleep(time.Second*5)
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "testtopic"
	partition := 1
	data := "testpayload"
	mid := nrand()

	pNode, err := rc.ZkCli.GetPartition(topic, partition)
	assert.Nil(t, err)

	args := &pb.PublishArgs{
		Topic:     topic,
		Partition: int32(partition),
		Payload:   data,
		Mid:       mid,
	}
	reply, err := s.ProcessPub(context.TODO(), args)
	assert.Nil(t, err)

	msgdata, err := s.GetMsg(&msg.PullArg{Topic: topic, Partition: partition}, reply.Msid)
	assert.Nil(t, err)
	assert.Equal(t, pNode.Mnum+1, msgdata.Msid)
	assert.Equal(t, mid, msgdata.Mid)
	assert.Equal(t, data, msgdata.Payload)

	time.Sleep(time.Second)
	newPNode, err := rc.ZkCli.GetPartition(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, msgdata.Msid, newPNode.Mnum)

	// err = s.DeleteMsg(topic, partition, reply.Msid)
	// assert.Nil(t, err)
}

func TestSubscribeAndPull(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)
	c := &Client{}

	topic := "testtopic"
	partition := 1
	subscription := "testsubscriptionn"

	err = c.connect()
	assert.Nil(t, err)

	conArgs := &pb.ConnectArgs{
		Name:      "suber",
		Url:       "127.0.0.1:7777",
		Topic:     topic,
		Partition: int32(partition),
		Type:      Suber,
		Id:        nrand(),
	}
	reply, err := s.Connect(context.TODO(), conArgs)
	assert.Nil(t, err)

	name := reply.Name
	args := &pb.SubscribeArgs{
		Name:         name,
		Topic:        topic,
		Partition:    int32(partition),
		Subscription: subscription,
		Mode:         pb.SubscribeArgs_SubMode(SMode_Exclusive),
		SubOffset:    1,
	}

	_, err = s.ProcessSub(context.TODO(), args)
	assert.Nil(t, err)

	subNode := &rc.SubcriptionNode{TopicName: topic, Partition: partition, Name: subscription}
	_, err = rc.ZkCli.GetSub(subNode)
	assert.Nil(t, err)

	sub, err := s.GetSubcription(subNode)
	assert.Nil(t, err)
	assert.Equal(t, *subNode, sub.Data.Meta)
	assert.Equal(t, 1, len(sub.Data.Subers))
	assert.Equal(t, sub.Data.Subers[name], name)

	pullArgs := &pb.PullArgs{
		Name:         name,
		Topic:        topic,
		Partition:    int32(partition),
		Subscription: subscription,
		BufSize:      10,
		Timeout:      5,
	}
	_, err = s.ProcessPull(context.TODO(), pullArgs)
	assert.Nil(t, err)
}