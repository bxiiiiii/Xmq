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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct {
	server *grpc.Server
	conn   *grpc.ClientConn

	pb.UnimplementedClientServer
}

func (c *Client) connect(port int) error {
	srvUrl := fmt.Sprintf("%v:%v", config.SrvConf.Host, config.SrvConf.Port)
	conn, err := grpc.Dial(srvUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = conn

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	c.server = s
	pb.RegisterClientServer(s, &Client{})
	go s.Serve(lis)

	return nil
}

func (c *Client) ProcessMsg(ctx context.Context, args *pb.MsgArgs) (*pb.MsgReply, error) {
	reply := &pb.MsgReply{}
	fmt.Println(args)

	return reply, nil
}

func (c *Client) AliveCheck(ctx context.Context, args *pb.AliveCheckArgs) (*pb.AliveCheckReply, error) {
	reply := &pb.AliveCheckReply{}
	fmt.Println("-----reveive alive check")

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

func getErrorString(err error) string {
	statusErr, ok := status.FromError(err)
	if !ok {
		return ""
	}
	return statusErr.Message()
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

func TestPMode_ExclusiveOfPuber(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "TestPMode_ExclusiveOfPuber"
	partition := 1

	cli1 := &Client{}
	err = cli1.connect(7777)
	assert.Nil(t, err)

	conArgs1 := &pb.ConnectArgs{
		Name:         "puber1",
		Url:          "127.0.0.1:7777",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_Exclusive),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs1)
	assert.Nil(t, err)

	pubers, err := rc.ZkCli.HowManyPubers(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, 1, pubers)

	cli2 := &Client{}
	err = cli2.connect(7778)
	assert.Nil(t, err)

	conArgs2 := &pb.ConnectArgs{
		Name:         "puber2",
		Url:          "127.0.0.1:7778",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_Exclusive),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs2)
	assert.Equal(t, "This Exclusive topic already has a puber", err.Error())

	newPubers, err := rc.ZkCli.HowManyPubers(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, 1, newPubers)
}

func TestReConnect(t *testing.T) {

}

func TestPMode_WaitExclusiveOfPuber_Timeout(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "TestPMode_WaitExclusiveOfPuber_Timeout"
	partition := 1

	cli1 := &Client{}
	id1 := nrand()
	err = cli1.connect(7777)
	assert.Nil(t, err)

	conArgs1 := &pb.ConnectArgs{
		Name:         "puber1",
		Url:          "127.0.0.1:7777",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           id1,
		PubMode:      int32(PMode_WaitExclusive),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs1)
	assert.Nil(t, err)

	pubers, err := rc.ZkCli.HowManyPubers(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, 1, pubers)

	cli2 := &Client{}
	err = cli2.connect(7778)
	assert.Nil(t, err)

	conArgs2 := &pb.ConnectArgs{
		Name:         "puber2",
		Url:          "127.0.0.1:7778",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_WaitExclusive),
		PartitionNum: 1,
	}

	ch := make(chan bool)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2*time.Duration(config.SrvConf.HeartBeatInterval))
		defer cancel()
		_, err := s.Connect(ctx, conArgs2)
		if ok := checkTimeout(err); ok {
			ch <- true
		} else {
			fmt.Println(err)
			ch <- false
		}
	}()

	re := <-ch
	assert.True(t, re)

	newPubers, err := rc.ZkCli.HowManyPubers(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, 1, newPubers)

	LeadPuber, err := rc.ZkCli.GetLeadPuber(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, id1, LeadPuber.ID)
}

func TestPMode_WaitExclusiveOfPuber_Failover(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "TestPMode_WaitExclusiveOfPuber_Failover"
	partition := 1

	cli1 := &Client{}
	id1 := nrand()
	err = cli1.connect(7777)
	assert.Nil(t, err)

	conArgs1 := &pb.ConnectArgs{
		Name:         "puber1",
		Url:          "127.0.0.1:7777",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           id1,
		PubMode:      int32(PMode_WaitExclusive),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs1)
	assert.Nil(t, err)

	pubers, err := rc.ZkCli.HowManyPubers(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, 1, pubers)

	cli2 := &Client{}
	id2 := nrand()
	err = cli2.connect(7778)
	assert.Nil(t, err)

	conArgs2 := &pb.ConnectArgs{
		Name:         "puber2",
		Url:          "127.0.0.1:7778",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           id2,
		PubMode:      int32(PMode_WaitExclusive),
		PartitionNum: 1,
	}

	ch := make(chan bool)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2*time.Duration(config.SrvConf.HeartBeatInterval))
		defer cancel()
		_, err := s.Connect(ctx, conArgs2)
		if err == nil {
			ch <- true
		} else {
			ch <- false
		}
	}()

	time.Sleep(time.Second)
	cli1.server.GracefulStop()

	re := <-ch
	assert.True(t, re)

	newPubers, err := rc.ZkCli.HowManyPubers(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, 1, newPubers)

	LeadPuber, err := rc.ZkCli.GetLeadPuber(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, id2, LeadPuber.ID)
}

func TestPMode_SharedOfPuber(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "TestPMode_SharedOfPuber"
	partition := 1

	cli1 := &Client{}
	err = cli1.connect(7777)
	assert.Nil(t, err)
	conArgs1 := &pb.ConnectArgs{
		Name:         "puber1",
		Url:          "127.0.0.1:7777",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_Shared),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs1)
	assert.Nil(t, err)

	cli2 := &Client{}
	err = cli2.connect(7778)
	assert.Nil(t, err)
	conArgs2 := &pb.ConnectArgs{
		Name:         "puber2",
		Url:          "127.0.0.1:7778",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_Shared),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs2)
	assert.Nil(t, err)

	pubers, err := rc.ZkCli.GetPubers(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(pubers))
}

func TestMutiPublish2SamePartition(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic := "TestMutiPubers2SamePartition"
	partition := 1
	data := "testpayload"

	cli1 := &Client{}
	err = cli1.connect(7777)
	assert.Nil(t, err)
	conArgs1 := &pb.ConnectArgs{
		Name:         "puber1",
		Url:          "127.0.0.1:7777",
		Topic:        topic,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_Shared),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs1)
	assert.Nil(t, err)

	pNode, err := rc.ZkCli.GetPartition(topic, partition)
	assert.Nil(t, err)

	// msgs := make(map[uint64]int64)
	var msgs sync.Map
	for i := 1; i <= 10; i++ {
		args := &pb.PublishArgs{
			Topic:     topic,
			Partition: int32(partition),
			Payload:   data,
			Mid:       nrand(),
		}
		reply, err := s.ProcessPub(context.TODO(), args)
		assert.Nil(t, err)

		_, ok := msgs.LoadOrStore(reply.Msid, args.Mid)
		assert.False(t, ok)
	}

	newPNode, err := rc.ZkCli.GetPartition(topic, partition)
	assert.Nil(t, err)
	assert.Equal(t, pNode.Mnum+10, newPNode.Mnum)
}

func TestMutiPublish2MutiPartition(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)

	topic1 := "TestMutiPublish2MutiPartition1"
	topic2 := "TestMutiPublish2MutiPartition2"
	partition := 1
	data := "testpayload"

	cli1 := &Client{}
	err = cli1.connect(7777)
	assert.Nil(t, err)
	conArgs1 := &pb.ConnectArgs{
		Name:         "puber1",
		Url:          "127.0.0.1:7777",
		Topic:        topic1,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_Shared),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs1)
	assert.Nil(t, err)

	cli2 := &Client{}
	err = cli2.connect(7778)
	assert.Nil(t, err)
	conArgs2 := &pb.ConnectArgs{
		Name:         "puber1",
		Url:          "127.0.0.1:7778",
		Topic:        topic2,
		Partition:    int32(partition),
		Type:         Puber,
		Id:           nrand(),
		PubMode:      int32(PMode_Shared),
		PartitionNum: 1,
	}
	_, err = s.Connect(context.TODO(), conArgs2)
	assert.Nil(t, err)

	pNode1, err := rc.ZkCli.GetPartition(topic1, partition)
	assert.Nil(t, err)
	pNode2, err := rc.ZkCli.GetPartition(topic2, partition)
	assert.Nil(t, err)

	ch := make(chan bool)
	go func() {
		var msgs sync.Map
		for i := 1; i <= 10; i++ {
			args := &pb.PublishArgs{
				Topic:     topic1,
				Partition: int32(partition),
				Payload:   data,
				Mid:       nrand(),
			}
			reply, err := s.ProcessPub(context.TODO(), args)
			assert.Nil(t, err)

			_, ok := msgs.LoadOrStore(reply.Msid, args.Mid)
			assert.False(t, ok)
		}
		ch <- true
	}()
	go func() {
		var msgs sync.Map
		for i := 1; i <= 10; i++ {
			args := &pb.PublishArgs{
				Topic:     topic2,
				Partition: int32(partition),
				Payload:   data,
				Mid:       nrand(),
			}
			reply, err := s.ProcessPub(context.TODO(), args)
			assert.Nil(t, err)

			_, ok := msgs.LoadOrStore(reply.Msid, args.Mid)
			assert.False(t, ok)
		}
		ch <- true
	}()

	<-ch
	<-ch
	newPNode1, err := rc.ZkCli.GetPartition(topic1, partition)
	assert.Nil(t, err)
	assert.Equal(t, pNode1.Mnum+10, newPNode1.Mnum)
	newPNode2, err := rc.ZkCli.GetPartition(topic2, partition)
	assert.Nil(t, err)
	assert.Equal(t, pNode2.Mnum+10, newPNode2.Mnum)
}

func TestSubscribeAndPull(t *testing.T) {
	s, err := RunServer()
	assert.Nil(t, err)
	c := &Client{}

	topic := "testtopic"
	partition := 1
	subscription := "testsubscriptionn"

	err = c.connect(7777)
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