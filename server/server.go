package server

import (
	"Xmq/bundle"
	"Xmq/config"
	"Xmq/logger"
	"Xmq/msg"
	"Xmq/persist"
	rc "Xmq/registrationCenter"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lm "Xmq/loadManager"
	pb "Xmq/proto"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerInfo struct {
	ServerName string
	Version    string
	Host       string
	Port       int
	MaxPayload int
}

type Server struct {
	Info       ServerInfo
	Running    bool
	Sl         *sublist
	ps         map[string]*rc.PartitionNode
	partitions sync.Map
	msgs       map[string]*msg.MsgData
	gcid       uint64
	zkCli      *rc.ZkClient //TODO: create
	kv         clientv3.KV
	bundles    bundle.Bundles //TODO: create

	grpcServer *grpc.Server
	conns      sync.Map
	// conns      map[string]*grpc.ClientConn

	bundle2broker map[bundle.BundleInfo]ServerInfo

	pb.UnimplementedServerServer

	loadManager lm.LoadManager
}

type partitionData struct {
	mu sync.Mutex
	*rc.PartitionNode
}

const (
	subcriptionKey = "%s/p%d/%s" // topic/partition/subcriptionName
	msgKey         = "%s/p%d/%d" // topic/partition/msid
	partitionKey   = "%s/p%d"    //topic/partition
)

// subscribe/publish mode
const (
	Exclusive     = iota
	WaitExclusive //pub only
	Failover      // sub only
	Shared
	Key_Shared // sub only
)

const (
	Puber = iota
	Suber
)

var defaultSendSize int

func NewServerFromConfig() *Server {
	s := &Server{
		ps:   make(map[string]*rc.PartitionNode),
		msgs: make(map[string]*msg.MsgData),
	}
	info := ServerInfo{
		ServerName: viper.GetString("broker.name"),
		Host:       viper.GetString("broker.host"),
		Port:       viper.GetInt("broker.port"),
	}
	s.Info = info

	defaultSendSize = viper.GetInt("broker.defaultSendSize")

	s.grpcServer = grpc.NewServer()

	return s
}

func (s *Server) Online() error {
	bNode := rc.BrokerNode{
		Name: config.SrvConf.Name,
		Host: config.SrvConf.Host,
		Port: config.SrvConf.Port,
		Pnum: 0,
		// Load: load,
		LoadIndex: 0,
	}
	if err := rc.ZkCli.RegisterBnode(bNode); err != nil {
		return err
	}

	s.loadManager.Run()
	return nil
}

func (s *Server) RunWithGrpc() {
	address := fmt.Sprintf("%v:%v", s.Info.Host, s.Info.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(logger.Errorf("failed to listen: %v", err))
	}
	logger.Debugf("server listening at %v", listener.Addr())

	pb.RegisterServerServer(s.grpcServer, &Server{})
	if err := s.grpcServer.Serve(listener); err != nil {
		logger.Warnf("failed to serve: %v", err)
	}
}

func (s *Server) ShutDown() {
	s.grpcServer.GracefulStop()
	// notify registry
}

func NewServer(si ServerInfo) *Server {
	grpc.NewServer()
	s := &Server{
		Info:    si,
		Running: false,
		Sl:      NewSublist(),
		kv:      clientv3.NewKV(persist.EtcdCli),
	}
	return s
}

func (s *Server) Run() {
	s.Running = true
	s.AcceptLoop()
}

func (s *Server) isrunning() bool {
	isrunning := s.Running
	return isrunning
}

func (s *Server) AcceptLoop() {
	listener, err := net.Listen("tcp", "0.0.0.0:4222")
	if err != nil {
		logger.Debugf("net.Listen failed %v", err)
	}
	logger.Debugf("Listening on %v", listener.Addr())
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Errorf("accept failed: %v", err)
		}
		s.createClient(conn)
	}
}

func (s *Server) createClient(conn net.Conn) *client {
	c := &client{conn: conn, srv: s}
	c.cid = atomic.AddUint64(&s.gcid, 1)
	c.bw = bufio.NewWriterSize(c.conn, defaultBufSize)
	c.br = bufio.NewReaderSize(c.conn, defaultBufSize)

	go c.readLoop()
	return c
}

// func (s *Server) put(m msg.PubArg) error {
// 	key := fmt.Sprintf("%s/%d/%v", m.Topic, m.Partition, m.Msid)
// 	val := fmt.Sprintf("%d/r/n%v", m.Mid, m.Payload)
// 	_, err := s.kv.Put(context.TODO(), key, val)

// 	return err
// }
func (s *Server) PutMsg(m *msg.PubArg, mData msg.MsgData) error {
	key := fmt.Sprintf(msgKey, m.Topic, m.Partition, m.Mid)
	data, err := json.Marshal(mData)
	if err != nil {
		return err
	}
	return s.put(key, data)
}

func (s *Server) PutSubcription(sub *subcription) error {
	key := fmt.Sprintf(subcriptionKey, sub.Data.Meta.TopicName, sub.Data.Meta.Partition, sub.Data.Meta.Name)
	data, err := json.Marshal(sub.Data)
	if err != nil {
		return err
	}
	return s.put(key, data)
}

func (s *Server) put(key string, data []byte) error {
	_, err := s.kv.Put(context.TODO(), key, string(data))
	return err
}

func (s *Server) GetSubcription(sNode *rc.SubcriptionNode) (*subcription, error) {
	sub := NewSubcription()
	key := fmt.Sprintf(subcriptionKey, sNode.TopicName, sNode.Partition, sNode.Name)
	data, err := s.get(key)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, sub.Data); err != nil {
		return nil, err
	}
	//TODO: connect between broker and suber ?
	return sub, nil
}

func (s *Server) GetMsg(pua *msg.PullArg, msid uint64) (*msg.MsgData, error) {
	m := &msg.MsgData{}
	key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, msid)
	data, err := s.get(key)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Server) get(key string) ([]byte, error) {
	resp, err := s.kv.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	//TODO: resp.Count
	return resp.Kvs[0].Value, nil
}

func (s *Server) Connect(ctx context.Context, args *pb.ConnectArgs) (*pb.ConnectReply, error) {
	reply := &pb.ConnectReply{}
	conn, err := grpc.Dial(args.Url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return reply, err
	}

	preName := fmt.Sprintf(rc.PnodePath, rc.ZkCli.ZkTopicRoot, args.Topic, args.Partition)
	switch args.Type {
	case Puber:
		preName = preName + "-publisher-" + args.Name
		tNode, err := rc.ZkCli.GetTopic(args.Topic)
		if err != nil {
			logger.Errorf("GetTopic failed: %v")
			conn.Close()
			return reply, errors.New("404")
		}
		switch tNode.PulishMode {
		case Exclusive:
			isExists, err := rc.ZkCli.IsPubersExists(args.Topic, int(args.Partition))
			if err != nil {
				logger.Errorf("GetTopic failed: %v")
				conn.Close()
				return reply, errors.New("404")
			}
			if isExists {
				logger.Debugf("This Exclusive topic already has a puber")
				conn.Close()
				return reply, errors.New("This Exclusive topic already has a puber")
			} else {
				if err := rc.ZkCli.RegisterLeadPuberNode(args.Topic, int(args.Partition)); err != nil {
					logger.Errorf("RegisterLeadPuberNode failed: %v")
					conn.Close()
					return reply, errors.New("404")
				} 
			}
		case WaitExclusive:
			isExists, err := rc.ZkCli.IsPubersExists(args.Topic, int(args.Partition))
			if err != nil {
				logger.Errorf("IsPubersExists failed: %v")
				conn.Close()
				return reply, errors.New("404")
			}
			if isExists {
				if _, ch, err := rc.ZkCli.RegisterLeadPuberWatch(args.Topic, int(args.Partition)); err != nil {
					<- ch
					//TODO: consider timeout and restart election
				}
			} else {
				if err := rc.ZkCli.RegisterLeadPuberNode(args.Topic, int(args.Partition)); err != nil {
					logger.Errorf("RegisterLeadPuberNode failed: %v")
					conn.Close()
					return reply, errors.New("404")
				} 
			}
		}
	case Suber:
		preName = preName + "-subscriber-" + args.Name
	}

	curName := preName
	if config.SrvConf.AllowRenameForClient {
		index := 1
		for _, ok := s.conns.Load(curName); ok; index++ {
			curName = preName
			curName += "(" + strconv.Itoa(index) + ")"
		}
	} else {
		if _, ok := s.conns.Load(curName); !ok {
			return nil, errors.New("Name conflict, rename plz")
		}
	}

	s.conns.Store(curName, conn)
	reply.Name = curName
	if preName != curName {
		return reply, errors.New("Automatically rename")
	}
	return reply, nil
}

func (c *client) waitcallback(ch <-chan zk.Event) {
	<-ch
}

func (c *client) ProcessSub(ctx context.Context, args *pb.SubscribeArgs) (*pb.SubscribeReply, error) {
	reply := &pb.SubscribeReply{}
	sub := NewSubcription()
	sub.Data.Meta.TopicName = args.Topic
	sub.Data.Meta.Partition = int(args.Partition)
	sub.Data.Meta.Name = args.Subscription
	sub.Data.Meta.Subtype = int(args.SubType)

	snode := &rc.SubcriptionNode{
		Name:      sub.Data.Meta.Name,
		TopicName: sub.Data.Meta.TopicName,
		Partition: sub.Data.Meta.Partition,
		Subtype:   sub.Data.Meta.Subtype,
	}

	var exSub *subcription
	name := c.info.name + strconv.Itoa(int(c.info.id))
	key := fmt.Sprintf(subcriptionKey, snode.TopicName, snode.Partition, snode.Name)
	if sub, ok := c.srv.Sl.Subs[key]; ok {
		exSub = sub
	} else {
		isExists, err := c.srv.zkCli.IsSubcriptionExist(snode)
		if err != nil {
			reply.Error = err.Error()
			return reply, err
		}
		if isExists {
			existSnode, err := c.srv.zkCli.GetSub(snode)
			if err != nil {
				reply.Error = err.Error()
				return reply, err
			}
			if existSnode.Subtype != snode.Subtype {
				reply.Error = "there is confict between existing subcription and yours"
				return reply, nil
			}
			existSdata, err := c.srv.GetSubcription(existSnode)
			if err != nil {
				reply.Error = err.Error()
				return reply, err
			}
			exSub = existSdata
			c.srv.Sl.Subs[name] = exSub
		}
	}

	if exSub != nil {
		if exSub.Data.Meta.Subtype == snode.Subtype && exSub.Data.Subers[name] == name {
			reply.Error = "repeat sub"
			return reply, nil
		}

		switch exSub.Data.Meta.Subtype {
		case Exclusive:
			if len(exSub.Data.Subers) == 0 {
				exSub.Data.Subers[name] = name
				exSub.Clients[name] = c

				if err := c.srv.PutSubcription(exSub); err != nil {
					reply.Error = err.Error()
					return reply, err
				}
			} else {
				reply.Error = "there is a suber in existing subcription"
				return reply, nil
			}
		case Shared:
			exSub.Data.Subers[name] = name
			exSub.Clients[name] = c

			if err := c.srv.PutSubcription(exSub); err != nil {
				reply.Error = err.Error()
				return reply, err
			}
			//TODO: need some extra action
		}
	} else {
		if err := c.srv.zkCli.RegisterSnode(snode); err != nil {
			reply.Error = err.Error()
			return reply, err
		}
		sub.Clients[name] = c
		sub.Data.Subers[name] = name
		c.srv.Sl.Subs[key] = sub
		if err := c.srv.PutSubcription(sub); err != nil {
			reply.Error = err.Error()
			return reply, err
		}
	}

	//TODO: map operations above all are safe ?

	// if c.srv != nil {
	// 	c.srv.Sl.insertORupdate(sub)
	// }

	return reply, nil
}

func (s *Server) ProcessPull(ctx context.Context, args *pb.PullArgs) (*pb.PullReply, error) {
	reply := &pb.PullReply{}
	pua := &msg.PullArg{
		Topic:     args.Topic,
		Partition: int(args.Partition),
		Subname:   args.Subscription,
		Bufsize:   int(args.BufSize),
	}

	pkey := fmt.Sprintf(partitionKey, pua.Topic, pua.Partition)
	pNode := s.ps[pkey]

	skey := fmt.Sprintf(subcriptionKey, pua.Topic, pua.Partition, pua.Subname)
	exSub := s.Sl.Subs[skey]

	go pua.CheckTimeout()

	for {
		if _, ok := <-pua.Timeout; ok {
			break
		}
		if pNode.Mnum > exSub.Data.PushOffset || pua.Bufsize <= 0 {
			pua.Full <- true
			break
		}

		pushedNum := 0
		var msgs []*msg.MsgData
		i := exSub.Data.PushOffset + 1
		for i <= pNode.Mnum && pushedNum%defaultSendSize < defaultSendSize {
			if pua.Bufsize <= 0 {
				pua.Full <- true
				break
			}
			var m *msg.MsgData
			key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, i)
			if en, ok := s.msgs[key]; ok {
				m = en
			} else {
				ms, err := s.GetMsg(pua, i)
				if err != nil {
					reply.Error = err.Error()
					return reply, err
				}
				m = ms
			}
			msgs = append(msgs, m)
			i++
			pushedNum++
			if pNode.PushOffset < i {
				pNode.PushOffset = i
			}
		}
		conn, ok := s.conns.Load(args.Name)
		if !ok {
			return nil, errors.New("connection does not exist")
		}
		c := pb.NewClientClient(conn.(*grpc.ClientConn))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		payload, err := json.Marshal(msgs)
		if err != nil {
			return reply, err
		}
		msgArgs := &pb.MsgArgs{Payload: payload}
		c.Msg(ctx, msgArgs)
		exSub.Data.PushOffset += uint64(len(msgs))
	}
	return reply, nil
}

func (s *Server) MsgAck(ctx context.Context, args *pb.MsgAckArgs) (*pb.MsgAckReply, error) {
	reply := &pb.MsgAckReply{}
	pkey := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	pNode := s.ps[pkey]

	skey := fmt.Sprintf(subcriptionKey, args.Topic, args.Partition, args.Subscription)
	exSub := s.Sl.Subs[skey]
	if exSub.Data.AckOffset > args.AckOffset {
		exSub.Data.AckOffset = args.AckOffset
	}
	if pNode.AckOffset > exSub.Data.AckOffset {
		pNode.AckOffset = exSub.Data.AckOffset
	}

	//TODO: retry ?
	return reply, nil
}

func (s *Server) reTry() {

}

func (s *Server) ProcessUnsub(ctx context.Context, args *pb.UnSubscribeArgs) (*pb.UnSubscribeReply, error) {
	reply := &pb.UnSubscribeReply{}
	sub := NewSubcription()
	sub.Data.Meta.TopicName = args.Topic
	sub.Data.Meta.Partition = int(args.Partition)
	sub.Data.Meta.Name = args.Subscription

	var exSub *subcription
	name := args.Name
	key := fmt.Sprintf(subcriptionKey, sub.Data.Meta.TopicName, sub.Data.Meta.Partition, sub.Data.Meta.Name)
	if sub, ok := s.Sl.Subs[key]; ok {
		exSub = sub
	} else {
		isExists, err := s.zkCli.IsSubcriptionExist(&sub.Data.Meta)
		if err != nil {
			return nil, err
		}
		if isExists {
			existSnode, err := s.zkCli.GetSub(&sub.Data.Meta)
			if err != nil {
				return nil, err
			}
			existSdata, err := s.GetSubcription(existSnode)
			if err != nil {
				return nil, err
			}
			exSub = existSdata
			s.Sl.Subs[name] = exSub
		}
	}

	if exSub != nil {
		if _, ok := exSub.Data.Subers[name]; !ok {
			return nil, errors.New("not exist in this subcription")
		}

		delete(s.Sl.Subs, name)
		if err := s.PutSubcription(exSub); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("subcription not exist")
	}
	return reply, nil
}

func (s *Server) ProcessPub(ctx context.Context, args *pb.PublishArgs) (*pb.PublishReply, error) {
	reply := &pb.PublishReply{}
	var pNode *partitionData
	path := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	if v, ok := s.partitions.Load(path); ok {
		pNode = v.(*partitionData)
	} else {
		isExists, err := s.zkCli.IsPartitionExists(args.Topic, int(args.Partition))
		if err != nil {
			return reply, err
		}
		if isExists {
			pNode.PartitionNode, err = s.zkCli.GetPartition(args.Topic, int(args.Partition))
			s.partitions.Store(path, pNode)
		} else {
			return reply, errors.New("there is no this topic/partition")
		}
	}

	pNode.mu.Lock()
	mData := msg.MsgData{
		Msid:    pNode.Mnum + 1,
		Payload: args.Payload,
	}
	pa := &msg.PubArg{
		Topic:     args.Topic,
		Partition: int(args.Partition),
		Mid:       args.Mid,
	}
	if err := s.PutMsg(pa, mData); err != nil {
		return reply, err
	}
	pNode.Mnum += 1
	pNode.mu.Unlock()
	return reply, nil
}
