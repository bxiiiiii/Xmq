package server

import (
	"Xmq/bundle"
	ct "Xmq/collect"
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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	Info       *rc.BrokerNode
	Running    bool
	Sl         *sublist
	ps         map[string]*partitionData
	partitions sync.Map

	gcid    uint64 // deprecate
	kv      clientv3.KV
	bundles *bundle.Bundles

	grpcServer *grpc.Server
	conns      sync.Map

	// bundle2broker map[bundle.BundleInfo]rc.BrokerNode

	loadManager *lm.LoadManager

	pb.UnimplementedServerServer
}

type partitionData struct {
	mu    sync.Mutex
	pNode *rc.PartitionNode
	msgs  sync.Map
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
		ps: make(map[string]*partitionData),
		kv: clientv3.NewKV(persist.EtcdCli),
		// bundle2broker: make(map[bundle.BundleInfo]rc.BrokerNode),
	}
	s.Info = &rc.BrokerNode{
		Name:      config.SrvConf.Name,
		Host:      config.SrvConf.Host,
		Port:      config.SrvConf.Port,
		Pnum:      0,
		LoadIndex: 0,
	}

	s.Info.Load.Cpu.Limit = config.SrvConf.CpuLimit
	s.Info.Load.VirtualMemory.Limit = config.SrvConf.VirtualMemoryLimit
	s.Info.Load.SwapMemory.Limit = config.SrvConf.SwapMemoryLimit
	s.Info.Load.BandwidthIn.Limit = config.SrvConf.BandwidthInLimit
	s.Info.Load.BandwidthOut.Limit = config.SrvConf.BandwidthOutLimit

	s.grpcServer = grpc.NewServer()

	s.loadManager = lm.NewLoadManager(s.Info)
	return s
}

func (s *Server) Online() (err error) {
	isExists, err := rc.ZkCli.IsBrokerExists(s.Info.Name)
	if err != nil {
		return err
	}
	if isExists {
		return logger.Errorf("there is a exist broker %v", s.Info.Name)
	}

	s.Info.Load, err = ct.CollectLoadData()
	if err != nil {
		return logger.Errorf("CollectLoadData failed: %v", err)
	}
	if err := rc.ZkCli.RegisterBnode(*s.Info); err != nil {
		return err
	}

	s.bundles, err = bundle.NewBundles()
	if err != nil {
		return logger.Errorf("NewBundles failed: %v", err)
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
	logger.Infof("server listening at %v", listener.Addr())

	pb.RegisterServerServer(s.grpcServer, s)
	if err := s.grpcServer.Serve(listener); err != nil {
		logger.Warnf("failed to serve: %v", err)
	}
}

func (s *Server) ShutDown() {
	s.grpcServer.GracefulStop()
	// notify registry
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
	logger.Infof("Receive Connect rq from %v", args)
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
				logger.Errorf("IsPubersExists failed: %v")
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
					<-ch
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

func (s *Server) ProcessSub(ctx context.Context, args *pb.SubscribeArgs) (*pb.SubscribeReply, error) {
	reply := &pb.SubscribeReply{}
	sub := NewSubcription()
	sub.Data.Meta.TopicName = args.Topic
	sub.Data.Meta.Partition = int(args.Partition)
	sub.Data.Meta.Name = args.Subscription
	sub.Data.Meta.Subtype = int(args.Mode)

	snode := &rc.SubcriptionNode{
		Name:      sub.Data.Meta.Name,
		TopicName: sub.Data.Meta.TopicName,
		Partition: sub.Data.Meta.Partition,
		Subtype:   sub.Data.Meta.Subtype,
	}

	var exSub *subcription
	key := fmt.Sprintf(subcriptionKey, snode.TopicName, snode.Partition, snode.Name)
	if sub, ok := s.Sl.Subs[key]; ok {
		exSub = sub
	} else {
		isExists, err := rc.ZkCli.IsSubcriptionExist(snode)
		if err != nil {
			reply.Error = err.Error()
			return reply, err
		}
		if isExists {
			existSnode, err := rc.ZkCli.GetSub(snode)
			if err != nil {
				reply.Error = err.Error()
				return reply, err
			}
			if existSnode.Subtype != snode.Subtype {
				reply.Error = "there is confict between existing subcription and yours"
				return reply, nil
			}
			existSdata, err := s.GetSubcription(existSnode)
			if err != nil {
				reply.Error = err.Error()
				return reply, err
			}
			exSub = existSdata
			s.Sl.Subs[key] = exSub
		}
	}

	if exSub != nil {
		if exSub.Data.Meta.Subtype == snode.Subtype && exSub.Data.Subers[args.Name] == args.Name {
			return reply, errors.New("Repeat subscription")
		}

		switch exSub.Data.Meta.Subtype {
		case Exclusive:
			if len(exSub.Data.Subers) == 0 {
				conn, _ := s.conns.LoadAndDelete(args.Name)
				exSub.Data.Subers[args.Name] = args.Name
				exSub.clients[args.Name] = conn.(*grpc.ClientConn)

				if err := s.PutSubcription(exSub); err != nil {
					reply.Error = err.Error()
					return reply, err
				}
			} else {
				reply.Error = "there is a suber in existing subcription"
				return reply, nil
			}
		case Failover:
			isExists, err := rc.ZkCli.IsSubersExists(args.Topic, int(args.Partition), args.Name)
			if err != nil {
				logger.Errorf("IsSubersExists failed: %v")
				//todo: close conn
				return reply, errors.New("404")
			}
			if isExists {
				if _, ch, err := rc.ZkCli.RegisterLeadSuberWatch(args.Topic, int(args.Partition), args.Subscription); err != nil {
					<-ch
					//TODO: consider timeout and restart election
				}
			} else {
				if err := rc.ZkCli.RegisterLeadSuberNode(args.Topic, int(args.Partition), args.Subscription); err != nil {
					logger.Errorf("RegisterLeadSuberNode failed: %v")
					// conn.Close()
					return reply, errors.New("404")
				}
				conn, _ := s.conns.LoadAndDelete(args.Name)
				exSub.Data.Subers[args.Name] = args.Name
				exSub.clients[args.Name] = conn.(*grpc.ClientConn)
			}
		case Shared:
			// exSub.Data.Subers[name] = name
			// exSub.Clients[name] = c

			// if err := c.srv.PutSubcription(exSub); err != nil {
			// 	reply.Error = err.Error()
			// 	return reply, err
			// }
			//TODO: need some extra action
		}
	} else {
		if err := rc.ZkCli.RegisterSnode(snode); err != nil {
			logger.Errorf("RegisterSnode failed: %v", err)
			return reply, errors.New("404")
		}
		conn, _ := s.conns.LoadAndDelete(args.Name)
		sub.Data.Subers[args.Name] = args.Name
		sub.clients[args.Name] = conn.(*grpc.ClientConn)
		s.Sl.Subs[key] = sub
	}

	name := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	if _, ok := s.ps[name]; !ok {
		pNode, err := rc.ZkCli.GetPartition(args.Topic, int(args.Partition))
		if err != nil {
			// todo
		}
		s.ps[name].pNode = pNode
	}

	s.ps[name].mu.Lock()
	switch args.SubOffset {
	case 0:
		sub.Data.PushOffset = s.ps[name].pNode.PushOffset + 1
	default:
		if sub.Data.PushOffset >= s.ps[name].pNode.Mnum {
			sub.Data.PushOffset = s.ps[name].pNode.PushOffset + 1
		} else {
			sub.Data.PushOffset = args.SubOffset
		}
	}
	s.ps[name].mu.Unlock()

	if err := s.PutSubcription(sub); err != nil {
		reply.Error = err.Error()
		return reply, err
	}

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

	go pua.CheckTimeout(int(args.Timeout))

	for {
		if _, ok := <-pua.Timeout; ok {
			break
		}
		if pNode.pNode.Mnum > exSub.Data.PushOffset || pua.Bufsize <= 0 {
			pua.Full <- true
			break
		}

		exSub.mu.Lock()
		i := exSub.Data.PushOffset
		if i <= pNode.pNode.Mnum {
			var m *msg.MsgData
			key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, i)
			if en, ok := pNode.msgs.Load(key); ok {
				m = en.(*msg.MsgData)
			} else {
				ms, err := s.GetMsg(pua, i)
				if err != nil {
					reply.Error = err.Error()
					return reply, err
				}
				m = ms
			}
			i++
			exSub.mu.Unlock()
			mArgs := &pb.MsgArgs{
				Name:      key,
				Topic:     args.Topic,
				Partition: args.Partition,
				Mid:       m.Mid,
				Msid:      m.Msid,
				Payload:   m.Payload,
				Redo:      0,
			}
			_, err := s.sendMsgWithRedo(mArgs, exSub, config.SrvConf.OperationTimeout)
			if err != nil {
				logger.Errorf("sendMsgWithRedo failed: %v", err)
				// dead letter
			}
			pua.Bufsize--

			pNode.mu.Lock()
			if pNode.pNode.PushOffset < i {
				pNode.pNode.PushOffset = i
			}
			pNode.mu.Unlock()
		}
	}
	return reply, nil
}

func (s *Server) sendMsgWithRedo(args *pb.MsgArgs, sub *subcription, timeout int) (*pb.MsgReply, error) {
	if args.Redo >= int32(config.SrvConf.OperationRedoNum) {
		return nil, errors.New("match max redo")
	}

	reply, err := s.sendMsg(args, sub, timeout)
	if err != nil {
		logger.Errorf("sendMsg failed, try to resend: %v", err)
		args.Redo++
		return s.sendMsgWithRedo(args, sub, timeout)
	}
	return reply, nil
}

func (s *Server) sendMsg(args *pb.MsgArgs, sub *subcription, timeout int) (*pb.MsgReply, error) {
	cli := pb.NewClientClient(sub.clients[args.Name])
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout))
	defer cancel()
	return cli.ProcessMsg(ctx, args)
}

func (s *Server) batchPull(ctx context.Context, args *pb.PullArgs) (*pb.PullReply, error) {
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

	go pua.CheckTimeout(int(args.Timeout))

	for {
		if _, ok := <-pua.Timeout; ok {
			break
		}
		if pNode.pNode.Mnum > exSub.Data.PushOffset || pua.Bufsize <= 0 {
			pua.Full <- true
			break
		}

		pushedNum := 0
		var msgs []*msg.MsgData
		exSub.mu.Lock()
		i := exSub.Data.PushOffset + 1
		for i <= pNode.pNode.Mnum && pushedNum%defaultSendSize < defaultSendSize {
			if pua.Bufsize <= 0 {
				pua.Full <- true
				break
			}
			var m *msg.MsgData
			key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, i)
			if en, ok := pNode.msgs.Load(key); ok {
				m = en.(*msg.MsgData)
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
			if pNode.pNode.PushOffset < i {
				pNode.pNode.PushOffset = i
			}
		}

		// batch
		c := pb.NewClientClient(exSub.clients[args.Name])
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		payload, err := json.Marshal(msgs)
		if err != nil {
			return reply, err
		}
		msgArgs := &pb.MsgArgs{Payload: string(payload)}

		c.ProcessMsg(ctx, msgArgs)
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
	if pNode.pNode.AckOffset > exSub.Data.AckOffset {
		pNode.pNode.AckOffset = exSub.Data.AckOffset
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
		isExists, err := rc.ZkCli.IsSubcriptionExist(&sub.Data.Meta)
		if err != nil {
			return nil, err
		}
		if isExists {
			existSnode, err := rc.ZkCli.GetSub(&sub.Data.Meta)
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
	logger.Infof("Receive Publish rq from %v", args)
	reply := &pb.PublishReply{}
	var pNode *partitionData
	path := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	if v, ok := s.partitions.Load(path); ok {
		pNode = v.(*partitionData)
	} else {
		isExists, err := rc.ZkCli.IsPartitionExists(args.Topic, int(args.Partition))
		if err != nil {
			return reply, err
		}
		if isExists {
			pNode.pNode, err = rc.ZkCli.GetPartition(args.Topic, int(args.Partition))
			s.partitions.Store(path, pNode)
		} else {
			return reply, errors.New("there is no this topic/partition")
		}
	}

	pNode.mu.Lock()
	mData := msg.MsgData{
		Msid:    pNode.pNode.Mnum + 1,
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
	pNode.pNode.Mnum += 1
	pNode.mu.Unlock()
	return reply, nil
}
