package server

import (
	"Xmq/bundle"
	"Xmq/logger"
	"Xmq/msg"
	"Xmq/persist"
	rc "Xmq/registrationCenter"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type ServerInfo struct {
	ServerName string
	Version    string
	Host       string
	Port       uint
	MaxPayload int
}

type Server struct {
	Info    ServerInfo
	Running bool
	Sl      *sublist
	ps      map[string]*rc.PartitionNode
	gcid    uint64
	zkCli   *rc.ZkClient //TODO: create
	kv      clientv3.KV
	bundle  bundle.Bundle //TODO: create
}

const (
	subcriptionKey = "%s/p%d/%s" // topic/partition/subcriptionName
	msgKey         = "%s/p%d/%d" // topic/partition/msid
)

func NewServer(si ServerInfo) *Server {
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

	err = json.Unmarshal(data, sub.Data)
	if err != nil {
		return nil, err
	}
	//TODO: connect between broker and suber ?
	return sub, nil
}

func (s *Server) get(key string) ([]byte, error) {
	resp, err := s.kv.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	//TODO: resp.Count
	return resp.Kvs[0].Value, nil
}

// func (s *Server) Gets()
