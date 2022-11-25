package server

import (
	"Xmq/logger"
	"Xmq/msg"
	rc "Xmq/registrationCenter"
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBufSize = 65536
// const defaultSendSize = 30

type client struct {
	mu   sync.Mutex
	cid  uint64
	name string
	conn net.Conn
	srv  *Server
	info clientInfo
	cstats
	praseState
	bw *bufio.Writer
	br *bufio.Reader
}

type cstats struct {
	nr int
	nb int
	nm int
}

type praseState struct {
	state  int
	as     int
	drop   int
	pa     msg.PubArg
	pua    msg.PullArg
	argBuf []byte
	msgBuf []byte
}

type clientInfo struct {
	name string
	id   uint64
}

func (c *client) readLoop() {
	buffer := make([]byte, defaultBufSize)
	for {
		n, err := c.conn.Read(buffer)
		logger.Debugf("", buffer)
		if err != nil {
			logger.Errorf("", err)
			c.closeConnection()
			return
		}
		if err := c.prase(buffer[:n]); err != nil {
			logger.Errorf("", err)
			c.closeConnection()
			return
		}
	}
}

func (c *client) processPub(arg []byte) error {
	logger.Debugf("Pub arg: '%s'", arg)
	args := spiltArg(arg)
	switch len(args) {
	case 2:
		c.pa.Topic = string(args[0])
		c.pa.Reply = nil
		c.pa.Size = parseSize(args[1])
		c.pa.Szb = args[1]
	case 3:
		c.pa.Topic = string(args[0])
		c.pa.Reply = args[1]
		c.pa.Size = parseSize(args[2])
		c.pa.Szb = args[2]
	default:
		return logger.Errorf("processPub Parse failed: '%s'", arg)
	}
	if c.pa.Size < 0 {
		return logger.Errorf("processPub Bad or Missing Size: '%s'", arg)
	}
	logger.Debugf("Pub parsed: %+v", c.pa)
	return nil
}

func (c *client) processSub(arg []byte) error {
	args := spiltArg(arg)
	sub := NewSubcription()
	switch len(args) {
	case 4:
		sub.Data.Meta.TopicName = string(args[0])
		sub.Data.Meta.Partition = parseSize(args[1])
		sub.Data.Meta.Name = string(args[2])
		sub.Data.Meta.Subtype = parseSize(args[3])
	default:
		return logger.Errorf("num of args :%v", len(args))
	}

	snode := &rc.SubcriptionNode{
		Name:      sub.Data.Meta.Name,
		TopicName: sub.Data.Meta.TopicName,
		Partition: sub.Data.Meta.Partition,
		Subtype:   sub.Data.Meta.Subtype,
	}
	//TODO: consider cache
	var exSub *subcription
	name := c.info.name + strconv.Itoa(int(c.info.id))
	key := fmt.Sprintf(subcriptionKey, snode.TopicName, snode.Partition, snode.Name)
	if sub, ok := c.srv.Sl.Subs[key]; ok {
		exSub = sub
	} else {
		isExists, err := c.srv.zkCli.IsSubcriptionExist(snode)
		if err != nil {
			return err
		}
		if isExists {
			existSnode, err := c.srv.zkCli.GetSub(snode)
			if err != nil {
				return err
			}
			if existSnode.Subtype != snode.Subtype {
				c.respAction("there is confict between existing subcription and yours")
				return nil
			}
			existSdata, err := c.srv.GetSubcription(existSnode)
			if err != nil {
				return err
			}
			exSub = existSdata
			c.srv.Sl.Subs[name] = exSub
		}
	}

	if exSub != nil {
		if exSub.Data.Meta.Subtype == snode.Subtype && exSub.Data.Subers[name] == name {
			c.respAction("repeat sub")
			return nil
		}

		switch exSub.Data.Meta.Subtype {
		case Exclusive:
			if len(exSub.Data.Subers) == 0 {
				exSub.Data.Subers[name] = name
				exSub.Clients[name] = c

				if err := c.srv.PutSubcription(exSub); err != nil {
					return err
				}
			} else {
				c.respAction("there is a suber in existing subcription")
				return nil
			}
		case Shared:
			exSub.Data.Subers[name] = name
			exSub.Clients[name] = c

			if err := c.srv.PutSubcription(exSub); err != nil {
				return err
			}
			//TODO: need some extra action
		}
	} else {
		if err := c.srv.zkCli.RegisterSnode(snode); err != nil {
			return err
		}
		sub.Clients[name] = c
		sub.Data.Subers[name] = name
		c.srv.Sl.Subs[key] = sub
		if err := c.srv.PutSubcription(sub); err != nil {
			return err
		}
	}

	//TODO: map operations above all are safe ?

	// if c.srv != nil {
	// 	c.srv.Sl.insertORupdate(sub)
	// }
	c.respAction("success")
	return nil
}

func (c *client) processPull(arg []byte) error {
	args := spiltArg(arg)
	pua := &msg.PullArg{}
	switch len(args) {
	case 4:
		pua.Topic = string(args[0])
		pua.Partition = parseSize(args[1])
		pua.Subname = string(args[2])
		pua.Bufsize = parseSize(args[3])
	default:
		return errors.New("wrong number of arg")
	}

	// TODO
	// pNode, err := c.srv.zkCli.GetPartition(pua.Topic, pua.Partition)
	// if err != nil {
	// 	//do something
	// }
	pkey := fmt.Sprintf(partitionKey, pua.Topic, pua.Partition)
	pNode := c.srv.ps[pkey]

	skey := fmt.Sprintf(subcriptionKey, pua.Topic, pua.Partition, pua.Subname)
	exSub := c.srv.Sl.Subs[skey]

	prePushOffset := exSub.Data.PushOffset
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
			if en, ok := c.srv.msgs[key]; ok {
				m = en
			} else {
				ms, err := c.srv.GetMsg(pua, i)
				if err != nil {
					return err
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
		if err := c.sendMsg(pua, msgs); err != nil {
			return err
		}
		exSub.Data.PushOffset += uint64(len(msgs))
	}
	c.respAction("")

	if ok := c.waitAck(exSub.Ackch); ok {
		exSub.Data.AckOffset = exSub.Data.PushOffset
		if pNode.AckOffset < exSub.Data.AckOffset {
			pNode.AckOffset = exSub.Data.AckOffset
		}
	} else {
		var msgs []*msg.MsgData
		for i := prePushOffset + 1; i <= pNode.AckOffset; i++ {
			var m *msg.MsgData
			key := fmt.Sprintf(msgKey, pua.Topic, pua.Partition, i)
			if en, ok := c.srv.msgs[key]; ok {
				m = en
			} else {
				ms, err := c.srv.GetMsg(pua, i)
				if err != nil {
					return err
				}
				m = ms
			}
			msgs = append(msgs, m)
		}
	}
	return nil
}


// consider ack msid
func (c *client) waitAck(ch chan uint64) bool {
	select {
	case <-time.After(time.Second * 10):
		return false
	case <-ch:
		return true
	}
}

func (c *client) processUnsub(arg []byte) error {
	//TOTO:need to update
	args := spiltArg(arg)
	sub := NewSubcription()
	// sub.clients[c.name] = c
	switch len(args) {
	case 3:
		sub.Data.Meta.TopicName = string(args[0])
		sub.Data.Meta.Partition = parseSize(args[1])
		sub.Data.Meta.Name = string(args[2])
	default:
		return logger.Errorf("num of args :%v", len(args))
	}

	var exSub *subcription
	name := c.info.name + strconv.Itoa(int(c.info.id))
	key := fmt.Sprintf(subcriptionKey, sub.Data.Meta.TopicName, sub.Data.Meta.Partition, sub.Data.Meta.Name)
	if sub, ok := c.srv.Sl.Subs[key]; ok {
		exSub = sub
	} else {
		isExists, err := c.srv.zkCli.IsSubcriptionExist(&sub.Data.Meta)
		if err != nil {
			return err
		}
		if isExists {
			existSnode, err := c.srv.zkCli.GetSub(&sub.Data.Meta)
			if err != nil {
				return err
			}
			existSdata, err := c.srv.GetSubcription(existSnode)
			if err != nil {
				return err
			}
			exSub = existSdata
			c.srv.Sl.Subs[name] = exSub
		}
	}

	if exSub != nil {
		if _, ok := exSub.Data.Subers[name]; !ok {
			c.respAction("not exist in this subcription")
			return nil
		}

		delete(c.srv.Sl.Subs, name)
		if err := c.srv.PutSubcription(exSub); err != nil {
			return err
		}
	} else {
		c.respAction("subcription not exist.")
		return nil
	}

	//TODO: map operations above all are safe ?

	// return c.srv.Sl.delete(sub)

	c.respAction("success")
	return nil
}

func (c *client) processPub(arg []byte) error {
	logger.Debugf("Pub arg: '%s'", arg)
	args := spiltArg(arg)
	switch len(args) {
	case 4:
		c.pa.Topic = string(args[0])
		c.pa.Partition = parseSize(args[1])
		c.pa.Mid = bytes2int64(args[2])
		c.pa.Size = parseSize(args[3])
		c.pa.Szb = args[3]
	case 5:
		c.pa.Topic = string(args[0])
		c.pa.Partition = parseSize(args[1])
		c.pa.Key = string(args[2])
		c.pa.Mid = bytes2int64(args[3])
		c.pa.Size = parseSize(args[4])
		c.pa.Szb = args[4]
	default:
		return logger.Errorf("processPub Parse failed: '%s'", arg)
	}
	if c.pa.Size < 0 {
		return logger.Errorf("processPub Bad or Missing Size: '%s'", arg)
	}
	logger.Debugf("Pub parsed: %+v", c.pa)
	return nil
}

func (c *client) processMsg(p []byte) {
	c.nm++
	if c.srv == nil {
		return
	}
	// c.pa.Payload = p
	var pNode *rc.PartitionNode
	path := c.pa.Topic + "/p" + strconv.Itoa(c.pa.Partition)
	if _, ok := c.srv.ps[path]; ok {
		pNode = c.srv.ps[path]
	} else {
		isExists, err := c.srv.zkCli.IsPartitionExists(c.pa.Topic, c.pa.Partition)
		if err != nil {
			//do something
			return
		}
		if isExists {
			pNode, err = c.srv.zkCli.GetPartition(c.pa.Topic, c.pa.Partition)
			c.srv.ps[path] = pNode
		} else {
			c.respAction("there is no this topic/partition")
			return
		}
	}
	//lock ?
	atomic.AddUint64(&pNode.Mnum, 1)

	mData := msg.MsgData{
		Mid:     c.pa.Mid,
		Payload: string(p),
	}
	if err := c.srv.PutMsg(&c.pa, mData); err != nil {
		//do something
	}
	c.respAction("success")

	if err := c.srv.zkCli.UpdatePartition(pNode); err != nil {
		//do something
	}

	// scratch := [512]byte{}
	// msgh := scratch[:0]

	// msgh = append(msgh, "MSG "...)
	// msgh = append(msgh, []byte(c.pa.Topic)...)
	// msgh = append(msgh, ' ')
	// ms := len(msgh)

	// subs := c.srv.Sl.getSuber(c.pa.Topic)
	// if len(subs) <= 0 {
	// 	return
	// }

	// for _, sub := range subs {
	// 	mh := c.msgHeader(msgh[:ms], sub)
	// 	sub.deliverMsg(mh, msg)
	// }
	// TODO
}

func (c *client) sendMsg(pa *msg.PullArg, m []*msg.MsgData) error {
	var msgh []byte
	msgh = append(msgh, "MSG "...)
	msgh = append(msgh, []byte(pa.Topic)...)
	msgh = append(msgh, " "...)
	msgh = append(msgh, []byte(strconv.Itoa(pa.Partition))...)
	
	c.mu.Lock()
	c.bw.Write(msgh)
	c.bw.WriteString("\r\n")

	return nil
}

func (c *client) processPing() {

}

func (c *client) processConnect(arg []byte) error {
	return nil
}

func (c *client) closeConnection() {

}

func (c *client) msgHeader(msgh []byte, sub *subcription) []byte {
	//TODO: need to updata
	msgh = append(msgh, []byte(sub.Data.Meta.TopicName)...)
	msgh = append(msgh, ' ')
	msgh = append(msgh, c.pa.Szb...)
	msgh = append(msgh, "\r\n"...)
	return msgh
}

func (c *client) respAction(data string) {

}

const argsLenMax = 3

func spiltArg(arg []byte) [][]byte {
	a := [argsLenMax][]byte{}
	args := a[:0]
	start := -1
	for i, en := range arg {
		switch en {
		case ' ', '\n', '\t', '\r':
			if start >= 0 {
				args = append(args, arg[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		args = append(args, arg[start:])
	}
	return args
}
