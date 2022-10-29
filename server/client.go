package server

import (
	"Xmq/logger"
	"bufio"
	"net"
	"sync"
)

const defaultBufSize = 65536

type client struct {
	mu     sync.Mutex
	cid    uint64
	name   string
	conn   net.Conn
	srv    *Server
	broker *broker
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
	pa     pubArg
	argBuf []byte
	msgBuf []byte
}

type pubArg struct {
	topic string
	size  int
	reply []byte
	szb   []byte
}

// subscribe type
const (
	Exclusive = iota
	Shared
	Key_Shared
)

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
		c.pa.topic = string(args[0])
		c.pa.reply = nil
		c.pa.size = parseSize(args[1])
		c.pa.szb = args[1]
	case 3:
		c.pa.topic = string(args[0])
		c.pa.reply = args[1]
		c.pa.size = parseSize(args[2])
		c.pa.szb = args[2]
	default:
		return logger.Errorf("processPub Parse failed: '%s'", arg)
	}
	if c.pa.size < 0 {
		return logger.Errorf("processPub Bad or Missing Size: '%s'", arg)
	}
	logger.Debugf("Pub parsed: %+v", c.pa)
	return nil
}

func (c *client) processSub(arg []byte) error {
	args := spiltArg(arg)
	sub := NewSubcription()
	sub.clients[c.name] = c
	switch len(args) {
	case 3:
		sub.topic.name = string(args[0])
		sub.name = string(args[1])
		sub.subtype = int(arg[2])
	default:
		return logger.Errorf("num of args :%v", len(args))
	}
	if c.srv != nil {
		c.srv.sl.insertORupdate(sub)
	}
	return nil
}

func (c *client) processUnsub(arg []byte) error {
	args := spiltArg(arg)
	sub := NewSubcription()
	sub.clients[c.name] = c
	switch len(args) {
	case 1:
		//TODO
	case 2:
		sub.topic.name = string(args[0])
		sub.name = string(args[1])
	default:
		return logger.Errorf("num of args :%v", len(args))
	}
	return c.srv.sl.delete(sub)
}

func (c *client) processMsg(msg []byte) {
	c.nm++
	if c.srv == nil {
		return
	}
	scratch := [512]byte{}
	msgh := scratch[:0]

	msgh = append(msgh, "MSG "...)
	msgh = append(msgh, []byte(c.pa.topic)...)
	msgh = append(msgh, ' ')
	ms := len(msgh)

	subs := c.srv.sl.getSuber(c.pa.topic)
	if len(subs) <= 0 {
		return
	}

	for _, sub := range subs {
		mh := c.msgHeader(msgh[:ms], sub)
		sub.deliverMsg(mh, msg)
	}
	// TODO
}

func (c *client) processPing() {

}

func (c *client) processConnect(arg []byte) error {
	return nil
}

func (c *client) closeConnection() {

}

func (c *client) msgHeader(msgh []byte, sub *subcription) []byte {
	msgh = append(msgh, []byte(sub.name)...)
	msgh = append(msgh, ' ')
	msgh = append(msgh, c.pa.szb...)
	msgh = append(msgh, "\r\n"...)
	return msgh
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
