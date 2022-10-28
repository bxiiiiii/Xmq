package server

import (
	"Xmq/logger"
	"net"
)

const defaultBufSize = 65536

type client struct {
	cid    uint64
	conn   net.Conn
	srv    *Server
	broker *broker
	cstats
	praseState
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
	topic []byte
	size  int
}

// subscribe type
const (
	Exclusive = iota
	Shared
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
		c.pa.topic = args[0]
		c.pa.size = parseSize(args[1])
	case 3:
		c.pa.topic = args[0]
		c.pa.size = parseSize(args[2])
	default:
		return logger.Errorf("processPub Parse failed: '%s'", arg)
	}
	if c.pa.size < 0 {
		return logger.Errorf("processPub Bad or Missing Size: '%s'", arg)
	}
	logger.Debugf("Pub parsed: %+v", c.pa)
	return nil
}

func (c *client) processSub() {

}

func (c *client) processUnsub() {

}

func (c *client) processMsg(msg []byte) {
	c.nm++
	if c.srv == nil {
		return
	}

	// TODO
}

func (c *client) closeConnection() {

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
