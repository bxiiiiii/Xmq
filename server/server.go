package server

import (
	"Xmq/logger"
	"bufio"
	"net"
	"sync/atomic"
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
	gcid    uint64
}

func NewServer(si ServerInfo) *Server {
	s := &Server{
		Info:    si,
		Running: false,
		Sl:      NewSublist(),
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
