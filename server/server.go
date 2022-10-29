package server

import (
	"Xmq/logger"
	"net"
)

type serverInfo struct {
	serverName string
}

type Server struct {
	info    serverInfo
	running bool
	sl *sublist
}

func New() *Server {
	return &Server{}
}

func (s *Server) start() {

}

func isrunning() bool {
	var isrunning bool
	return isrunning
}

func (s *Server) AcceptLoop() {
	listener, err := net.Listen("tcp", "0.0.0.0:4222")
	if err != nil {
		logger.Debugf("net.Listen failed %v", err)
	}
	logger.Tracef("Listening on %v", listener.Addr())
	for {
		conn, err := listener.Accept()
		if err != nil {

		}
		s.createClient(conn)
	}
}

func (s *Server) createClient(net.Conn) *client {
	return &client{}
}
