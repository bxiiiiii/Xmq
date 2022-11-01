package main

import (
	"Xmq/server"
)

func main() {
	si := server.ServerInfo{}
	server := server.NewServer(si)

	server.Run()
}
