package main

import (
	"Xmq/server"
	rc "Xmq/registrationCenter"
)

func main() {
	server := server.NewServerFromConfig()
	server.RunWithGrpc()
}
