package main

import (
	_ "Xmq/config"
	"Xmq/logger"
	_ "Xmq/persist"
	_ "Xmq/registrationCenter"
	"Xmq/server"
)

func main() {
	server := server.NewServerFromConfig()
	if err := server.Online(); err != nil {
		logger.Errorf("Online failed: %v", err)
	}

	server.RunWithGrpc()
}
