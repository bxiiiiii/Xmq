package main

import (
	"Xmq/config"
	"Xmq/logger"
	"Xmq/persist"
	rc "Xmq/registrationCenter"
	"Xmq/server"
	"os"
	"time"

	"github.com/urfave/cli/v2"
)

var (
	cmdStart = cli.Command{
		Name:  "start",
		Usage: "start server. For example: ./Xmq start -c config/config.yaml",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "c",
				Usage: "Load configuration from `FILE`",
			},
		},
		Action: func(c *cli.Context) error {
			config.GetConfig(c.String("c"))
			persist.PersistInit()
			rc.RcInit()
			
			server := server.NewServerFromConfig()
			if err := server.Online(); err != nil {
				logger.Errorf("Online failed: %v", err)
			}

			server.RunWithGrpc()
			return nil
		},
	}
)

func main() {
	app := newapp(&cmdStart)
	_ = app.Run(os.Args)
}

func newapp(startCmd *cli.Command) *cli.App {
	app := cli.NewApp()
	app.Name = "Xmq"
	app.Compiled = time.Now()
	app.Usage = "A High Performance Xmq Server written in Go."
	app.Flags = cmdStart.Flags

	app.Commands = []*cli.Command{
		&cmdStart,
	}

	app.Action = func(c *cli.Context) error {
		if c.NumFlags() == 0 {
			return cli.ShowAppHelp(c)
		}

		return startCmd.Action(c)
	}

	return app
}
