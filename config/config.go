package config

import (
	"fmt"

	"github.com/spf13/viper"
)

var SrvConf = new(ServerConf)

type ServerConf struct {
	Name            string
	Host            string
	Port            int
	DefaultSendSize int
	RpcTimeout      int

	Lw loadWeight

	SyncWrite2disk     bool
	AsyncWriteMsglimit int

	IsLoadBalancerEnabled   bool
	CollectLoadDataInterval int
	PushLoadDataInterval    int

	defaultNumberOfBundles int
}

type loadWeight struct {
	Cpu          float64
	Memory       float64
	BandwidthIn  float64
	BandwidthOut float64
}

func init() {
	GetConfig()
}

func GetConfig() {
	viper.SetConfigFile("./config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("ReadInConfig failed, err: %v", err))
	}
	if err := viper.Unmarshal(&SrvConf); err != nil {
		panic(fmt.Errorf("Unmarshal to Conf failed, err: %v", err))
	}
}
