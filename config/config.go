package config

import (
	"fmt"

	"github.com/spf13/viper"
)

var SrvConf = new(ServerConf)

var ZkConf = new(ZookeeperConf)

var EConf = new(EtcdConf)

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

	DefaultNumberOfBundles int

	AllowRenameForClient bool

	OperationRedoNum int
	OperationTimeout int
}

type ZookeeperConf struct {
	Host           []string
	Root           string
	BrokerRoot     string
	TopicRoot      string
	BundleRoot     string
	SessionTimeout int
}

type EtcdConf struct {
	Endpoints   []string
	DialTimeout int
}

type loadWeight struct {
	Cpu           float64
	VirtualMemory float64
	SwapMemory    float64
	BandwidthIn   float64
	BandwidthOut  float64
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
		panic(fmt.Errorf("Unmarshal to SrvConf failed, err: %v", err))
	}

	if err := viper.Unmarshal(&ZkConf); err != nil {
		panic(fmt.Errorf("Unmarshal to ZkConf failed, err: %v", err))
	}
	
	if err := viper.Unmarshal(&EConf); err != nil {
		panic(fmt.Errorf("Unmarshal to EtcdConf failed, err: %v", err))
	}
}
