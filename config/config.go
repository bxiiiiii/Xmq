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

	// load weight
	CpuWeight           float64
	VirtualMemoryWeight float64
	SwapMemoryWeight    float64
	BandwidthInWeight   float64
	BandwidthOutWeight  float64

	// load limit
	CpuLimit           float64
	VirtualMemoryLimit float64
	SwapMemoryLimit    float64
	BandwidthInLimit   float64
	BandwidthOutLimit  float64

	SyncWrite2disk     bool
	AsyncWriteMsglimit int

	IsLoadBalancerEnabled   bool
	CollectLoadDataInterval int
	PushLoadDataInterval    int

	DefaultNumberOfBundles int
	DefaultMaxAddress      int

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

func init() {
	GetConfig()
}

func GetConfig() {
	viper.SetConfigFile("../config/config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("ReadInConfig failed, err: %v", err))
	}

	if err := viper.UnmarshalKey("broker", SrvConf); err != nil {
		panic(fmt.Errorf("Unmarshal to SrvConf failed, err: %v", err))
	}

	if err := viper.UnmarshalKey("zookeeper", ZkConf); err != nil {
		panic(fmt.Errorf("Unmarshal to ZkConf failed, err: %v", err))
	}

	if err := viper.UnmarshalKey("etcd", EConf); err != nil {
		panic(fmt.Errorf("Unmarshal to EtcdConf failed, err: %v", err))
	}
}
