package RegistraionCenter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	ct "Xmq/collect"

	"github.com/samuel/go-zookeeper/zk"
)

var ZkCli *ZkClient

var (
	host       = []string{"localhost:2181"}
	root       = "/Xmq"
	brokerRoot = root + "/broker"
	topicRoot  = root + "/topic"
	bundleRoot = root + "/bundle"
	leaderPath = root + "/leader"
	timeout    = 0
)

var (
	BnodePath     = "%v/%v"               // BrokerRoot/BrokerName
	TnodePath     = "%v/%v"               // TopicRoot/TopicName
	BunodePath    = "%v/bundle%v"         // BundleRoot/BundleName
	PnodePath     = "%v/%v/p%v"           // TopicRoot/TopicName/PartitionName
	SnodePath     = "%v/%v/p%v/%v"        // TopicRoot/TopicName/PartitionName/SubcriptionName
	LeadPuberPath = "%v/%v/p%v/leader"    // TopicRoot/TopicName/PartitionName
	LeadSuberPath = "%v/%v/p%v/%v/leader" // TopicRoot/TopicName/PartitionName/SubcriptionName
)

type ZkClient struct {
	ZkServers    []string
	ZkRoot       string
	ZkBrokerRoot string
	ZkTopicRoot  string
	ZkBundleRoot string
	ZkLeaderRoot string
	Conn         *zk.Conn
}

type BrokerNode struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Pnum      int    `json:"pnum"`
	Load      ct.BrokerUsage
	LoadIndex float64
}

type TopicNode struct {
	Name       string `json:"name"`
	Pnum       int    `json:"pnum"`
	PulishMode int
}

type PartitionNode struct {
	ID         int    `json:"name"`
	TopicName  string `json:"topicName"`
	Mnum       uint64 `json:"mnum"`
	AckOffset  uint64 `json:"ackoffset"`
	PushOffset uint64 `json:"pushoffset"`
	Url        string
	// Version    int32
}

type BundleNode struct {
	ID        int
	Start     uint32
	End       uint32
	BrokerUrl string
}

type SubcriptionNode struct {
	Name      string
	TopicName string
	Partition int
	Subtype   int
}

type LeaderNode struct {
	LeaderUrl string
}

func init() {
	ZkCli, _ = NewClient(host, root, 3)
	err := ZkCli.ensureExist(ZkCli.ZkRoot)
	if err != nil {
		panic(err)
	}
	err = ZkCli.ensureExist(ZkCli.ZkBrokerRoot)
	if err != nil {
		panic(err)
	}
	err = ZkCli.ensureExist(ZkCli.ZkTopicRoot)
	if err != nil {
		panic(err)
	}
	err = ZkCli.ensureExist(ZkCli.ZkBundleRoot)
	if err != nil {
		panic(err)
	}
}

func callback(e zk.Event) {}

func NewClient(zkServers []string, zkRoot string, timeout int) (*ZkClient, error) {
	c := &ZkClient{
		//todo: get from config
	}
	c.ZkRoot = zkRoot
	c.ZkServers = zkServers
	c.ZkBrokerRoot = brokerRoot
	c.ZkTopicRoot = topicRoot
	c.ZkBundleRoot = bundleRoot

	Conn, _, err := zk.Connect(zkServers, time.Duration(timeout)*time.Second)
	if err != nil {
		panic("connect zk failed.")
	}
	c.Conn = Conn

	return c, nil
}

func NewClientWithCallback(cb func(e zk.Event)) (*ZkClient, error) {
	c := &ZkClient{
		//todo: get from config
	}
	eventCallbackOption := zk.WithEventCallback(cb)
	conn, _, err := zk.Connect(host, time.Second*time.Duration(timeout), eventCallbackOption)
	if err != nil {
		return nil, err
	}
	c.Conn = conn

	return c, nil
}

func (c *ZkClient) RegisterBnode(bnode BrokerNode) error {
	path := fmt.Sprintf(BnodePath, c.ZkBrokerRoot, bnode.Name)
	data, err := json.Marshal(bnode)
	if err != nil {
		return err
	}
	// TODO: create temporary node
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterTnode(tnode TopicNode) error {
	path := fmt.Sprintf(TnodePath, c.ZkTopicRoot, tnode.Name)
	data, err := json.Marshal(tnode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterPnode(pnode PartitionNode) error {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, pnode.TopicName, pnode.ID)
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterBunode(bunode BundleNode) error {
	path := fmt.Sprintf(BunodePath, c.ZkBundleRoot, bunode.ID)
	data, err := json.Marshal(bunode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterSnode(snode *SubcriptionNode) error {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, snode.TopicName, snode.Partition, snode.Name)
	data, err := json.Marshal(snode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterLeadPuberNode(topic string, partition int) error {
	path := fmt.Sprintf(LeadPuberPath, c.ZkTopicRoot, topic, partition)
	return c.registerTemNode(path, []byte{65})
}

func (c *ZkClient) RegisterLeadSuberNode(topic string, partition int, subscription string) error {
	path := fmt.Sprintf(LeadSuberPath, c.ZkTopicRoot, topic, partition, subscription)
	return c.registerTemNode(path, []byte{65})
}

func (c *ZkClient) RegisterLeadBrokernode() error {
	return c.registerTemNode(leaderPath, []byte{65})
}

func (c *ZkClient) RegisterNode(path string, data []byte) error {
	_, err := c.Conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (c *ZkClient) registerTemNode(path string, data []byte) error {
	//todo: choose one?
	// _, err := c.Conn.CreateProtectedEphemeralSequential(path, data, zk.AuthACL(zk.PermAll))
	_, err := c.Conn.Create(path, data, zk.FlagEphemeral, zk.AuthACL(zk.PermAll))
	return err
}

func (c *ZkClient) RegisterLeadBrokerWatch() (bool, <-chan zk.Event, error) {
	return c.registerWatcher(leaderPath)
}

func (c *ZkClient) RegisterLeadPuberWatch(topic string, partition int) (bool, <-chan zk.Event, error) {
	path := fmt.Sprintf(LeadPuberPath, c.ZkTopicRoot, topic, partition)
	return c.registerWatcher(path)
}

func (c *ZkClient) RegisterLeadSuberWatch(topic string, partition int, subscription string) (bool, <-chan zk.Event, error) {
	path := fmt.Sprintf(LeadSuberPath, c.ZkTopicRoot, topic, partition, subscription)
	return c.registerWatcher(path)
}

func (c *ZkClient) registerWatcher(path string) (bool, <-chan zk.Event, error) {
	isExists, _, ch, err := c.Conn.ExistsW(path)
	return isExists, ch, err
}

func (c *ZkClient) RegisterChildrenWatcher(path string) ([]string, <-chan zk.Event, error) {
	znodes, _, ch, err := c.Conn.ChildrenW(path)
	return znodes, ch, err
}

func (c *ZkClient) GetBrokers(topic string) ([]*PartitionNode, error) {
	var pNodes []*PartitionNode
	path := fmt.Sprintf(TnodePath, c.ZkTopicRoot, topic)

	isExists, err := c.IsTopicExists(topic)
	if err != nil {
		return nil, err
	}

	if !isExists {
		//Todo: how to create? p / no p
		//default: no p
		err := c.createTopic(topic)
		if err != nil {
			return nil, err
		}
	}

	znodes, _, err := c.Conn.Children(path)
	if err != nil {
		return nil, err
	}

	for _, znode := range znodes {
		pPath := path + "/" + znode
		data, _, err := c.Conn.Get(pPath)
		if err != nil {
			return nil, err
		}
		pNode := &PartitionNode{}
		err = json.Unmarshal(data, pNode)
		if err != nil {
			return nil, err
		}
		pNodes = append(pNodes, pNode)
	}

	return pNodes, nil
}

func (c *ZkClient) GetBroker(topic string, partition int) (*PartitionNode, error) {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, topic, partition)
	isExists, err := c.IsPartitionExists(topic, partition)
	if err != nil {
		return nil, err
	}
	if !isExists {
		return nil, errors.New("znode is not exists")
	}

	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	pNode := &PartitionNode{}
	err = json.Unmarshal(data, pNode)
	if err != nil {
		return nil, err
	}

	return pNode, nil
}

func (c *ZkClient) GetTopic(topic string) (*TopicNode, error) {
	path := fmt.Sprintf(TnodePath, c.ZkTopicRoot, topic)
	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	tNode := &TopicNode{}
	if err = json.Unmarshal(data, tNode); err != nil {
		return nil, err
	}
	return tNode, nil
}

func (c *ZkClient) GetSub(snode *SubcriptionNode) (*SubcriptionNode, error) {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, snode.TopicName, snode.Partition, snode.Name)
	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	sNode := &SubcriptionNode{}
	if err = json.Unmarshal(data, sNode); err != nil {
		return nil, err
	}
	return sNode, nil
}

func (c *ZkClient) GetPartition(topic string, partition int) (*PartitionNode, error) {
	pNode := &PartitionNode{}
	return pNode, nil
}

func (c *ZkClient) GetBundles(bnum int) ([]*BundleNode, error) {
	var bundles []*BundleNode
	for bnum > 0 {
		bNode, err := c.GetBundle(bnum)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, bNode)
		bnum--
	}
	return bundles, nil
}

func (c *ZkClient) GetBundle(id int) (*BundleNode, error) {
	path := fmt.Sprintf(BunodePath, id)
	data, _, err := c.Conn.Get(path)
	if err != nil {
		return nil, err
	}

	bNode := &BundleNode{}
	if err = json.Unmarshal(data, bNode); err != nil {
		return nil, err
	}
	return bNode, nil
}

func (c *ZkClient) GetLeader() (*LeaderNode, error) {
	data, _, err := c.Conn.Get(leaderPath)
	if err != nil {
		return nil, err
	}

	lNode := &LeaderNode{}
	if err = json.Unmarshal(data, lNode); err != nil {
		return nil, err
	}
	return lNode, nil
}

func (c *ZkClient) GetAllBrokers() ([]*BrokerNode, error) {
	var brokers []*BrokerNode
	znodes, _, err := c.Conn.Children(brokerRoot)
	if err != nil {
		return nil, err
	}

	for _, znode := range znodes {
		bPath := brokerRoot + "/" + znode
		data, _, err := c.Conn.Get(bPath)
		if err != nil {
			return nil, err
		}
		bNode := &BrokerNode{}
		err = json.Unmarshal(data, bNode)
		if err != nil {
			return nil, err
		}
		brokers = append(brokers, bNode)
	}
	return brokers, nil
}

func (c *ZkClient) IsPubersExists(topic string, partition int) (bool, error) {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, topic, partition)
	pubers, _, err := c.Conn.Children(path)
	if err != nil {
		return false, err
	}
	return len(pubers) == 0, nil
}

func (c *ZkClient) IsSubersExists(topic string, partition int, subscription string) (bool, error) {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, topic, partition, subscription)
	subers, _, err := c.Conn.Children(path)
	if err != nil {
		return false, err
	}
	return len(subers) == 0, nil
}

// func (c *ZkClient) getZnode(path string) (){

// }

func (c *ZkClient) ensureExist(name string) error {
	isExists, _, err := c.Conn.Exists(name)
	if err != nil {
		return err
	}
	if !isExists {
		_, err := c.Conn.Create(name, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}

	return nil
}

func (c *ZkClient) IsTopicExists(topic string) (bool, error) {
	// path := c.ZkTopicRoot + "/" + topic
	// isExists, _, err := c.Conn.Exists(path)
	// if err != nil {
	// 	return false, err
	// }

	// return isExists, nil

	// Todo: above is needed?
	return c.IsPartitionExists(topic, 1)
}

func (c *ZkClient) IsPartitionExists(topic string, partition int) (bool, error) {
	path := c.ZkTopicRoot + "/" + topic + "p" + strconv.Itoa(partition)
	return c.isZnodeExists(path)
}

func (c *ZkClient) IsSubcriptionExist(snode *SubcriptionNode) (bool, error) {
	path := fmt.Sprintf(SnodePath, c.ZkTopicRoot, snode.TopicName, snode.Partition, snode.Name)
	return c.isZnodeExists(path)
}

func (c *ZkClient) IsLeaderExist() (bool, error) {
	return c.isZnodeExists(leaderPath)
}

func (c *ZkClient) isZnodeExists(path string) (bool, error) {
	isExists, _, err := c.Conn.Exists(path)
	return isExists, err
}

func (c *ZkClient) createTopic(topic string) error {
	return c.createPartitionTopic(topic, 1)
}

func (c *ZkClient) createPartitionTopic(topic string, partition int) error {
	tPath := c.ZkTopicRoot + "/" + topic
	tData := TopicNode{
		Name: topic,
		Pnum: 1,
	}
	tdata, _ := json.Marshal(tData)
	_, err := c.Conn.Create(tPath, tdata, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	for i := 1; i <= partition; i++ {
		pPath := tPath + "/" + "p" + strconv.Itoa(i)
		pData := PartitionNode{
			ID:        i,
			TopicName: topic,
		}
		pdata, _ := json.Marshal(pData)
		_, err = c.Conn.Create(pPath, pdata, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ZkClient) UpdatePartition(pNode *PartitionNode) error {
	path := fmt.Sprintf(PnodePath, c.ZkTopicRoot, pNode.TopicName, pNode.ID)
	data, err := json.Marshal(pNode)
	if err != nil {
		return err
	}
	//TODO: version 0?
	_, err = c.Conn.Set(path, data, 0)
	return err
}

func (c *ZkClient) UpdateBroker(bNode BrokerNode) error {
	path := fmt.Sprintf(BnodePath, c.ZkBrokerRoot, bNode.Name)
	data, err := json.Marshal(bNode)
	if err != nil {
		return err
	}
	_, err = c.Conn.Set(path, data, 0)
	return err
}
