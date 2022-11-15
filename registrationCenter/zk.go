package RegistraionCenter

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var ZkCli *ZkClient

var (
	host       = []string{"localhost:2181"}
	root       = "/Xmq"
	brokerRoot = root + "/broker"
	topicRoot  = root + "/topic"
	bundleRoot = root + "/bundle"
)

type ZkClient struct {
	ZkServers    []string
	ZkRoot       string
	ZkBrokerRoot string
	ZkTopicRoot  string
	ZkBundleRoot string
	Conn         *zk.Conn
}

type BrokerNode struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
	Pnum int    `json:"pnum"`
}

type TopicNode struct {
	Name string `json:"name"`
	Pnum int    `json:"pnum"`
}

type PartitionNode struct {
	Name       string `json:"name"`
	TopicName  string `json:"topicName"`
	Mnum       uint64 `json:"mnum"`
	ackOffset  uint64 `json:"ackoffset"`
	pushOffset uint64 `json:"pushoffset"`
}

type BundleNode struct {
	Name string
	B    BrokerNode
}

type SubcriptionNode struct {
	Name      string
	TopicName string
	Partition string
	Subtype   int
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

func NewClient(zkServers []string, zkRoot string, timeout int) (*ZkClient, error) {
	c := &ZkClient{}
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

func (c *ZkClient) RegisterBnode(bnode BrokerNode) error {
	path := c.ZkBrokerRoot + "/" + bnode.Name
	data, err := json.Marshal(bnode)
	if err != nil {
		return err
	}
	// TODO: create temporary node
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterTnode(tnode TopicNode) error {
	path := c.ZkTopicRoot + "/" + tnode.Name
	data, err := json.Marshal(tnode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterPnode(pnode PartitionNode) error {
	path := c.ZkTopicRoot + "/" + pnode.TopicName + "/" + pnode.Name
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterBunode(bunode BundleNode) error {
	path := c.ZkBundleRoot + "/" + bunode.Name
	data, err := json.Marshal(bunode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegistesSnode(snode *SubcriptionNode) error {
	path := c.ZkTopicRoot + "/" + snode.TopicName + "/p" + snode.Partition + "/" + snode.Name
	data, err := json.Marshal(snode)
	if err != nil {
		return err
	}
	return c.RegisterNode(path, data)
}

func (c *ZkClient) RegisterNode(path string, data []byte) error {
	_, err := c.Conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (c *ZkClient) GetBrokers(topic string) ([]*PartitionNode, error) {
	var pNodes []*PartitionNode
	path := c.ZkTopicRoot + "/" + topic

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
	path := c.ZkTopicRoot + "/" + topic + "/" + "p" + strconv.Itoa(partition)

	isExists, err := c.isPartitionExists(topic, partition)
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

func (c *ZkClient) GetSub(snode *SubcriptionNode) (*SubcriptionNode, error) {
	path := c.ZkTopicRoot + "/" + snode.TopicName + "/p" + snode.Partition + "/" + snode.Name
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

func (c *ZkClient) GetPartion(topic string, partition int) (*PartitionNode, error) {
	pNode := &PartitionNode{}
	return pNode, nil
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
	return c.isPartitionExists(topic, 1)
}

func (c *ZkClient) isPartitionExists(topic string, partition int) (bool, error) {
	path := c.ZkTopicRoot + "/" + topic + "p" + strconv.Itoa(partition)
	return c.isZnodeExists(path)
}

func (c *ZkClient) IsSubcriptionExist(snode *SubcriptionNode) (bool, error) {
	path := c.ZkTopicRoot + "/" + snode.TopicName + "/p" + snode.Partition + "/" + snode.Name
	return c.isZnodeExists(path)
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
			Name:      "p" + strconv.Itoa(i),
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
