package zookeeper

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var ZkCli *zkClient

var (
	host       = []string{"localhost:2181"}
	root       = "/Xmq"
	brokerRoot = root + "/broker"
	topicRoot  = root + "/topic"
)

type zkClient struct {
	ZkServers    []string
	ZkRoot       string
	ZkBrokerRoot string
	ZkTopicRoot  string
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
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
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
}

func NewClient(zkServers []string, zkRoot string, timeout int) (*zkClient, error) {
	c := &zkClient{}
	c.ZkRoot = zkRoot
	c.ZkServers = zkServers
	c.ZkBrokerRoot = brokerRoot
	c.ZkTopicRoot = topicRoot

	Conn, _, err := zk.Connect(zkServers, time.Duration(timeout)*time.Second)
	if err != nil {
		panic("connect zk failed.")
	}
	c.Conn = Conn

	return c, nil
}

func (c *zkClient) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode

	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode)
		path += c.ZkBrokerRoot + "/" + bnode.Name
		data, err = json.Marshal(bnode)
	case "TopicNode":
		tnode = znode.(TopicNode)
		path += c.ZkTopicRoot + "/" + tnode.Name
		data, err = json.Marshal(tnode)
	case "PartitionNode":
		pnode = znode.(PartitionNode)
		path += c.ZkTopicRoot + "/" + pnode.TopicName + "/" + pnode.Name
		data, err = json.Marshal(pnode)
	}
	if err != nil {
		return err
	}

	_, err = c.Conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	return nil
}

func (c *zkClient) GetBrokers(topic string) ([]*PartitionNode, error) {
	var pNodes []*PartitionNode
	path := c.ZkTopicRoot + "/" + topic

	isExists, err := c.isTopicExists(topic)
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

func (c *zkClient) GetBroker(topic string, partition int) (*PartitionNode, error) {
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

// func (c *zkClient) getZnode(path string) (){

// }

func (c *zkClient) ensureExist(name string) error {
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

func (c *zkClient) isTopicExists(topic string) (bool, error) {
	// path := c.ZkTopicRoot + "/" + topic
	// isExists, _, err := c.Conn.Exists(path)
	// if err != nil {
	// 	return false, err
	// }

	// return isExists, nil

	// Todo: above is needed?
	return c.isPartitionExists(topic, 1)
}

func (c *zkClient) isPartitionExists(topic string, partition int) (bool, error) {
	path := c.ZkTopicRoot + "/" + topic + "p" + strconv.Itoa(partition)
	return c.isZnodeExists(path)
}

func (c *zkClient) isZnodeExists(path string) (bool, error) {
	isExists, _, err := c.Conn.Exists(path)
	return isExists, err
}

func (c *zkClient) createTopic(topic string) error {
	return c.createPartitionTopic(topic, 1)
}

func (c *zkClient) createPartitionTopic(topic string, partition int) error {
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
