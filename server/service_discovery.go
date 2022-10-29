package server

import (
	"Xmq/logger"
	"encoding/json"
)

type tnode struct {
}

func getBroker(t string) (*broker, error) {
	b := new(topic)
	path := sdClient.topicRoot + "/" + t
	exists, _, err := sdClient.conn.Exists(path)
	if err != nil {
		logger.Errorf("topic znode '%v' exists() failed: %v", path, err)
		return nil, err
	}
	if exists {
		data, _, err := sdClient.conn.Get(path)
		if err != nil {
			logger.Errorf("topic znode '%v' get() failed: %v", path)
			return nil, err
		}
		err = json.Unmarshal(data, b)
		if err != nil {
			logger.Errorf("Unmarshal failed: %v", data)
			return nil, err
		}
	} else {
		topicRegister(t)
	}

	return &b.b, nil
}

func topicRegister(t string) {

}

func getBrokers() ([]*broker, error){
	path := sdClient.brokerRoot
	childs, _, err := sdClient.conn.Children(path)
	if err != nil {
		logger.Errorf("")
		return nil, err
	}

	brokers := []*broker{}
	for _, child := range childs {
		fullPath := path + "/" + child
		data, _, err 
	}
	return brokers, nil
}
