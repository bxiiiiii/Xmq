package server

import (
	"Xmq/bundle"
	lm "Xmq/loadManager"
	"Xmq/logger"
	pb "Xmq/proto"
	rc "Xmq/registrationCenter"
	"context"
	"errors"
	"fmt"
)

func (s *Server) LookUp(ctx context.Context, args *pb.LookUpArgs) (*pb.LookUpReply, error) {
	logger.Infof("Receive LookUp rq from %v", args)
	reply := &pb.LookUpReply{}
	path := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	bundleID, err := s.bundles.GetBundle(path)
	if err != nil {
		return nil, err
	}

	if _, ok := s.bundles.Bundles[bundleID]; !ok {
		bNode, err := rc.ZkCli.GetBundle(bundleID)
		if err != nil {
			return nil, err
		}
		s.bundles.Bundles[bundleID] = &bundle.Bundle{Info: bNode}
	}
	if s.bundles.Bundles[bundleID].Info.BrokerUrl == "" {
		lNode, err := rc.ZkCli.GetLeader()
		if err != nil {
			return nil, err
		}
		reply.Url = lNode.LeaderUrl
		return reply, errors.New("need to connect leader to alloc")
	}
	reply.Url = s.bundles.Bundles[bundleID].Info.BrokerUrl
	logger.Debugf("LookUp reply: %v", reply)
	return reply, nil
}

func (s *Server) RequestAlloc(ctx context.Context, args *pb.RequestAllocArgs) (*pb.RequestAllocReply, error) {
	s.loadManager.Mu.Lock()
	logger.Infof("Receive Alloc rq from %v, %v", args, s.loadManager.State)
	reply := &pb.RequestAllocReply{}

	if s.loadManager.State != lm.Leader {
		s.loadManager.Mu.Unlock()
		lNode, err := rc.ZkCli.GetLeader()
		if err != nil {
			logger.Errorf("GetLeader failed: %v", err)
			return nil, errors.New("404")
		}
		reply.Url = lNode.LeaderUrl
		return reply, errors.New("need to connect leader to alloc")
	}
	s.loadManager.Mu.Unlock()

	path := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	bundleID, err := s.bundles.GetBundle(path)
	if err != nil {
		logger.Errorf("GetBundle failed: %v", err)
		return nil, errors.New("404")
	}

	var buNode *rc.BundleNode
	if b, ok := s.bundles.Bundles[bundleID]; !ok {
		buNode, err = rc.ZkCli.GetBundle(bundleID)
		if err != nil {
			return nil, err
		}
		s.bundles.Bundles[bundleID] = &bundle.Bundle{Info: buNode}
	} else {
		buNode = b.Info
	}

	bNode, err := s.loadManager.AllocateBundle()

	buNode.BrokerUrl = fmt.Sprintf("%v:%v", bNode.Host, bNode.Port)
	if err := rc.ZkCli.UpdateBundle(buNode); err != nil {
		logger.Errorf("UpdateBundle failed: %v, %v", err, *buNode)
		return nil, errors.New("404")
	}

	reply.Url = buNode.BrokerUrl
	return reply, nil
}
