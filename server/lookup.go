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
	return reply, nil
}

func (s *Server) RequestAlloc(ctx context.Context, args *pb.RequestAllocArgs) (*pb.RequestAllocReply, error) {
	reply := &pb.RequestAllocReply{}
	if s.loadManager.State != lm.Leader {
		lNode, err := rc.ZkCli.GetLeader()
		if err != nil {
			logger.Errorf("GetLeader failed: %v", err)
			return nil, errors.New("404")
		}
		reply.Url = lNode.LeaderUrl
		return reply, errors.New("need to connect leader to alloc")
	}

	path := fmt.Sprintf(partitionKey, args.Topic, args.Partition)
	bundleID, err := s.bundles.GetBundle(path)
	if err != nil {
		logger.Errorf("GetBundle failed: %v", err)
		return nil, errors.New("404")
	}

	var buNode *rc.BundleNode
	if _, ok := s.bundles.Bundles[bundleID]; !ok {
		buNode, err = rc.ZkCli.GetBundle(bundleID)
		if err != nil {
			return nil, err
		}
		s.bundles.Bundles[bundleID] = &bundle.Bundle{Info: buNode}
	}

	bNode, err := s.loadManager.AllocateBundle()
	buNode.BrokerUrl = fmt.Sprintf("%v:%v", bNode.Host, bNode.Port)
	if err := rc.ZkCli.UpdateBundle(buNode); err != nil {
		logger.Errorf("UpdateBundle failed: %v", err)
		return nil, errors.New("404")
	}

	reply.Url = buNode.BrokerUrl
	return reply, nil
}
