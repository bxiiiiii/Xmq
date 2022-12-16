package bundle

import (
	"Xmq/config"
	rc "Xmq/registrationCenter"
	"errors"
	"hash/crc32"

	"github.com/samuel/go-zookeeper/zk"
)

type Bundles struct {
	Bundles map[int]*Bundle
}

type Bundle struct {
	Info *rc.BundleNode
	// Partitions sync.Map
}

func NewBundles() (*Bundles, error) {
	bs := &Bundles{
		Bundles: make(map[int]*Bundle),
	}
	// check bundle num
	for i := 1; i <= config.SrvConf.DefaultNumberOfBundles; i++ {
		b, err := NewBundle(i)
		if err != nil {
			return nil, err
		}
		bs.Bundles[i] = b
	}
	return bs, nil
}

func NewBundle(id int) (*Bundle, error) {
	shard := config.SrvConf.DefaultMaxAddress / config.SrvConf.DefaultNumberOfBundles
	uint32Shard := uint32(shard)
	info := &rc.BundleNode{
		ID:    id,
		End:   uint32Shard * uint32(id),
		Start: uint32Shard*uint32(id) - uint32Shard,
	}
	b := &Bundle{Info: info}

	if err := rc.ZkCli.RegisterBunode(info); err != nil {
		if err == zk.ErrNodeExists {
			pre, err1 := rc.ZkCli.GetBundle(id)
			if err1 != nil {
				return nil, err1
			}
			b.Info = pre
		} else {
			return nil, err
		}
	}
	return b, nil
}

func (bs *Bundles) GetBundle(topic string) (int, error) {
	address := crc32.ChecksumIEEE([]byte(topic))
	return bs.bsearch(address)
}

func (bs *Bundles) bsearch(key uint32) (int, error) {
	if key == uint32(config.SrvConf.DefaultMaxAddress) {
		return 1, nil
	}

	bnum := len(bs.Bundles)
	left := 1
	right := bnum
	for left <= right {
		mid := bs.Bundles[(left+right)/2]
		// fmt.Println(left, " ", key," ", *mid.Info, " ", right)
		if key > mid.Info.End-1 {
			left = mid.Info.ID+1
		} else if key < mid.Info.Start {
			right = mid.Info.ID - 1
		} else {
			return mid.Info.ID, nil
		}
	}

	return 0, errors.New("??? not found")
}
