package msg

import "time"

type PubArg struct {
	Topic     string
	Size      int
	Reply     []byte
	Szb       []byte
	Partition int
	Mid       int64
	Msid      uint64
	Payload   []byte
	Key       string
}

type PullArg struct {
	Topic     string
	Partition int
	Subname   string
	Bufsize   int
	Full      chan bool
	Timeout   chan bool
}

type MsgData struct {
	Msid    uint64
	Mid     int64
	Payload string
}

func (pa *PullArg) CheckTimeout() {
	select {
	case <-pa.Full:
		return
	case <-time.After(time.Second * 5):
		pa.Timeout <- true
	}
}
