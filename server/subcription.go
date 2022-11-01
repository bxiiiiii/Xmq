package server

import (
	"Xmq/logger"
	"container/list"
	"sync"
)

type subcription struct {
	sid     uint64
	name    string
	subtype int
	clients map[string]*client
	topic   topic
	offset  uint64
	clist   *list.List
}

type sublist struct {
	mu sync.RWMutex
	s  map[string]*subcription
}

func NewSublist() *sublist {
	s := make(map[string]*subcription)
	return &sublist{s: s}
}

func NewSubcription() *subcription {
	c := make(map[string]*client)
	l := list.New().Init()
	return &subcription{clients: c, clist: l}
}

func (s *sublist) insertORupdate(sub *subcription) {
	defer s.mu.Unlock()
	s.mu.Lock()
	for k, v := range s.s {
		if k == sub.name && v.name == sub.topic.name && v.subtype == sub.subtype {
			for n, c := range sub.clients {
				v.clients[n] = c
			}
			return
		}
	}
	s.s[sub.name] = sub
}

func (s *sublist) delete(sub *subcription) error {
	defer s.mu.Unlock()
	s.mu.Lock()
	for k, v := range s.s {
		if k == sub.name && v.name == sub.topic.name {
			for _, c := range sub.clients {
				delete(v.clients, c.name)
				return nil
			}
		}
	}
	return logger.Errorf("")
}

func (s *sublist) deleteAll() {

}

func (s *sublist) getSuber(t string) []*subcription {
	s.mu.RLock()
	defer s.mu.RLocker().Unlock()
	subs := []*subcription{}
	for _, v := range s.s {
		if t == v.topic.name {
			subs = append(subs, v)
		}
	}
	return subs
}

func (s *subcription) deliverMsg(msgh []byte, msg []byte) {
	switch s.subtype {
	case Exclusive:
		for _, v := range s.clients {
			v.mu.Lock()
			v.bw.Write(msgh)
			v.bw.Write(msg)
			v.bw.WriteString("\r\n")
			v.bw.Flush()
			v.mu.Unlock()
		}
	case Shared:
		
		// for _, v := range s.clients {
		// 	v.mu.Lock()
		// 	v.bw.Write(msgh)
		// 	v.bw.Write(msg)
		// 	v.bw.WriteString("\r\n")
		// 	v.bw.Flush()
		// 	v.mu.Unlock()
		// }
	case Key_Shared:
		//TODO
	}
}
