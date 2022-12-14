package server

import "encoding/binary"

const (
	ascii_0 = 48
	ascii_9 = 57
)

func parseSize(d []byte) (n int) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < ascii_0 || dec > ascii_9 {
			return -1
		}
		n = n*10 + (int(dec) - ascii_0)
	}

	return n
}

func bytes2int64(bytes []byte) int64 {
	return int64(binary.BigEndian.Uint64(bytes))
}
