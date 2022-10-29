package server

type topic struct {
	tid  uint64 `json:"tid"`
	name string `json:"name"`
	b    broker `json:"broker"`
}

type broker struct {
	bid uint64
}
