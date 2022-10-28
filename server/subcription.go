package server

type subcription struct {
	sid    uint64
	client *client
	topic  *topic
	offset uint64
}
