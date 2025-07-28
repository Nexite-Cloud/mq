package client

type Record struct {
	Partition int
	Topic     string
	Value     []byte
}
