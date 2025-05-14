package client

import (
	"context"
)

type Consumer interface {
	Next(ctx context.Context) (topic string, rawData []byte, err error)
	Close() error
}

type Producer interface {
	Produce(ctx context.Context, topic string, data []byte) error
}
