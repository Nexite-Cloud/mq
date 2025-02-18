package client

import (
	"context"
)

type Consumer interface {
	Next(ctx context.Context) ([]byte, error)
	Close() error
}

type Producer interface {
	Produce(ctx context.Context, topic string, data []byte) error
}
