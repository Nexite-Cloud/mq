package client

import (
	"context"
)

type Consumer interface {
	Next(ctx context.Context) (*Record, error)
	Chan(ctx context.Context) <-chan *Record
	Close() error
}

type Producer interface {
	Produce(ctx context.Context, topic string, data []byte) error
}
