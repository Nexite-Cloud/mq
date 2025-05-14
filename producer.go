package mq

import (
	"context"

	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type Producer[T any] interface {
	Produce(ctx context.Context, topic string, data T) error
	SetCodec(encoder codec.Codec[T])
	SetLogger(logger Logger)
	Codec() codec.Codec[T]
}
type producer[T any] struct {
	client client.Producer
	codec  codec.Codec[T]
	logger Logger
}

func NewProducer(client client.Producer) Producer[any] {
	return &producer[any]{
		client: client,
		codec:  codec.JSON[any](),
		logger: NewSlogLogger(nil),
	}
}

func NewTypedProducer[T any](client client.Producer) Producer[T] {
	return &producer[T]{
		client: client,
		codec:  codec.JSON[T](),
		logger: NewSlogLogger(nil),
	}
}

func (p *producer[T]) SetLogger(logger Logger) {
	p.logger = logger
}

func (p *producer[T]) SetCodec(codec codec.Codec[T]) {
	p.codec = codec
}

func (p *producer[T]) Produce(ctx context.Context, topic string, data T) error {
	msg, err := p.codec.Encode(data)
	if err != nil {
		return err
	}
	p.logger.Info(ctx, "produce message", "topic", topic, "data", data, "msg", string(msg))
	return p.client.Produce(ctx, topic, msg)
}

func (p *producer[T]) Codec() codec.Codec[T] {
	return p.codec
}
