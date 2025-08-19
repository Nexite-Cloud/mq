package mq

import (
	"context"
	"time"

	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type ProducerOpt struct {
	MaxRetryCount int
	Timeout       time.Duration
}

func defaultProduceOpt() *ProducerOpt {
	return &ProducerOpt{
		MaxRetryCount: 3,
		Timeout:       time.Minute * 15,
	}
}

type ProducerOption func(*ProducerOpt)

func ProduceTimeout(timeout time.Duration) ProducerOption {
	return func(opt *ProducerOpt) {
		opt.Timeout = timeout
	}
}

func ProduceMaxRetry(count int) ProducerOption {
	return func(opt *ProducerOpt) {
		opt.MaxRetryCount = count
	}
}

type Producer[T any] interface {
	Produce(ctx context.Context, topic string, data T, ops ...ProducerOption) error
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

func (p *producer[T]) Produce(ctx context.Context, topic string, data T, opt ...ProducerOption) error {
	op := defaultProduceOpt()
	for _, o := range opt {
		o(op)
	}
	msg, err := p.codec.Encode(data)
	if err != nil {
		return err
	}
	if op.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, op.Timeout)
		defer cancel()
	}
	var lastErr error
	for i := range op.MaxRetryCount {
		lastErr = nil
		p.logger.Info(ctx, "produce message", "topic", topic, "data", data, "attempt", i, "msg", string(msg))
		if err := p.client.Produce(ctx, topic, msg); err != nil {
			lastErr = err
			p.logger.Error(ctx, "error producing message", "topic", topic, "data", data, "attempt", i, "error", err)
		}
	}
	return lastErr
}

func (p *producer[T]) Codec() codec.Codec[T] {
	return p.codec
}
