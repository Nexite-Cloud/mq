package mq

import (
	"context"

	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type Producer interface {
	Produce(ctx context.Context, topic string, data any) error
	SetEncoder(encoder codec.Encoder)
	SetLogger(logger Logger)
}
type producer struct {
	client  client.Producer
	encoder codec.Encoder
	logger  Logger
}

func NewProducer(client client.Producer) Producer {
	return &producer{
		client:  client,
		encoder: codec.JSONEncoder,
		logger:  NewSlogLogger(nil),
	}
}

func (p *producer) SetLogger(logger Logger) {
	p.logger = logger
}

func (p *producer) SetEncoder(encoder codec.Encoder) {
	p.encoder = encoder
}

func (p *producer) Produce(ctx context.Context, topic string, data any) error {
	msg, err := p.encoder(data)
	if err != nil {
		return err
	}
	p.logger.Info(ctx, "produce message", "topic", topic, "data", data, "msg", string(msg))
	return p.client.Produce(ctx, topic, msg)
}
