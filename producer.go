package mq

import (
	"context"
	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type Producer interface {
	Produce(ctx context.Context, topic string, data any) error
	SetEncoder(encoder codec.Encoder)
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
		logger:  DefaultLogger{},
	}
}

func (p *producer) SetEncoder(encoder codec.Encoder) {
	p.encoder = encoder
}

func (p *producer) Produce(ctx context.Context, topic string, data any) error {
	msg, err := p.encoder(data)
	if err != nil {
		return err
	}
	p.logger.Info(ctx, "Produce to topic %s, data: %s", topic, msg)
	return p.client.Produce(ctx, topic, msg)
}
