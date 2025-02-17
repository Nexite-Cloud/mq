package kafka

import (
	"context"
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Encoder func(data any) ([]byte, error)

type Callback func(r *kgo.Record, err error)

var JSONEncoder Encoder = func(data any) ([]byte, error) {
	return json.Marshal(data)
}

var defaultCallback Callback = func(r *kgo.Record, err error) {
}

type Producer struct {
	client        *kgo.Client
	encoder       Encoder
	asyncCallback Callback
}

func NewProducer(client *kgo.Client) *Producer {
	return &Producer{
		client:        client,
		encoder:       JSONEncoder,
		asyncCallback: defaultCallback,
	}
}

func (p *Producer) WithEncoder(encoder Encoder) *Producer {
	p.encoder = encoder
	return p
}

func (p *Producer) WithAsyncCallback(callback Callback) *Producer {
	p.asyncCallback = callback
	return p
}

func (p *Producer) Produce(ctx context.Context, topic string, data any) error {
	msg, err := p.encoder(data)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Value: msg,
		Topic: topic,
	}
	p.client.Produce(ctx, record, p.asyncCallback)
	return nil
}

func (p *Producer) ProduceSync(ctx context.Context, topic string, data any) error {
	msg, err := p.encoder(data)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Value: msg,
		Topic: topic,
	}
	result := p.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return err
	}
	return nil
}
