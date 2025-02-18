package client

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
)

type Kafka struct {
	once   sync.Once
	client *kgo.Client
	iter   *kgo.FetchesRecordIter
}

func (k *Kafka) Produce(ctx context.Context, topic string, data []byte) error {
	record := &kgo.Record{
		Value:   data,
		Topic:   topic,
		Context: ctx,
	}
	res := k.client.ProduceSync(ctx, record)
	return res.FirstErr()
}

func (k *Kafka) Close() error {
	return nil
}

func (k *Kafka) Next(ctx context.Context) ([]byte, error) {
	var err error
	k.once.Do(func() {
		fetches := k.client.PollFetches(ctx)
		if err = fetches.Err(); err != nil {
			return
		}
		k.iter = fetches.RecordIter()
	})
	if err != nil {
		return nil, err
	}
	if k.iter.Done() {
		return nil, errors.New("no more message")
	}
	record := k.iter.Next()

	return record.Value, nil
}

func NewKafka(client *kgo.Client) *Kafka {
	return &Kafka{
		client: client,
	}
}
