package client

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
)

type Kafka struct {
	once   sync.Once
	client *kgo.Client
	iter   *kgo.FetchesRecordIter
	data   chan *kgo.Record
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

func (k *Kafka) start(ctx context.Context) func() {
	return func() {
		go func() {
			for {
				fetches := k.client.PollFetches(ctx)
				if err := fetches.Err(); err != nil {
					panic(err)
				}
				k.iter = fetches.RecordIter()
				for !k.iter.Done() {
					record := k.iter.Next()
					k.data <- record
				}
			}
		}()
	}
}

func (k *Kafka) Next(ctx context.Context) (string, []byte, error) {
	k.once.Do(k.start(ctx))
	record := <-k.data
	return record.Topic, record.Value, nil
}

func NewKafka(client *kgo.Client) *Kafka {
	return &Kafka{
		client: client,
		data:   make(chan *kgo.Record),
	}
}
