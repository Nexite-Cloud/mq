package client

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Kafka struct {
	once   sync.Once
	client *kgo.Client
	iter   *kgo.FetchesRecordIter
	data   chan *Record
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
				select {
				case <-ctx.Done():
					return
				default:
					fetches := k.client.PollFetches(ctx)
					if err := fetches.Err(); err != nil {
						continue
					}
					k.iter = fetches.RecordIter()
					for !k.iter.Done() {
						record := k.iter.Next()
						k.data <- &Record{
							Topic: record.Topic,
							Value: record.Value,
						}
					}
				}
			}
		}()
	}
}

func (k *Kafka) Next(ctx context.Context) (*Record, error) {
	k.once.Do(k.start(ctx))
	record := <-k.data
	return record, nil
}

func (k *Kafka) Chan(ctx context.Context) <-chan *Record {
	k.once.Do(k.start(ctx))
	return k.data
}

func NewKafka(client *kgo.Client) *Kafka {
	return &Kafka{
		client: client,
		data:   make(chan *Record),
	}
}
