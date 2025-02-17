package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Decoder[T any] func(data []byte) (T, error)

func JSONDecoder[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

type Consumer[T any] struct {
	client      *kgo.Client
	totalWorker int
	close       chan struct{}
	decoder     Decoder[T]
	handler     func(T) error
	items       chan []byte
}

func NewConsumer[T any](client *kgo.Client) *Consumer[T] {
	return &Consumer[T]{
		client:      client,
		totalWorker: 1,
		close:       make(chan struct{}),
		decoder:     JSONDecoder[T],
		items:       make(chan []byte),
	}
}

func (s *Consumer[T]) WithDecoder(decoder Decoder[T]) *Consumer[T] {
	s.decoder = decoder
	return s
}

func (s *Consumer[T]) WithHandler(handler func(T) error) *Consumer[T] {
	s.handler = handler
	return s
}

func (s *Consumer[T]) WithTotalWorker(totalWorker int) *Consumer[T] {
	s.totalWorker = totalWorker
	return s
}

func (s *Consumer[_]) Wait() {
	<-s.close
}

func (s *Consumer[_]) Close() {
	s.close <- struct{}{}
}

func (s *Consumer[T]) Start() error {
	ctx := context.Background()
	go func() {
		fetch := s.client.PollFetches(ctx)
		if err := fetch.Err(); err != nil {
			fmt.Println("fetch error:", err)
			return
		}
		iter := fetch.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			s.items <- record.Value
		}
	}()

	for i := 0; i < s.totalWorker; i++ {
		workerID := i
		go func() {
			fmt.Println("worker", workerID, "started")
			for item := range s.items {
				fmt.Println("worker", workerID, ", received item:", string(item))
				data, err := s.decoder(item)
				if err != nil {
					continue
				}
				if err := s.handler(data); err != nil {
					continue
				}
			}
		}()
	}
	return nil
}
