package mq

import (
	"context"
	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type Consumer[T any] interface {
	Start(ctx context.Context) error
	Wait()
	AddHandler(handler ...func(T) error)
	SetDecoder(decoder codec.Decoder[T])
	SetTotalWorker(num int)
}

type consumer[T any] struct {
	client    client.Consumer
	decoder   codec.Decoder[T]
	handler   []func(T) error
	close     chan struct{}
	items     chan []byte
	numWorker int
}

func NewConsumer[T any](client client.Consumer) Consumer[T] {
	return &consumer[T]{
		client:    client,
		decoder:   codec.JSONDecoder[T],
		close:     make(chan struct{}),
		items:     make(chan []byte),
		numWorker: 1,
	}
}

func (c *consumer[T]) SetDecoder(decoder codec.Decoder[T]) {
	c.decoder = decoder
}

func (c *consumer[T]) SetTotalWorker(num int) {
	c.numWorker = num
}

func (c *consumer[T]) Start(ctx context.Context) error {

	go func() {
		for {
			item, err := c.client.Next(ctx)
			if err != nil {
				continue
			}
			c.items <- item
		}
	}()

	for i := 0; i < c.numWorker; i++ {
		go func() {
			for item := range c.items {
				data, err := c.decoder(item)
				if err != nil {
					continue
				}
				for _, h := range c.handler {
					if err := h(data); err != nil {
						continue
					}
				}
			}
		}()
	}

	return nil
}

func (c *consumer[T]) Wait() {
	<-c.close
}

func (c *consumer[T]) AddHandler(handler ...func(T) error) {
	c.handler = append(c.handler, handler...)
}
