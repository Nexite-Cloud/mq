package mq

import (
	"context"
	"errors"
	"sync"

	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

var mu sync.Mutex
var retryQueueSize int = 1000

func SetRetryQueueSize(size int) {
	mu.Lock()
	defer mu.Unlock()
	retryQueueSize = size
}

type Consumer[T any] interface {
	Start(ctx context.Context) error
	Wait()
	AddHandler(handler ...func(T) error)
	SetDecoder(decoder codec.Decoder[T])
	SetTotalWorker(num int)
	SetLogger(logger Logger)
	Close() error
}

type consumer[T any] struct {
	wg        sync.WaitGroup
	client    client.Consumer
	decoder   codec.Decoder[T]
	handler   []func(T) error
	close     chan struct{}
	items     chan []byte
	numWorker int
	logger    Logger
	retry     chan *retry[T]
}

type ConsumerOption[T any] func(*consumer[T])

func NewConsumer[T any](client client.Consumer) Consumer[T] {
	c := &consumer[T]{
		wg:        sync.WaitGroup{},
		client:    client,
		decoder:   codec.JSONDecoder[T],
		close:     make(chan struct{}),
		items:     make(chan []byte),
		numWorker: 1,
		logger:    NewSlogLogger(nil),
		retry:     make(chan *retry[T], retryQueueSize),
	}
	return c
}

func (c *consumer[T]) SetLogger(logger Logger) {
	c.logger = logger
}

func (c *consumer[T]) SetDecoder(decoder codec.Decoder[T]) {
	c.decoder = decoder
}

func (c *consumer[T]) SetTotalWorker(num int) {
	c.numWorker = num
}

func (c *consumer[T]) handleItem(ctx context.Context, workerID int, data T) {
	defer c.wg.Done()
	for _, h := range c.handler {
		if err := h(data); err != nil {
			c.logger.Error(ctx, "worker handle error", "worker_id", workerID, "error", err, "data", data)
			// check if drop
			var errDrop *errTypeDrop
			if errors.As(err, &errDrop) {
				break
			}
			// check if error is retryable
			var errRetry *errTypeRetry
			if ok := errors.As(err, &errRetry); ok {
				if errRetry.retryTime == 0 {
					continue
				}
				c.wg.Add(1)
				c.retry <- &retry[T]{
					Data:       data,
					MaxRetry:   errRetry.retryTime,
					RetryCount: 1,
				}
			}
			continue
		}
	}
}

func (c *consumer[T]) handleRetry(ctx context.Context, workerID int, retryItem *retry[T]) {
	defer c.wg.Done()
	retryItem.RetryCount++
	data := retryItem.Data
	for _, h := range c.handler {
		if err := h(data); err != nil {
			c.logger.Error(ctx, "worker handle retry error", "worker_id", workerID, "error", err, "data", data)
			// check if drop
			var errDrop *errTypeDrop
			if errors.As(err, &errDrop) {
				break
			}
			if retryItem.RetryCount > retryItem.MaxRetry {
				c.logger.Error(ctx, "retry max reached", "worker_id", workerID, "data", retryItem.Data, "max_retry", retryItem.MaxRetry)
				continue
			}
			c.wg.Add(1)
			c.retry <- &retry[T]{
				Data:       data,
				MaxRetry:   retryItem.MaxRetry,
				RetryCount: retryItem.RetryCount,
			}
		}
	}
}

func (c *consumer[T]) Start(ctx context.Context) error {
	go func() {
		for {
			item, err := c.client.Next(ctx)
			if err != nil {
				continue
			}
			c.wg.Add(1)
			c.items <- item
		}
	}()
	for i := 0; i < c.numWorker; i++ {
		workerID := i
		go func() {
			c.logger.Info(ctx, "worker started", "worker_id", workerID)
			for {
				select {
				case item := <-c.items:
					data, err := c.decoder(item)
					if err != nil {
						c.logger.Error(ctx, "worker decode error", "worker_id", workerID, "error", err, "raw", string(item))
						continue
					}
					c.logger.Info(ctx, "worker received", "worker_id", workerID, "data", data)
					c.handleItem(ctx, workerID, data)
				case retryItem := <-c.retry:
					c.logger.Info(ctx, "worker retry", "worker_id", workerID, "data", retryItem.Data, "max_retry", retryItem.MaxRetry, "retry_count", retryItem.RetryCount)
					c.handleRetry(ctx, workerID, retryItem)
				}
			}

		}()
	}
	return nil
}

func (c *consumer[T]) Wait() {
	c.wg.Wait()
	<-c.close
}

func (c *consumer[T]) Close() error {
	c.logger.Info(context.Background(), "waiting for workers to finish...")
	c.wg.Wait()
	c.close <- struct{}{}
	c.logger.Info(context.Background(), "closing consumer...")
	return c.client.Close()
}

func (c *consumer[T]) AddHandler(handler ...func(T) error) {
	c.handler = append(c.handler, handler...)
}
