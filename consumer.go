package mq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

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
	AddContextHandler(handler ...func(context.Context, T) error)
	SetCodec(codec codec.Codec[T])
	SetTotalWorker(num int)
	SetLogger(logger Logger)
	SetRetryProducer(producer Producer[Retry[T]], topic string)
	Close() error
}

type consumerItem struct {
	Topic string
	Data  []byte
}

type consumer[T any] struct {
	// concurrent management
	numWorker int
	wg        sync.WaitGroup
	items     chan *consumerItem
	retry     chan Retry[T]
	close     chan struct{}
	running   atomic.Bool

	// client
	retryTopic    string
	client        client.Consumer
	retryProducer Producer[Retry[T]]

	//handler
	handler []func(context.Context, T) error

	// misc
	codec  codec.Codec[T]
	logger Logger
}

type ConsumerOption[T any] func(*consumer[T])

func NewConsumer[T any](client client.Consumer) Consumer[T] {
	c := &consumer[T]{
		numWorker: 1,
		wg:        sync.WaitGroup{},
		items:     make(chan *consumerItem),
		retry:     make(chan Retry[T], retryQueueSize),
		close:     make(chan struct{}),
		running:   atomic.Bool{},

		client: client,

		logger: NewSlogLogger(nil),
		codec:  codec.JSON[T](),
	}
	return c
}

func (c *consumer[T]) SetLogger(logger Logger) {
	c.logger = logger
}

func (c *consumer[T]) SetCodec(cd codec.Codec[T]) {
	c.codec = cd
}

func (c *consumer[T]) SetTotalWorker(num int) {
	c.numWorker = num
}

func (c *consumer[T]) SetRetryProducer(producer Producer[Retry[T]], topic string) {
	close(c.retry)
	producer.SetLogger(c.logger)
	c.retryProducer = producer
	c.retryTopic = topic
}

func (c *consumer[T]) handleItem(ctx context.Context, workerID int, data T) {
	defer c.wg.Done()
	for _, h := range c.handler {
		if err := h(ctx, data); err != nil {
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
				retryItem := Retry[T]{
					Data:       data,
					MaxRetry:   errRetry.retryTime,
					RetryCount: 1,
				}
				if c.retryProducer == nil {
					c.wg.Add(1)
					c.retry <- retryItem
				} else {
					if err := c.retryProducer.Produce(ctx, c.retryTopic, retryItem); err != nil {
						c.logger.Error(ctx, "worker retry produce error", "worker_id", workerID, "error", err, "topic", c.retryTopic, "data", data)
					}
				}
			}
			continue
		}
	}
}

func (c *consumer[T]) handleRetry(ctx context.Context, workerID int, retryItem Retry[T]) {
	defer c.wg.Done()
	retryItem.RetryCount++
	data := retryItem.Data
	for _, h := range c.handler {
		if err := h(ctx, data); err != nil {
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
			nextRetryItem := Retry[T]{
				Data:       data,
				MaxRetry:   retryItem.MaxRetry,
				RetryCount: retryItem.RetryCount,
			}
			if c.retryProducer == nil {
				c.wg.Add(1)
				c.retry <- nextRetryItem
			} else {
				if err := c.retryProducer.Produce(ctx, c.retryTopic, nextRetryItem); err != nil {
					c.logger.Error(ctx, "worker retry produce error", "worker_id", workerID, "error", err, "topic", c.retryTopic, "data", data)
				}
			}
		}
	}
}

// startConsume starts consuming messages from the client and sends them to the items channel
func (c *consumer[T]) startConsume(ctx context.Context) {
	for {
		// if worker stopped, stop consuming
		if !c.running.Load() {
			return
		}
		topic, item, err := c.client.Next(ctx)
		c.logger.Info(ctx, "consume message", "topic", topic, "item", string(item))
		if err != nil {
			continue
		}
		c.wg.Add(1)
		c.items <- &consumerItem{
			Topic: topic,
			Data:  item,
		}
	}
}

func (c *consumer[T]) Start(ctx context.Context) error {
	// start consuming process
	go c.startConsume(ctx)

	// init worker pool
	for i := 0; i < c.numWorker; i++ {
		workerID := i
		go func() {
			c.logger.Info(ctx, "worker started", "worker_id", workerID)
			for {
				select {
				case item := <-c.items:
					topic := item.Topic
					rawData := item.Data
					if topic == c.retryTopic {
						data, err := c.retryProducer.Codec().Decode(rawData)
						if err != nil {
							c.logger.Error(ctx, "worker decode error", "worker_id", workerID, "error", err, "raw", string(rawData), "topic", topic)
							continue
						}
						c.logger.Info(ctx, "worker retry", "worker_id", workerID, "data", data.Data, "max_retry", data.MaxRetry, "retry_count", data.RetryCount, "topic", topic)
						c.handleRetry(ctx, workerID, data)
						continue
					}
					data, err := c.codec.Decode(rawData)
					if err != nil {
						c.logger.Error(ctx, "worker decode error", "worker_id", workerID, "error", err, "raw", string(rawData), "topic", topic)
						continue
					}
					c.logger.Info(ctx, "worker received", "worker_id", workerID, "data", data, "topic", topic)
					c.handleItem(ctx, workerID, data)
				case retryItem, ok := <-c.retry:
					if !ok {
						continue
					}
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
	ctxHandler := make([]func(context.Context, T) error, 0, len(handler))
	for _, h := range handler {
		if h == nil {
			continue
		}
		hCopy := h
		ctxHandler = append(ctxHandler, func(ctx context.Context, data T) error {
			return hCopy(data)
		})
	}
	c.AddContextHandler(ctxHandler...)
}

func (c *consumer[T]) AddContextHandler(handler ...func(context.Context, T) error) {
	c.handler = append(c.handler, handler...)
}
