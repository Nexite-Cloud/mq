package mq

import (
	"context"
	"errors"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type Consumer[T any] interface {
	Start(ctx context.Context) error
	Wait()
	SetHandler(handler func(context.Context, T) error)
	SetCodec(codec codec.Codec[T])
	SetTotalWorker(num int)
	SetLogger(logger Logger)
	SetRetryProducer(producer Producer[Retry[T]], topic string)
	Close(ctx context.Context)
	OnClose(fn func(ctx context.Context))
	OnError(fn func(ctx context.Context, data T, err error))
	Resize(ctx context.Context, totalWorker int)
}

type consumer[T any] struct {
	active atomic.Bool
	// concurrent management
	totalWorker int
	// properties mutex
	pMu        sync.Mutex
	wg         sync.WaitGroup
	workerWg   sync.WaitGroup
	cancelFn   context.CancelFunc
	quit       chan struct{}
	recordChan <-chan *client.Record

	// handler
	client        client.Consumer
	handler       func(ctx context.Context, data T) error
	onFailHandler func(ctx context.Context, data T, err error)
	closeHooks    []func(ctx context.Context)
	retryClient   Producer[Retry[T]]
	retryTopic    string

	// misc
	logger   Logger
	codec    codec.Codec[T]
	lastIdx  int
	idxMu    sync.Mutex
	idxQueue []int
}

func NewConsumer[T any](consumerClient client.Consumer) Consumer[T] {
	c := &consumer[T]{
		active:      atomic.Bool{},
		totalWorker: 1,
		pMu:         sync.Mutex{},
		wg:          sync.WaitGroup{},
		quit:        make(chan struct{}),
		cancelFn:    nil,
		client:      consumerClient,
		handler:     nil,
		closeHooks:  nil,
		retryClient: nil,
		retryTopic:  "",
		logger:      nil,
		codec:       codec.JSON[T](),
	}
	return c
}

func (c *consumer[T]) OnError(fn func(ctx context.Context, data T, err error)) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	if fn == nil {
		return
	}
	c.onFailHandler = fn
}

func (c *consumer[T]) getWorkerIdx() int {
	c.idxMu.Lock()
	defer c.idxMu.Unlock()
	if len(c.idxQueue) != 0 {
		idx := c.idxQueue[0]
		c.idxQueue = c.idxQueue[1:]
		return idx
	}
	idx := c.lastIdx
	c.lastIdx++
	return idx
}

func (c *consumer[T]) Resize(ctx context.Context, totalWorker int) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	if totalWorker == c.totalWorker {
		return
	}
	// scale up
	if totalWorker > c.totalWorker {
		for i := 0; i < totalWorker-c.totalWorker; i++ {
			go c.startWorker(ctx)
		}
		c.totalWorker = totalWorker
		return
	}
	// scale down
	for range c.totalWorker - totalWorker {
		c.quit <- struct{}{} // signal worker to stop
	}
	c.totalWorker = totalWorker
}

// Wait blocks until all workers have finished processing messages.
func (c *consumer[T]) Wait() {
	c.wg.Wait()
	c.workerWg.Wait()
}

// SetHandler sets the handler function that will be called for each message.
func (c *consumer[T]) SetHandler(handler func(context.Context, T) error) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	c.handler = handler
}

// SetCodec sets the codec used for encoding and decoding messages. Default is JSON codec.
func (c *consumer[T]) SetCodec(codec codec.Codec[T]) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	c.codec = codec
}

// SetTotalWorker sets the number of workers that will process messages concurrently. Default is 1.
func (c *consumer[T]) SetTotalWorker(num int) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	c.totalWorker = num
}

// SetLogger sets the logger used for logging messages. Default is a no-op logger. Logger should implement the Logger interface.
func (c *consumer[T]) SetLogger(logger Logger) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	c.logger = logger

}

// SetRetryProducer sets the producer for retrying messages. If set, messages that fail processing will be retried using this producer.
func (c *consumer[T]) SetRetryProducer(producer Producer[Retry[T]], topic string) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	c.retryClient = producer
	c.retryTopic = topic

}

func (c *consumer[T]) Close(ctx context.Context) {
	c.info(ctx, "waiting for workers to finish")
	c.Wait()
	c.info(ctx, "trigger close hooks")
	for _, fn := range c.closeHooks {
		fn(ctx)
	}
	c.cancelFn()
	c.info(ctx, "consumer closed")
}

func (c *consumer[T]) OnClose(fn func(ctx context.Context)) {
	c.pMu.Lock()
	defer c.pMu.Unlock()
	if fn == nil {
		return
	}
	c.closeHooks = append(c.closeHooks, fn)
}

func (c *consumer[T]) startWorker(ctx context.Context) {
	idx := c.getWorkerIdx()
	c.info(ctx, "starting worker", "idx", idx)
	c.workerWg.Add(1)
	defer func() {
		c.info(ctx, "worker stopped", "worker_idx", idx)
		c.workerWg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			c.info(ctx, "context done, waiting for work done", "worker_idx", idx)
			c.wg.Wait()
			return
		case <-c.quit:
			c.idxMu.Lock()
			c.idxQueue = append(c.idxQueue, idx)
			c.idxMu.Unlock()
			return
		case record := <-c.recordChan:
			c.info(ctx, "received", "data", record)
			c.wg.Add(1)
			if record.Topic == c.retryTopic {
				retryItem, err := c.retryClient.Codec().Decode(record.Value)
				if err != nil {
					c.error(ctx, "worker decode retry error", "worker_idx", idx, "error", err, "raw", string(record.Value), "topic", record.Topic)
					c.wg.Done()
					continue
				}
				c.handleRetry(ctx, idx, retryItem)
				continue
			}
			data, err := c.codec.Decode(record.Value)
			if err != nil {
				c.error(ctx, "worker decode error", "worker_idx", idx, "error", err, "raw", string(record.Value), "topic", record.Topic)
				c.wg.Done()
				continue
			}
			c.handle(ctx, idx, data)
		}
	}
}

func (c *consumer[T]) Start(ctx context.Context) error {
	ctx, c.cancelFn = signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

	c.active.Store(true)
	// async -> consumer message
	c.recordChan = c.client.Chan(ctx)

	// async -> handle message
	for range c.totalWorker {
		go c.startWorker(ctx)
	}

	return nil
}

func (c *consumer[T]) handleRetry(ctx context.Context, workerIdx int, retryItem Retry[T]) {
	defer c.wg.Done()
	retryItem.RetryCount++
	data := retryItem.Data
	if retryItem.RetryCount > retryItem.MaxRetry {
		c.error(ctx, "retry max retry exceeded", "worker_id", workerIdx, "data", retryItem.Data, "max_retry", retryItem.MaxRetry)
		if c.onFailHandler != nil {
			c.onFailHandler(ctx, data, errors.New(retryItem.BaseError))
		}
		return
	}
	if err := c.handler(ctx, data); err != nil {
		c.error(ctx, "worker retry handle error", "worker_id", workerIdx, "error", err, "data", data, "retry_count", retryItem.RetryCount, "max_retry", retryItem.MaxRetry)
		// check drop error
		var errDrop *errTypeDrop
		if errors.As(err, &errDrop) {
			return
		}
		if retryItem.RetryCount > retryItem.MaxRetry {
			c.error(ctx, "retry max retry exceeded", "worker_id", workerIdx, "data", data, "max_retry", retryItem.MaxRetry)
			if c.onFailHandler != nil {
				c.onFailHandler(ctx, data, err)
			}
			return
		}
		// if retry able
		if c.retryClient != nil {
			nextRetryItem := Retry[T]{
				Data:       data,
				MaxRetry:   retryItem.MaxRetry,
				RetryCount: retryItem.RetryCount,
				BaseError:  retryItem.BaseError,
			}
			if err := c.retryClient.Produce(context.WithoutCancel(ctx), c.retryTopic, nextRetryItem); err != nil {
				c.error(ctx, "worker retry produce error", "worker_id", workerIdx, "error", err, "topic", c.retryTopic, "data", data)
			}
			return
		}
		if c.onFailHandler != nil {
			c.onFailHandler(ctx, data, err)
		}

	}
}

func (c *consumer[T]) handle(ctx context.Context, workerIdx int, data T) {
	defer c.wg.Done()
	if err := c.handler(ctx, data); err != nil {
		c.error(ctx, "worker handle error", "worker_id", workerIdx, "error", err, "data", data)
		// check drop error
		var errDrop *errTypeDrop
		if errors.As(err, &errDrop) {
			return
		}
		// check if retry
		var errRetry *errTypeRetry
		if ok := errors.As(err, &errRetry); ok {
			if errRetry.retryTime == 0 {
				// no retry, just continue
				return
			}
			if c.retryClient != nil {
				retryItem := Retry[T]{
					Data:       data,
					MaxRetry:   errRetry.retryTime,
					RetryCount: 1,
					BaseError:  err.Error(),
				}
				if err := c.retryClient.Produce(context.WithoutCancel(ctx), c.retryTopic, retryItem); err != nil {
					c.error(ctx, "worker retry produce error", "worker_id", workerIdx, "error", err, "topic", c.retryTopic, "data", data)
				}
				return
			}
			if c.onFailHandler != nil {
				c.onFailHandler(ctx, data, err)
			}
			return
		}
		if c.onFailHandler != nil {
			c.onFailHandler(ctx, data, err)
		}
	}
}

func (c *consumer[T]) info(ctx context.Context, msg string, args ...any) {
	if c.logger == nil {
		return
	}
	c.logger.Info(ctx, msg, args...)
}

func (c *consumer[T]) error(ctx context.Context, msg string, args ...any) {
	if c.logger == nil {
		return
	}
	c.logger.Error(ctx, msg, args...)
}
