package client

import (
	"context"
	"github.com/redis/go-redis/v9"
	"sync"
)

type Redis struct {
	mu     sync.RWMutex
	client *redis.Client
	sub    map[string]*redis.PubSub
	close  map[string]chan struct{}
	c      chan *redis.Message
}

func NewRedis(client *redis.Client) *Redis {
	return &Redis{
		client: client,
		c:      make(chan *redis.Message),
		sub:    make(map[string]*redis.PubSub),
		close:  make(map[string]chan struct{}),
	}
}

func (r *Redis) ConsumeChannel(ctx context.Context, channel string) *Redis {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sub[channel] != nil {
		return r
	}
	sub := r.client.Subscribe(ctx, channel)
	r.sub[channel] = sub
	r.close[channel] = make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-r.close[channel]:
				return
			case msg, ok := <-sub.Channel():
				if !ok {
					return
				}
				r.c <- msg
			}
		}
	}()
	return r
}

func (r *Redis) Next(ctx context.Context) (string, []byte, error) {
	msg := <-r.c
	return msg.Channel, []byte(msg.Payload), nil
}

func (r *Redis) Close() error {
	for k, v := range r.sub {
		v.Close()
		r.close[k] <- struct{}{}
	}
	return nil
}

func (r *Redis) Produce(ctx context.Context, topic string, data []byte) error {
	res := r.client.Publish(ctx, topic, data)
	return res.Err()
}
