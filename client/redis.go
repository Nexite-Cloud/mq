package client

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	mu     sync.Mutex
	client redis.UniversalClient
	sub    map[string]bool
	c      chan *Record
}

func NewRedis(client redis.UniversalClient) *Redis {
	return &Redis{
		client: client,
		c:      make(chan *Record),
		sub:    make(map[string]bool),
	}
}

func (r *Redis) ConsumeChannel(ctx context.Context, channel string) *Redis {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sub[channel] {
		return r
	}
	sub := r.client.Subscribe(ctx, channel)
	r.sub[channel] = true
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-sub.Channel():
				if !ok {
					return
				}
				r.c <- &Record{
					Topic: msg.Channel,
					Value: []byte(msg.Payload),
				}
			}
		}
	}()
	return r
}

func (r *Redis) Next(ctx context.Context) (*Record, error) {
	msg := <-r.c
	return msg, nil
}

func (r *Redis) Chan(ctx context.Context) <-chan *Record {
	return r.c
}

func (r *Redis) Close() error {
	return nil
}

func (r *Redis) Produce(ctx context.Context, topic string, data []byte) error {
	res := r.client.Publish(ctx, topic, data)
	return res.Err()
}
