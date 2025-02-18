package client

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
	sub    *redis.PubSub
	c      <-chan *redis.Message
}

func NewRedis(client *redis.Client) *Redis {
	return &Redis{
		client: client,
		c:      nil,
	}
}

func (r *Redis) ConsumeChannel(ctx context.Context, channel string) *Redis {
	r.sub = r.client.Subscribe(ctx, channel)
	r.c = r.sub.Channel()
	return r
}

func (r *Redis) Next(ctx context.Context) ([]byte, error) {
	msg := <-r.c
	return []byte(msg.Payload), nil
}

func (r *Redis) Close() error {
	if r.sub != nil {
		r.sub.Close()
	}
	return nil
}

func (r *Redis) Produce(ctx context.Context, topic string, data []byte) error {
	res := r.client.Publish(ctx, topic, data)
	return res.Err()
}
