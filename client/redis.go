package client

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
	pubsub *redis.PubSub
	c      <-chan *redis.Message
}

func (r *Redis) Next(ctx context.Context) ([]byte, error) {
	msg := <-r.c
	return []byte(msg.Payload), nil
}

func (r *Redis) Close() error {
	return r.pubsub.Close()
}

func (r *Redis) Produce(ctx context.Context, topic string, data []byte) error {
	res := r.client.Publish(ctx, topic, data)
	return res.Err()
}
