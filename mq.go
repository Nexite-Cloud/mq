package mq

import "context"

type Producer interface {
	Produce(ctx context.Context, topic string, data any) error
	ProduceSync(ctx context.Context, topic string, data any) error
}

type Subscriber interface {
}
