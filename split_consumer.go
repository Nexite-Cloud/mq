package mq

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type tp struct {
	topic     string
	partition int32
}
type splitConsumer struct {
	consumers     map[tp]*pConsumer
	handler       func(ctx context.Context, data kgo.Record)
	maxPollRecord int
}

type pConsumer struct {
	cl        *kgo.Client
	topic     string
	partition int32

	quit chan struct{}
	done chan struct{}
	rec  chan kgo.FetchTopicPartition
}

func (pc *pConsumer) consume(ctx context.Context, handler func(ctx context.Context, data kgo.Record)) {
	defer close(pc.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.quit:
			return
		case p := <-pc.rec:
			// do work
			handler(ctx, p.Records)
			pc.cl.MarkCommitRecords(p.Records...)
		}
	}
}

func (s *splitConsumer) Assigned(ctx context.Context, client *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &pConsumer{
				cl:        client,
				topic:     topic,
				partition: partition,
				quit:      make(chan struct{}),
				done:      make(chan struct{}),
				rec:       make(chan kgo.FetchTopicPartition),
			}
			s.consumers[tp{topic: topic, partition: partition}] = pc
			go pc.consume(ctx, s.handler)
		}

	}
}

func (s *splitConsumer) Revoked(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
	s.killConsumer(revoked)
	if err := client.CommitMarkedOffsets(ctx); err != nil {
		// handle commit error
	}
}

func (s *splitConsumer) Lost(ctx context.Context, client *kgo.Client, lost map[string][]int32) {
	s.killConsumer(lost)
}

func (s *splitConsumer) killConsumer(lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := tp{topic: topic, partition: partition}
			pc := s.consumers[tp]
			delete(s.consumers, tp)
			close(pc.quit)
			wg.Add(1)
			go func() {
				<-pc.done
				wg.Done()
			}()
		}
	}
}

func (s *splitConsumer) poll(ctx context.Context, client *kgo.Client) {
	for {
		fetches := client.PollRecords(ctx, s.maxPollRecord)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(s string, i int32, err error) {

		})

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			tp := tp{topic: p.Topic, partition: p.Partition}
			s.consumers[tp].rec <- p
		})
		client.AllowRebalance()
	}
}
