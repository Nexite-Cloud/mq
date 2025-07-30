package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Nexite-Cloud/mq"
	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type Data struct {
	Number int `json:"number"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx := context.Background()
	topic := fmt.Sprintf("rand-%v", rand.Int63())
	retryTopic := fmt.Sprintf("retry-%s", topic)
	group := "test-group"
	// pub
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9094"),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic, retryTopic),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		panic(err)
	}

	if err := kafkaClient.Ping(ctx); err != nil {
		panic(err)
	}
	pub := mq.NewProducer(client.NewKafka(kafkaClient))
	pub.SetLogger(mq.NewSlogLogger(logger))

	retryPub := mq.NewTypedProducer[mq.Retry[Data]](client.NewKafka(kafkaClient))
	retryPub.SetLogger(mq.NewSlogLogger(logger))

	mqClient := client.NewKafka(kafkaClient)
	con := mq.NewConsumer[Data](mqClient)
	defer con.Close(ctx)

	con.OnClose(func(ctx context.Context) {
		slog.Info("close kafka client")
		if err := mqClient.Close(); err != nil {
			slog.Error("error closing kafka client", "error", err)
		}
	})
	con.SetTotalWorker(10)
	con.SetCodec(codec.JSON[Data]())
	con.SetLogger(mq.NewSlogLogger(logger))
	con.SetRetryProducer(retryPub, retryTopic)
	con.SetHandler(func(ctx context.Context, data Data) error {
		slog.Info("received", "data", data.Number)
		if data.Number >= 0 {
			return nil
		}
		time.Sleep(2 * time.Second)
		return mq.ErrorRetry(errors.New("negative number"), 3)

	})
	con.OnError(func(ctx context.Context, data Data, err error) {
		slog.Error("error processing data", "data", data.Number, "error", err)
	})
	if err := con.Start(ctx); err != nil {
		panic(err)
	}
	if err := pub.Produce(ctx, topic, Data{Number: -11}); err != nil {
		slog.Error(err.Error())
	}
	for i := 0; i < 10; i++ {
		if err := pub.Produce(ctx, topic, Data{i}); err != nil {
			slog.Error(err.Error())
		}
	}
	slog.Info("produce done, waiting for consumer to process")
	con.Wait(context.Background())
}
