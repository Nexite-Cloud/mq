package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Nexite-Cloud/mq"
	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
)

type Data struct {
	Number int
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	topic := fmt.Sprintf("rand-%v", rand.Int63())
	group := "test-group"
	// pub
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9094"),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		panic(err)
	}

	if err := kafkaClient.Ping(ctx); err != nil {
		panic(err)
	}
	pub := mq.NewProducer(client.NewKafka(kafkaClient))
	pub.SetEncoder(codec.JSONEncoder)
	pub.SetLogger(mq.NewSlogLogger(nil))

	wg := sync.WaitGroup{}
	con := mq.NewConsumer[Data](client.NewKafka(kafkaClient))
	con.SetTotalWorker(10)
	con.SetDecoder(codec.JSONDecoder[Data])
	con.SetLogger(mq.NewSlogLogger(logger))
	con.AddHandler(func(data Data) error {
		fmt.Println("received:", data.Number)
		if data.Number >= 0 {
			wg.Done()
			return nil
		}
		time.Sleep(3 * time.Second)
		return mq.ErrorRetry(errors.New("negative number"), 3)

	})
	if err := con.Start(ctx); err != nil {
		panic(err)
	}

	if err := pub.Produce(ctx, topic, Data{Number: -11}); err != nil {
		fmt.Println(err)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		if err := pub.Produce(ctx, topic, Data{i}); err != nil {
			fmt.Println(err)
		}
	}
	go func() {
		wg.Wait()
		con.Close()
	}()

	con.Wait()
}
