package main

import (
	"context"
	"fmt"
	"github.com/Nexite-Cloud/mq/core/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Data struct {
	Number int
}

func main() {
	ctx := context.Background()
	topic := "abc"
	group := "test-group"
	// pub
	pubClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9094"),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		panic(err)
	}

	if err := pubClient.Ping(ctx); err != nil {
		panic(err)
	}
	pub := kafka.NewProducer(pubClient)
	sub := kafka.NewConsumer[Data](pubClient).WithHandler(func(data Data) error {
		fmt.Println("received data:", data)
		return nil
	}).WithTotalWorker(10)

	sub.Start()

	for i := 0; i < 1000; i++ {
		if err := pub.ProduceSync(ctx, topic, Data{i}); err != nil {
			fmt.Println(err)
		}
	}

	sub.Wait()

}
