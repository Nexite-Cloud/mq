package main

import (
	"context"
	"fmt"
	"github.com/Nexite-Cloud/mq"
	"github.com/Nexite-Cloud/mq/client"
	"github.com/Nexite-Cloud/mq/codec"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
)

type Data struct {
	Number int
}

func main() {
	ctx := context.Background()
	topic := "tp"
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
	pub.SetEncoder(codec.GOBEncoder)

	wg := sync.WaitGroup{}
	con := mq.NewConsumer[Data](client.NewKafka(kafkaClient))
	con.SetTotalWorker(10)
	con.SetDecoder(codec.GOBDecoder[Data])
	con.AddHandler(func(data Data) error {
		defer wg.Done()
		fmt.Println("received data:", data)
		return nil
	})
	if err := con.Start(ctx); err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		if err := pub.Produce(ctx, topic, Data{i}); err != nil {
			fmt.Println(err)
		}
	}
	wg.Wait()
	con.Close()
}
