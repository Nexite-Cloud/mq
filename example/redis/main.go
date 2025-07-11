package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nexite-Cloud/mq"
	"github.com/Nexite-Cloud/mq/client"
)

type Data struct {
	Number int
}

func main() {
	ctx := context.Background()
	topic := "abc"
	retryTopic := fmt.Sprintf("retry-%s", topic)
	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	pro := mq.NewProducer(client.NewRedis(r))
	retryPub := mq.NewTypedProducer[mq.Retry[Data]](client.NewRedis(r))
	con := mq.NewConsumer[Data](client.NewRedis(r).ConsumeChannel(ctx, topic).ConsumeChannel(ctx, retryTopic))
	var wg sync.WaitGroup
	con.SetRetryProducer(retryPub, retryTopic)
	con.SetTotalWorker(10)

	con.AddHandler(func(data Data) error {
		defer wg.Done()
		fmt.Println("received:", data.Number)
		if data.Number >= 0 {
			return nil
		}
		time.Sleep(3 * time.Second)
		return mq.ErrorRetry(errors.New("negative number"), 3)

	})
	if err := con.Start(ctx); err != nil {
		panic(err)
	}
	wg.Add(14)
	if err := pro.Produce(ctx, topic, Data{Number: -11}); err != nil {
		fmt.Println(err)
	}
	for i := 0; i < 10; i++ {
		if err := pro.Produce(ctx, topic, Data{Number: i}); err != nil {
			fmt.Println(err)
		}
	}
	go func() {
		wg.Wait()
		con.Close()
	}()

	con.Wait()

}
