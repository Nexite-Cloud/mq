package main

import (
	"context"
	"fmt"
	"github.com/Nexite-Cloud/mq"
	"github.com/Nexite-Cloud/mq/client"
	"github.com/redis/go-redis/v9"
)

type Data struct {
	String string
	Number int
}

func main() {
	ctx := context.Background()
	topic := "abc"
	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	pro := mq.NewProducer(client.NewRedis(r))
	con := mq.NewConsumer[Data](client.NewRedis(r).ConsumeChannel(ctx, topic))
	con.SetTotalWorker(10)
	con.AddHandler(func(data Data) error {
		fmt.Println(data)
		return nil
	})

	con.Start(ctx)

	for i := 0; i < 1000; i++ {
		if err := pro.Produce(ctx, topic, &Data{
			String: fmt.Sprint("Hello ", i),
			Number: i,
		}); err != nil {
			panic(err)
		}
	}

	con.Wait()

}
