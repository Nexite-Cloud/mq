package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"

	"github.com/Nexite-Cloud/mq"
	"github.com/Nexite-Cloud/mq/client"
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
	var wg sync.WaitGroup
	con.SetTotalWorker(10)
	con.AddHandler(func(data Data) error {
		wg.Done()
		time.Sleep(time.Second * 5)
		fmt.Println(data)
		return nil
	})

	con.Start(ctx)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		if err := pro.Produce(ctx, topic, &Data{
			String: fmt.Sprint("Hello ", i),
			Number: i,
		}); err != nil {
			panic(err)
		}
	}

	go func() {
		wg.Wait()
		fmt.Println("all done")
		con.Close()
	}()

	con.Wait()

}
