package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
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
	topic := fmt.Sprintf("rand-%v", rand.Int63())
	retryTopic := fmt.Sprintf("retry-%s", topic)
	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	pro := mq.NewProducer(client.NewRedis(r))
	retryPub := mq.NewTypedProducer[mq.Retry[Data]](client.NewRedis(r))
	con := mq.NewConsumer[Data](client.NewRedis(r).ConsumeChannel(ctx, topic).ConsumeChannel(ctx, retryTopic))
	defer con.Close(ctx)
	con.SetRetryProducer(retryPub, retryTopic)
	con.SetTotalWorker(10)
	con.SetLogger(mq.NewSlogLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil))))

	con.SetHandler(func(ctx context.Context, data Data) error {
		slog.Info("process", "number", data.Number)
		if data.Number >= 0 {
			return nil
		}
		time.Sleep(2 * time.Second)
		return mq.ErrorRetry(errors.New("negative number"), 3)
	})
	con.OnError(func(ctx context.Context, data Data, err error) {
		slog.Error("error processing", "number", data.Number, "error", err)
	})
	if err := con.Start(ctx); err != nil {
		panic(err)
	}
	if err := pro.Produce(ctx, topic, Data{Number: -11}); err != nil {
		fmt.Println(err)
	}
	for i := 0; i < 10; i++ {
		if err := pro.Produce(ctx, topic, Data{Number: i}); err != nil {
			fmt.Println(err)
		}
	}
	con.Wait(context.Background())
}
