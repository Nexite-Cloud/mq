# MQ
Message queue library


## Usage
### Producer
```go
p := mq.NewProducer({{kafka/redis client}})

if err := p.Produce(ctx, "topic", data); err != nil {
    panic(err)
}
```
- To set encoder, you can use `p.SetEncoder(encoder)` before calling `Produce`. The encoder should implement the `codec.Encoder` interface. The default encoder is `codec.JSONEncoder`.
- To set logger, you can use `p.SetLogger(logger)` before calling `Produce`. The logger should implement the `mq.Logger` interface. The default logger is `mq.NewSlogLogger(slog.Default())`.
### Consumer
```go
c := mq.NewConsumer[T]({{kafka/redis client}})
defer c.Close(ctx)
c.SetHandler(func(ctx, context.Context,data T) error {
    // handle data
    return nil
})
if err := c.Start(ctx); err != nil {
    panic(err)
}
```
- To set decoder, you can use `c.SetDecoder(decoder)` before calling `Start`. The decoder should implement the `codec.Decoder` interface. The default decoder is `codec.JSONDecoder`.
- To set total number of workers, you can use `c.SetTotalWorkers(n)` before calling `Start`. The default number of workers is 1.
- To set logger, you can use `c.SetLogger(logger)` before calling `Start`. The logger should implement the `mq.Logger` interface. The default logger is `mq.NewSlogLogger(slog.Default())`.
- Default behavior is to consume all messages in the topic. If error occurs, the message will be skipped, alternative you can return `mq.ErrorDrop(err)`.
- To retry the message, return `mq.ErrorRetry(err, retryCount)`, where `retryCount` is the number of times to retry the message (exclude first time the message was read). 

## TODO
- [ ] Add more methods to retry/drop messages
- [ ] Support more clients (e.g. RabbitMQ, Cloudflare)
- [x] Graceful shutdown (message processing should be done before closing the consumer)