package mq

type Retry[T any] struct {
	Data       T
	MaxRetry   int
	RetryCount int
}
