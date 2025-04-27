package mq

import "fmt"

type retry[T any] struct {
	Data       T
	MaxRetry   int
	RetryCount int
}

type errTypeRetry struct {
	base      error
	retryTime int
}

func (e *errTypeRetry) Error() string {
	return fmt.Sprintf("retry, base error:\"%v\", max retry: %v", e.base, e.retryTime)
}

func ErrorRetry(err error, retryLeft int) error {
	return &errTypeRetry{
		base:      err,
		retryTime: retryLeft,
	}
}

type errTypeDrop struct {
	base error
}

func (e *errTypeDrop) Error() string {
	return fmt.Sprintf("drop, base error: %v", e.base)
}

func ErrorDrop(err error) error {
	return &errTypeDrop{err}
}
