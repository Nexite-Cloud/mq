package mq

import (
	"context"
	"fmt"
)

type Logger interface {
	Error(ctx context.Context, template string, args ...interface{})
	Info(ctx context.Context, template string, args ...interface{})
}

type DefaultLogger struct {
}

func (l DefaultLogger) Error(ctx context.Context, template string, args ...interface{}) {
	fmt.Println("ERROR: ", fmt.Sprintf(template, args...))
}

func (l DefaultLogger) Info(ctx context.Context, template string, args ...interface{}) {
	fmt.Println("INFO: ", fmt.Sprintf(template, args...))
}
