package mq

import (
	"context"
	"log/slog"
)

type Logger interface {
	Error(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
}

type slogLogger struct {
	logger *slog.Logger
}

func NewSlogLogger(logger *slog.Logger) Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return slogLogger{logger: logger}
}

func (l slogLogger) Error(ctx context.Context, msg string, kv ...any) {
	l.logger.ErrorContext(ctx, msg, kv...)
}

func (l slogLogger) Info(ctx context.Context, msg string, kv ...any) {
	l.logger.InfoContext(ctx, msg, kv...)
}
