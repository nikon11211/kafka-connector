package kafka

import "github.com/twmb/franz-go/pkg/kgo"

type Logger interface {
	Level() kgo.LogLevel
	Log(level kgo.LogLevel, msg string, keyvals ...any)
}

type NoopLogger struct{}

func (n NoopLogger) Level() kgo.LogLevel {
	return kgo.LogLevelError
}

func (n NoopLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {}
