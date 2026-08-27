package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNoopLogger(t *testing.T) {
	logger := NoopLogger{}

	assert.Equal(t, kgo.LogLevelError, logger.Level())

	assert.NotPanics(t, func() {
		logger.Log(kgo.LogLevelInfo, "test message")
		logger.Log(kgo.LogLevelError, "error message", "key", "value")
		logger.Log(kgo.LogLevelDebug, "debug message", "key1", "val1", "key2", "val2")
	})
}

func TestNoopLoggerLevel(t *testing.T) {
	logger := NoopLogger{}
	level := logger.Level()
	assert.Equal(t, kgo.LogLevelError, level)
}

type customLogger struct {
	logs []string
}

func (c *customLogger) Level() kgo.LogLevel {
	return kgo.LogLevelInfo
}

func (c *customLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	c.logs = append(c.logs, msg)
}

func TestCustomLogger(t *testing.T) {
	logger := &customLogger{}

	logger.Log(kgo.LogLevelInfo, "test message")
	logger.Log(kgo.LogLevelError, "error message")

	assert.Len(t, logger.logs, 2)
	assert.Equal(t, "test message", logger.logs[0])
	assert.Equal(t, "error message", logger.logs[1])
}
