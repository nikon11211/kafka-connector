package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/nikon11211/kafka-connector"
	"github.com/nikon11211/kafka-connector/consumer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := kafka.DefaultConsumerConfig()

	reg := prometheus.NewRegistry()
	logger := kafka.NoopLogger{}
	c, err := consumer.New(cfg, logger, reg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
		return
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := c.PollFetches(ctx)
			fetches.EachRecord(func(record *kgo.Record) {
				log.Printf("Received: %s", record.Value)
			})
		}
	}
}
