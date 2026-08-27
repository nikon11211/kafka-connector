package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/nikon11211/kafka-connector"
	"github.com/nikon11211/kafka-connector/producer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := kafka.DefaultProducerConfig()

	reg := prometheus.NewRegistry()
	logger := kafka.NoopLogger{}

	p, err := producer.New(cfg, logger, reg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
		return
	}
	defer p.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	record := &kgo.Record{
		Topic: "example-topic",
		Value: []byte("Hello, Kafka!"),
	}

	if err := p.ProduceSync(ctx, record).FirstErr(); err != nil {
		log.Printf("Failed to produce: %v", err)
		return
	}

	log.Println("Message sent successfully")
}
