package consumer

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	MessagesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_messages_consumed_total",
			Help: "Total number of messages consumed by the Kafka consumer.",
		},
		[]string{"topic", "partition"},
	)

	FetchRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_fetch_rate",
			Help: "Number of fetch requests made by the consumer per second.",
		},
		nil,
	)

	FetchLatencyAvg = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_fetch_latency_avg",
			Help: "Average latency for fetch requests to Kafka brokers.",
		},
		nil,
	)

	OffsetLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_offset_lag",
			Help: "Current offset lag for the consumer group.",
		},
		[]string{"topic", "partition"},
	)

	MessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_messages_processed_total",
			Help: "Total number of messages processed by the consumer.",
		},
		[]string{"topic", "partition"},
	)

	ErrorsOccurred = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_consumer_errors_total",
			Help: "Total number of errors occurred while consuming messages.",
		},
		[]string{"topic"},
	)

	BatchSizeAvg = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_batch_size_avg",
			Help: "Average size of batches received from Kafka.",
		},
		nil,
	)
)

func registerMetrics(registerer prometheus.Registerer) {
	for _, collector := range []prometheus.Collector{
		MessagesConsumed,
		FetchRate,
		FetchLatencyAvg,
		OffsetLag,
		MessagesProcessed,
		ErrorsOccurred,
		BatchSizeAvg,
	} {
		if err := registerer.Register(collector); err != nil {
			var already prometheus.AlreadyRegisteredError
			if !errors.As(err, &already) {
				panic(fmt.Sprintf("failed to register consumer metric: %v", err))
			}
		}
	}
}
