package producer

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	MessagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_producer_messages_sent_total",
			Help: "Total number of messages sent by the Kafka producer.",
		},
		[]string{"topic"},
	)

	ErrorsOccurred = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_producer_errors_total",
			Help: "Total number of errors occurred while sending messages.",
		},
		[]string{"topic"},
	)

	RecordQueueTimeAvg = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_record_queue_time_avg",
			Help: "Average time messages spend in the queue before being sent.",
		},
		[]string{"topic"},
	)

	RequestRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_producer_request_rate",
			Help: "Number of requests made by the producer per second.",
		},
		nil,
	)

	RequestLatencyAvg = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_request_latency_avg",
			Help: "Average latency for requests sent to Kafka brokers.",
		},
		nil,
	)

	BatchSizeAvg = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_batch_size_avg",
			Help: "Average size of batches sent to Kafka.",
		},
		nil,
	)

	RecordSizeAvg = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_record_size_avg",
			Help: "Average size of records in batches.",
		},
		nil,
	)

	BufferAvailableBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_buffer_available_bytes",
			Help: "Available buffer memory for message buffering.",
		},
		nil,
	)

	RequestsInFlight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_requests_in_flight",
			Help: "Number of requests currently in flight.",
		},
		nil,
	)

	RequestSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_request_size_bytes",
			Help: "Size of the request sent to Kafka brokers.",
		},
		nil,
	)

	CompressionRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_compression_rate",
			Help: "Compression rate of messages sent by the producer.",
		},
		nil,
	)

	BufferMemory = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_buffer_memory_bytes",
			Help: "Total buffer memory allocated for the producer.",
		},
		nil,
	)

	MaxInFlightRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_producer_max_in_flight_requests",
			Help: "Maximum number of in-flight requests allowed.",
		},
		nil,
	)
)

func registerMetrics(registerer prometheus.Registerer) {
	for _, collector := range []prometheus.Collector{
		MessagesSent,
		ErrorsOccurred,
		RecordQueueTimeAvg,
		RequestRate,
		RequestLatencyAvg,
		BatchSizeAvg,
		RecordSizeAvg,
		BufferAvailableBytes,
		RequestsInFlight,
		RequestSize,
		CompressionRate,
		BufferMemory,
		MaxInFlightRequests,
	} {
		if err := registerer.Register(collector); err != nil {
			var already prometheus.AlreadyRegisteredError
			if !errors.As(err, &already) {
				panic(fmt.Sprintf("failed to register producer metric: %v", err))
			}
		}
	}
}
