package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
)

type Partitioner string

const (
	UniformBytesPartitioner Partitioner = "uniform-bytes"
	LeastBackupPartitioner  Partitioner = "least-backup"
	ManualPartitioner       Partitioner = "manual"
	RoundRobinPartitioner   Partitioner = "round-robin"
	StickyKeyPartitioner    Partitioner = "sticky-key"
	StickyPartitioner       Partitioner = "sticky"
)

type Ack string

const (
	NoAck     Ack = "No"
	LeaderAck Ack = "Leader"
	AllAck    Ack = "All"
)

type Compression string

const (
	NoneCompression   Compression = "none"
	SnappyCompression Compression = "snappy"
	ZstdCompression   Compression = "zstd"
	GzipCompression   Compression = "gzip"
	Lz4Compression    Compression = "lz4"
)

type Balancer string

const (
	CooperativeStickyBalancer Balancer = "cooperative-sticky"
	StickyBalancer            Balancer = "sticky"
	RangeBalancer             Balancer = "range"
	RoundRobinBalancer        Balancer = "round-robin"
)

type OffsetReset string

const (
	StartOffset OffsetReset = "start"
	EndOffset   OffsetReset = "end"
)

type SASL struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type TLS struct {
	Enabled            bool   `mapstructure:"enabled"`
	MinVersion         string `mapstructure:"min_version"`
	MaxVersion         string `mapstructure:"max_version"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	CAFile             string `mapstructure:"ca_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

type Metrics struct {
	Namespace   string `mapstructure:"namespace"`
	Port        uint32 `mapstructure:"port"`
	EnabledHTTP bool   `mapstructure:"enabled_http"`
}

type Timeout struct {
	Dial               time.Duration `mapstructure:"dial"`
	ConnIdle           time.Duration `mapstructure:"idle"`
	RequestOverhead    time.Duration `mapstructure:"request_overhead"`
	Rebalance          time.Duration `mapstructure:"rebalance"`
	Retry              time.Duration `mapstructure:"retry"`
	Session            time.Duration `mapstructure:"session"`
	ProduceRequest     time.Duration `mapstructure:"produce_request"`
	RecordDelivery     time.Duration `mapstructure:"record_delivery"`
	TransactionTimeout time.Duration `mapstructure:"transaction_timeout"`
}

type Config struct {
	Brokers []string `mapstructure:"brokers" validate:"required,min=1"`
	Topics  []string `mapstructure:"topics"`
	SASL    SASL     `mapstructure:"sasl"`
	TLS     TLS      `mapstructure:"tls"`
	Metrics Metrics  `mapstructure:"metrics"`
	Timeout Timeout  `mapstructure:"timeout"`
}

type ProducerConfig struct {
	Config                `mapstructure:",squash" validate:"required"`
	ProducerPartitioner   Partitioner   `mapstructure:"producer_partitioner"`
	RequireAcks           Ack           `mapstructure:"require_acks"`
	Compression           []Compression `mapstructure:"compression"`
	RecordRetries         int           `mapstructure:"record_retries"`
	ProducerBatchMaxBytes int32         `mapstructure:"producer_batch_max_bytes"`
}

type ConsumerConfig struct {
	Config               `mapstructure:",squash" validate:"required"`
	ConsumerGroup        string        `mapstructure:"consumer_group" validate:"required"`
	DisableAutocommit    bool          `mapstructure:"disable_autocommit"`
	BlockRebalanceOnPoll bool          `mapstructure:"block_rebalance_on_poll"`
	FetchMinBytes        int32         `mapstructure:"fetch_min_bytes"`
	FetchMaxMB           int32         `mapstructure:"fetch_max_mb"`
	HeartbeatInterval    time.Duration `mapstructure:"heartbeat_interval"`
	Balancers            []Balancer    `mapstructure:"balancer"`
	OffsetReset          OffsetReset   `mapstructure:"offset_reset"`
}

func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Config: Config{
			Brokers: []string{"localhost:9092"},
			Metrics: Metrics{
				Namespace: "kafka",
				Port:      9090,
			},
			Timeout: Timeout{
				Dial:               30 * time.Second,
				ConnIdle:           300 * time.Second,
				RequestOverhead:    10 * time.Second,
				Retry:              60 * time.Second,
				Session:            45 * time.Second,
				ProduceRequest:     30 * time.Second,
				RecordDelivery:     120 * time.Second,
				TransactionTimeout: 60 * time.Second,
			},
		},
		ProducerPartitioner:   UniformBytesPartitioner,
		RequireAcks:           AllAck,
		Compression:           []Compression{SnappyCompression, NoneCompression},
		RecordRetries:         3,
		ProducerBatchMaxBytes: 1024 * 1024,
	}
}

func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Config: Config{
			Brokers: []string{"localhost:9092"},
			Metrics: Metrics{
				Namespace: "kafka",
				Port:      9090,
			},
			Timeout: Timeout{
				Dial:            30 * time.Second,
				ConnIdle:        300 * time.Second,
				RequestOverhead: 10 * time.Second,
				Rebalance:       60 * time.Second,
				Retry:           60 * time.Second,
				Session:         45 * time.Second,
			},
		},
		ConsumerGroup:        "default-group",
		DisableAutocommit:    false,
		BlockRebalanceOnPoll: false,
		FetchMinBytes:        1,
		FetchMaxMB:           50,
		HeartbeatInterval:    3 * time.Second,
		Balancers:            []Balancer{StickyBalancer},
		OffsetReset:          StartOffset,
	}
}

func (c Config) Validate() error {
	v := validator.New()
	if err := v.Struct(c); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	if len(c.Topics) == 0 {
		return errors.New("either topics must be specified")
	}

	if c.TLS.Enabled {
		if c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
			return errors.New("cert_file and key_file are required when TLS is enabled")
		}
	}

	if c.SASL.Username != "" && c.SASL.Password == "" {
		return errors.New("password is required when username is specified")
	}

	if c.SASL.Password != "" && c.SASL.Username == "" {
		return errors.New("username is required when password is specified")
	}

	return nil
}
