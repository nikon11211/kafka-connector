package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultProducerConfig(t *testing.T) {
	cfg := DefaultProducerConfig()

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, UniformBytesPartitioner, cfg.ProducerPartitioner)
	assert.Equal(t, AllAck, cfg.RequireAcks)
	assert.Equal(t, []Compression{SnappyCompression, NoneCompression}, cfg.Compression)
	assert.Equal(t, 3, cfg.RecordRetries)
	assert.Equal(t, int32(1024*1024), cfg.ProducerBatchMaxBytes)
	assert.Equal(t, "kafka", cfg.Metrics.Namespace)
	assert.Equal(t, uint32(9090), cfg.Metrics.Port)
	assert.Equal(t, 30*time.Second, cfg.Timeout.Dial)
	assert.Equal(t, 300*time.Second, cfg.Timeout.ConnIdle)
	assert.Equal(t, 10*time.Second, cfg.Timeout.RequestOverhead)
	assert.Equal(t, 60*time.Second, cfg.Timeout.Retry)
	assert.Equal(t, 45*time.Second, cfg.Timeout.Session)
	assert.Equal(t, 30*time.Second, cfg.Timeout.ProduceRequest)
	assert.Equal(t, 120*time.Second, cfg.Timeout.RecordDelivery)
	assert.Equal(t, 60*time.Second, cfg.Timeout.TransactionTimeout)
}

func TestDefaultConsumerConfig(t *testing.T) {
	cfg := DefaultConsumerConfig()

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, "default-group", cfg.ConsumerGroup)
	assert.False(t, cfg.DisableAutocommit)
	assert.False(t, cfg.BlockRebalanceOnPoll)
	assert.Equal(t, int32(1), cfg.FetchMinBytes)
	assert.Equal(t, int32(50), cfg.FetchMaxMB)
	assert.Equal(t, 3*time.Second, cfg.HeartbeatInterval)
	assert.Equal(t, []Balancer{StickyBalancer}, cfg.Balancers)
	assert.Equal(t, StartOffset, cfg.OffsetReset)
	assert.Equal(t, 30*time.Second, cfg.Timeout.Dial)
	assert.Equal(t, 300*time.Second, cfg.Timeout.ConnIdle)
	assert.Equal(t, 10*time.Second, cfg.Timeout.RequestOverhead)
	assert.Equal(t, 60*time.Second, cfg.Timeout.Rebalance)
	assert.Equal(t, 60*time.Second, cfg.Timeout.Retry)
	assert.Equal(t, 45*time.Second, cfg.Timeout.Session)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with topic",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
			},
			wantErr: false,
		},
		{
			name: "valid config with topics",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"topic1", "topic2"},
			},
			wantErr: false,
		},
		{
			name: "no brokers",
			cfg: Config{
				Topics: []string{"test-topic"},
			},
			wantErr: true,
			errMsg:  "Brokers",
		},
		{
			name: "no topic or topics",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
			},
			wantErr: true,
			errMsg:  "either topics must be specified",
		},
		{
			name: "tls enabled without cert files",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				TLS: TLS{
					Enabled: true,
				},
			},
			wantErr: true,
			errMsg:  "cert_file and key_file are required when TLS is enabled",
		},
		{
			name: "sasl username without password",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				SASL: SASL{
					Username: "user",
				},
			},
			wantErr: true,
			errMsg:  "password is required when username is specified",
		},
		{
			name: "sasl password without username",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				SASL: SASL{
					Password: "pass",
				},
			},
			wantErr: true,
			errMsg:  "username is required when password is specified",
		},
		{
			name: "valid tls config",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				TLS: TLS{
					Enabled:  true,
					CertFile: "/path/to/cert.pem",
					KeyFile:  "/path/to/key.pem",
				},
			},
			wantErr: false,
		},
		{
			name: "valid sasl config",
			cfg: Config{
				Brokers: []string{"localhost:9092"},
				Topics:  []string{"test-topic"},
				SASL: SASL{
					Username: "user",
					Password: "pass",
				},
			},
			wantErr: false,
		},
		{
			name: "empty brokers",
			cfg: Config{
				Brokers: []string{},
				Topics:  []string{"test-topic"},
			},
			wantErr: true,
			errMsg:  "Brokers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPartitionerConstants(t *testing.T) {
	assert.Equal(t, Partitioner("uniform-bytes"), UniformBytesPartitioner)
	assert.Equal(t, Partitioner("least-backup"), LeastBackupPartitioner)
	assert.Equal(t, Partitioner("manual"), ManualPartitioner)
	assert.Equal(t, Partitioner("round-robin"), RoundRobinPartitioner)
	assert.Equal(t, Partitioner("sticky-key"), StickyKeyPartitioner)
	assert.Equal(t, Partitioner("sticky"), StickyPartitioner)
}

func TestAckConstants(t *testing.T) {
	assert.Equal(t, Ack("No"), NoAck)
	assert.Equal(t, Ack("Leader"), LeaderAck)
	assert.Equal(t, Ack("All"), AllAck)
}

func TestCompressionConstants(t *testing.T) {
	assert.Equal(t, Compression("none"), NoneCompression)
	assert.Equal(t, Compression("snappy"), SnappyCompression)
	assert.Equal(t, Compression("zstd"), ZstdCompression)
	assert.Equal(t, Compression("gzip"), GzipCompression)
	assert.Equal(t, Compression("lz4"), Lz4Compression)
}

func TestBalancerConstants(t *testing.T) {
	assert.Equal(t, Balancer("cooperative-sticky"), CooperativeStickyBalancer)
	assert.Equal(t, Balancer("sticky"), StickyBalancer)
	assert.Equal(t, Balancer("range"), RangeBalancer)
	assert.Equal(t, Balancer("round-robin"), RoundRobinBalancer)
}

func TestOffsetResetConstants(t *testing.T) {
	assert.Equal(t, OffsetReset("start"), StartOffset)
	assert.Equal(t, OffsetReset("end"), EndOffset)
}
