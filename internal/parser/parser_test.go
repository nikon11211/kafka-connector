package parser

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nikon11211/kafka-connector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type testLogger struct {
	level kgo.LogLevel
}

func (t *testLogger) Level() kgo.LogLevel {
	return t.level
}

func (t *testLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {}

func generateTestCertificates(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()

	tmpDir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caFile = filepath.Join(tmpDir, "ca.pem")
	caPEM, err := os.Create(caFile)
	require.NoError(t, err)
	defer func(caPEM *os.File) {
		err := caPEM.Close()
		if err != nil {
			return
		}
	}(caPEM)
	err = pem.Encode(caPEM, &pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	require.NoError(t, err)

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	require.NoError(t, err)

	certFile = filepath.Join(tmpDir, "cert.pem")
	certPEM, err := os.Create(certFile)
	require.NoError(t, err)
	defer func(certPEM *os.File) {
		err := certPEM.Close()
		if err != nil {
			return
		}
	}(certPEM)
	err = pem.Encode(certPEM, &pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	require.NoError(t, err)

	keyFile = filepath.Join(tmpDir, "key.pem")
	keyPEM, err := os.Create(keyFile)
	require.NoError(t, err)
	defer func(keyPEM *os.File) {
		err := keyPEM.Close()
		if err != nil {
			return
		}
	}(keyPEM)
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	require.NoError(t, err)
	err = pem.Encode(keyPEM, &pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER})
	require.NoError(t, err)

	return certFile, keyFile, caFile
}

func TestParseConfig(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		logger := &testLogger{level: kgo.LogLevelInfo}

		opts, err := ParseConfig(nil, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "config is nil")
	})

	t.Run("basic config", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Topics:  []string{"example-topic"},
			Metrics: kafka.Metrics{
				Namespace: "test",
			},
		}
		reg := prometheus.NewRegistry()
		logger := &testLogger{level: kgo.LogLevelInfo}

		opts, err := ParseConfig(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("config with timeouts", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Topics:  []string{"example-topic"},
			Metrics: kafka.Metrics{
				Namespace: "test",
			},
			Timeout: kafka.Timeout{
				Dial:               30 * time.Second,
				ConnIdle:           300 * time.Second,
				RequestOverhead:    10 * time.Second,
				Rebalance:          60 * time.Second,
				Retry:              60 * time.Second,
				Session:            45 * time.Second,
				ProduceRequest:     30 * time.Second,
				RecordDelivery:     120 * time.Second,
				TransactionTimeout: 60 * time.Second,
			},
		}
		reg := prometheus.NewRegistry()
		logger := &testLogger{level: kgo.LogLevelInfo}

		opts, err := ParseConfig(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("config with sasl", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Topics:  []string{"example-topic"},
			Metrics: kafka.Metrics{
				Namespace: "test",
			},
			SASL: kafka.SASL{
				Username: "user",
				Password: "pass",
			},
		}
		reg := prometheus.NewRegistry()
		logger := &testLogger{level: kgo.LogLevelInfo}

		opts, err := ParseConfig(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("config with tls error", func(t *testing.T) {
		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Topics:  []string{"example-topic"},
			Metrics: kafka.Metrics{
				Namespace: "test",
			},
			TLS: kafka.TLS{
				Enabled:  true,
				CertFile: "/nonexistent/cert.pem",
				KeyFile:  "/nonexistent/key.pem",
			},
		}
		reg := prometheus.NewRegistry()
		logger := &testLogger{level: kgo.LogLevelInfo}

		opts, err := ParseConfig(cfg, logger, reg)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "TLS configuration error")
	})
}

func TestParseConfigTLS(t *testing.T) {
	t.Run("with valid TLS certs", func(t *testing.T) {
		certFile, keyFile, caFile := generateTestCertificates(t)

		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Topics:  []string{"example-topic"},
			Metrics: kafka.Metrics{
				Namespace: "test",
			},
			TLS: kafka.TLS{
				Enabled:  true,
				CertFile: certFile,
				KeyFile:  keyFile,
				CAFile:   caFile,
			},
		}
		reg := prometheus.NewRegistry()
		logger := &testLogger{level: kgo.LogLevelInfo}

		opts, err := ParseConfig(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("with valid TLS certs without ca", func(t *testing.T) {
		certFile, keyFile, _ := generateTestCertificates(t)

		cfg := &kafka.Config{
			Brokers: []string{"localhost:9092"},
			Topics:  []string{"example-topic"},
			Metrics: kafka.Metrics{
				Namespace: "test",
			},
			TLS: kafka.TLS{
				Enabled:  true,
				CertFile: certFile,
				KeyFile:  keyFile,
			},
		}
		reg := prometheus.NewRegistry()
		logger := &testLogger{level: kgo.LogLevelInfo}

		opts, err := ParseConfig(cfg, logger, reg)
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})
}

func TestParseTimeouts(t *testing.T) {
	t.Run("nil timeouts", func(t *testing.T) {
		opts := parseTimeouts(nil)
		assert.Nil(t, opts)
	})

	t.Run("all timeouts set", func(t *testing.T) {
		cfg := &kafka.Timeout{
			Dial:               1 * time.Second,
			ConnIdle:           2 * time.Second,
			RequestOverhead:    3 * time.Second,
			Rebalance:          4 * time.Second,
			Retry:              5 * time.Second,
			Session:            6 * time.Second,
			ProduceRequest:     7 * time.Second,
			RecordDelivery:     8 * time.Second,
			TransactionTimeout: 9 * time.Second,
		}
		opts := parseTimeouts(cfg)
		assert.Len(t, opts, 9)
	})

	t.Run("zero timeouts", func(t *testing.T) {
		cfg := &kafka.Timeout{}
		opts := parseTimeouts(cfg)
		assert.Empty(t, opts)
	})

	t.Run("partial timeouts", func(t *testing.T) {
		cfg := &kafka.Timeout{
			Dial:  30 * time.Second,
			Retry: 60 * time.Second,
		}
		opts := parseTimeouts(cfg)
		assert.Len(t, opts, 2)
	})
}

func TestParseTLS(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		opts, err := parseTLS(nil)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "TLS config is nil")
	})

	t.Run("invalid cert files", func(t *testing.T) {
		cfg := &kafka.TLS{
			Enabled:  true,
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  "/nonexistent/key.pem",
		}
		opts, err := parseTLS(cfg)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "failed to load certificate")
	})

	t.Run("valid cert files with ca", func(t *testing.T) {
		certFile, keyFile, caFile := generateTestCertificates(t)

		cfg := &kafka.TLS{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		}

		opts, err := parseTLS(cfg)
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("valid cert files without ca", func(t *testing.T) {
		certFile, keyFile, _ := generateTestCertificates(t)

		cfg := &kafka.TLS{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
		}

		opts, err := parseTLS(cfg)
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("invalid ca file", func(t *testing.T) {
		certFile, keyFile, _ := generateTestCertificates(t)

		cfg := &kafka.TLS{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   "/nonexistent/ca.pem",
		}

		opts, err := parseTLS(cfg)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "failed to read CA certificate")
	})
}
