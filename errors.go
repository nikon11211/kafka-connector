package kafka

import "errors"

var (
	ErrUnsupportedPartitioner   = errors.New("unsupported partitioner type")
	ErrUnsupportedGroupBalancer = errors.New("unsupported group balancer type")
)
