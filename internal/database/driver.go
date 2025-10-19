package database

import (
	"context"
	"time"
)

type Workload interface {
	Setup(ctx context.Context, db DatabaseDriver) error
	Run(ctx context.Context, db DatabaseDriver, concurrency int, duration time.Duration) (*Result, error)
	Teardown(ctx context.Context, db DatabaseDriver) error
}

type Result struct {
	Operations      int64
	Errors          int64
	Throughput      float64
	P95Latency      time.Duration
	P99Latency      time.Duration
	AverageLatency  time.Duration
	ErrorRate       float64
	TotalTime       time.Duration
	DataIntegrity   bool
}

type Row interface {
	Scan(dest ...interface{}) error
}

type DatabaseDriver interface {
	Connect(dsn string) error
	Close() error
	ExecuteTx(ctx context.Context, txFunc func(interface{}) error) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) Row
}
