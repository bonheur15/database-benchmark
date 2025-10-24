package runner

import (
	"context"
	"database-benchmark/internal/database"
	"time"
)

func Run(ctx context.Context, driver database.DatabaseDriver, workload database.Workload, concurrency int, duration time.Duration) (*database.Result, error) {
	return workload.Run(ctx, driver, concurrency, duration)
}