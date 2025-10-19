package runner

import (
	"context"
	"database-benchmark/internal/database"
	"time"
)

func Run(ctx context.Context, driver database.DatabaseDriver, workload database.Workload, concurrency int, duration time.Duration) (*database.Result, error) {
	if err := workload.Setup(ctx, driver); err != nil {
		return nil, err
	}

	result, err := workload.Run(ctx, driver, concurrency, duration)
	if err != nil {
		return nil, err
	}

	if err := workload.Teardown(ctx, driver); err != nil {
		return nil, err
	}

	return result, nil
}
