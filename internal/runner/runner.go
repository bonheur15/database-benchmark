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

	defer func() {
		if err := workload.Teardown(ctx, driver); err != nil {
			// We can't do much here, just log the error
			println("Failed to teardown workload:", err.Error())
		}
	}()

	return workload.Run(ctx, driver, concurrency, duration)
}
