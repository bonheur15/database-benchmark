package runner

import (
	"context"
	"database-benchmark/internal/database"
	"log"
	"time"
)

func Run(ctx context.Context, db database.DatabaseDriver, workload database.Workload, concurrency int, duration time.Duration, logger *log.Logger) (*database.Result, error) {
	// Setup phase (if any) is handled by main.go

	// Execute the workload's Run method
	result, err := workload.Run(ctx, db, concurrency, duration, logger)
	if err != nil {
		return nil, err
	}

	return result, nil
}