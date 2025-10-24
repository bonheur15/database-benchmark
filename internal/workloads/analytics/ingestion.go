package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	NumEvents = 100000
)

type IngestionTest struct{}

func (t *IngestionTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		_, err := db.ExecContext(ctx, GetEventsSchema())
		return err
	})
}

func (t *IngestionTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.ExecuteTx(context.Background(), func(tx interface{}) error {
				for i := 0; i < NumEvents/concurrency; i++ {
					eventID := uuid.New().String()
					userID := fmt.Sprintf("user%d", i%1000)
					productID := fmt.Sprintf("product%d", i%100)
					region := fmt.Sprintf("region%d", i%10)
					metricValue := float64(i)
					_, err := db.ExecContext(context.WithValue(context.Background(), "tx", tx), "INSERT INTO analytics_events (event_id, event_timestamp, user_id, product_id, region, metric_value) VALUES ($1, $2, $3, $4, $5, $6)", eventID, time.Now(), userID, productID, region, metricValue)
					if err != nil {
						return err
					}
				}
				return nil
			})
		}()
	}

	wg.Wait()

	totalTime := time.Since(startTime)
	ingestionRate := float64(NumEvents) / totalTime.Seconds()

	result := &database.Result{
		TotalTime:  totalTime,
		Throughput: ingestionRate,
	}

	return result, nil
}

func (t *IngestionTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		_, err := db.ExecContext(ctx, "TRUNCATE TABLE analytics_events")
		return err
	})
}
