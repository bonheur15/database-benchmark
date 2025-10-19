package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type DashboardQueryTest struct{}

func (t *DashboardQueryTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		_, err := db.ExecContext(ctx, GetEventsSchema())
		if err != nil {
			return err
		}

		for i := 0; i < 100000; i++ {
			eventID := uuid.New().String()
			userID := fmt.Sprintf("user%d", i%1000)
			productID := fmt.Sprintf("product%d", i%100)
			region := fmt.Sprintf("region%d", i%10)
			metricValue := float64(i)
			_, err := db.ExecContext(ctx, "INSERT INTO events (event_id, event_timestamp, user_id, product_id, region, metric_value) VALUES ($1, $2, $3, $4, $5, $6)", eventID, time.Now(), userID, productID, region, metricValue)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (t *DashboardQueryTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(startTime) < duration {
				rows, err := db.QueryContext(ctx, "SELECT region, SUM(metric_value) FROM events WHERE event_timestamp > $1 GROUP BY region", time.Now().Add(-1*time.Hour))
				if err != nil {
					continue
				}
				rows.Close()
			}
		}()
	}

	wg.Wait()

	totalTime := time.Since(startTime)

	result := &database.Result{
		TotalTime: totalTime,
	}

	return result, nil
}

func (t *DashboardQueryTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		_, err := db.ExecContext(ctx, "DROP TABLE events")
		return err
	})
}
