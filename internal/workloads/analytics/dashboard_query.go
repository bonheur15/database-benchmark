package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

type DashboardQueryTest struct{}

func (t *DashboardQueryTest) Setup(ctx context.Context, db database.DatabaseDriver, logger *log.Logger) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)

		if _, ok := db.(*database.MongoDriver); !ok {
			// Only execute schema for SQL databases
			_, err := db.ExecContext(ctx, GetEventsSchema())
			if err != nil {
				return err
			}
			// Create index on event_timestamp for faster queries
			_, err = db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_event_timestamp ON analytics_events (event_timestamp)")
			if err != nil {
				return err
			}
		}

		for i := 0; i < 10000; i++ {
			eventID := uuid.New().String()
			userID := fmt.Sprintf("user%d", i%1000)
			productID := fmt.Sprintf("product%d", i%100)
			region := fmt.Sprintf("region%d", i%10)
			metricValue := float64(i)

			if _, ok := db.(*database.MongoDriver); ok {
				// For MongoDB, insert directly into the collection
				_, err := db.ExecContext(ctx, "analytics_events", bson.M{
					"_id":             eventID,
					"event_timestamp": time.Now(),
					"user_id":         userID,
					"product_id":      productID,
					"region":          region,
					"metric_value":    metricValue,
				})
				if err != nil {
					return err
				}
			} else {
				// For SQL databases, use parameterized insert
				query := "INSERT INTO analytics_events (event_id, event_timestamp, user_id, product_id, region, metric_value) VALUES ($1, $2, $3, $4, $5, $6)"
				if _, ok := db.(*database.MySQLDriver); ok {
					query = "INSERT INTO analytics_events (event_id, event_timestamp, user_id, product_id, region, metric_value) VALUES (?, ?, ?, ?, ?, ?)"
				}
				_, err := db.ExecContext(ctx, query, eventID, time.Now(), userID, productID, region, metricValue)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (t *DashboardQueryTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration, logger *log.Logger) (*database.Result, error) {
	var wg sync.WaitGroup
	histogram := hdrhistogram.New(1, 10000, 3)
	deadline := time.Now().Add(duration)
	result := &database.Result{}
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				startTime := time.Now()
				var err error
				if _, ok := db.(*database.MongoDriver); ok {
					// For MongoDB, use aggregation pipeline
					pipeline := []bson.M{
						{"$match": bson.M{"event_timestamp": bson.M{"$gt": time.Now().Add(-1 * time.Hour)}}},
						{"$group": bson.M{"_id": "$region", "total_metric": bson.M{"$sum": "$metric_value"}}},
					}
					rows, queryErr := db.QueryContext(ctx, "analytics_events", pipeline)
					err = queryErr
					if err == nil {
						rows.Close()
					}
				} else {
					query := "SELECT region, SUM(metric_value) FROM analytics_events WHERE event_timestamp > $1 GROUP BY region"
					if _, ok := db.(*database.MySQLDriver); ok {
						query = "SELECT region, SUM(metric_value) FROM analytics_events WHERE event_timestamp > ? GROUP BY region"
					}
					rows, queryErr := db.QueryContext(ctx, query, time.Now().Add(-1*time.Hour))
					err = queryErr
					if err == nil {
						rows.Close()
					}
				}

				mu.Lock()
				if err != nil {
					result.Errors++
				} else {
					result.Operations++
					histogram.RecordValue(time.Since(startTime).Milliseconds())
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	result.TotalTime = duration
	result.Throughput = float64(result.Operations) / duration.Seconds()
	result.P95Latency = time.Duration(histogram.ValueAtQuantile(95)) * time.Millisecond
	result.P99Latency = time.Duration(histogram.ValueAtQuantile(99)) * time.Millisecond
	result.AverageLatency = time.Duration(histogram.Mean()) * time.Millisecond

	return result, nil
}

func (t *DashboardQueryTest) Teardown(ctx context.Context, db database.DatabaseDriver, logger *log.Logger) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "analytics_events") // Delete all documents from the collection
			return err
		} else {
			query := "TRUNCATE TABLE analytics_events"
			if _, ok := db.(*database.MySQLDriver); ok {
				query = "TRUNCATE TABLE analytics_events"
			}
			_, err := db.ExecContext(ctx, query)
			return err
		}
	})
}
