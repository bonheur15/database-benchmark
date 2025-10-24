package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

type DashboardQueryTest struct{}

func (t *DashboardQueryTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)

		if _, ok := db.(*database.MongoDriver); !ok {
			// Only execute schema for SQL databases
			_, err := db.ExecContext(ctx, GetEventsSchema())
			if err != nil {
				return err
			}
		}

		for i := 0; i < 100000; i++ {
			eventID := uuid.New().String()
			userID := fmt.Sprintf("user%d", i%1000)
			productID := fmt.Sprintf("product%d", i%100)
			region := fmt.Sprintf("region%d", i%10)
			metricValue := float64(i)

			if _, ok := db.(*database.MongoDriver); ok {
				// For MongoDB, insert directly into the collection
				_, err := db.ExecContext(ctx, "analytics_events", bson.M{
					"_id": eventID,
					"event_timestamp": time.Now(),
					"user_id": userID,
					"product_id": productID,
					"region": region,
					"metric_value": metricValue,
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

func (t *DashboardQueryTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(startTime) < duration {
				rows, err := db.QueryContext(ctx, "analytics_events", bson.M{"event_timestamp": bson.M{"$gt": time.Now().Add(-1 * time.Hour)}})
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
