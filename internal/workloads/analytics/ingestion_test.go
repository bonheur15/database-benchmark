package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"time"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
)

type IngestionTest struct{}

func (t *IngestionTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, GetEventsSchema())
			return err
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS events (event_id VARCHAR(255) PRIMARY KEY,event_timestamp TIMESTAMP NOT NULL,user_id VARCHAR(255) NOT NULL,product_id VARCHAR(255) NOT NULL,region VARCHAR(255) NOT NULL,metric_value DECIMAL(10, 2) NOT NULL);
")
			return err
		case mongo.SessionContext:
			// Schema is flexible
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}
	return db.ExecuteTx(ctx, txFunc)
}

func (t *IngestionTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	var ingestedRows uint64
	deadline := time.Now().Add(duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				// Bulk insert logic will be here
			}
		}()
	}

	wg.Wait()

	result := &database.Result{
		Throughput: float64(ingestedRows) / duration.Seconds(),
	}

	return result, nil
}

func (t *IngestionTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, "DROP TABLE events")
			return err
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "DROP TABLE events")
			return err
		case mongo.SessionContext:
			return tx.Client().Database("benchmarkdb").Collection("events").Drop(ctx)
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
	}
	return db.ExecuteTx(ctx, txFunc)
}
