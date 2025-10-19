package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
)

type DashboardQueryTest struct{}

func (t *DashboardQueryTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	// Seeding logic will be here
	return nil
}

func (t *DashboardQueryTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	startTime := time.Now()

	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Query(ctx, "SELECT region, SUM(metric_value) FROM events WHERE event_timestamp > $1 GROUP BY region", time.Now().Add(-time.Hour))
			return err
		case *sql.Tx:
			_, err := tx.QueryContext(ctx, "SELECT region, SUM(metric_value) FROM events WHERE event_timestamp > ? GROUP BY region", time.Now().Add(-time.Hour))
			return err
		case mongo.SessionContext:
			// MongoDB aggregation pipeline
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}

	if err := db.ExecuteTx(ctx, txFunc); err != nil {
		return nil, err
	}

	totalTime := time.Since(startTime)

	result := &database.Result{
		TotalTime: totalTime,
	}

	return result, nil
}

func (t *DashboardQueryTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
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
