package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"time"
	"sync"
	"sync/atomic"
	"math/rand"
	"strings"

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
				const batchSize = 1000
				rows := make([][]interface{}, batchSize)
				for i := 0; i < batchSize; i++ {
					rows[i] = []interface{}{
						uuid.New().String(),
						time.Now(),
						uuid.New().String(),
						uuid.New().String(),
						"us-east-1",
						rand.Float64() * 100,
					}
				}

				txFunc := func(tx interface{}) error {
					switch tx := tx.(type) {
					case pgx.Tx:
						_, err := tx.CopyFrom(
							ctx,
							pgx.Identifier{"events"},
							[]string{"event_id", "event_timestamp", "user_id", "product_id", "region", "metric_value"},
							pgx.CopyFromRows(rows),
						)
						return err
					case *sql.Tx:
						valueStrings := make([]string, 0, batchSize)
						valueArgs := make([]interface{}, 0, batchSize*6)
						for _, row := range rows {
							valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
							valueArgs = append(valueArgs, row...)
						}
						stmt := fmt.Sprintf("INSERT INTO events (event_id, event_timestamp, user_id, product_id, region, metric_value) VALUES %s", strings.Join(valueStrings, ","))
						_, err := tx.ExecContext(ctx, stmt, valueArgs...)
						return err
					case mongo.SessionContext:
						docs := make([]interface{}, batchSize)
						for i, row := range rows {
							docs[i] = bson.M{
								"_id": row[0],
								"event_timestamp": row[1],
								"user_id": row[2],
								"product_id": row[3],
								"region": row[4],
								"metric_value": row[5],
							}
						}
						_, err := tx.Client().Database("benchmarkdb").Collection("events").InsertMany(ctx, docs)
						return err
					default:
						return fmt.Errorf("unsupported transaction type: %T", tx)
					}
				}

				if err := db.ExecuteTx(ctx, txFunc); err == nil {
					atomic.AddUint64(&ingestedRows, batchSize)
				}
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
