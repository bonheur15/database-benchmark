package analytics

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"time"
	"math/rand"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
)

type DashboardQueryTest struct{}

func (t *DashboardQueryTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
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
	if err := db.ExecuteTx(ctx, txFunc); err != nil {
		return err
	}

	// Seed with 1 million rows
	const numRows = 1000000
	const batchSize = 1000
	for i := 0; i < numRows; i += batchSize {
		rows := make([][]interface{}, batchSize)
		for j := 0; j < batchSize; j++ {
			rows[j] = []interface{}{
				uuid.New().String(),
					time.Now().Add(-time.Duration(rand.Intn(3600)) * time.Second),
					uuid.New().String(),
					uuid.New().String(),
					fmt.Sprintf("us-east-%d", rand.Intn(5)+1),
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
		if err := db.ExecuteTx(ctx, txFunc); err != nil {
			return err
		}
	}
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
			pipeline := mongo.Pipeline{
				{{"$match", bson.D{{Key: "event_timestamp", Value: bson.D{{Key: "$gt", Value: time.Now().Add(-time.Hour)}}}}}},
				{{"$group", bson.D{{Key: "_id", Value: "$region"}, {Key: "total", Value: bson.D{{Key: "$sum", Value: "$metric_value"}}}}}},
			}
			_, err := tx.Client().Database("benchmarkdb").Collection("events").Aggregate(ctx, pipeline)
			return err
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
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