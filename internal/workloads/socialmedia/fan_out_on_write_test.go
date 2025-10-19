package socialmedia

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"time"
	"sync"
	"math/rand"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"github.com/HdrHistogram/hdrhistogram-go"
)

type FanOutOnWriteTest struct{}

func (t *FanOutOnWriteTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	userID := uuid.New().String()
	followerIDs := []string{}
	for i := 0; i < 10000; i++ {
		followerIDs = append(followerIDs, uuid.New().String())
	}

	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, GetUsersSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, GetTimelinesSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)", userID, "test_user")
			if err != nil {
				return err
			}
			for _, followerID := range followerIDs {
				_, err = tx.Exec(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)", followerID, "follower")
				if err != nil {
					return err
				}
				_, err = tx.Exec(ctx, "INSERT INTO timelines (user_id, post_ids) VALUES ($1, $2)", followerID, "")
				if err != nil {
					return err
				}
			}
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS users (id VARCHAR(255) PRIMARY KEY,name VARCHAR(255) NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS timelines (user_id VARCHAR(255) PRIMARY KEY,post_ids TEXT NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", userID, "test_user")
			if err != nil {
				return err
			}
			for _, followerID := range followerIDs {
				_, err = tx.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", followerID, "follower")
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(ctx, "INSERT INTO timelines (user_id, post_ids) VALUES (?, ?)", followerID, "")
				if err != nil {
					return err
				}
			}
		case mongo.SessionContext:
			users := tx.Client().Database("benchmarkdb").Collection("users")
			timelines := tx.Client().Database("benchmarkdb").Collection("timelines")
			_, err := users.InsertOne(ctx, bson.M{"_id": userID, "name": "test_user"})
			if err != nil {
				return err
			}
			for _, followerID := range followerIDs {
				_, err = users.InsertOne(ctx, bson.M{"_id": followerID, "name": "follower"})
				if err != nil {
					return err
				}
				_, err = timelines.InsertOne(ctx, bson.M{"user_id": followerID, "post_ids": []string{}})
				if err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}
	return db.ExecuteTx(ctx, txFunc)
}

func (t *FanOutOnWriteTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	// Write Phase
	writeStartTime := time.Now()
	postID := uuid.New().String()
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, "INSERT INTO posts (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)", postID, "test_user", "test content", time.Now())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "UPDATE timelines SET post_ids = post_ids || ',' || $1", postID)
			return err
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "INSERT INTO posts (id, user_id, content, created_at) VALUES (?, ?, ?, ?)", postID, "test_user", "test content", time.Now())
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "UPDATE timelines SET post_ids = CONCAT(post_ids, ',', ?)", postID)
			return err
		case mongo.SessionContext:
			posts := tx.Client().Database("benchmarkdb").Collection("posts")
			_, err := posts.InsertOne(ctx, bson.M{"_id": postID, "user_id": "test_user", "content": "test content", "created_at": time.Now()})
			if err != nil {
				return err
			}
			timelines := tx.Client().Database("benchmarkdb").Collection("timelines")
			_, err = timelines.UpdateMany(ctx, bson.M{}, bson.M{"$push": bson.M{"post_ids": postID}})
			return err
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
	}
	if err := db.ExecuteTx(ctx, txFunc); err != nil {
		return nil, err
	}
	writeTotalTime := time.Since(writeStartTime)

	// Read Phase
	var wg sync.WaitGroup
	histogram := hdrhistogram.New(1, 10000, 3)
	deadline := time.Now().Add(duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				startTime := time.Now()
				txFunc := func(tx interface{}) error {
					switch tx := tx.(type) {
					case pgx.Tx:
						_, err := tx.Query(ctx, "SELECT post_ids FROM timelines WHERE user_id = $1", "follower")
						return err
					case *sql.Tx:
						_, err := tx.QueryContext(ctx, "SELECT post_ids FROM timelines WHERE user_id = ?", "follower")
						return err
					case mongo.SessionContext:
						timelines := tx.Client().Database("benchmarkdb").Collection("timelines")
						_, err := timelines.Find(ctx, bson.M{"user_id": "follower"})
						return err
					default:
						return fmt.Errorf("unsupported transaction type: %T", tx)
					}
				}
				db.ExecuteTx(ctx, txFunc)
				histogram.RecordValue(time.Since(startTime).Milliseconds())
			}
		}()
	}

	wg.Wait()

	result := &database.Result{
		TotalTime:  writeTotalTime,
		P95Latency: time.Duration(histogram.ValueAtQuantile(95)) * time.Millisecond,
	}

	return result, nil
}

func (t *FanOutOnWriteTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, "DROP TABLE users")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "DROP TABLE posts")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "DROP TABLE timelines")
			if err != nil {
				return err
			}
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "DROP TABLE users")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "DROP TABLE posts")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "DROP TABLE timelines")
			if err != nil {
				return err
			}
		case mongo.SessionContext:
			err := tx.Client().Database("benchmarkdb").Collection("users").Drop(ctx)
			if err != nil {
				return err
			}
			err = tx.Client().Database("benchmarkdb").Collection("posts").Drop(ctx)
			if err != nil {
				return err
			}
			return tx.Client().Database("benchmarkdb").Collection("timelines").Drop(ctx)
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}
	return db.ExecuteTx(ctx, txFunc)
}
