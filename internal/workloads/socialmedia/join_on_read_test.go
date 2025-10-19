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

type JoinOnReadTest struct{
	Follows int
}

func (t *JoinOnReadTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	userID := uuid.New().String()
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, GetUsersSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, GetPostsSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, GetFollowsSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)", userID, "test_user")
			if err != nil {
				return err
			}
			for i := 0; i < t.Follows; i++ {
				followeeID := uuid.New().String()
				_, err = tx.Exec(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)", followeeID, fmt.Sprintf("followee-%d", i))
				if err != nil {
					return err
				}
				_, err = tx.Exec(ctx, "INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)", userID, followeeID)
				if err != nil {
					return err
				}
				for j := 0; j < 10; j++ {
					postID := uuid.New().String()
					_, err = tx.Exec(ctx, "INSERT INTO posts (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)", postID, followeeID, "test content", time.Now())
					if err != nil {
						return err
					}
				}
			}
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS users (id VARCHAR(255) PRIMARY KEY,name VARCHAR(255) NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS posts (id VARCHAR(255) PRIMARY KEY,user_id VARCHAR(255) NOT NULL,content TEXT NOT NULL,created_at TIMESTAMP NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS follows (follower_id VARCHAR(255) NOT NULL,followee_id VARCHAR(255) NOT NULL,PRIMARY KEY (follower_id, followee_id));
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", userID, "test_user")
			if err != nil {
				return err
			}
			for i := 0; i < t.Follows; i++ {
				followeeID := uuid.New().String()
				_, err = tx.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", followeeID, fmt.Sprintf("followee-%d", i))
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(ctx, "INSERT INTO follows (follower_id, followee_id) VALUES (?, ?)", userID, followeeID)
				if err != nil {
					return err
				}
				for j := 0; j < 10; j++ {
					postID := uuid.New().String()
					_, err = tx.ExecContext(ctx, "INSERT INTO posts (id, user_id, content, created_at) VALUES (?, ?, ?, ?)", postID, followeeID, "test content", time.Now())
					if err != nil {
						return err
					}
				}
			}
		case mongo.SessionContext:
			users := tx.Client().Database("benchmarkdb").Collection("users")
			posts := tx.Client().Database("benchmarkdb").Collection("posts")
			follows := tx.Client().Database("benchmarkdb").Collection("follows")
			_, err := users.InsertOne(ctx, bson.M{"_id": userID, "name": "test_user"})
			if err != nil {
				return err
			}
			for i := 0; i < t.Follows; i++ {
				followeeID := uuid.New().String()
				_, err = users.InsertOne(ctx, bson.M{"_id": followeeID, "name": fmt.Sprintf("followee-%d", i)})
				if err != nil {
					return err
				}
				_, err = follows.InsertOne(ctx, bson.M{"follower_id": userID, "followee_id": followeeID})
				if err != nil {
					return err
				}
				for j := 0; j < 10; j++ {
					postID := uuid.New().String()
					_, err = posts.InsertOne(ctx, bson.M{"_id": postID, "user_id": followeeID, "content": "test content", "created_at": time.Now()})
					if err != nil {
						return err
					}
				}
			}
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}
	return db.ExecuteTx(ctx, txFunc)
}

func (t *JoinOnReadTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	histogram := hdrhistogram.New(1, 10000, 3)
	deadline := time.Now().Add(duration)
	userID := "test_user"

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				startTime := time.Now()
				txFunc := func(tx interface{}) error {
					switch tx := tx.(type) {
					case pgx.Tx:
						rows, err := tx.Query(ctx, "SELECT p.* FROM posts p JOIN follows f ON p.user_id = f.followee_id WHERE f.follower_id = $1 ORDER BY p.created_at DESC LIMIT 10", userID)
						if err != nil {
							return err
						}
						defer rows.Close()
					case *sql.Tx:
						rows, err := tx.QueryContext(ctx, "SELECT p.* FROM posts p JOIN follows f ON p.user_id = f.followee_id WHERE f.follower_id = ? ORDER BY p.created_at DESC LIMIT 10", userID)
						if err != nil {
							return err
						}
						defer rows.Close()
					case mongo.SessionContext:
						followsCollection := tx.Client().Database("benchmarkdb").Collection("follows")
						cursor, err := followsCollection.Find(ctx, bson.M{"follower_id": userID})
						if err != nil {
							return err
						}
						var followees []string
						for cursor.Next(ctx) {
							var follow struct{ FolloweeID string `bson:"followee_id"` }
							if err := cursor.Decode(&follow); err != nil {
								return err
							}
							followees = append(followees, follow.FolloweeID)
						}
						postsCollection := tx.Client().Database("benchmarkdb").Collection("posts")
						postsCursor, err := postsCollection.Find(ctx, bson.M{"user_id": bson.M{"$in": followees}})
						if err != nil {
							return err
						}
						defer postsCursor.Close(ctx)
					default:
						return fmt.Errorf("unsupported transaction type: %T", tx)
					}
					return nil
				}
				db.ExecuteTx(ctx, txFunc)
				histogram.RecordValue(time.Since(startTime).Milliseconds())
			}
		}()
	}

	wg.Wait()

	result := &database.Result{
		P95Latency: time.Duration(histogram.ValueAtQuantile(95)) * time.Millisecond,
	}

	return result, nil
}

func (t *JoinOnReadTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
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
			_, err = tx.Exec(ctx, "DROP TABLE follows")
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
			_, err = tx.ExecContext(ctx, "DROP TABLE follows")
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
			return tx.Client().Database("benchmarkdb").Collection("follows").Drop(ctx)
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}
	return db.ExecuteTx(ctx, txFunc)
}