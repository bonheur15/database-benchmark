package socialmedia

import (
	"context"
	"database-benchmark/internal/database"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	NumPosts       = 10000
	PostsPerUser   = NumPosts / 1000
	FollowsPerUser = 100000 / 1000
)

type JoinOnReadTest struct{}

func (t *JoinOnReadTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	if _, ok := db.(*database.MongoDriver); ok {
		// MongoDB setup
		return db.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			for i := 0; i < NumUsers; i++ {
				userID := fmt.Sprintf("user%d", i)
				_, err := db.ExecContext(ctx, "users", bson.M{"_id": userID, "name": fmt.Sprintf("user-%d", i)})
				if err != nil {
					return err
				}
			}
			for i := 0; i < NumPosts; i++ {
				postID := uuid.New().String()
				userID := fmt.Sprintf("user%d", i%NumUsers)
				_, err := db.ExecContext(ctx, "posts", bson.M{"_id": postID, "user_id": userID, "content": "post content", "created_at": time.Now()})
				if err != nil {
					return err
				}
			}
			for i := 0; i < NumFollows; i++ {
				followerID := fmt.Sprintf("user%d", i%NumUsers)
				followeeID := fmt.Sprintf("user%d", (i+1)%NumUsers)
				_, err := db.ExecContext(ctx, "follows", bson.M{"follower_id": followerID, "followee_id": followeeID})
				if err != nil {
					// Ignore duplicate key errors
				}
			}
			return nil
		})
	}

	// SQL setup
	_, err := db.ExecContext(ctx, GetUsersSchema())
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, GetPostsSchema())
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, GetFollowsSchema())
	if err != nil {
		return err
	}

	for i := 0; i < NumUsers; i++ {
		userID := fmt.Sprintf("user%d", i)
		_, err := db.ExecContext(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)", userID, fmt.Sprintf("user-%d", i))
		if err != nil {
			return err
		}
	}

	for i := 0; i < NumPosts; i++ {
		postID := uuid.New().String()
		userID := fmt.Sprintf("user%d", i%NumUsers)
		_, err := db.ExecContext(ctx, "INSERT INTO posts (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)", postID, userID, "post content", time.Now())
		if err != nil {
			return err
		}
	}

	for i := 0; i < NumFollows; i++ {
		followerID := fmt.Sprintf("user%d", i%NumUsers)
		followeeID := fmt.Sprintf("user%d", (i+1)%NumUsers)
		_, err := db.ExecContext(ctx, "INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)", followerID, followeeID)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
				// do nothing
			} else {
				return err
			}
		}
	}

	return nil
}

func (t *JoinOnReadTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
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
				userID := fmt.Sprintf("user%d", time.Now().UnixNano()%NumUsers)
				var err error
				if _, ok := db.(*database.MongoDriver); ok {
					rows, queryErr := db.QueryContext(ctx, "posts", bson.M{"user_id": bson.M{"$in": getFolloweeIDs(ctx, db, userID)}})
					err = queryErr
					if err == nil {
						rows.Close()
					}
				} else {
					rows, queryErr := db.QueryContext(ctx, "SELECT p.* FROM posts p JOIN follows f ON p.user_id = f.followee_id WHERE f.follower_id = $1", userID)
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

func (t *JoinOnReadTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "follows", bson.M{})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "posts", bson.M{})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "users", bson.M{})
			if err != nil {
				return err
			}
		} else {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS follows CASCADE")
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS posts CASCADE")
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS users CASCADE")
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func getFolloweeIDs(ctx context.Context, db database.DatabaseDriver, userID string) []string {
	var rows database.Rows
	var err error
	if _, ok := db.(*database.MongoDriver); ok {
		rows, err = db.QueryContext(ctx, "follows", bson.M{"follower_id": userID})
	} else {
		rows, err = db.QueryContext(ctx, "SELECT followee_id FROM follows WHERE follower_id = $1", userID)
	}

	if err != nil {
		return []string{}
	}
	defer rows.Close()

	var followeeIDs []string
	for rows.Next() {
		if _, ok := db.(*database.MongoDriver); ok {
			var follow struct {
				FolloweeID string `bson:"followee_id"`
			}
			if err := rows.Scan(&follow); err != nil {
				continue
			}
			followeeIDs = append(followeeIDs, follow.FolloweeID)
		} else {
			var followeeID string
			if err := rows.Scan(&followeeID); err != nil {
				continue
			}
			followeeIDs = append(followeeIDs, followeeID)
		}
	}

	return followeeIDs
}