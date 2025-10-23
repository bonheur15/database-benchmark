package socialmedia

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	NumUsers   = 100
	NumFollows = 1000
)

type FanOutOnWriteTest struct{}

func (t *FanOutOnWriteTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	if _, ok := db.(*database.MongoDriver); !ok {
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
		_, err = db.ExecContext(ctx, GetTimelinesSchema())
		if err != nil {
			return err
		}
	}

	for i := 0; i < NumUsers; i++ {
		userID := fmt.Sprintf("user%d", i)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "users", bson.M{"_id": userID, "name": fmt.Sprintf("user-%d", i)})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "timelines", bson.M{"_id": userID, "post_ids": []string{}})
			if err != nil {
				return err
			}
		} else {
			_, err := db.ExecContext(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)", userID, fmt.Sprintf("user-%d", i))
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "INSERT INTO timelines (user_id, post_ids) VALUES ($1, $2)", userID, "{}")
			if err != nil {
				return err
			}
		}
	}

	for i := 0; i < NumFollows; i++ {
		followerID := fmt.Sprintf("user%d", i%NumUsers)
		followeeID := fmt.Sprintf("user%d", (i+1)%NumUsers)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "follows", bson.M{"follower_id": followerID, "followee_id": followeeID})
			if err != nil {
				// Ignore duplicate key errors
			}
		} else {
			_, err := db.ExecContext(ctx, "INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)", followerID, followeeID)
			if err != nil {
				// Ignore duplicate key errors
			}
		}
	}

	return nil
}

func (t *FanOutOnWriteTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	result := &database.Result{}

	// Write Phase
	startTime := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			postID := uuid.New().String()
			userID := fmt.Sprintf("user%d", i%NumUsers)

			var err error
			err = db.ExecuteTx(ctx, func(tx interface{}) error {
				ctx = context.WithValue(ctx, "tx", tx)
				if _, ok := db.(*database.MongoDriver); ok {
					_, err := db.ExecContext(ctx, "posts", bson.M{"_id": postID, "user_id": userID, "content": "post content", "created_at": time.Now()})
					if err != nil {
						return err
					}

					rows, err := db.QueryContext(ctx, "follows", bson.M{"followee_id": userID})
					if err != nil {
						return err
					}
					defer rows.Close()

					for rows.Next() {
						var follow struct {
							FollowerID string `bson:"follower_id"`
						}
						if err := rows.Scan(&follow); err != nil {
							return err
						}

						_, err = db.ExecContext(ctx, "timelines", bson.M{"_id": follow.FollowerID}, bson.M{"_id": follow.FollowerID, "$push": bson.M{"post_ids": postID}})
						if err != nil {
							return err
						}
					}
				} else {
					_, err := db.ExecContext(ctx, "INSERT INTO posts (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)", postID, userID, "post content", time.Now())
					if err != nil {
						return err
					}

					rows, err := db.QueryContext(ctx, "SELECT follower_id FROM follows WHERE followee_id = $1", userID)
					if err != nil {
						return err
					}
					defer rows.Close()

					for rows.Next() {
						var followerID string
						if err := rows.Scan(&followerID); err != nil {
							return err
						}

						_, err = db.ExecContext(ctx, "UPDATE timelines SET post_ids = array_append(post_ids, $1) WHERE user_id = $2", postID, followerID)
						if err != nil {
							return err
						}
					}
				}

				return nil
			})
			if err != nil {
				result.Errors++
			}
		}(i)
	}
	wg.Wait()
	result.TotalTime = time.Since(startTime)

	// Read Phase
	var readWg sync.WaitGroup
	histogram := hdrhistogram.New(1, 10000, 3)
	deadline := time.Now().Add(duration)
	for i := 0; i < concurrency; i++ {
		readWg.Add(1)
		go func() {
			defer readWg.Done()
			var err error // Declare err once outside the inner loop
			for time.Now().Before(deadline) {
				startTime := time.Now()
				userID := fmt.Sprintf("user%d", time.Now().UnixNano()%NumUsers)

				if _, ok := db.(*database.MongoDriver); ok {
					row := db.QueryRowContext(ctx, "timelines", bson.M{"_id": userID})
					var timeline struct {
						PostIDs []string `bson:"post_ids"`
					}
					err = row.Scan(&timeline)
				} else {
					row := db.QueryRowContext(ctx, "SELECT post_ids FROM timelines WHERE user_id = $1", userID)
					var postIDs []string
					err = row.Scan(&postIDs)
				}

				if err != nil {
					result.Errors++
				} else {
					result.Operations++
					histogram.RecordValue(time.Since(startTime).Milliseconds())
				}
			}
		}()
	}
	readWg.Wait()

	result.P95Latency = time.Duration(histogram.ValueAtQuantile(95)) * time.Millisecond
	result.P99Latency = time.Duration(histogram.ValueAtQuantile(99)) * time.Millisecond
	result.AverageLatency = time.Duration(histogram.Mean()) * time.Millisecond
	result.Throughput = float64(result.Operations) / duration.Seconds()

	return result, nil
}

func (t *FanOutOnWriteTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "timelines", bson.M{})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "follows", bson.M{})
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
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS posts CASCADE")
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS follows CASCADE")
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS timelines CASCADE")
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
