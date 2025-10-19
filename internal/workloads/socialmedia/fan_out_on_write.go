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

type FanOutOnWriteTest struct{}

func (t *FanOutOnWriteTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
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

		for i := 0; i < NumUsers; i++ {
			userID := fmt.Sprintf("user%d", i)
			_, err := db.ExecContext(ctx, "users", bson.M{"_id": userID, "name": fmt.Sprintf("user-%d", i)})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "timelines", bson.M{"_id": userID, "post_ids": []string{}})
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

func (t *FanOutOnWriteTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	var writeTime time.Duration
	var readLatency time.Duration

	go func() {
		defer wg.Done()
		startTime := time.Now()
		// Write Phase
		for i := 0; i < 10; i++ {
			postID := uuid.New().String()
			userID := fmt.Sprintf("user%d", i)
			db.ExecuteTx(ctx, func(tx interface{}) error {
				ctx = context.WithValue(ctx, "tx", tx)
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

				return nil
			})
		}
		writeTime = time.Since(startTime)
	}()

	go func() {
		defer wg.Done()
		// Read Phase
		histogram := hdrhistogram.New(1, 10000, 3)
		deadline := time.Now().Add(duration)
		for time.Now().Before(deadline) {
			startTime := time.Now()
			userID := fmt.Sprintf("user%d", time.Now().UnixNano()%NumUsers)
			row := db.QueryRowContext(ctx, "timelines", bson.M{"_id": userID})
			var timeline struct {
				PostIDs []string `bson:"post_ids"`
			}
			if err := row.Scan(&timeline); err != nil {
				continue
			}
			histogram.RecordValue(time.Since(startTime).Milliseconds())
		}
		readLatency = time.Duration(histogram.ValueAtQuantile(95)) * time.Millisecond
	}()

	wg.Wait()

	result := &database.Result{
		TotalTime:  writeTime,
		P95Latency: readLatency,
	}

	return result, nil
}

func (t *FanOutOnWriteTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
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
		return nil
	})
}
