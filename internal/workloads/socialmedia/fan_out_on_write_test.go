package socialmedia

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
)

type FanOutOnWriteTest struct{}

func (t *FanOutOnWriteTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
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
			_, err := db.ExecContext(ctx, "INSERT INTO users (id, name) VALUES ($1, $2)", userID, fmt.Sprintf("user-%d", i))
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "INSERT INTO timelines (user_id, post_ids) VALUES ($1, $2)", userID, "")
			if err != nil {
				return err
			}
		}

		for i := 0; i < NumFollows; i++ {
			followerID := fmt.Sprintf("user%d", i%NumUsers)
			followeeID := fmt.Sprintf("user%d", (i+1)%NumUsers)
			_, err := db.ExecContext(ctx, "INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)", followerID, followeeID)
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

					// In a real application, you would have a better way to update the timeline.
					// For simplicity, we are just appending the post ID to a comma-separated string.
					_, err = db.ExecContext(ctx, "UPDATE timelines SET post_ids = post_ids || $1 WHERE user_id = $2", ","+postID, followerID)
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
			row := db.QueryRowContext(ctx, "SELECT post_ids FROM timelines WHERE user_id = $1", userID)
			var postIDs string
			if err := row.Scan(&postIDs); err != nil {
				continue
			}
			_ = strings.Split(postIDs, ",")
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
		_, err := db.ExecContext(ctx, "DROP TABLE users")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE posts")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE follows")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE timelines")
		if err != nil {
			return err
		}
		return nil
	})
}