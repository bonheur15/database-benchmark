package socialmedia

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
)

const (
	NumUsers    = 1000
	NumPosts    = 10000
	NumFollows  = 100000
	PostsPerUser = NumPosts / NumUsers
	FollowsPerUser = NumFollows / NumUsers
)

type JoinOnReadTest struct{}

func (t *JoinOnReadTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
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
				// Ignore duplicate key errors
			}
		}

		return nil
	})
}

func (t *JoinOnReadTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	histogram := hdrhistogram.New(1, 10000, 3)
	deadline := time.Now().Add(duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				startTime := time.Now()
				userID := fmt.Sprintf("user%d", time.Now().UnixNano()%NumUsers)
				rows, err := db.QueryContext(ctx, "SELECT p.id, p.content, p.created_at FROM posts p JOIN follows f ON p.user_id = f.followee_id WHERE f.follower_id = $1 ORDER BY p.created_at DESC LIMIT 10", userID)
				if err != nil {
					continue
				}
				rows.Close()
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
		return nil
	})
}
