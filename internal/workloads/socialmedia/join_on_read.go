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
	NumUsers    = 1000
	NumPosts    = 10000
	NumFollows  = 100000
	PostsPerUser = NumPosts / NumUsers
	FollowsPerUser = NumFollows / NumUsers
)

type JoinOnReadTest struct{}

func (t *JoinOnReadTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
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
				rows, err := db.QueryContext(ctx, "posts", bson.M{"user_id": bson.M{"$in": getFolloweeIDs(ctx, db, userID)}})
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
		ctx = context.WithValue(ctx, "tx", tx)
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
		return nil
	})
}

func getFolloweeIDs(ctx context.Context, db database.DatabaseDriver, userID string) []string {
	rows, err := db.QueryContext(ctx, "follows", bson.M{"follower_id": userID})
	if err != nil {
		return []string{}
	}
	defer rows.Close()

	var followeeIDs []string
	for rows.Next() {
		var follow struct {
			FolloweeID string `bson:"followee_id"`
		}
		if err := rows.Scan(&follow); err != nil {
			continue
		}
		followeeIDs = append(followeeIDs, follow.FolloweeID)
	}

	return followeeIDs
}
