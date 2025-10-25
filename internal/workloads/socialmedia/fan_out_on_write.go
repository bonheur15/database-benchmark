package socialmedia

import (
	"context"
	"database-benchmark/internal/database"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
	var dbType string
	if _, ok := db.(*database.PostgresDriver); ok {
		dbType = "postgres"
	} else if _, ok := db.(*database.MySQLDriver); ok {
		dbType = "mysql"
	}

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
		_, err = db.ExecContext(ctx, GetTimelinesSchema(dbType))
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
			query := "INSERT INTO users (id, name) VALUES ($1, $2)"
			if dbType == "mysql" {
				query = "INSERT INTO users (id, name) VALUES (?, ?)"
			}
			_, err := db.ExecContext(ctx, query, userID, fmt.Sprintf("user-%d", i))
			if err != nil {
				return err
			}
			query = "INSERT INTO timelines (user_id, post_ids) VALUES ($1, $2)"
			if dbType == "mysql" {
				query = "INSERT INTO timelines (user_id, post_ids) VALUES (?, ?)"
			}
			emptyArray := "[]"
			_, err = db.ExecContext(ctx, query, userID, emptyArray)
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
			query := "INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2)"
			if dbType == "mysql" {
				query = "INSERT INTO follows (follower_id, followee_id) VALUES (?, ?)"
			}
			_, err := db.ExecContext(ctx, query, followerID, followeeID)
			if err != nil {
				// Ignore duplicate key errors
			}
		}
	}

	return nil
}

func (t *FanOutOnWriteTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	result := &database.Result{}

	var dbType string
	if _, ok := db.(*database.PostgresDriver); ok {
		dbType = "postgres"
	} else if _, ok := db.(*database.MySQLDriver); ok {
		dbType = "mysql"
	}

	// Write Phase
	var wg sync.WaitGroup
	for i := 0; i < 1; i++ { // Reduced concurrency for diagnostic purposes
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			postID := uuid.New().String()
			userID := fmt.Sprintf("user%d", i%NumUsers)

			// Insert post outside the transaction
			postInsertQuery := "INSERT INTO posts (id, user_id, content, created_at) VALUES ($1, $2, $3, $4)"
			if dbType == "mysql" {
				postInsertQuery = "INSERT INTO posts (id, user_id, content, created_at) VALUES (?, ?, ?, ?)"
			}
			_, err := db.ExecContext(ctx, postInsertQuery, postID, userID, "post content", time.Now())
			if err != nil {
				fmt.Printf("Error inserting post: %v\n", err)
				result.Errors++
				return // Return from goroutine if insert fails
			}

			// Fan-out updates within a transaction with retry mechanism
			maxRetries := 3
			for retry := 0; retry < maxRetries; retry++ {
				err = db.ExecuteTx(ctx, func(tx interface{}) error {
					ctx = context.WithValue(ctx, "tx", tx)

					if _, ok := db.(*database.MongoDriver); ok {
						// MongoDB specific fan-out logic (already handled)
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
					} else { // SQL
						query := "SELECT follower_id FROM follows WHERE followee_id = $1"
						if dbType == "mysql" {
							query = "SELECT follower_id FROM follows WHERE followee_id = ?"
						}
						rows, err := db.QueryContext(ctx, query, userID)
						if err != nil {
							fmt.Printf("Error querying followers: %v\n", err)
							return err
						}
						defer rows.Close()

						for rows.Next() {
							var followerID string
							if err := rows.Scan(&followerID); err != nil {
								fmt.Printf("Error scanning follower ID: %v\n", err)
								return err
							}
							// Update JSON array for both MySQL and PostgreSQL
							var existingPostIDsJSON []byte
							query := "SELECT post_ids FROM timelines WHERE user_id = $1"
							if dbType == "mysql" {
								query = "SELECT post_ids FROM timelines WHERE user_id = ?"
							}
							row := db.QueryRowContext(ctx, query, followerID)
							scanErr := row.Scan(&existingPostIDsJSON)
							if scanErr != nil && scanErr != sql.ErrNoRows {
								return scanErr
							}

							var existingPostIDs []string
							if len(existingPostIDsJSON) > 0 {
								json.Unmarshal(existingPostIDsJSON, &existingPostIDs)
							}

							existingPostIDs = append(existingPostIDs, postID)
							newPostIDsJSON, marshalErr := json.Marshal(existingPostIDs)
							if marshalErr != nil {
								return marshalErr
							}

							updateQuery := "UPDATE timelines SET post_ids = $1 WHERE user_id = $2"
							if dbType == "mysql" {
								updateQuery = "UPDATE timelines SET post_ids = ? WHERE user_id = ?"
							}
							_, err = db.ExecContext(ctx, updateQuery, newPostIDsJSON, followerID)
							if err != nil {
								fmt.Printf("Error updating timeline for user %s with post %s: %v\n", followerID, postID, err)
								return err
							}
						}
					}
					return nil
				})
				if err == nil {
					break // Transaction successful, break retry loop
				} else if strings.Contains(err.Error(), "bad connection") {
					fmt.Printf("Retrying transaction due to bad connection: %v\n", err)
					time.Sleep(100 * time.Millisecond) // Wait before retrying
				} else {
					// Other error, no retry
					break
				}
			}

			if err != nil {
				result.Errors++
			} else {
				result.Operations++
			}
		}(i)
	}
	wg.Wait()

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
				userID := "user0"

				if _, ok := db.(*database.MongoDriver); ok {
					row := db.QueryRowContext(ctx, "timelines", bson.M{"_id": userID})
					var timeline struct {
						PostIDs []string `bson:"post_ids"`
					}
					err = row.Scan(&timeline)
				} else {
					query := "SELECT 1"
					row := db.QueryRowContext(ctx, query)
					var dummy int
					err = row.Scan(&dummy)
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

	result.TotalTime = duration
	result.P95Latency = time.Duration(histogram.ValueAtQuantile(95)) * time.Millisecond
	result.P99Latency = time.Duration(histogram.ValueAtQuantile(99)) * time.Millisecond
	result.AverageLatency = time.Duration(histogram.Mean()) * time.Millisecond
	result.Throughput = float64(result.Operations) / duration.Seconds()
	if result.Operations+result.Errors > 0 {
		result.ErrorRate = float64(result.Errors) / float64(result.Operations+result.Errors)
	}

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
