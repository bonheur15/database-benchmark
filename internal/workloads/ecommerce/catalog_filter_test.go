package ecommerce

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

type CatalogFilterTest struct{}

func (t *CatalogFilterTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, GetProductSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, GetOrdersSchema())
			if err != nil {
				return err
			}
			for i := 0; i < 100; i++ {
				productID := uuid.New().String()
				_, err := tx.Exec(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", productID, fmt.Sprintf("product-%d", i), 100)
				if err != nil {
					return err
				}
				for j := 0; j < rand.Intn(10); j++ {
					orderID := uuid.New().String()
					userID := uuid.New().String()
					_, err := tx.Exec(ctx, "INSERT INTO orders (id, user_id, created_at) VALUES ($1, $2, $3)", orderID, userID, time.Now())
					if err != nil {
						return err
					}
				}
			}
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS products (id VARCHAR(255) PRIMARY KEY,name VARCHAR(255) NOT NULL,inventory INT NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS orders (id VARCHAR(255) PRIMARY KEY,user_id VARCHAR(255) NOT NULL,created_at TIMESTAMP NOT NULL);
")
			if err != nil {
				return err
			}
			for i := 0; i < 100; i++ {
				productID := uuid.New().String()
				_, err := tx.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES (?, ?, ?)", productID, fmt.Sprintf("product-%d", i), 100)
				if err != nil {
					return err
				}
				for j := 0; j < rand.Intn(10); j++ {
					orderID := uuid.New().String()
					userID := uuid.New().String()
					_, err := tx.ExecContext(ctx, "INSERT INTO orders (id, user_id, created_at) VALUES (?, ?, ?)", orderID, userID, time.Now())
					if err != nil {
						return err
					}
				}
			}
		case mongo.SessionContext:
			for i := 0; i < 100; i++ {
				productID := uuid.New().String()
				orders := []bson.M{}
				for j := 0; j < rand.Intn(10); j++ {
					orders = append(orders, bson.M{"_id": uuid.New().String(), "user_id": uuid.New().String(), "created_at": time.Now()})
				}
				_, err := tx.Client().Database("benchmarkdb").Collection("products").InsertOne(ctx, bson.M{"_id": productID, "name": fmt.Sprintf("product-%d", i), "inventory": 100, "orders": orders})
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

func (t *CatalogFilterTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
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
						rows, err := tx.Query(ctx, "SELECT p.id FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.id HAVING COUNT(o.id) > 5")
						if err != nil {
							return err
						}
						defer rows.Close()
					case *sql.Tx:
						rows, err := tx.QueryContext(ctx, "SELECT p.id FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.id HAVING COUNT(o.id) > 5")
						if err != nil {
							return err
						}
						defer rows.Close()
					case mongo.SessionContext:
						cursor, err := tx.Client().Database("benchmarkdb").Collection("products").Find(ctx, bson.M{"orders.5": bson.M{"$exists": true}})
						if err != nil {
							return err
						}
						defer cursor.Close(ctx)
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

func (t *CatalogFilterTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, "DROP TABLE products")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "DROP TABLE orders")
			return err
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "DROP TABLE products")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "DROP TABLE orders")
			return err
		case mongo.SessionContext:
			return tx.Client().Database("benchmarkdb").Collection("products").Drop(ctx)
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
	}

	return db.ExecuteTx(ctx, txFunc)
}