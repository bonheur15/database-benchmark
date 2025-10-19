package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

type CatalogFilterTest struct{}

func (t *CatalogFilterTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	if _, ok := db.(*database.MongoDriver); ok {
		// MongoDB does not use SQL schemas, collections are created implicitly
		return db.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			for i := 0; i < 100; i++ {
				productID := uuid.New().String()
				_, err := db.ExecContext(ctx, "products", bson.M{"_id": productID, "name": fmt.Sprintf("product-%d", i), "inventory": 100})
				if err != nil {
					return err
				}
				for j := 0; j < rand.Intn(10); j++ {
					orderID := uuid.New().String()
					userID := uuid.New().String()
					_, err = db.ExecContext(ctx, "orders", bson.M{"_id": orderID, "user_id": userID, "created_at": time.Now()})
					if err != nil {
						return err
					}
					orderItemID := uuid.New().String()
					_, err = db.ExecContext(ctx, "order_items", bson.M{"_id": orderItemID, "order_id": orderID, "product_id": productID, "quantity": 1})
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		_, err := db.ExecContext(ctx, GetProductSchema())
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, GetOrdersSchema())
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, GetOrderItemsSchema())
		if err != nil {
			return err
		}

		for i := 0; i < 100; i++ {
			productID := uuid.New().String()
			_, err := db.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", productID, fmt.Sprintf("product-%d", i), 100)
			if err != nil {
				return err
			}
			for j := 0; j < rand.Intn(10); j++ {
				orderID := uuid.New().String()
				userID := uuid.New().String()
				_, err = db.ExecContext(ctx, "INSERT INTO orders (id, user_id, created_at) VALUES ($1, $2, $3)", orderID, userID, time.Now())
				if err != nil {
					return err
				}
				orderItemID := uuid.New().String()
				_, err = db.ExecContext(ctx, "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES ($1, $2, $3, $4)", orderItemID, orderID, productID, 1)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
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
				if _, ok := db.(*database.MongoDriver); ok {
					rows, err := db.QueryContext(ctx, "products", bson.M{"order_items": bson.M{"$size": bson.M{"$gt": 5}}})
					if err != nil {
						continue
					}
					rows.Close()
				} else {
					rows, err := db.QueryContext(ctx, "SELECT p.id FROM products p JOIN order_items oi ON p.id = oi.product_id GROUP BY p.id HAVING COUNT(oi.id) > 5")
					if err != nil {
						continue
					}
					rows.Close()
				}
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
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "order_items", bson.M{})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "orders", bson.M{})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "products", bson.M{})
			if err != nil {
				return err
			}
		} else {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS order_items")
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS orders")
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS products")
			if err != nil {
				return err
			}
		}
		return nil
	})
}