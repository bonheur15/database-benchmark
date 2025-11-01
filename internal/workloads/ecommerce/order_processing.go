package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"log"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

type OrderProcessingTest struct{}

func (t *OrderProcessingTest) Setup(ctx context.Context, db database.DatabaseDriver, logger *log.Logger) error {
	logger.Println("Setting up OrderProcessingTest...")
	if _, ok := db.(*database.MongoDriver); ok {
		// MongoDB does not use SQL schemas, collections are created implicitly
		// Seed a product for MongoDB
		return db.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			_, err := db.ExecContext(ctx, "products", bson.M{"_id": "product1", "name": "test product", "inventory": 100})
			return err
		})
	}

	if err := db.ExecuteTx(ctx, func(tx interface{}) error {
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
		_, err = db.ExecContext(ctx, GetPaymentsSchema())
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Seed a product for SQL databases
	logger.Println("Seeding product for SQL databases...")
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		query := "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)"
		if _, ok := db.(*database.MySQLDriver); ok {
			query = "INSERT INTO products (id, name, inventory) VALUES (?, ?, ?)"
		}
		_, err := db.ExecContext(ctx, query, "product1", "test product", 100)
		return err
	})
}

func (t *OrderProcessingTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration, logger *log.Logger) (*database.Result, error) {
	result := &database.Result{}
	totalStartTime := time.Now()

	// Initialize HDR Histogram for latency measurements
	// Max latency of 10 seconds, significant figures of 3
	histogram := hdrhistogram.New(1, 10000000000, 3)

	for time.Since(totalStartTime) < duration {
		opStartTime := time.Now()
		err := db.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			orderID := uuid.New().String()
			userID := uuid.New().String()
			if _, ok := db.(*database.MongoDriver); ok {
				_, err := db.ExecContext(ctx, "orders", bson.M{"_id": orderID, "user_id": userID, "created_at": time.Now()})
				if err != nil {
					return err
				}

				orderItemID := uuid.New().String()
				_, err = db.ExecContext(ctx, "order_items", bson.M{"_id": orderItemID, "order_id": orderID, "product_id": "product1", "quantity": 1})
				if err != nil {
					return err
				}

				paymentID := uuid.New().String()
				_, err = db.ExecContext(ctx, "payments", bson.M{"_id": paymentID, "order_id": orderID, "amount": 10.50})
				if err != nil {
					return err
				}

				_, err = db.ExecContext(ctx, "products", bson.M{"_id": "product1", "inventory": bson.M{"$gt": 0}}, bson.M{"$inc": bson.M{"inventory": -1}})
				if err != nil {
					return err
				}
			} else {
				query := "INSERT INTO orders (id, user_id, created_at) VALUES ($1, $2, $3)"
				if _, ok := db.(*database.MySQLDriver); ok {
					query = "INSERT INTO orders (id, user_id, created_at) VALUES (?, ?, ?)"
				}
				_, err := db.ExecContext(ctx, query, orderID, userID, time.Now())
				if err != nil {
					return err
				}

				orderItemID := uuid.New().String()
				query = "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES ($1, $2, 'product1', 1)"
				if _, ok := db.(*database.MySQLDriver); ok {
					query = "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES (?, ?, 'product1', 1)"
				}
				_, err = db.ExecContext(ctx, query, orderItemID, orderID)
				if err != nil {
					return err
				}

				paymentID := uuid.New().String()
				query = "INSERT INTO payments (id, order_id, amount) VALUES ($1, $2, 10.50)"
				if _, ok := db.(*database.MySQLDriver); ok {
					query = "INSERT INTO payments (id, order_id, amount) VALUES (?, ?, 10.50)"
				}
				_, err = db.ExecContext(ctx, query, paymentID, orderID)
				if err != nil {
					return err
				}

				query = "UPDATE products SET inventory = inventory - 1 WHERE id = $1 AND inventory > 0"
				if _, ok := db.(*database.MySQLDriver); ok {
					query = "UPDATE products SET inventory = inventory - 1 WHERE id = ? AND inventory > 0"
				}
				_, err = db.ExecContext(ctx, query, "product1")
				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			result.Errors++
		} else {
			result.Operations++
			latency := time.Since(opStartTime)
			histogram.RecordValue(latency.Microseconds())
		}
	}

	result.TotalTime = time.Since(totalStartTime)
	result.Throughput = float64(result.Operations) / result.TotalTime.Seconds()
	result.AverageLatency = time.Duration(histogram.Mean()) * time.Microsecond
	result.P95Latency = time.Duration(histogram.ValueAtQuantile(95)) * time.Microsecond
	result.P99Latency = time.Duration(histogram.ValueAtQuantile(99)) * time.Microsecond

	return result, nil
}

func (t *OrderProcessingTest) Teardown(ctx context.Context, db database.DatabaseDriver, logger *log.Logger) error {
	if _, ok := db.(*database.MongoDriver); ok {
		// MongoDB drop collections
		return db.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			_, err := db.ExecContext(ctx, "order_items", bson.M{})
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "payments", bson.M{})
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
			return nil
		})
	}

	// SQL drop tables
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS order_items CASCADE")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS payments CASCADE")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS orders CASCADE")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS products CASCADE")
		return err
	})
}
