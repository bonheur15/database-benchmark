package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"time"

	"github.com/google/uuid"
)

type OrderProcessingTest struct{}

func (t *OrderProcessingTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	if err := db.ExecuteTx(ctx, func(tx interface{}) error {
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

	// Seed a product
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		_, err := db.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", "product1", "test product", 100)
		return err
	})
}

func (t *OrderProcessingTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	result := &database.Result{}
	start := time.Now()

	for time.Since(start) < duration {
		err := db.ExecuteTx(ctx, func(tx interface{}) error {
			orderID := uuid.New().String()
			userID := uuid.New().String()
			_, err := db.ExecContext(ctx, "INSERT INTO orders (id, user_id, created_at) VALUES ($1, $2, $3)", orderID, userID, time.Now())
			if err != nil {
				return err
			}

			orderItemID := uuid.New().String()
			_, err = db.ExecContext(ctx, "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES ($1, $2, 'product1', 1)", orderItemID, orderID)
			if err != nil {
				return err
			}

			paymentID := uuid.New().String()
			_, err = db.ExecContext(ctx, "INSERT INTO payments (id, order_id, amount) VALUES ($1, $2, 10.50)", paymentID, orderID)
			if err != nil {
				return err
			}

			_, err = db.ExecContext(ctx, "UPDATE products SET inventory = inventory - 1 WHERE id = $1 AND inventory > 0", "product1")
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			result.Errors++
		} else {
			result.Operations++
		}
	}

	result.Throughput = float64(result.Operations) / duration.Seconds()
	return result, nil
}

func (t *OrderProcessingTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	if err := db.ExecuteTx(ctx, func(tx interface{}) error {
		_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS products")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS orders")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS order_items")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS payments")
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}