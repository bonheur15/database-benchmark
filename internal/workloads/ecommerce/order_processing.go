package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

type OrderProcessingTest struct{}

func (t *OrderProcessingTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
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
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		_, err := db.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", "product1", "test product", 100)
		return err
	})
}

func (t *OrderProcessingTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	result := &database.Result{}
	start := time.Now()

	for time.Since(start) < duration {
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
	}); err != nil {
		return err
	}
	return nil
}
