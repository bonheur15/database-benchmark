package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type InventoryUpdateTest struct{}

func (t *InventoryUpdateTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	if _, ok := db.(*database.MongoDriver); ok {
		return db.ExecuteTx(ctx, func(tx interface{}) error {
			txCtx := context.WithValue(ctx, "tx", tx)
			_, err := db.ExecContext(txCtx, "products", bson.M{
				"_id":       "product1",
				"name":      "test product",
				"inventory": 100,
			})
			return err
		})
	}

	return db.ExecuteTx(ctx, func(tx interface{}) error {
		// Create a context with the transaction
		txCtx := context.WithValue(ctx, "tx", tx)

		// Use db.ExecContext with the transaction context
		_, err := db.ExecContext(txCtx, GetProductSchema())
		if err != nil {
			return err
		}

		// Insert product, ignoring duplicate key errors
		_, err = db.ExecContext(txCtx,
			"INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)",
			"product6", "test product", 100)

		if err != nil && !isDuplicateKeyError(err) {
			return err
		}

		return nil
	})
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "duplicate key") ||
		strings.Contains(errStr, "UNIQUE constraint") ||
		strings.Contains(errStr, "already exists")
}

func (t *InventoryUpdateTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	startTime := time.Now()

	err := db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "products", bson.M{"_id": "product1", "inventory": bson.M{"$gt": 0}}, bson.M{"$inc": bson.M{"inventory": -1}})
			return err
		} else {
			_, err := db.ExecContext(ctx, "UPDATE products SET inventory = inventory - 1 WHERE id = $1 AND inventory > 0", "product1")
			return err
		}
	})

	if err != nil {
		return nil, err
	}

	totalTime := time.Since(startTime)

	// Verify that the final inventory is 99
	var inventory int
	if _, ok := db.(*database.MongoDriver); ok {
		var product struct {
			Inventory int `bson:"inventory"`
		}
		row := db.QueryRowContext(ctx, "products", bson.M{"_id": "product1"})
		if err := row.Scan(&product); err != nil {
			return nil, err
		}
		inventory = product.Inventory
	} else {
		row := db.QueryRowContext(ctx, "SELECT inventory FROM products WHERE id = $1", "product1")
		if err := row.Scan(&inventory); err != nil {
			return nil, err
		}
	}

	result := &database.Result{
		TotalTime:     totalTime,
		DataIntegrity: inventory == 99,
	}

	return result, nil
}

func (t *InventoryUpdateTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		if _, ok := db.(*database.MongoDriver); ok {
			_, err := db.ExecContext(ctx, "products", bson.M{})
			return err
		} else {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS products")
			return err
		}
	})
}
