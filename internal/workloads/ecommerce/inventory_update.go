package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"sync"
	"time"
)

type InventoryUpdateTest struct{}

func (t *InventoryUpdateTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		_, err := db.ExecContext(ctx, GetProductSchema())
		if err != nil {
			return err
		}
		// Try to insert a product, but ignore the error if it already exists.
		_, _ = db.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", "product1", "test product", 100)
		return nil
	})
}

func (t *InventoryUpdateTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(startTime) < duration {
				db.ExecuteTx(ctx, func(tx interface{}) error {
					ctx = context.WithValue(ctx, "tx", tx)
					_, err := db.ExecContext(ctx, "UPDATE products SET inventory = inventory - 1 WHERE id = $1 AND inventory > 0", "product1")
					return err
				})
			}
		}()
	}

	wg.Wait()

	totalTime := time.Since(startTime)

	// Verify that the final inventory is 0
	var inventory int
	row := db.QueryRowContext(ctx, "SELECT inventory FROM products WHERE id = $1", "product1")
	if err := row.Scan(&inventory); err != nil {
		return nil, err
	}

	result := &database.Result{
		TotalTime:     totalTime,
		DataIntegrity: inventory == 0,
	}

	return result, nil
}

func (t *InventoryUpdateTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		ctx = context.WithValue(ctx, "tx", tx)
		_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS products")
		return err
	})
}
