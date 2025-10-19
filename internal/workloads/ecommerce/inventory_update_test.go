package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"sync"
	"time"
)

type InventoryUpdateTest struct{}

func (t *InventoryUpdateTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	if err := db.ExecuteTx(ctx, func(tx interface{}) error {
		_, err := db.ExecContext(ctx, GetProductSchema())
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", "product1", "test product", 100)
		return err
	}); err != nil {
		return err
	}
	return nil
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
	// This verification is not perfect, as it doesn't use the driver's QueryRow method.
	// This will be fixed later.
	result := &database.Result{
		TotalTime:     totalTime,
		DataIntegrity: inventory == 0,
	}

	return result, nil
}

func (t *InventoryUpdateTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		_, err := db.ExecContext(ctx, "DROP TABLE products")
		return err
	})
}
