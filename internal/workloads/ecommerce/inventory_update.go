package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type InventoryUpdateTest struct{}

func (t *InventoryUpdateTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	fmt.Println("Starting setup")
	if mongoDriver, ok := db.(*database.MongoDriver); ok {
		return mongoDriver.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			// Drop collection if it exists to ensure a clean state
			mongoDriver.ExecContext(ctx, "products", bson.M{})
			_, err := mongoDriver.ExecContext(ctx, "products", bson.M{"_id": "product1", "name": "test product", "inventory": 10000})
			return err
		})
	}

	// For SQL, drop and recreate table
	if err := db.ExecuteTx(ctx, func(tx interface{}) error {
		sqlTx, ok := tx.(*sql.Tx)
		if !ok {
			return fmt.Errorf("unexpected transaction type: %T", tx)
		}
		_, err := sqlTx.ExecContext(ctx, "DROP TABLE IF EXISTS products")
		if err != nil {
			return err
		}
		_, err = sqlTx.ExecContext(ctx, GetProductSchema())
		return err
	}); err != nil {
		return err
	}

	fmt.Println("Schema created successfully")

	return db.ExecuteTx(ctx, func(tx interface{}) error {
		sqlTx, ok := tx.(*sql.Tx)
		if !ok {
			return fmt.Errorf("unexpected transaction type: %T", tx)
		}
		_, err := sqlTx.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES (?, ?, ?)", "product1", "test product", 10000)
		return err
	})
}

func (t *InventoryUpdateTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	startTime := time.Now()
	result := &database.Result{}
	var mu sync.Mutex

	// We'll use a context that can be canceled once the inventory reaches zero.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				default:
					err := db.ExecuteTx(runCtx, func(tx interface{}) error {
						if mongoDriver, ok := db.(*database.MongoDriver); ok {
							txCtx := context.WithValue(runCtx, "tx", tx)
							res, err := mongoDriver.ExecContext(txCtx, "products", bson.M{"_id": "product1", "inventory": bson.M{"$gt": 0}}, bson.M{"$inc": bson.M{"inventory": -1}})
							if err != nil {
								return err
							}
							if updateResult, ok := res.(*mongo.UpdateResult); ok && updateResult.ModifiedCount == 0 {
								return errors.New("inventory depleted")
							}
							return nil
						}
						sqlTx, ok := tx.(*sql.Tx)
						if !ok {
							return fmt.Errorf("unexpected transaction type: %T", tx)
						}
						res, err := sqlTx.ExecContext(runCtx, "UPDATE products SET inventory = inventory - 1 WHERE id = ? AND inventory > 0", "product1")
						if err != nil {
							return err
						}
						rowsAffected, err := res.RowsAffected()
						if err != nil {
							return err
						}
						if rowsAffected == 0 {
							return errors.New("inventory depleted")
						}
						return nil
					})

					mu.Lock()
					if err != nil {
						if err.Error() == "inventory depleted" {
							cancel()
							mu.Unlock()
							return
						}
						var pgErr *pgconn.PgError
						if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
							// Serialization error, just retry
						} else {
							result.Errors++
						}
					} else {
						result.Operations++
					}
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	result.TotalTime = time.Since(startTime)
	result.Throughput = float64(result.Operations) / result.TotalTime.Seconds()

	// Final integrity check
	var finalInventory int
	if _, ok := db.(*database.MongoDriver); ok {
		var product struct {
			Inventory int `bson:"inventory"`
		}
		row := db.QueryRowContext(ctx, "products", bson.M{"_id": "product1"})
		if err := row.Scan(&product); err != nil {
			return nil, fmt.Errorf("failed to read final inventory: %w", err)
		}
		finalInventory = product.Inventory
	} else {
		row := db.QueryRowContext(ctx, "SELECT inventory FROM products WHERE id = $1", "product1")
		if err := row.Scan(&finalInventory); err != nil {
			return nil, fmt.Errorf("failed to read final inventory: %w", err)
		}
	}

	result.DataIntegrity = finalInventory == 0
	if !result.DataIntegrity {
		fmt.Printf("Data integrity check failed: final inventory is %d, expected 0\n", finalInventory)
	}

	return result, nil
}

func (t *InventoryUpdateTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	fmt.Println("Teardown started")
	if mongoDriver, ok := db.(*database.MongoDriver); ok {
		return mongoDriver.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			_, err := mongoDriver.ExecContext(ctx, "products", bson.M{})
			return err
		})
	}
	return db.ExecuteTx(ctx, func(tx interface{}) error {
		sqlTx, ok := tx.(*sql.Tx)
		if !ok {
			return fmt.Errorf("unexpected transaction type: %T", tx)
		}
		_, err := sqlTx.ExecContext(ctx, "DROP TABLE IF EXISTS products")
		return err
	})
}
