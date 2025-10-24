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

	"github.com/jackc/pgx/v5"
)

type InventoryUpdateTest struct{}

type Tx interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

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
		var sqlTx Tx
		if pgxTx, ok := tx.(pgx.Tx); ok {
			sqlTx = &pgxTxAdapter{pgxTx}
		} else if genericTx, ok := tx.(Tx); ok {
			sqlTx = genericTx
		} else {
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
		var sqlTx Tx
		if pgxTx, ok := tx.(pgx.Tx); ok {
			sqlTx = &pgxTxAdapter{pgxTx}
		} else if genericTx, ok := tx.(Tx); ok {
			sqlTx = genericTx
		} else {
			return fmt.Errorf("unexpected transaction type: %T", tx)
		}
		query := "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)"
		if _, ok := db.(*database.MySQLDriver); ok {
			query = "INSERT INTO products (id, name, inventory) VALUES (?, ?, ?)"
		}
		_, err := sqlTx.ExecContext(ctx, query, "product1", "test product", 10000)
		return err
	})
}

type pgxTxAdapter struct {
	pgx.Tx
}

func (a *pgxTxAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tag, err := a.Tx.Exec(ctx, query, args...)
	return pgxResult{tag}, err
}

type pgxResult struct {
	pgconn.CommandTag
}

func (r pgxResult) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId not supported by pgx")
}

func (r pgxResult) RowsAffected() (int64, error) {
	return r.CommandTag.RowsAffected(), nil
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

						var sqlTx Tx
						var ok bool
						if sqlTx, ok = tx.(Tx); !ok {
							if pgxTx, pgxOK := tx.(pgx.Tx); pgxOK {
								sqlTx = &pgxTxAdapter{pgxTx}
							} else {
								return fmt.Errorf("unexpected transaction type: %T", tx)
							}
						}
						query := "UPDATE products SET inventory = inventory - 1 WHERE id = $1 AND inventory > 0"
						if _, ok := db.(*database.MySQLDriver); ok {
							query = "UPDATE products SET inventory = inventory - 1 WHERE id = ? AND inventory > 0"
						}
						res, err := sqlTx.ExecContext(runCtx, query, "product1")
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
		query := "SELECT inventory FROM products WHERE id = $1"
		if _, ok := db.(*database.MySQLDriver); ok {
			query = "SELECT inventory FROM products WHERE id = ?"
		}
		row := db.QueryRowContext(ctx, query, "product1")
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
		var sqlTx Tx
		if pgxTx, ok := tx.(pgx.Tx); ok {
			sqlTx = &pgxTxAdapter{pgxTx}
		} else if genericTx, ok := tx.(Tx); ok {
			sqlTx = genericTx
		} else {
			return fmt.Errorf("unexpected transaction type: %T", tx)
		}
		_, err := sqlTx.ExecContext(ctx, "DROP TABLE IF EXISTS products")
		return err
	})
}
