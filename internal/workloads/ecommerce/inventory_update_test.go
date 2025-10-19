package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"time"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
)

type InventoryUpdateTest struct{}

func (t *InventoryUpdateTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	productID := uuid.New().String()
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, GetProductSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", productID, "test_product", 100)
			return err
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS products (id VARCHAR(255) PRIMARY KEY,name VARCHAR(255) NOT NULL,inventory INT NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES (?, ?, ?)", productID, "test_product", 100)
			return err
		case mongo.SessionContext:
			_, err := tx.Client().Database("benchmarkdb").Collection("products").InsertOne(ctx, bson.M{"_id": productID, "name": "test_product", "inventory": 100})
			return err
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
	}

	return db.ExecuteTx(ctx, txFunc)
}

func (t *InventoryUpdateTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			txFunc := func(tx interface{}) error {
				switch tx := tx.(type) {
				case pgx.Tx:
					_, err := tx.Exec(ctx, "UPDATE products SET inventory = inventory - 1 WHERE inventory > 0")
					return err
				case *sql.Tx:
					_, err := tx.ExecContext(ctx, "UPDATE products SET inventory = inventory - 1 WHERE inventory > 0")
					return err
				case mongo.SessionContext:
					_, err := tx.Client().Database("benchmarkdb").Collection("products").UpdateOne(ctx, bson.M{"inventory": bson.M{"$gt": 0}}, bson.M{"$inc": bson.M{"inventory": -1}})
					return err
				default:
					return fmt.Errorf("unsupported transaction type: %T", tx)
				}
			}
			db.ExecuteTx(ctx, txFunc)
		}()
	}

	wg.Wait()

	totalTime := time.Since(startTime)

	// Verify that the final inventory is 0
	var inventory int
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			return tx.QueryRow(ctx, "SELECT inventory FROM products").Scan(&inventory)
		case *sql.Tx:
			return tx.QueryRowContext(ctx, "SELECT inventory FROM products").Scan(&inventory)
		case mongo.SessionContext:
			var result struct{ Inventory int }
			err := tx.Client().Database("benchmarkdb").Collection("products").FindOne(ctx, bson.M{}).Decode(&result)
			if err != nil {
				return err
			}
			inventory = result.Inventory
			return nil
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
	}

	if err := db.ExecuteTx(ctx, txFunc); err != nil {
		return nil, err
	}

	result := &database.Result{
		TotalTime:     totalTime,
		DataIntegrity: inventory == 0,
	}

	return result, nil
}

func (t *InventoryUpdateTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, "DROP TABLE products")
			return err
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "DROP TABLE products")
			return err
		case mongo.SessionContext:
			return tx.Client().Database("benchmarkdb").Collection("products").Drop(ctx)
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
	}

	return db.ExecuteTx(ctx, txFunc)
}