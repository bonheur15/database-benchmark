package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"errors"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/bson"
)

type InventoryUpdateTest struct{}

func (t *InventoryUpdateTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	if _, ok := db.(*database.MongoDriver); ok {
		// MongoDB does not use SQL schemas, collections are created implicitly
		// Seed a product for MongoDB
		return db.ExecuteTx(ctx, func(tx interface{}) error {
			ctx = context.WithValue(ctx, "tx", tx)
			_, err := db.ExecContext(ctx, "products", bson.M{"_id": "product1", "name": "test product", "inventory": 100})
			return err
		})
	}

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
				err := db.ExecuteTx(ctx, func(tx interface{}) error {
					ctx := context.WithValue(ctx, "tx", tx)
					if _, ok := db.(*database.MongoDriver); ok {
						_, err := db.ExecContext(ctx, "products", bson.M{"_id": "product1", "inventory": bson.M{"$gt": 0}}, bson.M{"$inc": bson.M{"inventory": -1}})
						return err
					} else {
						_, err := db.ExecContext(ctx, "UPDATE products SET inventory = inventory - 1 WHERE id = $1 AND inventory > 0", "product1")
						return err
					}
				})
				if err != nil {
					// Retry on serialization errors or other transient transaction errors.
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) {
						continue
					}
					var mysqlErr *mysql.MySQLError
					if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1213 || mysqlErr.Number == 1205) {
						continue
					}
				}
			}
		}()
	}

	wg.Wait()

	totalTime := time.Since(startTime)

	// Verify that the final inventory is 0
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
		DataIntegrity: inventory == 0,
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