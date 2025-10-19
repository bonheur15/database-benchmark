package ecommerce

import (
	"context"
	"database-benchmark/internal/database"
	"fmt"
	"time"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
)

type OrderProcessingTest struct{}

func (t *OrderProcessingTest) Setup(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, GetProductSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, GetOrdersSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, GetOrderItemsSchema())
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, GetPaymentsSchema())
			if err != nil {
				return err
			}
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS products (id VARCHAR(255) PRIMARY KEY,name VARCHAR(255) NOT NULL,inventory INT NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS orders (id VARCHAR(255) PRIMARY KEY,user_id VARCHAR(255) NOT NULL,created_at TIMESTAMP NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS order_items (id VARCHAR(255) PRIMARY KEY,order_id VARCHAR(255) NOT NULL,product_id VARCHAR(255) NOT NULL,quantity INT NOT NULL);
")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS payments (id VARCHAR(255) PRIMARY KEY,order_id VARCHAR(255) NOT NULL,amount DECIMAL(10, 2) NOT NULL);
")
			if err != nil {
				return err
			}
		case mongo.SessionContext:
			// MongoDB schema is flexible, no need to create collections explicitly
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}

	return db.ExecuteTx(ctx, txFunc)
}

func (t *OrderProcessingTest) Run(ctx context.Context, db database.DatabaseDriver, concurrency int, duration time.Duration) (*database.Result, error) {
	var wg sync.WaitGroup
	var transactions uint64
	var errors uint64

	productID := uuid.New().String()

	// Insert a product to be updated
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, "INSERT INTO products (id, name, inventory) VALUES ($1, $2, $3)", productID, "test_product", 100000)
			return err
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "INSERT INTO products (id, name, inventory) VALUES (?, ?, ?)", productID, "test_product", 100000)
			return err
		case mongo.SessionContext:
			_, err := tx.Client().Database("benchmarkdb").Collection("products").InsertOne(ctx, bson.M{"_id": productID, "name": "test_product", "inventory": 100000})
			return err
		}
		return nil
	}
	if err := db.ExecuteTx(ctx, txFunc); err != nil {
		return nil, err
	}


	deadline := time.Now().Add(duration)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				orderID := uuid.New().String()
				userID := uuid.New().String()
				orderItemID := uuid.New().String()
				paymentID := uuid.New().String()

				txFunc := func(tx interface{}) error {
					switch tx := tx.(type) {
					case pgx.Tx:
						_, err := tx.Exec(ctx, "UPDATE products SET inventory = inventory - 1 WHERE id = $1", productID)
						if err != nil {
							return err
						}
						_, err = tx.Exec(ctx, "INSERT INTO orders (id, user_id, created_at) VALUES ($1, $2, $3)", orderID, userID, time.Now())
						if err != nil {
							return err
						}
						_, err = tx.Exec(ctx, "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES ($1, $2, $3, $4)", orderItemID, orderID, productID, 1)
						if err != nil {
							return err
						}
						_, err = tx.Exec(ctx, "INSERT INTO payments (id, order_id, amount) VALUES ($1, $2, $3)", paymentID, orderID, 10.0)
						if err != nil {
							return err
						}
					case *sql.Tx:
						_, err := tx.ExecContext(ctx, "UPDATE products SET inventory = inventory - 1 WHERE id = ?", productID)
						if err != nil {
							return err
						}
						_, err = tx.ExecContext(ctx, "INSERT INTO orders (id, user_id, created_at) VALUES (?, ?, ?)", orderID, userID, time.Now())
						if err != nil {
							return err
						}
						_, err = tx.ExecContext(ctx, "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES (?, ?, ?, ?)", orderItemID, orderID, productID, 1)
						if err != nil {
							return err
						}
						_, err = tx.ExecContext(ctx, "INSERT INTO payments (id, order_id, amount) VALUES (?, ?, ?)", paymentID, orderID, 10.0)
						if err != nil {
							return err
						}
					case mongo.SessionContext:
						// MongoDB does not support multi-document transactions in the same way as SQL databases.
						// We will simulate this by performing the operations sequentially.
						_, err := tx.Client().Database("benchmarkdb").Collection("products").UpdateOne(ctx, bson.M{"_id": productID}, bson.M{"$inc": bson.M{"inventory": -1}})
						if err != nil {
							return err
						}
						_, err = tx.Client().Database("benchmarkdb").Collection("orders").InsertOne(ctx, bson.M{
							"_id": orderID,
							"user_id": userID,
							"created_at": time.Now(),
							"items": []bson.M{
								{
									"product_id": productID,
									"quantity": 1,
								},
							},
							"payment": bson.M{
								"_id": paymentID,
								"amount": 10.0,
							},
						})
						if err != nil {
							return err
						}
					default:
						return fmt.Errorf("unsupported transaction type: %T", tx)
					}
					return nil
				}

				if err := db.ExecuteTx(ctx, txFunc); err != nil {
					atomic.AddUint64(&errors, 1)
				} else {
					atomic.AddUint64(&transactions, 1)
				}
			}
		}()
	}

	wg.Wait()

	result := &database.Result{
		Throughput: float64(transactions) / duration.Seconds(),
		ErrorRate:  float64(errors) / float64(transactions+errors),
	}

	return result, nil
}

func (t *OrderProcessingTest) Teardown(ctx context.Context, db database.DatabaseDriver) error {
	txFunc := func(tx interface{}) error {
		switch tx := tx.(type) {
		case pgx.Tx:
			_, err := tx.Exec(ctx, "DROP TABLE products")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "DROP TABLE orders")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "DROP TABLE order_items")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "DROP TABLE payments")
			if err != nil {
				return err
			}
		case *sql.Tx:
			_, err := tx.ExecContext(ctx, "DROP TABLE products")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "DROP TABLE orders")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "DROP TABLE order_items")
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, "DROP TABLE payments")
			if err != nil {
				return err
			}
		case mongo.SessionContext:
			err := tx.Client().Database("benchmarkdb").Collection("products").Drop(ctx)
			if err != nil {
				return err
			}
			return tx.Client().Database("benchmarkdb").Collection("orders").Drop(ctx)
		default:
			return fmt.Errorf("unsupported transaction type: %T", tx)
		}
		return nil
	}

	return db.ExecuteTx(ctx, txFunc)
}
