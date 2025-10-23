package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDriver struct {
	pool *pgxpool.Pool
}

func (pd *PostgresDriver) Connect(dsn string) error {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return err
	}
	pd.pool = pool
	return nil
}

func (pd *PostgresDriver) Close() error {
	pd.pool.Close()
	return nil
}

func (pd *PostgresDriver) Reset(ctx context.Context) error {
	rows, err := pd.pool.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
	if err != nil {
		return err
	}

	// Collect all table names first
	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			rows.Close()
			return err
		}
		tableNames = append(tableNames, tableName)
	}
	rows.Close()

	// Check for any errors during iteration
	if err := rows.Err(); err != nil {
		return err
	}

	// Now drop all tables
	for _, tableName := range tableNames {
		_, err = pd.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
		if err != nil {
			return err
		}
		fmt.Println("Dropped table:", tableName)
	}

	return nil
}

func (pd *PostgresDriver) ExecuteTx(ctx context.Context, txFunc func(interface{}) error) (err error) {
	tx, err := pd.pool.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			_ = tx.Rollback(ctx) // Ignore rollback errors
		} else {
			err = tx.Commit(ctx)
		}
	}()

	err = txFunc(tx)
	return err
}

func (pd *PostgresDriver) ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	if tx, ok := ctx.Value("tx").(pgx.Tx); ok {
		return tx.Exec(ctx, query, args...)
	}
	return pd.pool.Exec(ctx, query, args...)
}

func (pd *PostgresDriver) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	if tx, ok := ctx.Value("tx").(pgx.Tx); ok {
		return tx.Query(ctx, query, args...)
	}
	return pd.pool.Query(ctx, query, args...)
}

func (pd *PostgresDriver) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	if tx, ok := ctx.Value("tx").(pgx.Tx); ok {
		return tx.QueryRow(ctx, query, args...)
	}
	return pd.pool.QueryRow(ctx, query, args...)
}
