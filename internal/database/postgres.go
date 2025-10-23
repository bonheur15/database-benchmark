package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type PostgresDriver struct {
	conn *pgx.Conn
}

func (pd *PostgresDriver) Connect(dsn string) error {
	conn, err := pgx.Connect(context.Background(), dsn)
	fmt.Print(err)
	if err != nil {
		return err
	}
	pd.conn = conn
	return nil
}

func (pd *PostgresDriver) Close() error {
	return pd.conn.Close(context.Background())
}

func (pd *PostgresDriver) Reset(ctx context.Context) error {
	rows, err := pd.conn.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
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
		_, err = pd.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
		if err != nil {
			return err
		}
		fmt.Println("Dropped table:", tableName)
	}

	return nil
}

func (pd *PostgresDriver) ExecuteTx(ctx context.Context, txFunc func(interface{}) error) (err error) {
	tx, err := pd.conn.Begin(ctx)
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
	return pd.conn.Exec(ctx, query, args...)
}

func (pd *PostgresDriver) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	if tx, ok := ctx.Value("tx").(pgx.Tx); ok {
		return tx.Query(ctx, query, args...)
	}
	return pd.conn.Query(ctx, query, args...)
}

func (pd *PostgresDriver) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	if tx, ok := ctx.Value("tx").(pgx.Tx); ok {
		return tx.QueryRow(ctx, query, args...)
	}
	return pd.conn.QueryRow(ctx, query, args...)
}
