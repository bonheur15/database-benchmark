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
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		_, err = pd.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
		if err != nil {
			return err
		}
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
			tx.Rollback(ctx)
			panic(p) // re-panic after rollback
		} else if err != nil {
			tx.Rollback(ctx) // err is non-nil; don't change it
		} else {
			err = tx.Commit(ctx) // err is nil; if Commit returns error, update err
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
