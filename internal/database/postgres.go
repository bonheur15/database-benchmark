package database

import (
	"context"

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

func (pd *PostgresDriver) ExecuteTx(ctx context.Context, txFunc func(interface{}) error) error {
	tx, err := pd.conn.Begin(ctx)
	if err != nil {
		return err
	}

	if err := txFunc(tx); err != nil {
		// Rollback but return the original error, not the rollback error
		_ = tx.Rollback(ctx)
		return err
	}

	return tx.Commit(ctx)
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
