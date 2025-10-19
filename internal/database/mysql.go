package database

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDriver struct {
	db *sql.DB
}

func (md *MySQLDriver) Connect(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	md.db = db
	return nil
}

func (md *MySQLDriver) Close() error {
	return md.db.Close()
}

func (md *MySQLDriver) ExecuteTx(ctx context.Context, txFunc func(interface{}) error) error {
	tx, err := md.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := txFunc(tx); err != nil {
		return tx.Rollback()
	}

	return tx.Commit()
}

func (md *MySQLDriver) ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.ExecContext(ctx, query, args...)
	}
	return md.db.ExecContext(ctx, query, args...)
}

type MySQLRows struct {
	*sql.Rows
}

func (r *MySQLRows) Close() {
	r.Rows.Close()
}

func (md *MySQLDriver) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &MySQLRows{rows}, nil
	}
	rows, err := md.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &MySQLRows{rows}, nil
}

func (md *MySQLDriver) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return md.db.QueryRowContext(ctx, query, args...)
}