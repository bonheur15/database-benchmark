package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDriver struct {
	db  *sql.DB
	dsn string
}

func (md *MySQLDriver) Connect(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	md.db = db
	md.dsn = dsn
	return nil
}

func (md *MySQLDriver) Close() error {
	return md.db.Close()
}

func (md *MySQLDriver) Reset(ctx context.Context) error {
	rows, err := md.db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
	if err != nil {
		return err
	}
	defer rows.Close()

	// Disable foreign key checks to avoid errors when dropping tables with dependencies.
	_, err = md.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		return err
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		_, err = md.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		if err != nil {
			return err
		}
	}

	// Re-enable foreign key checks.
	_, err = md.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1")
	if err != nil {
		return err
	}

	return nil
}

func (md *MySQLDriver) ExecuteTx(ctx context.Context, txFunc func(interface{}) error) (err error) {
	tx, err := md.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-panic after rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error, update err
		}
	}()

	err = txFunc(tx)
	return err
}

func (md *MySQLDriver) ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	query = replacePlaceholders(query)
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
	query = replacePlaceholders(query)
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
	query = replacePlaceholders(query)
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return md.db.QueryRowContext(ctx, query, args...)
}

func replacePlaceholders(query string) string {
	// Replace $1, $2, etc. with ?
	// This is a simple implementation that might not cover all edge cases,
	// but it should work for the queries in this benchmark.
	for i := 1; ; i++ {
		placeholder := fmt.Sprintf("$%d", i)
		if !strings.Contains(query, placeholder) {
			break
		}
		query = strings.Replace(query, placeholder, "?", 1)
	}
	return query
}
