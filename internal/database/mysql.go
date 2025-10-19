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
