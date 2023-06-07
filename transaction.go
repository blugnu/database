package database

import (
	"context"
	"database/sql"
)

type transaction struct {
	*sql.Tx
}

// Exec is a wrapper around Tx.ExecContext
func (tx *transaction) Exec(ctx context.Context, sql string, args ...any) (sql.Result, error) {
	return tx.ExecContext(ctx, sql, args...)
}

// Prepare is a wrapper around Tx.PrepareContext
func (tx *transaction) Prepare(ctx context.Context, sql string) (*sql.Stmt, error) {
	return tx.PrepareContext(ctx, sql)
}

// Query is a wrapper around Tx.QueryContext
func (tx *transaction) Query(ctx context.Context, sql string, args ...any) (*sql.Rows, error) {
	return tx.QueryContext(ctx, sql, args...)
}

// QueryRow is a wrapper around Tx.QueryRowContext
func (tx *transaction) QueryRow(ctx context.Context, sq string, args ...any) (*sql.Row, error) {
	row := tx.QueryRowContext(ctx, sq, args...)
	return row, row.Err()
}

// Statement is a wrapper around Tx.StmtContext
func (tx *transaction) Statement(ctx context.Context, stmt *sql.Stmt) *sql.Stmt {
	return tx.StmtContext(ctx, stmt)
}
