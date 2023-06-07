package database

import (
	"context"
	"database/sql"
)

type Connector interface {
	ConnectionString() string
	Driver() string
}

type TransactMethod interface {
	Transact(context.Context, string, func(Transaction) error, *sql.TxOptions) error
}

type TransactionMethods interface {
	Exec(context.Context, string, ...any) (sql.Result, error)
	Prepare(context.Context, string) (*sql.Stmt, error)
	Query(context.Context, string, ...any) (*sql.Rows, error)
	QueryRow(context.Context, string, ...any) (*sql.Row, error)
}

type Transaction interface {
	TransactionMethods
	Statement(context.Context, *sql.Stmt) *sql.Stmt
}

type Connection interface {
	Ping(context.Context) error
	TransactionMethods
	TransactMethod
}
