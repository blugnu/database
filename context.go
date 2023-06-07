package database

import "context"

type key int

const (
	transactionKey key = iota
)

// ContextWithTransaction adds a transaction to a context.
func ContextWithTransaction(ctx context.Context, tx Transaction) context.Context {
	return context.WithValue(ctx, transactionKey, tx)
}

// TransactionFromContext returns a transaction from a context.
func TransactionFromContext(ctx context.Context) Transaction {
	if tx := ctx.Value(transactionKey); tx != nil {
		return tx.(Transaction)
	}
	return nil
}
