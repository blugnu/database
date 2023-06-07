package database

import (
	"context"
	"testing"
)

func TestContextWithTransaction(t *testing.T) {
	// ARRANGE
	bg := context.Background()
	tx := &transaction{}

	// ACT
	ctx := ContextWithTransaction(bg, tx)

	// ASSERT
	t.Run("adds transaction to context", func(t *testing.T) {
		wanted := tx
		got := ctx.Value(transactionKey).(*transaction)
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestTransactionFromContext(t *testing.T) {
	// ARRANGE
	bg := context.Background()
	tx := &transaction{}
	ctx := context.WithValue(bg, transactionKey, tx)

	t.Run("returns transaction from context", func(t *testing.T) {
		// ACT
		result := TransactionFromContext(ctx)

		// ASSERT
		wanted := tx
		got := result
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("returns nil when context does not contain transaction", func(t *testing.T) {
		// ACT
		result := TransactionFromContext(bg)

		// ASSERT
		wanted := (Transaction)(nil)
		got := result
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}
