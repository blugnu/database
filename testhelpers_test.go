package database

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// arrangeMultipleBadConnections sets up a connection with multiple connectors
// where each connector is a bad connection.  It returns the mock database and
// the connection.
func arrangeMultipleBadConnections() (*sql.DB, *connection) {
	db := MockBadConnection()

	sut := &connection{
		connectors: []Connector{
			MockConnector("a bad connection"),
			MockConnector("another bad connection"),
		},
		open: func(string, string) (*sql.DB, error) { return db, nil },
		db:   db,
		mru:  0,
	}
	sut.connect = sut.connectany
	sut.trymethod = &retry{sut}

	return db, sut
}

// arrangeTransactionMethodTest sets up a connection with a mock database using
// the noretry try method.  It returns the connection, the mock database, and
// the sqlmock.Sqlmock.
//
// This helper is used in the arrange phase of tests for the methods of the
// connection type that implement the TransactionMethods interface.
func arrangeTransactionMethodTest(setup func(sqlmock.Sqlmock)) (*connection, *sql.DB, sqlmock.Sqlmock) {
	db, dbmock, _ := sqlmock.New()
	setup(dbmock)

	sut := &connection{
		db: db,
	}
	sut.trymethod = &noretry{sut}

	return sut, db, dbmock
}

// arrangeTransactionTest initialises a sqlmock database which is configured
// to expect a transaction to be started.  Additional mock expectations may be
// configured by passing a setup function which accepts the mock.
//
// After calling the setup function, a transaction is started on the mock
// database.  The function then returns the context, database, a transaction
// and the mock database.
//
// This helper is used in the arrange phase of tests for the methods of the
// transaction type.
func arrangeTransactionTest(t *testing.T, setup func(mock sqlmock.Sqlmock)) (context.Context, *sql.DB, *transaction, sqlmock.Sqlmock) {
	ctx := context.Background()

	db, dbmock, _ := sqlmock.New()
	dbmock.ExpectBegin()
	setup(dbmock)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	return ctx, db, &transaction{tx}, dbmock
}

func assertExecResult(t *testing.T, wanted, got sql.Result) {
	t.Run("returns expected result", func(t *testing.T) {
		if wanted == nil {
			if wanted != got {
				t.Errorf("\nwanted %v\ngot    %v", wanted, got)
			}
			return
		}

		wantedLastInsertID, _ := wanted.LastInsertId()
		wantedRowsAffected, _ := wanted.RowsAffected()

		gotLastInsertID, _ := got.LastInsertId()
		gotRowsAffected, _ := got.RowsAffected()

		if wantedLastInsertID != gotLastInsertID || wantedRowsAffected != gotRowsAffected {
			t.Errorf("\nwanted\n  last insert id: %d\n  rows affected : %d\ngot\n  last insert id: %d\n  rows affected : %d",
				wantedLastInsertID, wantedRowsAffected, gotLastInsertID, gotRowsAffected)
		}
	})
}

func assertErrorIsNil(t *testing.T, err error) {
	t.Run("returns expected error", func(t *testing.T) {
		wanted := (error)(nil)
		got := err
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func assertExpectedError(t *testing.T, wanted error, got error) {
	t.Run("returns expected error", func(t *testing.T) {
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func assertExpectationsMet(t *testing.T, mock sqlmock.Sqlmock) {
	t.Run("mock expectations were met", func(t *testing.T) {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet expectations: %v", err)
		}
	})
}
