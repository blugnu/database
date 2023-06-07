package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func Test_singleconnector(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	db := &sql.DB{}
	operr := errors.New("operation err")
	var calledWithDB *sql.DB
	op := func(db *sql.DB) error { calledWithDB = db; return operr }

	sut := &noretry{&connection{db: db}}

	// ACT
	err := sut.try(ctx, op)

	// ASSERT
	t.Run("calls op func", func(t *testing.T) {
		wanted := operr
		got := err
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}

		t.Run("with connected db", func(t *testing.T) {
			wanted := db
			got := calledWithDB
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}

func Test_multiconnector(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when operation returns an error which is not bad connection", func(t *testing.T) {
		// ARRANGE
		sqlerr := errors.New("sql error")

		sut := &retry{&connection{db: &sql.DB{}}}

		// ACT
		err := sut.try(ctx, func(*sql.DB) error { return sqlerr })

		// ASSERT
		assertExpectedError(t, sqlerr, err)
	})

	t.Run("when all connections are bad", func(t *testing.T) {
		// ARRANGE
		db, sut := arrangeMultipleBadConnections()
		defer db.Close()

		// ACT
		err := sut.try(ctx, func(*sql.DB) error { return driver.ErrBadConn })

		// ASSERT
		assertExpectedError(t, ConnectionFailedError{}, err)
		assertExpectedError(t, driver.ErrBadConn, err)
	})

	t.Run("when current connection is bad", func(t *testing.T) {
		badcnc := MockBadConnection()

		db, mockdb, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer db.Close()

		mockdb.ExpectPing()
		mockdb.ExpectExec("update foo set bar = 1").WillReturnResult(sqlmock.NewResult(0, 1))
		defer assertExpectationsMet(t, mockdb)

		cnc := &connection{
			connectors: []Connector{
				MockConnector("bad"),
				MockConnector("good"),
			},
			mru: 0,
			db:  badcnc,
			open: func(string, string) (*sql.DB, error) {
				return db, nil
			},
		}
		cnc.connect = cnc.connectany

		sut := &retry{cnc}

		gotBadConnection := false

		// ACT
		err = sut.try(ctx, func(db *sql.DB) error {
			_, err := db.Exec("update foo set bar = 1")
			if errors.Is(err, driver.ErrBadConn) {
				gotBadConnection = true
			}
			return err
		})

		// ASSERT
		assertErrorIsNil(t, err)

		t.Run("got bad connection", func(t *testing.T) {
			wanted := true
			got := gotBadConnection
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}
