package database

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestTransaction_Exec(t *testing.T) {
	// ARRANGE
	ctx, db, sut, mock := arrangeTransactionTest(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectExec("update foo set bar = 1").WillReturnResult(sqlmock.NewResult(0, 1))
	})
	defer db.Close()
	defer assertExpectationsMet(t, mock)

	// ACT
	result, err := sut.Exec(ctx, "update foo set bar = 1")

	// ASSERT
	assertErrorIsNil(t, err)

	t.Run("returns expected result", func(t *testing.T) {
		lii, _ := result.LastInsertId()
		ra, _ := result.RowsAffected()
		wanted := true
		got := lii == 0 && ra == 1
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestTransaction_Prepare(t *testing.T) {
	// ARRANGE
	ctx, db, sut, mock := arrangeTransactionTest(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectPrepare("update foo set bar = 1")
	})
	defer db.Close()
	defer assertExpectationsMet(t, mock)

	// ACT
	result, err := sut.Prepare(ctx, "update foo set bar = 1")

	// ASSERT
	assertErrorIsNil(t, err)

	t.Run("returns a prepared statement", func(t *testing.T) {
		wanted := &sql.Stmt{}
		got := result
		if got == nil {
			t.Errorf("\nwanted %T\ngot    %#v", wanted, got)
		}
	})
}

func TestTransaction_Query(t *testing.T) {
	// ARRANGE
	qryerr := errors.New("query error")

	ctx, db, sut, mock := arrangeTransactionTest(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("select bar from foo").WillReturnError(qryerr)
	})
	defer db.Close()
	defer assertExpectationsMet(t, mock)

	// ACT
	_, err := sut.Query(ctx, "select bar from foo")

	// ASSERT
	assertExpectedError(t, qryerr, err)
}

func TestTransaction_QueryRow(t *testing.T) {
	// ARRANGE
	qryerr := errors.New("query error")

	ctx, db, sut, mock := arrangeTransactionTest(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("select bar from foo").WillReturnError(qryerr)
	})
	defer db.Close()
	defer assertExpectationsMet(t, mock)

	// ACT
	_, err := sut.QueryRow(ctx, "select bar from foo")

	// ASSERT
	assertExpectedError(t, qryerr, err)
}

func TestTransaction_Statement(t *testing.T) {
	// ARRANGE
	ctx, db, sut, mock := arrangeTransactionTest(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectPrepare("select bar from foo")
	})
	defer db.Close()
	defer assertExpectationsMet(t, mock)

	stmt, err := db.Prepare("select bar from foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ACT
	result := sut.Statement(ctx, stmt)

	// ASSERT
	t.Run("returns a new statement", func(t *testing.T) {
		wanted := true
		got := result != stmt
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}
