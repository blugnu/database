package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewConnection(t *testing.T) {
	// test helpers
	var result Connection
	var err error

	testReturnsConnection := func(t *testing.T, wanted bool) {
		t.Run("returns a connection", func(t *testing.T) {
			got := result != nil
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

	testReturnsError := func(t *testing.T, wanted error) {
		t.Run("returns expected error", func(t *testing.T) {
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %v", wanted, got)
			}
		})
	}

	testDB := func(t *testing.T, wanted *sql.DB) {
		cnc := result.(*connection)
		t.Run("with db", func(t *testing.T) {
			got := cnc.db
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

	testDBIsSet := func(t *testing.T) {
		cnc := result.(*connection)
		t.Run("with db set", func(t *testing.T) {
			wanted := true
			got := cnc.db != nil
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

	// testConnectors := func(t *testing.T, wanted []Connector) {
	// 	cnc := result.(*connection)
	// 	t.Run("with connectors", func(t *testing.T) {
	// 		got := cnc.connectors
	// 		if !slices.Equal(wanted, got) {
	// 			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	// 		}
	// 	})
	// }

	testMru := func(t *testing.T, wanted int) {
		cnc := result.(*connection)
		t.Run("with mru", func(t *testing.T) {
			got := cnc.mru
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

	testTryMethod := func(t *testing.T, wanted trymethod) {
		cnc := result.(*connection)
		t.Run("with trymethod", func(t *testing.T) {
			wanted := fmt.Sprintf("%T", wanted)
			got := fmt.Sprintf("%T", cnc.trymethod)
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

	// ARRANGE
	ctx := context.Background()

	t.Run("with no configuration", func(t *testing.T) {
		// ACT
		result, err = NewConnection(ctx)

		// ASSERT
		testReturnsConnection(t, false)
		testReturnsError(t, ErrNoConnectorsConfigured)
	})

	t.Run("with database", func(t *testing.T) {
		// ARRANGE
		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing()
		defer db.Close()

		// ACT
		result, err = NewConnection(ctx,
			WithDb(db),
			MockOpenFuncResult(db, nil),
		)

		// ASSERT
		testReturnsConnection(t, true)
		testReturnsError(t, nil)
		testDB(t, db)
		testMru(t, -1)
		testTryMethod(t, &noretry{})
	})

	t.Run("with database and connectors", func(t *testing.T) {
		// ARRANGE
		db, _, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		defer db.Close()

		// ACT
		result, err = NewConnection(ctx,
			WithConnector(MockConnector("mock connector")),
			WithDb(db),
			MockOpenFuncResult(db, nil),
		)

		// ASSERT
		testReturnsConnection(t, false)
		testReturnsError(t, ErrWithDbAndWithConnectorsIsInvalid)
	})

	t.Run("with connector", func(t *testing.T) {
		// ARRANGE
		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing()
		defer db.Close()

		// ACT
		result, err = NewConnection(ctx,
			WithConnector(SqlmockConnector("sqlmock_db_0")),
			MockOpenFuncResult(db, nil),
		)

		// ASSERT
		testReturnsError(t, nil)
		testReturnsConnection(t, true)
		testDBIsSet(t)
		testMru(t, 0)
		testTryMethod(t, &noretry{})
	})

	t.Run("with connectors", func(t *testing.T) {
		// ARRANGE
		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing()
		defer db.Close()

		// ACT
		result, err = NewConnection(ctx,
			WithConnectors([]Connector{
				SqlmockConnector("sqlmock_db_0"),
				SqlmockConnector("sqlmock_db_1"),
			}),
			MockOpenFuncResult(db, nil),
		)

		// ASSERT
		testReturnsError(t, nil)
		testReturnsConnection(t, true)
		testDBIsSet(t)
		testMru(t, 0)
		testTryMethod(t, &retry{})
	})

	t.Run("when connection fails", func(t *testing.T) {
		// ARRANGE
		openerr := errors.New("open error")

		// ACT
		result, err = NewConnection(ctx,
			WithConnector(MockConnector("mock connector")),
			MockOpenFuncResult(nil, openerr),
		)

		// ASSERT
		testReturnsError(t, ConnectionFailedError{})
		testReturnsConnection(t, false)
	})
}

func TestConnection_close(t *testing.T) {
	// ARRANGE
	closeerr := errors.New("close error")

	testcases := []struct {
		name     string
		closeerr error
		force    bool
		error
	}{
		{name: "closes ok"},
		{name: "close error, forced", closeerr: closeerr, force: true},
		{name: "close error, non-forced", closeerr: closeerr, error: closeerr},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			db, dbmock, _ := sqlmock.New()
			dbmock.ExpectClose().WillReturnError(tc.closeerr)
			defer db.Close()

			sut := &connection{
				db: db,
			}

			// ACT
			err := sut.close(tc.force)

			// ASSERT
			t.Run("returns expected error", func(t *testing.T) {
				wanted := tc.error
				got := err
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})

			t.Run("clears db", func(t *testing.T) {
				wanted := (*sql.DB)(nil)
				got := sut.db
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	}
}

func TestConnection_connect(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	sut := &connection{
		connectors: []Connector{},
		mru:        -1,
	}
	sut.connect = sut.connectany

	reset := func() {
		sut = &connection{
			connectors: []Connector{},
			mru:        -1,
		}
		sut.connect = sut.connectany
	}

	testSetsConnectionDB := func(t *testing.T, wanted *sql.DB) {
		t.Run("sets connection db", func(t *testing.T) {
			got := sut.db
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

	testSetsMruIndex := func(t *testing.T, wanted int) {
		t.Run("sets mru connector index", func(t *testing.T) {
			got := sut.mru
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

	testMeetsAllExpectations := func(t *testing.T, errs ...error) {
		t.Run("meets all expectations", func(t *testing.T) {
			err := errors.Join(errs...)
			wanted := true
			got := err == nil
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v\n%v", wanted, got, err)
			}
		})
	}

	t.Run("applies configuration", func(t *testing.T) {
		// ARRANGE
		defer reset()

		cfgerr := errors.New("configure error")

		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing()
		defer db.Close()

		sut.connectors = []Connector{SqlmockConnector("sqlmock_db_0")}
		sut.open = func(string, string) (*sql.DB, error) { return db, nil }
		sut.configure = func(db *sql.DB) error { return cfgerr }

		// ACT
		err := sut.connect(ctx)

		// ASSERT
		t.Run("returns expected error", func(t *testing.T) {
			wanted := ConfigurationError{cfgerr}
			got := err
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})

	t.Run("with a specific *sql.DB", func(t *testing.T) {
		// ARRANGE
		ctx := context.Background()

		db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		defer db.Close()

		pingerr := errors.New("ping error")

		mock.ExpectPing().WillReturnError(pingerr)
		defer assertExpectationsMet(t, mock)

		sut := &connection{
			db:  db,
			mru: -1,
		}
		sut.connect = sut.connectdb
		sut.trymethod = &noretry{sut}

		// ACT
		err := sut.connect(ctx)

		// ASSERT
		assertExpectedError(t, pingerr, err)
	})

	t.Run("when all connectors fail", func(t *testing.T) {
		// ARRANGE
		db := MockBadConnection()
		defer reset()

		sut.connectors = []Connector{
			MockConnector("mock_0"),
			MockConnector("mock_1"),
		}
		sut.open = func(d string, cs string) (*sql.DB, error) {
			return db, nil
		}

		// ACT
		err := sut.connect(ctx)

		// ASSERT
		t.Run("returns expected error", func(t *testing.T) {
			wanted := ConnectionFailedError{}
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})

		// ASSERT
		testSetsConnectionDB(t, nil)
		testSetsMruIndex(t, -1)
	})

	t.Run("when nth connector connects", func(t *testing.T) {
		// ARRANGE
		defer reset()

		openerr := errors.New("open error")

		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing()
		defer db.Close()

		sut.connectors = []Connector{
			MockConnector("mock_0"),
			MockConnector("mock_1"),
			SqlmockConnector("sqlmock"),
		}
		sut.open = func(d string, cs string) (*sql.DB, error) {
			if d == SqlmockConnectorDriver {
				return db, nil
			}
			return nil, openerr
		}
		sut.mru = -1

		// ACT
		err := sut.connect(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		testSetsConnectionDB(t, db)
		testSetsMruIndex(t, 2)
		testMeetsAllExpectations(t, dbmock.ExpectationsWereMet())
	})

	t.Run("when reconnecting", func(t *testing.T) {
		// ARRANGE
		defer reset()

		openerr := errors.New("open error")

		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing()
		defer db.Close()

		sut.connectors = []Connector{
			SqlmockConnector("sqlmock_db_0"),
			MockConnector("mock connector 1"),
			MockConnector("mock	connector 2"),
		}
		sut.open = func(d string, cs string) (*sql.DB, error) {
			if d == SqlmockConnectorDriver {
				return db, nil
			}
			return nil, openerr
		}
		sut.db = &sql.DB{}
		sut.mru = 1

		// ACT
		err := sut.connect(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		testSetsConnectionDB(t, db)
		testSetsMruIndex(t, 0)
		testMeetsAllExpectations(t, dbmock.ExpectationsWereMet())
	})
}

func TestConnection_reconnect(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	closeerr := errors.New("close error")

	dbcurr, mockcurr, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	mockcurr.ExpectClose().WillReturnError(closeerr)
	defer dbcurr.Close()

	dbnew, mocknew, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
	mocknew.ExpectPing()
	defer dbnew.Close()

	sut := &connection{
		connectors: []Connector{
			MockConnector("curr"),
			MockConnector("new"),
		},
		mru: 0, // currently "connected"
		db:  dbcurr,
		open: func(drv string, cs string) (*sql.DB, error) {
			switch cs {
			case "curr":
				return dbcurr, nil
			case "new":
				return dbnew, nil
			}
			return nil, nil
		},
	}
	sut.connect = sut.connectany

	// ACT
	err := sut.reconnect(ctx)

	t.Run("ignores any close error", func(t *testing.T) {
		wanted := (error)(nil)
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	// ASSERT
	t.Run("sets connection db", func(t *testing.T) {
		wanted := dbnew
		got := sut.db
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("sets mru connector index", func(t *testing.T) {
		wanted := 1
		got := sut.mru
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("meets all expectations", func(t *testing.T) {
		wanted := true
		got := errors.Join(mockcurr.ExpectationsWereMet(), mocknew.ExpectationsWereMet()) == (error)(nil)
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestConnection_try(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("with one connector", func(t *testing.T) {
		// ARRANGE
		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing()
		defer db.Close()

		sut := &connection{
			connectors: []Connector{
				MockConnector("sqlmock_db_0"),
			},
			mru:  -1,
			open: func(d string, cs string) (*sql.DB, error) { return db, nil },
		}
		sut.trymethod = &noretry{sut}

		t.Run("when operation is successful", func(t *testing.T) {
			// ACT
			err := sut.try(ctx, func(*sql.DB) error { return nil })

			// ASSERT
			t.Run("returns nil error", func(t *testing.T) {
				wanted := (error)(nil)
				got := err
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})

		t.Run("when operation fails", func(t *testing.T) {
			// ARRANGE
			sqlerr := errors.New("some sql error")

			// ACT
			err := sut.try(ctx, func(*sql.DB) error { return sqlerr })

			// ASSERT
			t.Run("returns expected error", func(t *testing.T) {
				wanted := sqlerr
				got := err
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	})
}

func TestConnection_Close(t *testing.T) {
	// ARRANGE
	closeerr := errors.New("close error")

	testcases := []struct {
		name     string
		closeerr error
		error
	}{
		{name: "closes ok"},
		{name: "close error", closeerr: closeerr, error: closeerr},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			db, dbmock, _ := sqlmock.New()
			dbmock.ExpectClose().WillReturnError(tc.closeerr)
			defer db.Close()

			sut := &connection{
				db: db,
			}

			// ACT
			err := sut.Close()

			// ASSERT
			t.Run("returns expected error", func(t *testing.T) {
				wanted := tc.error
				got := err
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})

			t.Run("clears db", func(t *testing.T) {
				wanted := (*sql.DB)(nil)
				got := sut.db
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	}
}

func TestConnection_Exec(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		execresult := sqlmock.NewResult(1, 1)

		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectExec("update foo set bar = 1").WillReturnResult(execresult)
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.Exec(ctx, "update foo set bar = 1")

		// ASSERT
		assertErrorIsNil(t, err)
		assertExecResult(t, execresult, result)
	})

	t.Run("when error occurs", func(t *testing.T) {
		// ARRANGE
		execerr := errors.New("exec error")
		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectExec("update foo set bar = 1").WillReturnError(execerr)
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.Exec(ctx, "update foo set bar = 1")

		// ASSERT
		assertExpectedError(t, execerr, err)
		assertExecResult(t, nil, result)
	})
}

func TestConnection_Ping(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	arrange := func(pingerr error) (*connection, *sql.DB, sqlmock.Sqlmock) {
		db, dbmock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbmock.ExpectPing().WillReturnError(pingerr)

		sut := &connection{
			db: db,
		}
		sut.trymethod = &noretry{sut}

		return sut, db, dbmock
	}

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		sut, db, dbmock := arrange(nil)
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		err := sut.Ping(ctx)

		// ASSERT
		assertErrorIsNil(t, err)
	})

	t.Run("when error occurs", func(t *testing.T) {
		// ARRANGE
		pingerr := errors.New("ping error")
		sut, db, dbmock := arrange(pingerr)
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		err := sut.Ping(ctx)

		// ASSERT
		assertExpectedError(t, pingerr, err)
	})

	t.Run("applies configured PingTimeout or package default", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name                string
			packageTimeoutMs    int
			connectionTimeoutMs int
			pingDelayMs         int
			error
		}{
			{name: "package timeout (timeout) ", packageTimeoutMs: 199, pingDelayMs: 200, error: context.DeadlineExceeded},
			{name: "package timeout (responsive) ", packageTimeoutMs: 200, pingDelayMs: 180, error: driver.ErrBadConn},
			{name: "connection timeout (timeout) ", packageTimeoutMs: 100, connectionTimeoutMs: 50, pingDelayMs: 75, error: context.DeadlineExceeded},
			{name: "connection timeout (responsive) ", packageTimeoutMs: 50, connectionTimeoutMs: 100, pingDelayMs: 75, error: driver.ErrBadConn},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ARRANGE
				db := MockBadConnectionWithPingTimeout(time.Duration(tc.pingDelayMs) * time.Millisecond)
				sut := &connection{
					db:          db,
					pingTimeout: time.Duration(tc.connectionTimeoutMs) * time.Millisecond,
				}
				sut.trymethod = &noretry{sut}
				defer db.Close()

				pto := PingTimeout
				defer func() { PingTimeout = pto }()
				PingTimeout = time.Duration(tc.packageTimeoutMs) * time.Millisecond

				// ACT
				err := sut.Ping(ctx)

				// ASSERT
				t.Run("returns expected error", func(t *testing.T) {
					wanted := tc.error
					got := err
					if !errors.Is(got, wanted) {
						t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
					}
				})
			})
		}
	})
}

func TestConnection_Prepare(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectPrepare("update foo set bar = 1")
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.Prepare(ctx, "update foo set bar = 1")

		// ASSERT
		assertErrorIsNil(t, err)

		t.Run("returns statement", func(t *testing.T) {
			wanted := &sql.Stmt{}
			got := result
			if got == nil {
				t.Errorf("\nwanted %T\ngot    nil", wanted)
			}
		})
	})

	t.Run("when error occurs", func(t *testing.T) {
		// ARRANGE
		preperr := errors.New("prepare error")
		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectPrepare("update foo set bar = 1").WillReturnError(preperr)
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.Prepare(ctx, "update foo set bar = 1")

		// ASSERT
		assertExpectedError(t, preperr, err)

		t.Run("returns nil statement", func(t *testing.T) {
			got := result
			if got != nil {
				t.Errorf("\nwanted nil\ngot    %T", got)
			}
		})
	})
}

func TestConnection_Query(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectQuery("select bar from foo").WillReturnRows(sqlmock.NewRows([]string{"bar"}).AddRow(1))
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.Query(ctx, "select bar from foo")

		// ASSERT
		assertErrorIsNil(t, err)

		t.Run("returns rows", func(t *testing.T) {
			wanted := &sql.Rows{}
			got := result
			if got == nil {
				t.Errorf("\nwanted %T\ngot    nil", wanted)
			}
		})
	})

	t.Run("when error occurs", func(t *testing.T) {
		// ARRANGE
		qryerr := errors.New("query error")
		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectQuery("select bar from foo").WillReturnError(qryerr)
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.Query(ctx, "select bar from foo")

		// ASSERT
		assertExpectedError(t, qryerr, err)

		t.Run("returns nil rows", func(t *testing.T) {
			got := result
			if got != nil {
				t.Errorf("\nwanted nil\ngot    %T", got)
			}
		})
	})
}

func TestConnection_QueryRow(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when successful", func(t *testing.T) {
		// ARRANGE
		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectQuery("select bar from foo").WillReturnRows(sqlmock.NewRows([]string{"bar"}).AddRow(1))
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.QueryRow(ctx, "select bar from foo")

		// ASSERT
		assertErrorIsNil(t, err)

		t.Run("returns rows", func(t *testing.T) {
			wanted := &sql.Row{}
			got := result
			if got == nil {
				t.Errorf("\nwanted %T\ngot    nil", wanted)
			}
		})
	})

	t.Run("when error occurs", func(t *testing.T) {
		// ARRANGE
		qryerr := errors.New("query error")
		sut, db, dbmock := arrangeTransactionMethodTest(func(dbmock sqlmock.Sqlmock) {
			dbmock.ExpectQuery("select bar from foo").WillReturnError(qryerr)
		})
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		// ACT
		result, err := sut.QueryRow(ctx, "select bar from foo")

		// ASSERT
		assertExpectedError(t, qryerr, err)

		t.Run("returns row with error", func(t *testing.T) {
			wanted := &sql.Row{}
			got := result
			if got == nil {
				t.Errorf("\nwanted %T\ngot    nil", wanted)
			}

			err := result.Err()
			assertExpectedError(t, qryerr, err)
		})
	})
}

func TestConnection_Transact(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when unable to start transaction", func(t *testing.T) {
		// ARRANGE
		txerr := errors.New("transaction error")

		db, dbmock, _ := sqlmock.New()
		dbmock.ExpectBegin().WillReturnError(txerr)
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		sut := &connection{db: db}
		sut.trymethod = &noretry{sut}

		// ACT
		err := sut.Transact(ctx, "test", func(tx Transaction) error { return nil }, nil)

		// ASSERT
		assertExpectedError(t, TransactionError{txn: "test", op: "begin"}, err)
		assertExpectedError(t, txerr, err)
	})

	t.Run("when all connections are bad", func(t *testing.T) {
		// ARRANGE
		db, sut := arrangeMultipleBadConnections()
		defer db.Close()

		// ACT
		err := sut.Transact(ctx, "test", func(tx Transaction) error { return nil }, nil)

		// ASSERT
		assertExpectedError(t, TransactionError{txn: "test", op: "begin"}, err)
		assertExpectedError(t, ConnectionFailedError{}, err)
	})

	t.Run("when operation fails", func(t *testing.T) {
		// ARRANGE
		operr := errors.New("operation error")

		db, dbmock, _ := sqlmock.New()
		dbmock.ExpectBegin()
		dbmock.ExpectRollback()
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		sut := &connection{db: db}
		sut.trymethod = &noretry{sut}

		// ACT
		err := sut.Transact(ctx, "test", func(tx Transaction) error { return operr }, nil)

		// ASSERT
		assertExpectedError(t, operr, err)
	})

	t.Run("when operation panics", func(t *testing.T) {
		// ARRANGE
		db, dbmock, _ := sqlmock.New()
		dbmock.ExpectBegin()
		dbmock.ExpectRollback()
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		sut := &connection{db: db}
		sut.trymethod = &noretry{sut}

		// ACT
		err := sut.Transact(ctx, "test", func(tx Transaction) error { panic("at the disco!") }, nil)

		// ASSERT
		assertExpectedError(t, TransactionError{txn: "test", op: "panic"}, err)
	})

	t.Run("when operation fails and rollback fails", func(t *testing.T) {
		// ARRANGE
		operr := errors.New("operation error")
		rberr := errors.New("rollback error")

		db, dbmock, _ := sqlmock.New()
		dbmock.ExpectBegin()
		dbmock.ExpectRollback().WillReturnError(rberr)
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		sut := &connection{db: db}
		sut.trymethod = &noretry{sut}

		// ACT
		err := sut.Transact(ctx, "test", func(tx Transaction) error { return operr }, nil)

		// ASSERT
		assertExpectedError(t, operr, err)
		assertExpectedError(t, rberr, err)
		assertExpectedError(t, TransactionError{txn: "test", op: "rollback"}, err)
	})

	t.Run("when operation completes", func(t *testing.T) {
		// ARRANGE
		db, dbmock, _ := sqlmock.New()
		dbmock.ExpectBegin()
		dbmock.ExpectCommit()
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		sut := &connection{db: db}
		sut.trymethod = &noretry{sut}

		// ACT
		err := sut.Transact(ctx, "test", func(tx Transaction) error { return nil }, nil)

		// ASSERT
		assertErrorIsNil(t, err)
	})

	t.Run("when operation completes but commit fails", func(t *testing.T) {
		// ARRANGE
		cmterr := errors.New("commit error")

		db, dbmock, _ := sqlmock.New()
		dbmock.ExpectBegin()
		dbmock.ExpectCommit().WillReturnError(cmterr)
		defer db.Close()
		defer assertExpectationsMet(t, dbmock)

		sut := &connection{db: db}
		sut.trymethod = &noretry{sut}

		// ACT
		err := sut.Transact(ctx, "test", func(tx Transaction) error { return nil }, nil)

		// ASSERT
		assertExpectedError(t, TransactionError{txn: "test", op: "commit"}, err)
	})
}
