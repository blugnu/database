package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

const MockConnectorDriver = "mock"
const SqlmockConnectorDriver = "sqlmock"

type MockConnector string

func (m MockConnector) ConnectionString() string { return string(m) }
func (m MockConnector) Driver() string           { return MockConnectorDriver }
func (m MockConnector) String() string           { return string(m) }

type SqlmockConnector string

func (m SqlmockConnector) ConnectionString() string { return string(m) }
func (m SqlmockConnector) Driver() string           { return SqlmockConnectorDriver }
func (m SqlmockConnector) String() string           { return string(m) }

func MockOpenFunc(fn func(string, string) (*sql.DB, error)) ConfigurationFunc {
	return func(cnc *connection) error {
		cnc.open = fn
		return nil
	}
}

func MockOpenFuncResult(db *sql.DB, err error) ConfigurationFunc {
	return func(cnc *connection) error {
		cnc.open = func(string, string) (*sql.DB, error) { return db, err }
		return nil
	}
}

var driverregistered = false

func registerdriver() {
	if !driverregistered {
		sql.Register("badconnection", &badconnection{})
		driverregistered = true
	}
}

// MockBadConnection returns a mock *sql.DB which returns driver.ErrBadConn on
// all operations except Open and Close.
//
// The mock has no spy or fake capabilities; it serves only to be used when
// testing higher-level operations in the presence of a bad connection.
func MockBadConnection() *sql.DB {
	registerdriver()

	db, _ := sql.Open("badconnection", "")
	return db
}

// MockBadConnection returns a mock *sql.DB which returns driver.ErrBadConn on
// all operations except Open and Close.
//
// The mock has no spy or fake capabilities; it serves only to be used when
// testing higher-level operations in the presence of a bad connection.
func MockBadConnectionWithPingTimeout(t time.Duration) *sql.DB {
	registerdriver()

	db, _ := sql.Open("badconnection", t.String())
	return db
}

// badconnection implements the interfaces necessary as a sql.Driver
// and sql.Conn.
//
// As a driver it returns itself as a connection.
//
// As a connection it returns driver.ErrBadConn on all operations except
// Open and Close.
type badconnection struct {
	pingdelay time.Duration
}

// Open implements the sql.Driver interface, returning itself as a connection.
func (d *badconnection) Open(cs string) (driver.Conn, error) {
	if len(cs) > 0 {
		d.pingdelay, _ = time.ParseDuration(cs)
	}
	return d, nil
}

// Prepare implements the sql.Conn interface, returning driver.ErrBadConn.
func (d *badconnection) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrBadConn
}

// Close implements the sql.Conn interface, returning nil.
func (d *badconnection) Close() error { return nil }

// Begin implements the sql.Conn interface, returning driver.ErrBadConn.
func (d *badconnection) Begin() (driver.Tx, error) {
	return nil, driver.ErrBadConn
}

// Ping implements the sql.Pinger interface, returning driver.ErrBadConn.
func (d *badconnection) Ping(ctx context.Context) error {
	result := make(chan error, 1)
	go func() {
		time.Sleep(d.pingdelay)
		result <- driver.ErrBadConn
	}()

	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		select {
		case <-time.After(timeout):
			return context.DeadlineExceeded
		case result := <-result:
			return result
		}
	}
	return <-result
}

// ExecContext implements the sql.ExecerContext interface, returning
// driver.ErrBadConn.
func (d *badconnection) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, driver.ErrBadConn
}
