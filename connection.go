package database

import (
	"context"
	"database/sql"
	"errors"
	"runtime/debug"
	"time"
)

var PingTimeout = 500 * time.Millisecond

type connection struct {
	db          *sql.DB
	pingTimeout time.Duration
	connectors  []Connector         // configured connectors
	mru         int                 // the index of the most recently used (successfully connected) connector
	configure   func(*sql.DB) error //TODO: support slice of funcs (multiple configuration funcs)
	connect     func(context.Context) error
	open        func(string, string) (*sql.DB, error)
	trymethod
}

// NewConnection initialises a new connection to the database using the
// provided connectors.  The connection is established using the first
// connector that successfully connects.
//
// If additional configuration of the database is desired a function can be
// supplied which will be called after the connection has been established.
func NewConnection(ctx context.Context, cfg ...ConfigurationFunc) (Connection, error) {
	c := &connection{
		mru:  -1,
		open: sql.Open,
	}

	// apply supplied configuration functions
	for _, cfg := range cfg {
		if err := cfg(c); err != nil {
			return nil, ConfigurationError{err}
		}
	}

	// set funcs according to whether we are configured with an injected db
	// or one (1) or more connectors
	switch len(c.connectors) {
	case 0:
		if c.db == nil {
			return nil, ConfigurationError{ErrNoConnectorsConfigured}
		}
		c.connect = c.connectdb
		c.trymethod = &noretry{c}
		return c, nil
	case 1:
		c.connect = c.connectany
		c.trymethod = &noretry{c}
	default:
		c.connect = c.connectany
		c.trymethod = &retry{c}
	}

	if err := c.connect(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

// connectany attempts to connect to the database using the configured connectors,
// starting with the connector following the most recently connected connector
// or the first connector if no connection has yet been made.
//
// All connectors will be tried until a connection is established or all
// connectors have been tried.
//
// If a connection is established a nil error is returned.
//
// If no connection can be established then a ConnectionFailedError is returned,
// wrapping the errors from each failed connection attempt.
func (c *connection) connectany(ctx context.Context) error {
	curr := c.mru
	ix := curr

	errs := make([]error, len(c.connectors))
	for i := 0; i < len(c.connectors); i++ {
		ix = (ix + 1) % len(c.connectors)
		cnc := c.connectors[ix]

		db, err := c.open(cnc.Driver(), cnc.ConnectionString())
		if err != nil {
			errs = append(errs, ConnectionError{cnc, "open db", err})
			continue
		}

		if err := db.PingContext(ctx); err != nil {
			errs = append(errs, ConnectionError{cnc, "ping", err})
			continue
		}

		c.db = db
		c.mru = ix
		break
	}

	if c.mru == curr {
		return ConnectionFailedError{errors.Join(errs...)}
	}

	if c.configure != nil {
		if err := c.configure(c.db); err != nil {
			return ConfigurationError{err}
		}
	}

	return nil
}

// connectdb verifies the validity of the current database connection
// by Ping()ing it.
func (c *connection) connectdb(ctx context.Context) error {
	return c.Ping(ctx)
}

// reconnect closes the current connection (ignoring any error)
// and attempts to reconnect
func (c *connection) reconnect(ctx context.Context) error {
	c.close(true)
	return c.connect(ctx)
}

// close closes the current database connection, if one exists.
//
// If force is true then the function always returns nil, otherwise
// any error returned by the database Close method is returned.
func (c *connection) close(force bool) error {
	if db := c.db; db != nil {
		c.db = nil
		if err := db.Close(); err != nil && !force {
			return err
		}
	}
	return nil
}

// Close closes the current database connection, if one exists.
func (c *connection) Close() error {
	return c.close(false)
}

// Exec executes a sql command or query returning a result (but no rows)
// and any error.
//
// If the connection is configured with multiple connectors and the current
// connector returns a driver.ErrBadConn error, the command will be retried on
// all connectors until it succeeds or all connectors have been tried.
//
// If all connectors return driver.ErrbadConn then a ConnectionFailedError
// is returned, wrapping the errors from each failed attempt.
//
// Connector retries are NOT performed for any other error.  All other errors
// (e.g. malformed SQL, database permissions, etc.) are immediately returned.
func (c *connection) Exec(ctx context.Context, cmd string, args ...any) (result sql.Result, err error) {
	err = c.try(ctx, func(db *sql.DB) error {
		result, err = db.ExecContext(ctx, cmd, args...)
		return err
	})
	return
}

// Ping verifies a connection to the database is still alive, establishing
// a connection if necessary.  The Ping honors any configured PingTimeout
// on the Connection.  If not set on the connection, the PingTimeout
// set at the package level is applied.
//
// If the connection is configured with multiple connectors and Ping
// returns driver.ErrBadConn, the command will be retried on all connectors
// until it succeeds or all connectors have been tried.
//
// If all connectors return driver.ErrbadConn then a ConnectionFailedError
// is returned, wrapping the errors from each failed attempt.
//
// Connector retries are NOT performed for any other error.  All other errors
// (e.g. malformed SQL, database permissions, etc.) are immediately returned.
func (c *connection) Ping(ctx context.Context) error {
	return c.try(ctx, func(db *sql.DB) error {
		t := c.pingTimeout
		if t == 0 {
			t = PingTimeout
		}

		ctx, cancel := context.WithTimeout(ctx, t)
		defer cancel()

		return db.PingContext(ctx)
	})
}

// Prepare creates and returns a prepared statement for later queries or
// executions.
//
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
//
// If the connection is configured with multiple connectors and Prepare
// returns driver.ErrBadConn, the command will be retried on all connectors
// until it succeeds or all connectors have been tried.
//
// If all connectors return driver.ErrbadConn then a ConnectionFailedError
// is returned, wrapping the errors from each failed attempt.
//
// Connector retries are NOT performed for any other error.  All other errors
// (e.g. malformed SQL, database permissions, etc.) are immediately returned.
//
// Connector retries are also not performed on subsequent executions of the
// prepared statement.  If the connection is lost during execution of a
// prepared statement, the statement's Close method should be called and a
// new statement prepared.
func (c *connection) Prepare(ctx context.Context, stmt string) (result *sql.Stmt, err error) {
	err = c.try(ctx, func(db *sql.DB) error {
		result, err = db.PrepareContext(ctx, stmt)
		return err
	})
	return
}

// Query executes a sql query that returns rows, typically a SELECT.
//
// If the connection is configured with multiple connectors and Query
// returns driver.ErrBadConn, the query will be retried on all connectors
// until it succeeds or all connectors have been tried.
//
// If all connectors return driver.ErrbadConn then a ConnectionFailedError
// is returned, wrapping the errors from each failed attempt.
//
// Connector retries are NOT performed for any other error.  All other errors
// (e.g. malformed SQL, database permissions, etc.) are immediately returned.
func (c *connection) Query(ctx context.Context, qry string, args ...any) (rows *sql.Rows, err error) {
	err = c.try(ctx, func(db *sql.DB) error {
		rows, err = db.QueryContext(ctx, qry, args...)
		return err
	})
	return
}

// QueryRow executes a sql query that is expected to return at most one row.
// QueryRow always returns a non-nil *sql.Row. Errors are deferred until the
// row's Scan() method is called.
//
// If the connection is configured with multiple connectors and QueryRow
// returns driver.ErrBadConn, the query will be retried on all connectors
// until it succeeds or all connectors have been tried.
//
// If all connectors return driver.ErrbadConn then a ConnectionFailedError
// is returned in the separate error return value, wrapping the errors from each
// failed attempt.
//
// Connector retries are NOT performed for any other error.  All other errors
// (e.g. malformed SQL, database permissions, etc.) are immediately returned.
func (c *connection) QueryRow(ctx context.Context, qry string, args ...any) (row *sql.Row, err error) {
	err = c.try(ctx, func(db *sql.DB) error {
		row = db.QueryRowContext(ctx, qry, args...)
		return row.Err()
	})
	return
}

// Transact starts a new transaction with a given name and executes the supplied
// function.  Any database operations performed within the function will be part
// of the transaction if they are performed using the supplied Transaction object.
//
// A transaction is automatically rolled back if the supplied function returns
// an error or panics.  If the supplied function returns nil then the transaction is
// committed.
//
// If the supplied function panics or returns an error or if any transaction
// control operation fails (begin, commit, rollback) then a TransactionError{} is
// returned, wrapping the error that occured.
//
// If a new transaction cannot be created due to a driver.ErrBadConn error and the
// connection is configured with multiple connectors, the begin transaction attempt
// will be retried on all connectors until it succeeds or all connectors have been
// tried.
//
// If all connectors return driver.ErrbadConn then a TransactionError{} is
// returned, warpping a ConnectionFailedError in turn wrapping the errors from
// each failed connection attempt.
//
// Connector retries are NOT performed for any other error.  All other errors
// (e.g. malformed SQL, database permissions, etc.) are immediately returned.
func (c *connection) Transact(ctx context.Context, name string, op func(tx Transaction) error, opts *sql.TxOptions) (err error) {
	// the transaction is started using the 'try' func so that any
	// connection errors are handled by the retry mechanism.
	var tx *sql.Tx
	err = c.try(ctx, func(db *sql.DB) error {
		tx, err = db.BeginTx(ctx, opts)
		return err
	})
	if err != nil {
		return TransactionError{name, "begin", err}
	}

	// set a flag to indicate that we should rollback at exit and defer a call
	// which will rollback the transaction if the flag is still set
	rollback := true
	defer func() {
		if r := recover(); r != nil {
			err = TransactionError{name, "panic", errors.New(string(debug.Stack()))}
		}
		if !rollback {
			return
		}
		if txerr := tx.Rollback(); txerr != nil {
			err = errors.Join(err, TransactionError{name, "rollback", txerr})
		}
	}()

	// transaction operations are performed without using the 'try' func
	// since all transaction operations must be performed on the same
	// connection; a connection error on a transacted operation fails
	// the transaction.
	if err = op(&transaction{tx}); err != nil {
		return TransactionError{txn: name, error: err}
	}

	// we successfully completed the transaction; whatever happens now
	// the transaction will either be commited or will fail to commit and be
	// rolled back.  Either way, we should no longer rollback at exit
	rollback = false

	// commit the transaction
	if err = tx.Commit(); err != nil {
		return TransactionError{name, "commit", err}
	}

	return nil
}
