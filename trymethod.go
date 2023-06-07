package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

type trymethod interface {
	try(context.Context, func(*sql.DB) error) error
}

// noretry implements the trymethod interface for a connection
// configured with a single connector.
//
// The operation to be performed is called and any error is returned.
type noretry struct {
	*connection
}

func (c noretry) try(ctx context.Context, op func(*sql.DB) error) error {
	return op(c.db)
}

// retry implements the trymethod interface for a connection
// configured with multiple connectors.
//
// The operation to be performed is called and if a ErrBadConn error is
// returned the associated connection is reconnected to the next available
// connector and the operation retried.
//
// If all connectors return ErrBadConn then a ConnectionFailedError is
// returned.
type retry struct {
	*connection
}

// try calls the supplied operation and if a ErrBadConn error is returned
// the associated connection is reconnected to the next available connector
// and the operation retried.
func (c retry) try(ctx context.Context, op func(*sql.DB) error) error {
	for {
		err := op(c.db)

		// no error to deal with
		if err == nil {
			return nil
		}

		// if the error is NOT due to a bad connection then return
		// the error (bad connections are handled below)
		if !errors.Is(err, driver.ErrBadConn) {
			return err
		}

		// TODO: unilog a warning

		// the connection is bad: reconnect and retry
		if cncerr := c.reconnect(ctx); cncerr != nil {
			return errors.Join(err, cncerr)
		}
	}
}
