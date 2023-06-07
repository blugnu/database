package database

import (
	"database/sql"
	"time"

	"golang.org/x/exp/slices"
)

type ConfigurationFunc func(*connection) error

// WithConnector adds a single connector to be used for establishing a
// database connection.
//
// If the connector has already been added it is ignored.
func WithConnector(c Connector) ConfigurationFunc {
	return func(cnc *connection) error {
		if cnc.db != nil {
			return ErrWithDbAndWithConnectorsIsInvalid
		}
		if !slices.Contains(cnc.connectors, c) {
			cnc.connectors = append(cnc.connectors, c)
		}
		return nil
	}
}

// WithConnectors adds a slice of connectors to be used for establishing a
// connection.
//
// Any connectors that have already been added are ignored.
//
// With multiple connectors, if a ErrBadConn error is returned from the current
// connection it will be closed and the operation retried on a new connection,
// established using the next connector.
//
// An operation will be retried until successful on a new connection or all
// connectors have been tried.
func WithConnectors(c []Connector) ConfigurationFunc {
	return func(cnc *connection) error {
		if cnc.db != nil {
			return ErrWithDbAndWithConnectorsIsInvalid
		}
		for _, c := range c {
			if !slices.Contains(cnc.connectors, c) {
				cnc.connectors = append(cnc.connectors, c)
			}
		}
		return nil
	}
}

// WithDbConfiguration establishes a configuration function that is called
// whenever a connection is established.  This can be used to configure the
// database connection, for example to set the maximum number of open
// connections.
//
// Returns ErrWithConfigurationIsInvalid if a database has already been
// configured
func WithDbConfiguration(cfg func(*sql.DB) error) ConfigurationFunc {
	return func(cnc *connection) error {
		if cnc.db != nil {
			return ErrWithDbAndWithConfigurationIsInvalid
		}
		cnc.configure = cfg
		return nil
	}
}

// WithDb establishes a database connection using the provided database
// handle.  This is intended primarily for use when mocking a database for
// testing purposes.
//
// Returns ErrConnectorsConfigured if any connectors have already been added.
//
// Returns ErrWithConfigurationIsInvalid if a configuration function has been
// configured. It is expected that when using WithDb the specified *sql.DB
// is already fully configured as required.
func WithDb(db *sql.DB) ConfigurationFunc {
	return func(cnc *connection) error {
		if len(cnc.connectors) > 0 {
			return ErrWithDbAndWithConnectorsIsInvalid
		}

		if cnc.configure != nil {
			return ErrWithDbAndWithConfigurationIsInvalid
		}

		cnc.db = db

		return nil
	}
}

// WithPingTimeout sets the timeout for a ping operation.
func WithPingTimeout(t time.Duration) ConfigurationFunc {
	return func(cnc *connection) error {
		if t < 0 {
			return ErrPingTimeoutIsInvalid
		}

		cnc.pingTimeout = t

		return nil
	}
}
