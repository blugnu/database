package database

import (
	"fmt"
)

type Error string

func (e Error) Error() string { return string(e) }

const ErrWithDbAndWithConnectorsIsInvalid = Error("cannot use WithConnector(s) when using WithDb")
const ErrWithDbAndWithConfigurationIsInvalid = Error("cannot use WithConfiguration when using WithDb")
const ErrNoConnectorsConfigured = Error("no connectors configured or database specified")
const ErrPingTimeoutIsInvalid = Error("ping timeout must be greater than or equal to zero")

// ConfigurationError wraps any error returned during configuration of
// a new connection.
type ConfigurationError struct {
	error
}

// Error implements the error interface.
func (e ConfigurationError) Error() string {
	return fmt.Sprintf("configuration error: %s", e.error)
}

// Is returns a boolean indicating whether the target error is a
// ConfigurationError.
func (e ConfigurationError) Is(target error) bool {
	_, ok := target.(ConfigurationError)
	return ok
}

// Unwrap returns the wrapped error.
func (e ConfigurationError) Unwrap() error {
	return e.error
}

// ConnectionFailedError wraps errors that occur when attempting to establish
// a connection and all configured connectors have failed.
type ConnectionFailedError struct {
	error
}

// Error implements the error interface.
func (e ConnectionFailedError) Error() string {
	return fmt.Sprintf("connection failed: %s", e.error)
}

// Is returns a boolean indicating whether the target error is a
// ConnectionFailedError.
func (e ConnectionFailedError) Is(target error) bool {
	_, ok := target.(ConnectionFailedError)
	return ok
}

// Unwrap returns the wrapped error.
func (e ConnectionFailedError) Unwrap() error {
	return e.error
}

// ConnectionError wraps an error from a connection attempt using
// a specific connector, identifying the operation that failed.
type ConnectionError struct {
	Connector
	op string
	error
}

// Error implements the error interface.
func (e ConnectionError) Error() string {
	return fmt.Sprintf("unable to connect: %s: %s: %s", e.Connector, e.op, e.error)
}

// Is returns a boolean indicating whether the target error is a
// ConnectionError.
func (e ConnectionError) Is(target error) bool {
	_, ok := target.(ConnectionError)
	return ok
}

// Unwrap returns the wrapped error.
func (e ConnectionError) Unwrap() error { return e.error }

// TransactionError wraps an error from a transaction operation, identifying
// the name of the transaction and the operation that failed.
type TransactionError struct {
	txn string
	op  string
	error
}

// Error implements the error interface.
func (e TransactionError) Error() string {
	if e.op == "" {
		return fmt.Sprintf("transaction: %s: %s", e.txn, e.error)
	}
	return fmt.Sprintf("transaction: %s: %s: %s", e.txn, e.op, e.error)
}

// Is returns a boolean indicating whether the target error is a
// TransactionError.
//
// A target TransactionError is considered equal if it has the same
// transaction name and operation name as the receiver.
func (e TransactionError) Is(target error) bool {
	if other, ok := target.(TransactionError); ok {
		return e.txn == other.txn && e.op == other.op
	}
	return false
}

// Unwrap returns the wrapped error.
func (e TransactionError) Unwrap() error { return e.error }
