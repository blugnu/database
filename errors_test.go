package database

import (
	"errors"
	"testing"
)

func TestConfigurationError(t *testing.T) {
	// ARRANGE
	suterr := errors.New("error")
	sut := ConfigurationError{suterr}

	t.Run("Error", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "configuration error: error"
		got := s
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("Is", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name   string
			target error
			result bool
		}{
			{name: "identical", target: sut, result: true},
			{name: "same wrapped error", target: ConfigurationError{suterr}, result: true},
			{name: "different wrapped error", target: ConfigurationError{errors.New("different")}, result: true},
			{name: "not ConnectionFailedError", target: errors.New("different"), result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				result := sut.Is(tc.target)

				// ASSERT
				wanted := tc.result
				got := result
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		// ACT
		result := sut.Unwrap()

		// ASSERT
		t.Run("returns inner error", func(t *testing.T) {
			wanted := suterr
			got := result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}

func TestConnectionFailedError(t *testing.T) {
	// ARRANGE
	suterr := errors.New("error")
	sut := ConnectionFailedError{suterr}

	t.Run("Error", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "connection failed: error"
		got := s
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("Is", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name   string
			target error
			result bool
		}{
			{name: "identical", target: sut, result: true},
			{name: "same wrapped error", target: ConnectionFailedError{suterr}, result: true},
			{name: "different wrapped error", target: ConnectionFailedError{errors.New("different")}, result: true},
			{name: "not ConnectionFailedError", target: errors.New("different"), result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				result := sut.Is(tc.target)

				// ASSERT
				wanted := tc.result
				got := result
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		// ACT
		result := sut.Unwrap()

		// ASSERT
		t.Run("returns inner error", func(t *testing.T) {
			wanted := suterr
			got := result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}

func TestConnectionError(t *testing.T) {
	// ARRANGE
	sutcnc := MockConnector("mock")
	suterr := errors.New("failed")
	sutop := "open db"

	sut := ConnectionError{sutcnc, sutop, suterr}

	t.Run("Error", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "unable to connect: mock: open db: failed"
		got := s
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("Is", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name   string
			target error
			result bool
		}{
			{name: "identical", target: sut, result: true},
			{name: "different connector", target: ConnectionError{MockConnector("other"), sutop, suterr}, result: true},
			{name: "different operation", target: ConnectionError{sutcnc, "other", suterr}, result: true},
			{name: "different wrapped error", target: ConnectionError{sutcnc, sutop, errors.New("different")}, result: true},
			{name: "not ConnectionError", target: errors.New("different"), result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				result := sut.Is(tc.target)

				// ASSERT
				wanted := tc.result
				got := result
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		// ACT
		result := sut.Unwrap()

		// ASSERT
		t.Run("returns inner error", func(t *testing.T) {
			wanted := suterr
			got := result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}

func TestTransactionError(t *testing.T) {
	// ARRANGE
	suttxn := "do foo"
	suterr := errors.New("error")
	sutop := "begin tx"

	sut := TransactionError{suttxn, sutop, suterr}

	t.Run("Error (with operation)", func(t *testing.T) {
		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "transaction: do foo: begin tx: error"
		got := s
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("Error (with operation)", func(t *testing.T) {
		// ARRANGE
		sut := sut
		sut.op = ""

		// ACT
		s := sut.Error()

		// ASSERT
		wanted := "transaction: do foo: error"
		got := s
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("Is", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name   string
			target error
			result bool
		}{
			{name: "identical", target: sut, result: true},
			{name: "different transaction name", target: TransactionError{"other name", "other op", suterr}, result: false},
			{name: "different operation", target: TransactionError{suttxn, "other op", suterr}, result: false},
			{name: "different wrapped error", target: TransactionError{suttxn, sutop, errors.New("different")}, result: true},
			{name: "not TransactionError", target: errors.New("different"), result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				result := sut.Is(tc.target)

				// ASSERT
				wanted := tc.result
				got := result
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		// ACT
		result := sut.Unwrap()

		// ASSERT
		t.Run("returns inner error", func(t *testing.T) {
			wanted := suterr
			got := result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}
