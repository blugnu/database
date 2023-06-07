package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestMockConnector(t *testing.T) {
	// ARRANGE
	sut := MockConnector("connector")

	// ACT
	cs := sut.ConnectionString()
	d := sut.Driver()
	s := sut.String()

	// ASSERT
	t.Run("ConnectionString()", func(t *testing.T) {
		wanted := "connector"
		got := cs
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("Driver()", func(t *testing.T) {
		wanted := "mock"
		got := d
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("String()", func(t *testing.T) {
		wanted := "connector"
		got := s
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestSqlmockConnector(t *testing.T) {
	// ARRANGE
	sut := SqlmockConnector("connector")

	// ACT
	cs := sut.ConnectionString()
	d := sut.Driver()
	s := sut.String()

	// ASSERT
	t.Run("ConnectionString()", func(t *testing.T) {
		wanted := "connector"
		got := cs
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("Driver()", func(t *testing.T) {
		wanted := "sqlmock"
		got := d
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("String()", func(t *testing.T) {
		wanted := "connector"
		got := s
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestMockOpenFunc(t *testing.T) {
	// ARRANGE
	cnc := &connection{}
	sut := MockOpenFunc(func(string, string) (*sql.DB, error) { return nil, nil })

	// ACT
	err := sut(cnc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("sets the open function", func(t *testing.T) {
		wanted := true
		got := cnc.open != nil
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestMockBadConnection(t *testing.T) {
	// ARRANGE

	// ACT
	db := MockBadConnection()

	// ASSERT
	t.Run("returns a database", func(t *testing.T) {
		if db == nil {
			t.Error("returned nil")
		}
	})
}

func Test_badconnection(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		name   string
		method func(c *badconnection) error
		error
	}{
		{name: "open", method: func(c *badconnection) error { _, err := c.Open(""); return err }},
		{name: "close", method: func(c *badconnection) error { err := c.Close(); return err }},
		{name: "begin", method: func(c *badconnection) error { _, err := c.Begin(); return err }, error: driver.ErrBadConn},
		{name: "ping", method: func(c *badconnection) error { err := c.Ping(context.Background()); return err }, error: driver.ErrBadConn},
		{name: "prepare", method: func(c *badconnection) error { _, err := c.Prepare(""); return err }, error: driver.ErrBadConn},
		{name: "exec", method: func(c *badconnection) error {
			_, err := c.ExecContext(context.Background(), "", []driver.NamedValue{})
			return err
		}, error: driver.ErrBadConn},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			err := tc.method(&badconnection{})

			// ASSERT
			wanted := tc.error
			got := err
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
	// ACT

	// ASSERT

}
