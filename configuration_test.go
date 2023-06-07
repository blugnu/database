package database

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"golang.org/x/exp/slices"
)

func TestWithConfiguration(t *testing.T) {
	// ARRANGE
	cnc := &connection{}
	sut := WithDbConfiguration(func(*sql.DB) error { return nil })

	// ACT
	err := sut(cnc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("sets the configure function", func(t *testing.T) {
		wanted := true
		got := cnc.configure != nil
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("cannot be used with WithDb", func(t *testing.T) {
		// ARRANGE
		cnc := &connection{db: &sql.DB{}}

		// ACT
		err := sut(cnc)

		// ASSERT
		assertExpectedError(t, ErrWithDbAndWithConfigurationIsInvalid, err)
	})
}

func TestWithConnector(t *testing.T) {
	// ARRANGE
	cnc := &connection{}
	ctr := MockConnector("connector_0")
	sut := WithConnector(ctr)

	// ACT
	err := sut(cnc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("adds the connector", func(t *testing.T) {
		wanted := []Connector{ctr}
		got := cnc.connectors
		if !slices.Equal(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("does not add duplicate connectors", func(t *testing.T) {
		// ACT
		err := sut(cnc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		wanted := []Connector{ctr}
		got := cnc.connectors
		if !slices.Equal(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("cannot be used with WithDb", func(t *testing.T) {
		// ARRANGE
		cnc := &connection{db: &sql.DB{}}

		// ACT
		err := sut(cnc)

		// ASSERT
		assertExpectedError(t, ErrWithDbAndWithConnectorsIsInvalid, err)
	})
}

func TestWithConnectors(t *testing.T) {
	// ARRANGE
	cnc := &connection{}
	ctr0 := MockConnector("connector_0")
	ctr1 := MockConnector("connector_1")
	sut := WithConnectors([]Connector{ctr0, ctr1})

	// ACT
	err := sut(cnc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("adds the connectors", func(t *testing.T) {
		wanted := []Connector{ctr0, ctr1}
		got := cnc.connectors
		if !slices.Equal(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("does not add duplicate connectors", func(t *testing.T) {
		// ARRANGE
		ctr2 := MockConnector("connector_2")
		sut := WithConnectors([]Connector{ctr1, ctr2})

		// ACT
		err := sut(cnc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		wanted := []Connector{ctr0, ctr1, ctr2}
		got := cnc.connectors
		if !slices.Equal(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("cannot be used with WithDb", func(t *testing.T) {
		// ARRANGE
		cnc := &connection{db: &sql.DB{}}

		// ACT
		err := sut(cnc)

		// ASSERT
		assertExpectedError(t, ErrWithDbAndWithConnectorsIsInvalid, err)
	})
}

func TestWithDb(t *testing.T) {
	// ARRANGE
	cnc := &connection{}
	db := &sql.DB{}
	sut := WithDb(db)

	// ACT
	err := sut(cnc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("adds the database", func(t *testing.T) {
		wanted := db
		got := cnc.db
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("cannot be used with connectors", func(t *testing.T) {
		// ARRANGE
		cnc := &connection{connectors: []Connector{MockConnector("mock connector")}}

		// ACT
		err := sut(cnc)

		// ASSERT
		assertExpectedError(t, ErrWithDbAndWithConnectorsIsInvalid, err)
	})

	t.Run("cannot be used with WithConfiguration", func(t *testing.T) {
		// ARRANGE
		cnc := &connection{configure: func(*sql.DB) error { return nil }}

		// ACT
		err := sut(cnc)

		// ASSERT
		assertExpectedError(t, ErrWithDbAndWithConfigurationIsInvalid, err)
	})
}

func TestWithPingTimeout(t *testing.T) {
	t.Run("with valid timeout", func(t *testing.T) {
		// ARRANGE
		cnc := &connection{}
		sut := WithPingTimeout(100)

		// ACT
		err := sut(cnc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		t.Run("sets the ping timeout", func(t *testing.T) {
			wanted := time.Duration(100)
			got := cnc.pingTimeout
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})

	t.Run("with invalid timeout", func(t *testing.T) {
		// ARRANGE
		cnc := &connection{}
		sut := WithPingTimeout(-1)

		// ACT
		err := sut(cnc)

		// ASSERT
		t.Run("returns expected error", func(t *testing.T) {
			wanted := ErrPingTimeoutIsInvalid
			got := err
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}
