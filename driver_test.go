package cubrid

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestDriverImplementsInterfaces(t *testing.T) {
	// These are compile-time checks in driver.go, but verify at runtime too.
	var _ driver.Driver = (*CubridDriver)(nil)
	var _ driver.DriverContext = (*CubridDriver)(nil)
}

func TestOpenConnector(t *testing.T) {
	d := &CubridDriver{}
	connector, err := d.OpenConnector("cubrid://dba:@localhost:33000/demodb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if connector == nil {
		t.Fatal("connector is nil")
	}
	if connector.Driver() != d {
		t.Error("Driver() returned wrong driver")
	}
}

func TestOpenConnectorInvalidDSN(t *testing.T) {
	d := &CubridDriver{}
	_, err := d.OpenConnector("")
	if err == nil {
		t.Error("expected error for empty DSN")
	}
}

func TestConnectorConnectNoServer(t *testing.T) {
	d := &CubridDriver{}
	connector, err := d.OpenConnector("cubrid://dba:@127.0.0.1:19999/testdb?connect_timeout=100ms")
	if err != nil {
		t.Fatal(err)
	}

	// Should fail because no server is listening on port 19999.
	ctx := context.Background()
	_, err = connector.Connect(ctx)
	if err == nil {
		t.Error("expected connection error")
	}
}

func TestRowsColumnInterfaces(t *testing.T) {
	// Verify that cubridRows implements the ColumnType interfaces.
	var _ interface {
		ColumnTypeDatabaseTypeName(int) string
	} = (*cubridRows)(nil)

	var _ interface {
		ColumnTypeNullable(int) (bool, bool)
	} = (*cubridRows)(nil)

	var _ interface {
		ColumnTypeScanType(int) interface{}
	} // cubridRows implements this via reflect.Type

	var _ interface {
		ColumnTypeLength(int) (int64, bool)
	} = (*cubridRows)(nil)

	var _ interface {
		ColumnTypePrecisionScale(int) (int64, int64, bool)
	} = (*cubridRows)(nil)
}

func TestCubridConnIsValid(t *testing.T) {
	c := &cubridConn{closed: false}
	if !c.IsValid() {
		t.Error("expected valid")
	}
	c.closed = true
	if c.IsValid() {
		t.Error("expected invalid")
	}
}
