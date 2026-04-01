// Package cubrid provides a pure Go database/sql driver for the CUBRID database.
//
// It implements the CCI (CAS Client Interface) binary protocol directly over TCP,
// with no CGo or C library dependency.
//
// Usage:
//
//	import (
//	    "database/sql"
//	    _ "github.com/search5/cubrid-go"
//	)
//
//	db, err := sql.Open("cubrid", "cubrid://dba:@localhost:33000/demodb")
package cubrid

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("cubrid", &CubridDriver{})
}

// CubridDriver implements driver.Driver and driver.DriverContext.
type CubridDriver struct{}

// Open opens a new connection using a DSN string.
// Prefer OpenConnector for better connection management.
func (d *CubridDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector returns a Connector that can create connections.
func (d *CubridDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &cubridConnector{dsn: cfg, driver: d}, nil
}

// cubridConnector implements driver.Connector.
type cubridConnector struct {
	dsn    DSN
	driver *CubridDriver
}

// Connect creates a new connection to the database.
func (c *cubridConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return connect(ctx, c.dsn)
}

// Driver returns the underlying driver.
func (c *cubridConnector) Driver() driver.Driver {
	return c.driver
}

// Compile-time interface checks.
var (
	_ driver.Driver        = (*CubridDriver)(nil)
	_ driver.DriverContext = (*CubridDriver)(nil)
	_ driver.Connector     = (*cubridConnector)(nil)
	_ driver.Conn          = (*cubridConn)(nil)
	_ driver.ConnBeginTx   = (*cubridConn)(nil)
	_ driver.Pinger        = (*cubridConn)(nil)
	_ driver.SessionResetter = (*cubridConn)(nil)
	_ driver.Validator          = (*cubridConn)(nil)
	_ driver.NamedValueChecker = (*cubridConn)(nil)
	_ driver.Stmt          = (*cubridStmt)(nil)
	_ driver.StmtExecContext  = (*cubridStmt)(nil)
	_ driver.StmtQueryContext = (*cubridStmt)(nil)
	_ driver.Rows          = (*cubridRows)(nil)
	_ driver.Tx            = (*cubridTx)(nil)
)
