package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/search5/cubrid-go/protocol"
)

// Savepoint operation codes.
const (
	savepointCreate   byte = 1
	savepointRollback byte = 2
)

// Savepoint creates a named savepoint within the current transaction.
// The conn must be a *sql.Conn obtained from a transaction's underlying connection.
//
// Usage:
//
//	conn, _ := db.Conn(ctx)
//	defer conn.Close()
//	conn.ExecContext(ctx, "BEGIN")  // or use conn.BeginTx
//	cubrid.Savepoint(conn, "sp1")
//	// ... do work ...
//	cubrid.RollbackToSavepoint(conn, "sp1")
func Savepoint(conn *sql.Conn, name string) error {
	return SavepointContext(context.Background(), conn, name)
}

// SavepointContext creates a named savepoint with context support.
func SavepointContext(ctx context.Context, conn *sql.Conn, name string) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: savepoint requires a cubrid connection")
		}
		return c.savepoint(ctx, savepointCreate, name)
	})
}

// RollbackToSavepoint rolls back to a named savepoint within the current transaction.
func RollbackToSavepoint(conn *sql.Conn, name string) error {
	return RollbackToSavepointContext(context.Background(), conn, name)
}

// RollbackToSavepointContext rolls back to a named savepoint with context support.
func RollbackToSavepointContext(ctx context.Context, conn *sql.Conn, name string) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: rollback to savepoint requires a cubrid connection")
		}
		return c.savepoint(ctx, savepointRollback, name)
	})
}

// savepoint sends a SAVEPOINT request to the CAS server.
func (c *cubridConn) savepoint(ctx context.Context, op byte, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer

	// Operation: 1 = create, 2 = rollback to savepoint.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, op)

	// Savepoint name (null-terminated string).
	protocol.WriteNullTermString(&buf, name)

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeSavepoint, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}
