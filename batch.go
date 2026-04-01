package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/search5/cubrid-go/protocol"
)

// BatchExec executes multiple SQL statements in a single network round-trip
// using the CAS EXECUTE_BATCH protocol. Each statement is executed independently
// on the server side. Returns an error if the batch execution fails.
func BatchExec(db *sql.DB, sqls []string) error {
	return BatchExecContext(context.Background(), db, sqls)
}

// BatchExecContext executes multiple SQL statements in a single network round-trip
// with context support.
func BatchExecContext(ctx context.Context, db *sql.DB, sqls []string) error {
	if len(sqls) == 0 {
		return nil
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("cubrid: batch exec get conn: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: batch exec requires a cubrid connection")
		}
		return c.executeBatch(ctx, sqls)
	})
}

// executeBatch sends an EXECUTE_BATCH request to the CAS server.
func (c *cubridConn) executeBatch(ctx context.Context, sqls []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer

	// Auto-commit flag.
	protocol.WriteInt(&buf, 1)
	if c.autoCommit && !c.inTx {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	// Query timeout (PROTOCOL_V4+).
	protocol.WriteInt(&buf, 4)
	if c.dsn.QueryTimeout > 0 {
		protocol.WriteInt(&buf, int32(c.dsn.QueryTimeout.Milliseconds()))
	} else {
		protocol.WriteInt(&buf, 0)
	}

	// Each SQL as a null-terminated string parameter.
	for _, sql := range sqls {
		protocol.WriteNullTermString(&buf, sql)
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeExecuteBatch, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}
