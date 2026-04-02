package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/search5/cubrid-go/protocol"
)

// --- NEXT_RESULT (19) ---

// NextResult advances to the next result set in a multi-result query.
// Returns the number of affected rows in the next result, or an error
// if there are no more results.
func NextResult(ctx context.Context, conn *sql.Conn, queryHandle int32) (int32, error) {
	var result int32
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: NextResult requires a cubrid connection")
		}
		val, err := c.nextResult(ctx, queryHandle)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, err
}

func (c *cubridConn) nextResult(ctx context.Context, queryHandle int32) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, queryHandle)

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeNextResult, buf.Bytes())
	if err != nil {
		return 0, err
	}
	if err := c.checkError(frame); err != nil {
		return 0, err
	}
	return frame.ResponseCode, nil
}

// --- CURSOR_UPDATE (22) ---

// CursorUpdate updates a row at the given position through a server-side cursor.
// The values are encoded bind parameter data for each column to update.
func CursorUpdate(ctx context.Context, conn *sql.Conn, queryHandle int32, cursorPos int32, values ...interface{}) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: CursorUpdate requires a cubrid connection")
		}
		return c.cursorUpdate(ctx, queryHandle, cursorPos, values)
	})
}

func (c *cubridConn) cursorUpdate(ctx context.Context, queryHandle int32, cursorPos int32, values []interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer

	// Query handle.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, queryHandle)

	// Cursor position (1-based).
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, cursorPos)

	// Column values.
	for i, val := range values {
		data, cubType, err := encodeBindValue(val)
		if err != nil {
			return fmt.Errorf("cubrid: encode cursor update value %d: %w", i+1, err)
		}
		if cubType == protocol.CubridTypeNull {
			protocol.WriteInt(&buf, 1)
			protocol.WriteByte(&buf, byte(protocol.CubridTypeNull))
			protocol.WriteInt(&buf, 0)
		} else {
			protocol.WriteInt(&buf, 1)
			protocol.WriteByte(&buf, byte(cubType))
			protocol.WriteInt(&buf, int32(len(data)))
			buf.Write(data)
		}
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeCursorUpdate, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}

// --- GET_GENERATED_KEYS (34) ---

// GeneratedKey holds an auto-generated key returned by the server.
type GeneratedKey struct {
	Value interface{}
}

// GetGeneratedKeys retrieves auto-generated keys from the last INSERT statement.
// The queryHandle should be obtained from a previous INSERT execution.
func GetGeneratedKeys(ctx context.Context, conn *sql.Conn, queryHandle int32) ([]GeneratedKey, error) {
	var keys []GeneratedKey
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: GetGeneratedKeys requires a cubrid connection")
		}
		k, err := c.getGeneratedKeys(ctx, queryHandle)
		if err != nil {
			return err
		}
		keys = k
		return nil
	})
	return keys, err
}

func (c *cubridConn) getGeneratedKeys(ctx context.Context, queryHandle int32) ([]GeneratedKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, queryHandle)

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeGetGeneratedKeys, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if err := c.checkError(frame); err != nil {
		return nil, err
	}

	// Response: response_code = key count, body = key values.
	keyCount := int(frame.ResponseCode)
	if keyCount <= 0 {
		return nil, nil
	}

	keys := make([]GeneratedKey, 0, keyCount)
	if len(frame.Body) >= 4 {
		r := bytes.NewReader(frame.Body)
		for i := 0; i < keyCount; i++ {
			val, err := protocol.ReadInt(r)
			if err != nil {
				break
			}
			keys = append(keys, GeneratedKey{Value: int64(val)})
		}
	}
	return keys, nil
}

// --- GET_ROW_COUNT (39) ---

// GetRowCount retrieves the number of rows affected by the last executed statement.
func GetRowCount(ctx context.Context, conn *sql.Conn) (int32, error) {
	var result int32
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: GetRowCount requires a cubrid connection")
		}
		val, err := c.getRowCount(ctx)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, err
}

func (c *cubridConn) getRowCount(ctx context.Context) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, fmt.Errorf("cubrid: connection is closed")
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeGetRowCount, nil)
	if err != nil {
		return 0, err
	}
	if err := c.checkError(frame); err != nil {
		return 0, err
	}
	return frame.ResponseCode, nil
}

// --- GET_LAST_INSERT_ID (40) ---

// GetLastInsertID retrieves the last auto-generated insert ID.
func GetLastInsertID(ctx context.Context, conn *sql.Conn) (string, error) {
	var result string
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: GetLastInsertID requires a cubrid connection")
		}
		val, err := c.getLastInsertID(ctx)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, err
}

func (c *cubridConn) getLastInsertID(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return "", fmt.Errorf("cubrid: connection is closed")
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeGetLastInsertID, nil)
	if err != nil {
		return "", err
	}
	if err := c.checkError(frame); err != nil {
		return "", err
	}

	// Response body contains the last insert ID as a null-terminated string.
	if len(frame.Body) > 0 {
		r := bytes.NewReader(frame.Body)
		val, err := protocol.ReadNullTermString(r)
		if err != nil {
			return "", err
		}
		return val, nil
	}
	return fmt.Sprintf("%d", frame.ResponseCode), nil
}

// --- CURSOR_CLOSE (42) ---

// CursorClose closes a server-side cursor identified by queryHandle.
func CursorClose(ctx context.Context, conn *sql.Conn, queryHandle int32) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: CursorClose requires a cubrid connection")
		}
		return c.cursorClose(ctx, queryHandle)
	})
}

func (c *cubridConn) cursorClose(ctx context.Context, queryHandle int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, queryHandle)

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeCursorClose, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}
