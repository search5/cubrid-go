package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/search5/cubrid-go/protocol"
)

// DBParam identifies a CUBRID database session parameter.
type DBParam int32

const (
	// ParamIsolationLevel is the transaction isolation level.
	ParamIsolationLevel DBParam = 1
	// ParamLockTimeout is the lock wait timeout in seconds.
	ParamLockTimeout DBParam = 2
	// ParamMaxStringLength is the maximum string result length.
	ParamMaxStringLength DBParam = 3
	// ParamAutoCommit is the autocommit flag (0 or 1).
	ParamAutoCommit DBParam = 4
)

// GetParam retrieves the current value of a database session parameter.
func GetParam(conn *sql.Conn, param DBParam) (int32, error) {
	return GetParamContext(context.Background(), conn, param)
}

// GetParamContext retrieves the current value of a database session parameter
// with context support.
func GetParamContext(ctx context.Context, conn *sql.Conn, param DBParam) (int32, error) {
	var result int32
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: GetParam requires a cubrid connection")
		}
		val, err := c.getDBParameter(ctx, param)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, err
}

// SetParam sets the value of a database session parameter.
func SetParam(conn *sql.Conn, param DBParam, value int32) error {
	return SetParamContext(context.Background(), conn, param, value)
}

// SetParamContext sets the value of a database session parameter
// with context support.
func SetParamContext(ctx context.Context, conn *sql.Conn, param DBParam, value int32) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: SetParam requires a cubrid connection")
		}
		return c.setDBParameter(ctx, param, value)
	})
}

// getDBParameter sends a GET_DB_PARAMETER request.
func (c *cubridConn) getDBParameter(ctx context.Context, param DBParam) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(param))

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeGetDBParameter, buf.Bytes())
	if err != nil {
		return 0, err
	}
	if err := c.checkError(frame); err != nil {
		return 0, err
	}

	// The parameter value is in the response body (first 4 bytes).
	if len(frame.Body) < 4 {
		return 0, fmt.Errorf("cubrid: GET_DB_PARAMETER response too short")
	}
	r := bytes.NewReader(frame.Body)
	val, err := protocol.ReadInt(r)
	if err != nil {
		return 0, fmt.Errorf("cubrid: read parameter value: %w", err)
	}
	return val, nil
}

// setDBParameter sends a SET_DB_PARAMETER request.
func (c *cubridConn) setDBParameter(ctx context.Context, param DBParam, value int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(param))
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, value)

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeSetDBParameter, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}
