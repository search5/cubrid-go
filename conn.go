package cubrid

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

// cubridConn implements driver.Conn and related interfaces.
type cubridConn struct {
	mu         sync.Mutex
	netConn    net.Conn
	dsn        DSN
	casInfo    protocol.CASInfo // Echoed as-is from server; NEVER modify the autocommit bit.
	broker     protocol.BrokerInfo
	session    [20]byte
	casPID     int32
	closed     bool
	inTx       bool // Whether a transaction is in progress.
	autoCommit bool // Client-side autocommit state, passed in request params.
}

// connect establishes a TCP connection to the CUBRID broker and performs the handshake.
func connect(ctx context.Context, dsn DSN) (*cubridConn, error) {
	addr := fmt.Sprintf("%s:%d", dsn.Host, dsn.Port)

	dialer := net.Dialer{Timeout: dsn.ConnectTimeout}
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cubrid: dial %s: %w", addr, err)
	}

	c := &cubridConn{
		netConn: netConn,
		dsn:     dsn,
		casInfo: protocol.NewCASInfo(),
	}

	if err := c.handshake(ctx); err != nil {
		netConn.Close()
		return nil, err
	}

	// Autocommit is managed client-side. CasInfo is echoed as-is from
	// the server — modifying its autocommit bit causes CAS to close the
	// TCP socket under KEEP_CONNECTION=AUTO mode.
	c.autoCommit = dsn.AutoCommit

	return c, nil
}

// handshake performs the two-phase CCI connection handshake.
func (c *cubridConn) handshake(ctx context.Context) error {
	// Set deadline if context has one.
	if deadline, ok := ctx.Deadline(); ok {
		c.netConn.SetDeadline(deadline)
		defer c.netConn.SetDeadline(time.Time{})
	}

	// Phase 1: Broker port negotiation.
	pkt := protocol.BuildClientInfoExchange(protocol.ProtocolLatest, false)
	if _, err := c.netConn.Write(pkt); err != nil {
		return fmt.Errorf("cubrid: handshake phase 1 write: %w", err)
	}

	casPort, err := protocol.ParseBrokerResponse(c.netConn)
	if err != nil {
		return fmt.Errorf("cubrid: handshake phase 1 read: %w", err)
	}

	if casPort < 0 {
		return fmt.Errorf("cubrid: connection refused by broker (code %d)", casPort)
	}

	// If broker assigned a new CAS port, reconnect.
	if casPort > 0 {
		c.netConn.Close()
		addr := fmt.Sprintf("%s:%d", c.dsn.Host, casPort)
		dialer := net.Dialer{Timeout: c.dsn.ConnectTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("cubrid: reconnect to CAS port %d: %w", casPort, err)
		}
		c.netConn = conn

		if deadline, ok := ctx.Deadline(); ok {
			c.netConn.SetDeadline(deadline)
		}
	}

	// Phase 2: Database authentication.
	authPkt := protocol.BuildOpenDatabase(c.dsn.Database, c.dsn.User, c.dsn.Password)
	if _, err := c.netConn.Write(authPkt); err != nil {
		return fmt.Errorf("cubrid: handshake phase 2 write: %w", err)
	}

	// OpenDatabase response uses NON-STANDARD framing:
	// - response_length INCLUDES the 4-byte CAS info (unlike standard messages)
	// - There is NO separate response_code field; body starts with cas_pid
	// - Error is detected by checking if cas_pid is negative
	openResp, err := c.readOpenDatabaseResponse()
	if err != nil {
		return err
	}

	c.broker = openResp.Broker
	c.session = openResp.SessionID
	c.casPID = openResp.CASPID

	return nil
}

const maxReconnectAttempts = 3

// sendRequest sends a CCI request and returns the response frame.
// If the CAS closes the connection (KEEP_CONNECTION=AUTO in autocommit mode),
// this method performs a full reconnection and retries the request.
func (c *cubridConn) sendRequest(fc protocol.FuncCode, payload []byte) (*protocol.ResponseFrame, error) {
	msg := protocol.BuildRequestMessage(c.casInfo, fc, payload)
	if _, err := c.netConn.Write(msg); err != nil {
		if !isConnectionLost(err) {
			return nil, fmt.Errorf("cubrid: write request: %w", err)
		}
		return c.reconnectAndRetry(fc, payload)
	}

	frame, err := c.readResponse()
	if err != nil && isConnectionLost(err) {
		return c.reconnectAndRetry(fc, payload)
	}
	return frame, err
}

// reconnectAndRetry performs a full two-phase CAS reconnection and retries the request.
func (c *cubridConn) reconnectAndRetry(fc protocol.FuncCode, payload []byte) (*protocol.ResponseFrame, error) {
	for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
		if err := c.reconnect(); err != nil {
			if attempt == maxReconnectAttempts {
				c.closed = true
				return nil, fmt.Errorf("cubrid: reconnection failed after %d attempts: %w", maxReconnectAttempts, err)
			}
			continue
		}

		// Rebuild message with updated CAS info from reconnection.
		msg := protocol.BuildRequestMessage(c.casInfo, fc, payload)
		if _, err := c.netConn.Write(msg); err != nil {
			if isConnectionLost(err) {
				continue
			}
			return nil, fmt.Errorf("cubrid: write after reconnect: %w", err)
		}

		frame, err := c.readResponse()
		if err != nil && isConnectionLost(err) {
			continue
		}
		return frame, err
	}
	c.closed = true
	return nil, fmt.Errorf("cubrid: reconnection exhausted")
}

// reconnect performs a full two-phase handshake to re-establish the CAS session.
func (c *cubridConn) reconnect() error {
	c.netConn.Close()

	addr := net.JoinHostPort(c.dsn.Host, fmt.Sprintf("%d", c.dsn.Port))
	dialer := net.Dialer{Timeout: c.dsn.ConnectTimeout}
	netConn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.netConn = netConn

	ctx := context.Background()
	if err := c.handshake(ctx); err != nil {
		c.netConn.Close()
		return fmt.Errorf("handshake: %w", err)
	}

	return nil
}

// isConnectionLost checks whether an error indicates the TCP connection was dropped.
func isConnectionLost(err error) bool {
	if err == nil {
		return false
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	// Check for common network errors.
	if netErr, ok := err.(net.Error); ok && !netErr.Timeout() {
		return true
	}
	// Check wrapped errors.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "EOF")
}

// sendRequestCtx sends a CCI request with context support.
func (c *cubridConn) sendRequestCtx(ctx context.Context, fc protocol.FuncCode, payload []byte) (*protocol.ResponseFrame, error) {
	if deadline, ok := ctx.Deadline(); ok {
		c.netConn.SetDeadline(deadline)
		defer c.netConn.SetDeadline(time.Time{})
	}

	// Handle context cancellation.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			c.netConn.SetDeadline(time.Now())
		case <-done:
		}
	}()

	return c.sendRequest(fc, payload)
}

// readResponse reads and parses a CCI response, updating the CAS info.
func (c *cubridConn) readResponse() (*protocol.ResponseFrame, error) {
	frame, err := protocol.ParseResponseFrame(c.netConn)
	if err != nil {
		return nil, err
	}
	c.casInfo = frame.CASInfo
	return frame, nil
}

// readOpenDatabaseResponse reads the non-standard OpenDatabase response.
// Unlike standard CAS responses, the OpenDatabase response:
// - response_length INCLUDES the 4-byte CAS info
// - There is NO separate response_code field; body starts directly with cas_pid
// - Errors are detected by checking if the first body int (cas_pid) is negative
func (c *cubridConn) readOpenDatabaseResponse() (*protocol.OpenDatabaseResponse, error) {
	// Read response length (includes CAS info).
	respLen, err := protocol.ReadInt(c.netConn)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read open database response length: %w", err)
	}

	// Read CAS info (4 bytes).
	var casInfoBuf [4]byte
	if _, err := io.ReadFull(c.netConn, casInfoBuf[:]); err != nil {
		return nil, fmt.Errorf("cubrid: read open database CAS info: %w", err)
	}
	copy(c.casInfo[:], casInfoBuf[:])

	// Body length = response_length - CAS info(4).
	bodyLen := int(respLen) - 4
	if bodyLen < 4 {
		return nil, fmt.Errorf("cubrid: open database response body too short: %d", bodyLen)
	}

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(c.netConn, body); err != nil {
		return nil, fmt.Errorf("cubrid: read open database body: %w", err)
	}

	// Check if first 4 bytes (cas_pid position) is negative → error.
	firstInt := int32(body[0])<<24 | int32(body[1])<<16 | int32(body[2])<<8 | int32(body[3])
	if firstInt < 0 {
		// Error response: parse error code and message from remaining body.
		if len(body) >= 8 {
			return nil, ParseErrorResponse(body[4:])
		}
		return nil, fmt.Errorf("cubrid: authentication failed (code %d)", firstInt)
	}

	return protocol.ParseOpenDatabaseResponse(body, true)
}

// checkError returns a *CubridError if the response code indicates an error.
func (c *cubridConn) checkError(frame *protocol.ResponseFrame) error {
	if frame.ResponseCode < 0 {
		if len(frame.Body) >= 4 {
			return ParseErrorResponse(frame.Body)
		}
		return &CubridError{Code: frame.ResponseCode, Message: "unknown error"}
	}
	return nil
}

// --- driver.Conn interface ---

// Prepare prepares a SQL statement.
func (c *cubridConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepareCtx(context.Background(), query)
}

func (c *cubridConn) prepareCtx(ctx context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	var buf bytes.Buffer
	protocol.WriteNullTermString(&buf, query)
	// Prepare flag: 0x00 (normal).
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x00)
	// Auto-commit flag (client-side state, NOT from CasInfo).
	protocol.WriteInt(&buf, 1)
	if c.autoCommit && !c.inTx {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodePrepare, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if err := c.checkError(frame); err != nil {
		return nil, err
	}

	return parsePrepareResponse(c, frame)
}

// Begin starts a new transaction.
func (c *cubridConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a new transaction with context and options.
func (c *cubridConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}
	if c.inTx {
		return nil, fmt.Errorf("cubrid: already in transaction")
	}

	c.inTx = true
	// autoCommit is checked via c.inTx in request builders — no need to
	// modify CasInfo (doing so would cause CAS to drop the connection).

	return &cubridTx{conn: c}, nil
}

// Close closes the connection.
func (c *cubridConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Send CON_CLOSE to the server.
	msg := protocol.BuildRequestMessage(c.casInfo, protocol.FuncCodeConClose, nil)
	c.netConn.Write(msg)

	return c.netConn.Close()
}

// --- driver.Pinger ---

// Ping verifies the connection is alive.
func (c *cubridConn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	// Use GET_DB_VERSION as a ping.
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)
	if c.autoCommit && !c.inTx {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeGetDBVersion, buf.Bytes())
	if err != nil {
		return driver.ErrBadConn
	}
	if err := c.checkError(frame); err != nil {
		return driver.ErrBadConn
	}

	return nil
}

// --- driver.SessionResetter ---

// ResetSession resets the session state.
func (c *cubridConn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	// Roll back any pending transaction.
	if c.inTx {
		if err := c.endTransaction(ctx, protocol.TranRollback); err != nil {
			return driver.ErrBadConn
		}
		c.inTx = false
	}

	// Restore auto-commit setting (client-side only).
	c.autoCommit = c.dsn.AutoCommit

	return nil
}

// --- driver.Validator ---

// IsValid reports whether the connection is still usable.
func (c *cubridConn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}

// --- driver.NamedValueChecker ---

// CheckNamedValue validates and optionally converts a bind parameter before
// it is passed to Exec or Query. This allows the driver to accept custom
// CUBRID types (CubridEnum, CubridMonetary, CubridNumeric, CubridJson,
// CubridOid, collections) that do not implement driver.Valuer directly.
func (c *cubridConn) CheckNamedValue(nv *driver.NamedValue) error {
	switch v := nv.Value.(type) {
	case *CubridEnum:
		nv.Value = v.Name
		return nil
	case CubridEnum:
		nv.Value = v.Name
		return nil
	case *CubridMonetary:
		nv.Value = v.Amount
		return nil
	case CubridMonetary:
		nv.Value = v.Amount
		return nil
	case *CubridNumeric:
		nv.Value = v.value
		return nil
	case CubridNumeric:
		nv.Value = v.value
		return nil
	case *CubridJson:
		nv.Value = v.value
		return nil
	case CubridJson:
		nv.Value = v.value
		return nil
	case *CubridOid:
		nv.Value = v.Encode()
		return nil
	case *CubridSet:
		nv.Value = v.Elements
		return nil
	case *CubridMultiSet:
		nv.Value = v.Elements
		return nil
	case *CubridSequence:
		nv.Value = v.Elements
		return nil
	default:
		return driver.ErrSkip
	}
}

// endTransaction sends a commit or rollback. Caller must hold c.mu.
func (c *cubridConn) endTransaction(ctx context.Context, op protocol.TransactionOp) error {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, byte(op))

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeEndTran, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}

// --- Prepare response parser ---

// ColumnMeta holds metadata for a single column in a result set.
// ColumnMeta holds metadata for a single column in a result set.
type ColumnMeta struct {
	Type        protocol.CubridType
	ElementType protocol.CubridType // For collection types (SET/MULTISET/SEQUENCE), the element type.
	Scale       int16
	Precision   int32
	Name        string
	RealName    string
	TableName   string
	Nullable      bool
	DefaultValue  string
	AutoIncrement bool
	UniqueKey     bool
	PrimaryKey    bool
	ForeignKey    bool
}

func parsePrepareResponse(c *cubridConn, frame *protocol.ResponseFrame) (*cubridStmt, error) {
	r := bytes.NewReader(frame.Body)

	// Cache lifetime.
	if _, err := protocol.ReadInt(r); err != nil {
		return nil, fmt.Errorf("cubrid: read cache lifetime: %w", err)
	}

	// Statement type.
	stmtTypeByte, err := protocol.ReadByte(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read stmt type: %w", err)
	}

	// Bind parameter count.
	bindCount, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read bind count: %w", err)
	}

	// Is updatable.
	if _, err := protocol.ReadByte(r); err != nil {
		return nil, fmt.Errorf("cubrid: read updatable: %w", err)
	}

	// Column count.
	colCount, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read column count: %w", err)
	}

	// Parse column metadata.
	columns := make([]ColumnMeta, colCount)
	for i := int32(0); i < colCount; i++ {
		col, err := parseColumnMeta(r, c.broker.ProtocolVersion)
		if err != nil {
			return nil, fmt.Errorf("cubrid: parse column %d: %w", i, err)
		}
		columns[i] = col
	}

	return &cubridStmt{
		conn:      c,
		handle:    frame.ResponseCode, // Response code is the query handle.
		stmtType:  protocol.StmtType(stmtTypeByte),
		bindCount: int(bindCount),
		columns:   columns,
	}, nil
}

func parseColumnMeta(r io.Reader, pv protocol.ProtocolVersion) (ColumnMeta, error) {
	var col ColumnMeta

	// Type byte (with possible collection encoding for PROTOCOL_V7+).
	typeByte, err := protocol.ReadByte(r)
	if err != nil {
		return col, err
	}

	if typeByte&protocol.CollectionMarker != 0 && pv >= protocol.ProtocolV7 {
		// Collection type: read element type from next byte.
		elemTypeByte, err := protocol.ReadByte(r)
		if err != nil {
			return col, err
		}
		elemType := protocol.CubridType(elemTypeByte)

		// Determine collection kind from bits 5-6.
		// When bits 5-6 are 0x00, the high bit simply indicates an element
		// type byte follows, but the column itself is not a collection.
		collCode := typeByte & protocol.CollectionFlagMask
		switch collCode {
		case protocol.CollectionSet:
			col.Type = protocol.CubridTypeSet
			col.ElementType = elemType
		case protocol.CollectionMultiSet:
			col.Type = protocol.CubridTypeMultiSet
			col.ElementType = elemType
		case protocol.CollectionSequence:
			col.Type = protocol.CubridTypeSequence
			col.ElementType = elemType
		default:
			// 0x00: not a collection, use element type directly.
			col.Type = elemType
		}
	} else {
		col.Type = protocol.CubridType(typeByte)
	}

	// Scale.
	col.Scale, err = protocol.ReadShort(r)
	if err != nil {
		return col, err
	}

	// Precision.
	col.Precision, err = protocol.ReadInt(r)
	if err != nil {
		return col, err
	}

	// Column name.
	col.Name, err = protocol.ReadNullTermString(r)
	if err != nil {
		return col, err
	}

	// Real name.
	col.RealName, err = protocol.ReadNullTermString(r)
	if err != nil {
		return col, err
	}

	// Table name.
	col.TableName, err = protocol.ReadNullTermString(r)
	if err != nil {
		return col, err
	}

	// Nullable.
	nullByte, err := protocol.ReadByte(r)
	if err != nil {
		return col, err
	}
	col.Nullable = nullByte != 0

	// Default value.
	col.DefaultValue, err = protocol.ReadNullTermString(r)
	if err != nil {
		return col, err
	}

	// Auto-increment.
	aiByte, err := protocol.ReadByte(r)
	if err != nil {
		return col, err
	}
	col.AutoIncrement = aiByte != 0

	// Unique key.
	ukByte, err := protocol.ReadByte(r)
	if err != nil {
		return col, err
	}
	col.UniqueKey = ukByte != 0

	// Primary key.
	pkByte, err := protocol.ReadByte(r)
	if err != nil {
		return col, err
	}
	col.PrimaryKey = pkByte != 0

	// Reverse index (read and discard).
	if _, err := protocol.ReadByte(r); err != nil {
		return col, err
	}

	// Reverse unique (read and discard).
	if _, err := protocol.ReadByte(r); err != nil {
		return col, err
	}

	// Foreign key.
	fkByte, err := protocol.ReadByte(r)
	if err != nil {
		return col, err
	}
	col.ForeignKey = fkByte != 0

	// Shared (read and discard).
	if _, err := protocol.ReadByte(r); err != nil {
		return col, err
	}

	return col, nil
}
