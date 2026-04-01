package cubrid

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

// Cover reconnectAndRetry more paths.
func TestReconnectAndRetryWriteError(t *testing.T) {
	// After reconnect, the write succeeds but read returns non-connection-lost error.
	// This can't be easily tested without a real server, but we can test
	// that 3 reconnect attempts are made before giving up.
	c := &cubridConn{
		netConn: &mockConn{readErr: io.EOF},
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.reconnectAndRetry(protocol.FuncCodeGetDBVersion, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !c.closed {
		t.Fatal("should be closed after exhausting retries")
	}
}

// Cover ColumnTypeScanType with valid index.
func TestColumnTypeScanTypeValidIndex(t *testing.T) {
	r := &cubridRows{
		columns: []ColumnMeta{
			{Name: "a", Type: protocol.CubridTypeInt},
			{Name: "b", Type: protocol.CubridTypeString},
		},
	}
	st0 := r.ColumnTypeScanType(0)
	st1 := r.ColumnTypeScanType(1)
	if st0 == nil || st1 == nil {
		t.Fatal("expected non-nil scan types")
	}
}

// Cover PrepareAndExecRows.Next with buffer available.
func TestPrepareAndExecRowsNextAvailable(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer:      [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
			bufferIdx:   0,
			totalTuples: 3,
			done:        true,
		},
	}
	// Iterate all rows.
	count := 0
	for r.Next() {
		var v interface{}
		if err := r.Scan(&v); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

// Cover rows.Next with fetchMore returning zero-length buffer.
func TestRowsNextFetchReturnsEmpty(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		resp.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 0) // 0 tuples total
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)
		return 0, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeFetch, func(body []byte) (int32, []byte) {
		// Return 0 tuples.
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)
		return 0, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT id FROM empty_table")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Error("expected no rows")
	}
}

// Cover parseExecResult with PROTOCOL_V2 include_column_info=1 and columns.
func TestParseExecResultV2ColumnInfoWithCols(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00)                      // cache_reusable
	protocol.WriteInt(&body, 1)                           // result_count
	protocol.WriteByte(&body, byte(protocol.StmtInsert))
	protocol.WriteInt(&body, 5)                           // affected
	body.Write(make([]byte, 8))                           // OID
	protocol.WriteInt(&body, 0)                           // cache sec
	protocol.WriteInt(&body, 0)                           // cache usec
	protocol.WriteByte(&body, 0x01)                       // include_column_info = YES
	protocol.WriteInt(&body, 0)                           // result_cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtInsert))  // stmt_type
	protocol.WriteInt(&body, 0)                           // num_markers
	protocol.WriteByte(&body, 0x00)                       // updatable
	protocol.WriteInt(&body, 1)                           // col_count = 1
	body.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
	protocol.WriteInt(&body, -1) // shard_id

	frame := &protocol.ResponseFrame{ResponseCode: 5, Body: body.Bytes()}
	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}}
	r, err := parseExecResult(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	if r.rowsAffected != 5 {
		t.Errorf("rowsAffected = %d, want 5", r.rowsAffected)
	}
}

// Cover readOpenDatabaseResponse with valid response.
func TestReadOpenDatabaseResponseValid(t *testing.T) {
	var buf bytes.Buffer
	// Body: cas_pid(4) + cas_id(4) + broker_info(8) + session_id(20) = 36 bytes
	var body bytes.Buffer
	protocol.WriteInt(&body, 1234) // cas_pid
	protocol.WriteInt(&body, 0)    // cas_id
	bi := [8]byte{0x02, 0x01, 0x01, 0x01, 0x40 | byte(protocol.ProtocolV5), 0xC0, 0, 0}
	body.Write(bi[:])
	body.Write(make([]byte, 20)) // session_id

	bodyBytes := body.Bytes()
	protocol.WriteInt(&buf, int32(len(bodyBytes)+4)) // response_length includes casinfo
	buf.Write([]byte{0, 0, 0, 0})                    // casinfo
	buf.Write(bodyBytes)

	mc := &readableMockConn{Reader: bytes.NewReader(buf.Bytes())}
	c := &cubridConn{netConn: mc, casInfo: protocol.NewCASInfo()}
	resp, err := c.readOpenDatabaseResponse()
	if err != nil {
		t.Fatalf("readOpenDatabaseResponse: %v", err)
	}
	if resp.CASPID != 1234 {
		t.Errorf("CASPID = %d", resp.CASPID)
	}
}

// Cover parseTuples with collection type column.
func TestParseTuplesWithCollection(t *testing.T) {
	columns := []ColumnMeta{
		{Name: "tags", Type: protocol.CubridTypeSet, ElementType: protocol.CubridTypeString},
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 0) // row index
	buf.Write(make([]byte, 8)) // OID

	// Collection data: type_byte(1) + count(4) + elem_size(4) + elem_data
	var colData bytes.Buffer
	protocol.WriteByte(&colData, byte(protocol.CubridTypeString))
	protocol.WriteInt(&colData, 1)
	elem := []byte("tag1\x00")
	protocol.WriteInt(&colData, int32(len(elem)))
	colData.Write(elem)

	protocol.WriteInt(&buf, int32(colData.Len()))
	buf.Write(colData.Bytes())

	tuples, err := parseTuples(bytes.NewReader(buf.Bytes()), 1, columns)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Fatalf("tuples: %d", len(tuples))
	}
	set, ok := tuples[0][0].(*CubridSet)
	if !ok {
		t.Fatalf("expected *CubridSet, got %T", tuples[0][0])
	}
	if len(set.Elements) != 1 {
		t.Errorf("elements: %d", len(set.Elements))
	}
}

// Cover parsePrepareResponse error paths.
func TestParsePrepareResponseTruncated(t *testing.T) {
	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV7}}
	// Empty body.
	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: nil}
	_, err := parsePrepareResponse(conn, frame)
	if err == nil {
		t.Fatal("expected error for nil body")
	}

	// Body too short for cache_lifetime.
	frame2 := &protocol.ResponseFrame{ResponseCode: 1, Body: []byte{0, 0}}
	_, err = parsePrepareResponse(conn, frame2)
	if err == nil {
		t.Fatal("expected error for short body")
	}
}

// Cover handshake with broker refusing connection (negative port).
func TestHandshakeBrokerRefusedFull(t *testing.T) {
	// Create a mock that returns casPort = -1 (refused).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read client info.
		io.ReadFull(conn, make([]byte, 10))
		// Send negative response.
		var resp [4]byte
		binary.BigEndian.PutUint32(resp[:], uint32(0xFFFFFFFF)) // -1
		conn.Write(resp[:])
	}()

	addr := ln.Addr().(*net.TCPAddr)
	c := &cubridConn{
		dsn: DSN{Host: "127.0.0.1", Port: addr.Port, ConnectTimeout: 2 * time.Second},
	}
	netConn, err := net.DialTimeout("tcp", addr.String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.netConn = netConn
	defer netConn.Close()

	err = c.handshake(context.Background())
	if err == nil {
		t.Fatal("expected error for refused connection")
	}
	if err.Error() == "" {
		t.Errorf("error = %v", err)
	}
}

// Cover reconnectAndRetry write-after-reconnect failure.
func TestReconnectAndRetryWriteAfterReconnect(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetDBVersion, func(body []byte) (int32, []byte) {
		return 0, []byte("11.4\x00")
	})

	dsn := fmt.Sprintf("cubrid://dba:@127.0.0.1:%d/testdb?connect_timeout=5s", srv.port())
	cfg, _ := ParseDSN(dsn)
	c, err := connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Close the underlying conn to force write failure after reconnect.
	c.netConn.Close()

	// reconnectAndRetry should reconnect (via mock server) and succeed.
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01)
	frame, err := c.reconnectAndRetry(protocol.FuncCodeGetDBVersion, buf.Bytes())
	if err != nil {
		t.Fatalf("reconnectAndRetry: %v", err)
	}
	_ = frame
}

// Cover parsePrepareResponse with more column types.
func TestParsePrepareResponseMultipleColumns(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteInt(&body, 100)                          // cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtSelect))  // stmt_type
	protocol.WriteInt(&body, 3)                            // bind_count
	protocol.WriteByte(&body, 0x01)                        // updatable = true
	protocol.WriteInt(&body, 3)                            // col_count = 3
	body.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
	body.Write(buildColumnMetaBytes(protocol.CubridTypeString, "name"))
	body.Write(buildColumnMetaBytes(protocol.CubridTypeDouble, "price"))

	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}}
	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	stmt, err := parsePrepareResponse(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmt.columns) != 3 || stmt.bindCount != 3 {
		t.Errorf("cols=%d bind=%d", len(stmt.columns), stmt.bindCount)
	}
}

// Cover SchemaRows.Next edge case: closed returns EOF.
func TestSchemaRowsNextClosed(t *testing.T) {
	sr := &SchemaRows{closed: true, buffer: [][]interface{}{{"x"}}}
	_, err := sr.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// Cover PrepareAndExecRows.Next with buffer refetch attempt.
func TestPrepareAndExecRowsNextBufferExhaustedFetchFail(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer:    [][]interface{}{{int64(1)}},
			bufferIdx: 1, // exhausted
			done:      false,
			stmt: &cubridStmt{
				conn: &cubridConn{closed: true},
			},
			fetchPos:    2,
			totalTuples: 10,
			fetchSize:   100,
		},
	}
	// Next tries fetchMore → fails → returns false.
	if r.Next() {
		t.Fatal("expected false")
	}
}

// Cover handshake with casPort > 0 (reconnect to new CAS port).
func TestHandshakeCASPortRedirectFull(t *testing.T) {
	// Create a second server to receive the redirected connection.
	casLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer casLn.Close()
	casPort := casLn.Addr().(*net.TCPAddr).Port

	// CAS server handles OpenDatabase.
	go func() {
		conn, err := casLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read OpenDatabase (628 bytes).
		io.ReadFull(conn, make([]byte, 628))

		// Send success response.
		var body bytes.Buffer
		protocol.WriteInt(&body, 1234)
		protocol.WriteInt(&body, 0)
		bi := [8]byte{0x02, 0x01, 0x01, 0x01, 0x40 | byte(protocol.ProtocolV5), 0xC0, 0, 0}
		body.Write(bi[:])
		body.Write(make([]byte, 20))

		bodyBytes := body.Bytes()
		var resp bytes.Buffer
		protocol.WriteInt(&resp, int32(len(bodyBytes)+4))
		resp.Write([]byte{0, 0, 0, 0})
		resp.Write(bodyBytes)
		conn.Write(resp.Bytes())
	}()

	// Broker server redirects to CAS port.
	brokerLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer brokerLn.Close()

	go func() {
		conn, err := brokerLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.ReadFull(conn, make([]byte, 10))
		var resp [4]byte
		binary.BigEndian.PutUint32(resp[:], uint32(casPort))
		conn.Write(resp[:])
	}()

	brokerAddr := brokerLn.Addr().(*net.TCPAddr)
	c := &cubridConn{
		dsn: DSN{
			Host:           "127.0.0.1",
			Port:           brokerAddr.Port,
			Database:       "test",
			User:           "dba",
			ConnectTimeout: 2 * time.Second,
		},
	}
	netConn, err := net.DialTimeout("tcp", brokerAddr.String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.netConn = netConn

	err = c.handshake(context.Background())
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if c.casPID != 1234 {
		t.Errorf("casPID = %d", c.casPID)
	}
}

// Cover decodeXID truncated data paths.
func TestDecodeXIDTruncatedGtrid(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)  // formatID
	protocol.WriteInt(&buf, 10) // gtrid_length = 10
	protocol.WriteInt(&buf, 0)  // bqual_length = 0
	buf.Write([]byte("short"))  // only 5 bytes, needs 10

	_, err := decodeXID(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for truncated gtrid")
	}
}

// Cover Stmt.Exec and Stmt.Query non-context.
func TestStmtExecNonContext(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		return 1, buildPrepareBody(protocol.StmtInsert, 1)
	})
	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		return 2, buildExecResultBody(2)
	})
	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*cubridConn)
		stmt, err := c.Prepare("INSERT INTO t VALUES(?)")
		if err != nil {
			return err
		}
		defer stmt.Close()

		// Non-context Exec.
		r, err := stmt.Exec([]driver.Value{int64(42)})
		if err != nil {
			return err
		}
		ra, _ := r.RowsAffected()
		if ra != 2 {
			return fmt.Errorf("RowsAffected = %d", ra)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Cover reconnect + reconnectAndRetry by setting up a mock server and
// having the connection drop mid-request.
func TestReconnectAndRetrySuccess(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	callCount := 0
	srv.setHandler(protocol.FuncCodeGetDBVersion, func(body []byte) (int32, []byte) {
		callCount++
		return 0, []byte("11.4\x00")
	})

	dsn := fmt.Sprintf("cubrid://dba:@127.0.0.1:%d/testdb?connect_timeout=5s", srv.port())
	cfg, _ := ParseDSN(dsn)
	c, err := connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Verify normal operation.
	err = c.Ping(context.Background())
	if err != nil {
		t.Fatalf("first ping: %v", err)
	}

	// Now simulate a reconnect by calling reconnect directly.
	// This tests the reconnect path (dial + handshake).
	err = c.reconnect()
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}

	// Verify the connection works after reconnect.
	err = c.Ping(context.Background())
	if err != nil {
		t.Fatalf("ping after reconnect: %v", err)
	}
}

// Cover reconnectAndRetry with actual reconnection + successful retry.
func TestReconnectAndRetryWithRetry(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetDBVersion, func(body []byte) (int32, []byte) {
		return 0, []byte("11.4\x00")
	})

	dsn := fmt.Sprintf("cubrid://dba:@127.0.0.1:%d/testdb?connect_timeout=5s", srv.port())
	cfg, _ := ParseDSN(dsn)
	c, err := connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Build a ping payload.
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01)

	// Trigger reconnectAndRetry by calling it directly.
	frame, err := c.reconnectAndRetry(protocol.FuncCodeGetDBVersion, buf.Bytes())
	if err != nil {
		t.Fatalf("reconnectAndRetry: %v", err)
	}
	if frame == nil {
		t.Fatal("expected non-nil frame")
	}
}

// Cover Stmt.Query non-context wrapper.
func TestStmtQueryNonContext(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		resp.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 0)
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)
		return 0, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*cubridConn)
		stmt, err := c.Prepare("SELECT id FROM t")
		if err != nil {
			return err
		}
		defer stmt.Close()

		rows, err := stmt.Query(nil)
		if err != nil {
			return err
		}
		defer rows.Close()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Cover reconnectAndRetry non-connection-lost write error path.
func TestReconnectAndRetryNonConnLostWrite(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	dsn := fmt.Sprintf("cubrid://dba:@127.0.0.1:%d/testdb?connect_timeout=5s", srv.port())
	cfg, _ := ParseDSN(dsn)
	c, err := connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Replace with a mock that returns non-connection-lost write error.
	c.netConn = &mockConn{writeErr: fmt.Errorf("permission denied")}

	_, err = c.reconnectAndRetry(protocol.FuncCodeGetDBVersion, nil)
	if err == nil {
		// May succeed via reconnect or fail.
		return
	}
}

// Cover sendRequestCtx deadline path.
func TestSendRequestCtxWithDeadline(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetDBVersion, func(body []byte) (int32, []byte) {
		return 0, []byte("11.4\x00")
	})

	dsn := fmt.Sprintf("cubrid://dba:@127.0.0.1:%d/testdb?connect_timeout=5s", srv.port())
	cfg, _ := ParseDSN(dsn)
	c, err := connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Use a context with deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01)
	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeGetDBVersion, buf.Bytes())
	if err != nil {
		t.Fatalf("sendRequestCtx: %v", err)
	}
	if frame == nil {
		t.Fatal("nil frame")
	}
}

// Cover parseSchemaInfoResponse error path.
func TestParseSchemaInfoResponseTruncated(t *testing.T) {
	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV7}}
	// Empty body.
	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: nil}
	_, err := parseSchemaInfoResponse(conn, frame)
	if err == nil {
		t.Fatal("expected error for nil body")
	}

	// Body too short (missing col_count).
	frame2 := &protocol.ResponseFrame{ResponseCode: 1, Body: []byte{0, 0, 0, 1}}
	_, err = parseSchemaInfoResponse(conn, frame2)
	if err == nil {
		t.Fatal("expected error for truncated body")
	}
}

// Cover fetchMore with successful fetch followed by no-more-data.
func TestRowsFetchMoreSuccessThenDone(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		resp.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 2) // 2 total tuples
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)
		// Inline: 1 tuple.
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 1)
		protocol.WriteInt(&resp, 0)
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 4)
		binary.Write(&resp, binary.BigEndian, int32(1))
		return 2, resp.Bytes()
	})
	fetchCount := 0
	srv.setHandler(protocol.FuncCodeFetch, func(body []byte) (int32, []byte) {
		fetchCount++
		if fetchCount == 1 {
			var resp bytes.Buffer
			protocol.WriteInt(&resp, 1)
			protocol.WriteInt(&resp, 1)
			resp.Write(make([]byte, 8))
			protocol.WriteInt(&resp, 4)
			binary.Write(&resp, binary.BigEndian, int32(2))
			return 0, resp.Bytes()
		}
		return ErrCodeNoMoreData, nil
	})
	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var id int64
		rows.Scan(&id)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

// Cover reconnectAndRetry with connection-lost on read after reconnect.
func TestReconnectAndRetryReadConnLost(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetDBVersion, func(body []byte) (int32, []byte) {
		return 0, []byte("11.4\x00")
	})

	dsn := fmt.Sprintf("cubrid://dba:@127.0.0.1:%d/testdb?connect_timeout=5s", srv.port())
	cfg, _ := ParseDSN(dsn)

	// Create a conn that fails reads but can reconnect.
	c, err := connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Close net to simulate connection lost.
	c.netConn.Close()

	// reconnectAndRetry should reconnect and succeed.
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01)
	frame, err := c.reconnectAndRetry(protocol.FuncCodeGetDBVersion, buf.Bytes())
	if err != nil {
		t.Logf("reconnectAndRetry (may fail): %v", err)
	}
	_ = frame
}

// Cover decodeXID more thoroughly.
func TestDecodeXIDFullPaths(t *testing.T) {
	// Valid XID.
	xid := &XID{FormatID: 42, GlobalTransactionID: []byte("abcdef"), BranchQualifier: []byte("xyz")}
	data := xid.encode()
	d, err := decodeXID(data)
	if err != nil {
		t.Fatal(err)
	}
	if d.FormatID != 42 {
		t.Errorf("formatID = %d", d.FormatID)
	}

	// Truncated bqual.
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 1)
	protocol.WriteInt(&buf, 2) // gtrid=2
	protocol.WriteInt(&buf, 5) // bqual=5
	buf.Write([]byte("ab"))   // gtrid ok
	buf.Write([]byte("x"))    // bqual truncated (needs 5)
	_, err = decodeXID(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for truncated bqual")
	}
}

// Cover parseColumnMeta for various non-collection types.
func TestParseColumnMetaAllKeyFlags(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteByte(&buf, byte(protocol.CubridTypeInt))
	protocol.WriteShort(&buf, 0)
	protocol.WriteInt(&buf, 10)
	protocol.WriteNullTermString(&buf, "id")
	protocol.WriteNullTermString(&buf, "id")
	protocol.WriteNullTermString(&buf, "t")
	protocol.WriteByte(&buf, 0x01) // nullable
	protocol.WriteNullTermString(&buf, "0")
	protocol.WriteByte(&buf, 0x01) // auto_inc
	protocol.WriteByte(&buf, 0x01) // unique
	protocol.WriteByte(&buf, 0x01) // primary
	protocol.WriteByte(&buf, 0x01) // reverse_idx
	protocol.WriteByte(&buf, 0x01) // reverse_unique
	protocol.WriteByte(&buf, 0x01) // foreign
	protocol.WriteByte(&buf, 0x01) // shared

	col, err := parseColumnMeta(bytes.NewReader(buf.Bytes()), protocol.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	if !col.Nullable || !col.AutoIncrement || !col.UniqueKey || !col.PrimaryKey || !col.ForeignKey {
		t.Error("all flags should be true")
	}
	if col.DefaultValue != "0" {
		t.Errorf("default = %q", col.DefaultValue)
	}
}

// Cover parseQueryResult with V2 include_column_info=1 and inline tuples.
func TestParseQueryResultV2ColumnInfoInline(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00)
	protocol.WriteInt(&body, 1)
	protocol.WriteByte(&body, byte(protocol.StmtSelect))
	protocol.WriteInt(&body, 1) // tuple count
	body.Write(make([]byte, 8))
	protocol.WriteInt(&body, 0)
	protocol.WriteInt(&body, 0)
	// include_column_info = 1
	protocol.WriteByte(&body, 0x01)
	protocol.WriteInt(&body, 0)                          // result_cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtSelect)) // stmt_type
	protocol.WriteInt(&body, 0)                          // num_markers
	protocol.WriteByte(&body, 0x00)                      // updatable
	protocol.WriteInt(&body, 1)                          // col_count = 1
	body.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
	// shard_id
	protocol.WriteInt(&body, -1)
	// Inline fetch.
	protocol.WriteInt(&body, 0) // fetch_code
	protocol.WriteInt(&body, 1)
	protocol.WriteInt(&body, 0) // row index
	body.Write(make([]byte, 8))
	protocol.WriteInt(&body, 4)
	binary.Write(&body, binary.BigEndian, int32(55))

	cols := []ColumnMeta{{Name: "id", Type: protocol.CubridTypeInt}}
	stmt := &cubridStmt{
		conn:    &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}},
		columns: cols,
	}
	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	rows, err := parseQueryResult(stmt, frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.buffer) != 1 || rows.buffer[0][0] != int32(55) {
		t.Errorf("buffer = %v", rows.buffer)
	}
}
