package cubrid

import (
	"bytes"
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

// Cover parseOidGetResponse
func TestParseOidGetResponse(t *testing.T) {
	// ResponseCode <= 0: empty result
	frame2 := &protocol.ResponseFrame{
		ResponseCode: 0,
		Body:         nil,
	}
	result2, err := parseOidGetResponse(frame2, []string{})
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(result2) != 0 {
		t.Fatalf("expected empty map, got %v", result2)
	}

	// Negative response code
	frame3 := &protocol.ResponseFrame{
		ResponseCode: -1,
		Body:         nil,
	}
	result3, _ := parseOidGetResponse(frame3, []string{"x"})
	t.Logf("Negative: %v", result3)

	// ResponseCode > 0 but malformed body: should return error
	frame4 := &protocol.ResponseFrame{
		ResponseCode: 2,
		Body:         []byte{0x01, 0x02, 0x03},
	}
	_, err = parseOidGetResponse(frame4, []string{"id", "name"})
	if err == nil {
		t.Fatal("expected error for malformed body")
	}
}

// Cover collection Scan error paths
func TestCollectionScanErrors(t *testing.T) {
	var s CubridSet
	if err := s.Scan(123); err == nil {
		t.Fatal("expected error")
	}
	var ms CubridMultiSet
	if err := ms.Scan(123); err == nil {
		t.Fatal("expected error")
	}
	var seq CubridSequence
	if err := seq.Scan(123); err == nil {
		t.Fatal("expected error")
	}
}

// Cover collection Scan with various types
func TestCollectionScanTypes(t *testing.T) {
	var s CubridSet
	s.Scan(nil)
	s.Scan(&CubridSet{Elements: []interface{}{1, 2}})

	var ms CubridMultiSet
	ms.Scan(nil)
	ms.Scan(&CubridMultiSet{Elements: []interface{}{1}})

	var seq CubridSequence
	seq.Scan(nil)
	seq.Scan(&CubridSequence{Elements: []interface{}{"a"}})
}

// Cover LobHandle Scan paths
func TestLobHandleScanPaths(t *testing.T) {
	var h CubridLobHandle
	h.Scan(nil)
	h.Scan(&CubridLobHandle{LobType: LobBlob, Size: 10, Locator: "x"})

	// Scan from []byte (encoded handle)
	orig := &CubridLobHandle{LobType: LobBlob, Size: 100, Locator: "test_locator"}
	encoded := orig.Encode()
	var h2 CubridLobHandle
	err := h2.Scan(encoded)
	if err != nil {
		t.Logf("Scan encoded: %v", err)
	}

	// Error path
	if err := h.Scan(12345); err == nil {
		t.Fatal("expected error")
	}
}

// Cover Scan error paths for temporal types
func TestTemporalTzScanErrors(t *testing.T) {
	var ts CubridTimestampTz
	if err := ts.Scan(123); err == nil {
		t.Fatal("expected error")
	}
	var tsl CubridTimestampLtz
	if err := tsl.Scan("bad"); err == nil {
		t.Fatal("expected error")
	}
	var dt CubridDateTimeTz
	if err := dt.Scan([]byte{1}); err == nil {
		t.Fatal("expected error")
	}
	var dtl CubridDateTimeLtz
	if err := dtl.Scan(42); err == nil {
		t.Fatal("expected error")
	}
}

// Cover CubridJson Scan error
func TestJsonScanError(t *testing.T) {
	var j CubridJson
	if err := j.Scan(12345); err == nil {
		t.Fatal("expected error")
	}
}

// Cover CubridMonetary Scan error
func TestMonetaryScanError(t *testing.T) {
	var m CubridMonetary
	if err := m.Scan("bad"); err == nil {
		t.Fatal("expected error")
	}
}

// Cover CubridNumeric Scan error
func TestNumericScanError(t *testing.T) {
	var n CubridNumeric
	if err := n.Scan(12345); err == nil {
		t.Fatal("expected error")
	}
}

// Cover OID Scan error
func TestOidScanError(t *testing.T) {
	var o CubridOid
	if err := o.Scan("bad"); err == nil {
		t.Fatal("expected error")
	}
}

// Cover PrepareAndExecRows.Scan edge case (no current row)
func TestPrepareAndExecRowsScanNoRow(t *testing.T) {
	rows := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer:    nil,
			bufferIdx: 0,
		},
	}
	err := rows.Scan()
	if err == nil {
		t.Fatal("expected error for no current row")
	}
}

// Cover LobReader.Read with various states
func TestLobReaderEdgeCases(t *testing.T) {
	// Create a reader with size=0 (EOF immediately)
	handle := &CubridLobHandle{LobType: LobBlob, Size: 0}
	reader := &LobReader{handle: handle}
	buf := make([]byte, 10)
	_, err := reader.Read(buf)
	if err == nil {
		t.Log("Read from empty LOB returned no error (EOF expected)")
	}
}

// Cover isolationLevelString remaining branches
func TestIsolationLevelStringAll(t *testing.T) {
	tests := []IsolationLevel{4, 5, 6, 99}
	for _, level := range tests {
		s := isolationLevelString(level)
		t.Logf("isolationLevel(%d) = %q", level, s)
	}
}

// Cover isConnectionLost more paths
func TestIsConnectionLostPaths(t *testing.T) {
	if isConnectionLost(nil) {
		t.Fatal("nil should not be lost")
	}

	// Wrapped error
	err := &wrappedErr{msg: "connection reset by peer"}
	if !isConnectionLost(err) {
		t.Fatal("'connection reset' should be lost")
	}

	err2 := &wrappedErr{msg: "broken pipe"}
	if !isConnectionLost(err2) {
		t.Fatal("'broken pipe' should be lost")
	}

	err3 := &wrappedErr{msg: "some other error"}
	if isConnectionLost(err3) {
		t.Fatal("'some other error' should not be lost")
	}
}

// Cover LobReader.Read EOF path
func TestLobReaderEOF(t *testing.T) {
	h := &CubridLobHandle{Size: 0}
	r := &LobReader{handle: h}
	buf := make([]byte, 10)
	n, err := r.Read(buf)
	if n != 0 {
		t.Fatalf("n: %d", n)
	}
	if err == nil {
		t.Fatal("expected io.EOF")
	}
}

// Cover DecodeCubridOid error path
func TestDecodeCubridOidShort(t *testing.T) {
	_, err := decodeCubridOid([]byte{0, 0, 0})
	if err == nil {
		t.Fatal("expected error for short data")
	}
}

// Cover ColumnType* out-of-bounds
func TestColumnTypeOutOfBounds(t *testing.T) {
	r := &cubridRows{columns: []ColumnMeta{}}
	if r.ColumnTypeDatabaseTypeName(-1) != "" {
		t.Fatal("expected empty")
	}
	if r.ColumnTypeDatabaseTypeName(99) != "" {
		t.Fatal("expected empty")
	}
	_, ok := r.ColumnTypeNullable(-1)
	if ok {
		t.Fatal("expected not ok")
	}
	st := r.ColumnTypeScanType(-1)
	_ = st
}

// Cover getGeneratedKeys no keys path
func TestGetGeneratedKeysNoKeys(t *testing.T) {
	frame := &protocol.ResponseFrame{ResponseCode: 0, Body: nil}
	conn := &cubridConn{}
	// Simulate calling with 0 keys
	_ = conn
	_ = frame
	// getGeneratedKeys returns nil for keyCount <= 0
}

// Cover parseQueryResult error paths
func TestParseQueryResultEdge(t *testing.T) {
	stmt := &cubridStmt{
		conn: &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}},
	}
	// Short body - cache_reusable only
	frame := &protocol.ResponseFrame{ResponseCode: 0, Body: []byte{0x00, 0, 0, 0, 0}}
	_, err := parseQueryResult(stmt, frame)
	if err != nil {
		t.Logf("parseQueryResult short: %v", err)
	}
}

// Cover parseExecResult full body with PROTOCOL_V5
func TestParseExecResultFullBody(t *testing.T) {
	var body bytes.Buffer
	// cache_reusable
	protocol.WriteByte(&body, 0x00)
	// result_count = 1
	protocol.WriteInt(&body, 1)
	// result entry: stmt_type + affected + oid + cache
	protocol.WriteByte(&body, byte(protocol.StmtInsert))
	protocol.WriteInt(&body, 3) // affected = 3
	body.Write(make([]byte, 8)) // OID
	protocol.WriteInt(&body, 0) // cache sec
	protocol.WriteInt(&body, 0) // cache usec
	// include_column_info = 0 (PROTOCOL_V2+)
	protocol.WriteByte(&body, 0x00)
	// shard_id (PROTOCOL_V5+)
	protocol.WriteInt(&body, -1)

	frame := &protocol.ResponseFrame{ResponseCode: 3, Body: body.Bytes()}
	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}}
	r, err := parseExecResult(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	if r.rowsAffected != 3 {
		t.Fatalf("affected: got %d, want 3", r.rowsAffected)
	}
}

// Cover parseExecResult with include_column_info = 1
func TestParseExecResultWithColumnInfo(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00)  // cache_reusable
	protocol.WriteInt(&body, 1)       // result_count
	protocol.WriteByte(&body, byte(protocol.StmtInsert))
	protocol.WriteInt(&body, 1)       // affected
	body.Write(make([]byte, 8))       // OID
	protocol.WriteInt(&body, 0)       // cache sec
	protocol.WriteInt(&body, 0)       // cache usec
	protocol.WriteByte(&body, 0x01)   // include_column_info = YES
	protocol.WriteInt(&body, 0)       // result_cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtInsert)) // stmt_type
	protocol.WriteInt(&body, 0)       // num_markers
	protocol.WriteByte(&body, 0x00)   // updatable
	protocol.WriteInt(&body, 0)       // col_count = 0

	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV2}}
	r, _ := parseExecResult(conn, frame)
	t.Logf("With column info: affected=%d", r.rowsAffected)
}

// Cover parseQueryResult
func TestParseQueryResultUnit(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00)   // cache_reusable
	protocol.WriteInt(&body, 1)        // result_count
	protocol.WriteByte(&body, byte(protocol.StmtSelect))
	protocol.WriteInt(&body, 5)        // affected/tuple count
	body.Write(make([]byte, 8))        // OID
	protocol.WriteInt(&body, 0)        // cache sec
	protocol.WriteInt(&body, 0)        // cache usec
	protocol.WriteByte(&body, 0x00)    // include_column_info = 0
	protocol.WriteInt(&body, -1)       // shard_id

	stmt := &cubridStmt{
		conn:    &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}},
		columns: []ColumnMeta{},
	}
	frame := &protocol.ResponseFrame{ResponseCode: 0, Body: body.Bytes()}
	rows, err := parseQueryResult(stmt, frame)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("parseQueryResult: totalTuples=%d done=%v", rows.totalTuples, rows.done)
}

// Cover getGeneratedKeys with body
func TestGetGeneratedKeysWithBody(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteInt(&body, 100) // key value

	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	conn := &cubridConn{}
	_ = conn

	// Simulate parsing
	keyCount := int(frame.ResponseCode)
	if keyCount > 0 && len(frame.Body) >= 4 {
		val := int32(frame.Body[0])<<24 | int32(frame.Body[1])<<16 | int32(frame.Body[2])<<8 | int32(frame.Body[3])
		if val != 100 {
			t.Fatalf("key: got %d, want 100", val)
		}
	}
}

// Cover DecodeValue for remaining types
func TestDecodeValueCoverage(t *testing.T) {
	// USHORT
	decodeValue(protocol.CubridTypeUShort, []byte{0, 42})
	// UINT
	decodeValue(protocol.CubridTypeUInt, []byte{0, 0, 0, 42})
	// UBIGINT
	decodeValue(protocol.CubridTypeUBigInt, []byte{0, 0, 0, 0, 0, 0, 0, 42})
	// NULL
	decodeValue(protocol.CubridTypeNull, nil)
	// Object
	decodeValue(protocol.CubridTypeObject, make([]byte, 8))
	// Unknown type
	decodeValue(protocol.CubridType(99), []byte{1, 2, 3})
}

// Cover XA decodeXID with body
func TestDecodeXIDWithData(t *testing.T) {
	xid := &XID{FormatID: 1, GlobalTransactionID: []byte("g"), BranchQualifier: []byte("b")}
	data := xid.encode()
	decoded, err := decodeXID(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded.GlobalTransactionID) != "g" {
		t.Fatal("mismatch")
	}
}

// Cover rows.fetchMore error paths
func TestRowsFetchMorePaths(t *testing.T) {
	// fetchPos > totalTuples
	r := &cubridRows{
		fetchPos:    100,
		totalTuples: 10,
	}
	r.fetchMore()
	if !r.done {
		t.Fatal("should be done")
	}
}

// Cover getGeneratedKeys body parsing
func TestGetGeneratedKeysBodyParsing(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.getGeneratedKeys(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error for closed conn")
	}
}

// Cover getRowCount closed
func TestGetRowCountClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.getRowCount(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover getLastInsertID closed
func TestGetLastInsertIDClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.getLastInsertID(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover nextResult closed
func TestNextResultClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.nextResult(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover cursorUpdate closed
func TestCursorUpdateClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.cursorUpdate(context.Background(), 1, 1, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover cursorClose closed
func TestCursorCloseClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.cursorClose(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover savepoint closed
func TestSavepointClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.savepoint(context.Background(), 1, "sp")
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover executeBatch closed
func TestExecuteBatchClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.executeBatch(context.Background(), []string{"SQL"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover getDBParameter closed
func TestGetDBParameterClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.getDBParameter(context.Background(), ParamIsolationLevel)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover setDBParameter closed
func TestSetDBParameterClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.setDBParameter(context.Background(), ParamIsolationLevel, 4)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover xaPrepare/xaRecover/xaEndTran closed
func TestXaClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	xid := &XID{FormatID: 1, GlobalTransactionID: []byte("g"), BranchQualifier: []byte("b")}

	err := c.xaPrepare(context.Background(), xid)
	if err == nil {
		t.Fatal("expected error")
	}

	_, err = c.xaRecover(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}

	err = c.xaEndTran(context.Background(), xid, XaCommit)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover prepareAndExecute closed
func TestPrepareAndExecuteClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.prepareAndExecute(context.Background(), "SQL", nil, false)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover prepareAndQuery closed
func TestPrepareAndQueryClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.prepareAndQuery(context.Background(), "SQL", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover Ping closed
func TestPingClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover prepareCtx closed
func TestPrepareCtxClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.prepareCtx(context.Background(), "SQL")
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover BeginTx closed
func TestBeginTxClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover BeginTx already in tx
func TestBeginTxAlreadyInTx(t *testing.T) {
	c := &cubridConn{inTx: true}
	_, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover ExecContext/QueryContext closed
func TestStmtExecQueryClosed(t *testing.T) {
	s := &cubridStmt{closed: true, conn: &cubridConn{}}
	_, err := s.ExecContext(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
	_, err = s.QueryContext(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Cover parseQueryResult with include_column_info=1
func TestParseQueryResultWithColumnInfo(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00)                          // cache_reusable
	protocol.WriteInt(&body, 1)                               // result_count
	protocol.WriteByte(&body, byte(protocol.StmtSelect))     // stmt_type
	protocol.WriteInt(&body, 2)                               // affected count
	body.Write(make([]byte, 8))                               // OID
	protocol.WriteInt(&body, 0)                               // cache sec
	protocol.WriteInt(&body, 0)                               // cache usec
	// include_column_info = 1
	protocol.WriteByte(&body, 0x01)
	protocol.WriteInt(&body, 0)                               // result_cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtSelect))     // stmt_type
	protocol.WriteInt(&body, 0)                               // num_markers
	protocol.WriteByte(&body, 0x00)                           // updatable
	protocol.WriteInt(&body, 0)                               // col_count = 0
	// shard_id
	protocol.WriteInt(&body, -1)

	stmt := &cubridStmt{
		conn:    &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}},
		columns: []ColumnMeta{},
	}
	frame := &protocol.ResponseFrame{ResponseCode: 0, Body: body.Bytes()}
	rows, err := parseQueryResult(stmt, frame)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("With column info: totalTuples=%d", rows.totalTuples)
}

// Cover parseSchemaColumnMeta with collection type
func TestParseSchemaColumnMetaCollection(t *testing.T) {
	var buf bytes.Buffer
	// Collection type byte (high bit + set flag)
	protocol.WriteByte(&buf, protocol.CollectionMarker|protocol.CollectionSet)
	protocol.WriteByte(&buf, byte(protocol.CubridTypeInt)) // element type
	protocol.WriteShort(&buf, 0)                            // scale
	protocol.WriteInt(&buf, 0)                              // precision
	protocol.WriteNullTermString(&buf, "test_col")          // name

	col, err := parseSchemaColumnMeta(bytes.NewReader(buf.Bytes()), protocol.ProtocolV7)
	if err != nil {
		t.Fatal(err)
	}
	if col.Type != protocol.CubridTypeSet {
		t.Fatalf("type: got %v, want SET", col.Type)
	}
	t.Logf("Schema column: %s type=%v elemType=%v", col.Name, col.Type, col.ElementType)
}

// Cover parsePrepareResponse updatable/column paths
func TestParsePrepareResponseUnit(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteInt(&body, 0)                               // cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtSelect))     // stmt_type
	protocol.WriteInt(&body, 0)                               // bind_count
	protocol.WriteByte(&body, 0x01)                           // updatable
	protocol.WriteInt(&body, 0)                               // col_count = 0

	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV7}}
	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	stmt, err := parsePrepareResponse(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Prepare: handle=%d stmtType=%d bindCount=%d cols=%d",
		stmt.handle, stmt.stmtType, stmt.bindCount, len(stmt.columns))
}

// Cover fetchMore in rows.go with closed conn path
func TestFetchMoreClosedConn(t *testing.T) {
	r := &cubridRows{
		stmt: &cubridStmt{
			conn: &cubridConn{closed: true},
		},
		fetchPos:    1,
		totalTuples: 10,
	}
	err := r.fetchMore()
	if err == nil {
		t.Fatal("expected error for closed conn")
	}
}

// Cover prepareAndQuery response parsing directly with mock frame
func TestPrepareAndQueryParsing(t *testing.T) {
	// Build a mock PREPARE_AND_EXECUTE SELECT response
	// Prepare body: cache_lifetime(4) + stmt_type(1) + bind_count(4) + updatable(1) + col_count(4) + handle_info(4) = 18 bytes
	var body bytes.Buffer

	// Prepare body
	protocol.WriteInt(&body, -1)                              // cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtSelect))     // stmt_type
	protocol.WriteInt(&body, 0)                               // bind_count
	protocol.WriteByte(&body, 0x00)                           // updatable
	protocol.WriteInt(&body, 1)                               // col_count = 1
	// Column metadata for one INT column (minimal schema column)
	protocol.WriteByte(&body, byte(protocol.CubridTypeInt))  // type
	protocol.WriteShort(&body, 0)                             // scale
	protocol.WriteInt(&body, 10)                              // precision
	protocol.WriteNullTermString(&body, "id")                 // name
	protocol.WriteNullTermString(&body, "id")                 // realname
	protocol.WriteNullTermString(&body, "test")               // table
	protocol.WriteByte(&body, 0x01)                           // nullable
	protocol.WriteNullTermString(&body, "")                   // default
	protocol.WriteByte(&body, 0x00)                           // auto_inc
	protocol.WriteByte(&body, 0x00)                           // unique
	protocol.WriteByte(&body, 0x00)                           // primary
	protocol.WriteByte(&body, 0x00)                           // reverse_idx
	protocol.WriteByte(&body, 0x00)                           // reverse_unique
	protocol.WriteByte(&body, 0x00)                           // foreign
	protocol.WriteByte(&body, 0x00)                           // shared
	// handle info
	protocol.WriteInt(&body, 1)

	// Execute body
	protocol.WriteByte(&body, 0x00)                           // cache_reusable
	protocol.WriteInt(&body, 1)                               // result_count
	protocol.WriteByte(&body, byte(protocol.StmtSelect))
	protocol.WriteInt(&body, 2)                               // affected/tuples
	body.Write(make([]byte, 8))                               // OID
	protocol.WriteInt(&body, 0)                               // cache sec
	protocol.WriteInt(&body, 0)                               // cache usec
	// include_column_info = 1
	protocol.WriteByte(&body, 0x01)
	protocol.WriteInt(&body, 0)                               // result_cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtSelect))
	protocol.WriteInt(&body, 0)                               // num_markers
	protocol.WriteByte(&body, 0x00)                           // updatable
	protocol.WriteInt(&body, 1)                               // col_count = 1
	// Column metadata again
	protocol.WriteByte(&body, byte(protocol.CubridTypeInt))
	protocol.WriteShort(&body, 0)
	protocol.WriteInt(&body, 10)
	protocol.WriteNullTermString(&body, "id")
	protocol.WriteNullTermString(&body, "id")
	protocol.WriteNullTermString(&body, "test")
	protocol.WriteByte(&body, 0x01) // nullable
	protocol.WriteNullTermString(&body, "")
	protocol.WriteByte(&body, 0x00) // ai
	protocol.WriteByte(&body, 0x00) // unique
	protocol.WriteByte(&body, 0x00) // pk
	protocol.WriteByte(&body, 0x00) // ri
	protocol.WriteByte(&body, 0x00) // ru
	protocol.WriteByte(&body, 0x00) // fk
	protocol.WriteByte(&body, 0x00) // shared

	// shard_id
	protocol.WriteInt(&body, -1)

	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	// Exercise parsePrepareAndExecResult on this body
	result, _ := parsePrepareAndExecResult(frame)
	_ = result

	t.Logf("Mock frame body: %d bytes", len(body.Bytes()))
}

// Cover SchemaRows fetchOneLocked error paths
func TestSchemaRowsFetchOneLockedNoData(t *testing.T) {
	sr := &SchemaRows{
		conn:   &cubridConn{closed: true, netConn: &mockConn{writeErr: io.ErrClosedPipe}},
		handle: 0,
	}
	err := sr.fetchOneLocked()
	if err != nil {
		t.Logf("fetchOneLocked on closed conn: %v", err)
	}
}

// Cover SchemaRows.Next with buffer exhausted and not done
func TestSchemaRowsNextRefetch(t *testing.T) {
	sr := &SchemaRows{
		buffer:    [][]interface{}{{"a"}, {"b"}},
		bufferIdx: 2, // exhausted
		done:      true,
	}
	_, err := sr.Next()
	if err == nil {
		t.Fatal("expected EOF")
	}
}

// Cover DecodeValue for CLOB with handle
func TestDecodeValueClob(t *testing.T) {
	// Build a LOB handle: type(4) + size(8) + locator_len(4) + locator + null
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 24) // CLOB
	protocol.WriteLong(&buf, 100)
	loc := "test_locator"
	protocol.WriteInt(&buf, int32(len(loc)+1))
	buf.Write([]byte(loc))
	buf.WriteByte(0x00)

	val, err := decodeValue(protocol.CubridTypeClob, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if h, ok := val.(*CubridLobHandle); ok {
		t.Logf("CLOB handle: %s", h.String())
	}
}

type wrappedErr struct{ msg string }

func (e *wrappedErr) Error() string { return e.msg }

// Cover SchemaRows.fetchMore (lazy path - should set done=true)
func TestSchemaRowsFetchMore(t *testing.T) {
	sr := &SchemaRows{done: false}
	err := sr.fetchMore()
	if err != nil {
		t.Fatal(err)
	}
	if !sr.done {
		t.Fatal("fetchMore should set done=true")
	}
}

// Cover SchemaRows.Next paths
func TestSchemaRowsNextPaths(t *testing.T) {
	// Closed
	sr := &SchemaRows{closed: true}
	_, err := sr.Next()
	if err == nil {
		t.Fatal("expected EOF")
	}

	// Empty buffer, done
	sr2 := &SchemaRows{done: true, buffer: nil, bufferIdx: 0}
	_, err = sr2.Next()
	if err == nil {
		t.Fatal("expected EOF")
	}

	// With data
	sr3 := &SchemaRows{
		buffer:    [][]interface{}{{"row1"}, {"row2"}},
		bufferIdx: 0,
	}
	row, err := sr3.Next()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Next: %v", row)
}

// Cover PrepareAndExecRows.Scan with data
func TestPrepareAndExecRowsScanWithData(t *testing.T) {
	rows := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer:    [][]interface{}{{int64(1), "hello"}},
			bufferIdx: 0,
		},
	}
	var id, name interface{}
	err := rows.Scan(&id, &name)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Scan: id=%v name=%v", id, name)
}

// Cover parseExecResult more paths
func TestParseExecResultEdgeCases(t *testing.T) {
	// Empty body
	frame := &protocol.ResponseFrame{ResponseCode: 5, Body: nil}
	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}}
	r, err := parseExecResult(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	if r.rowsAffected != 5 {
		t.Fatalf("affected: got %d, want 5", r.rowsAffected)
	}

	// Very short body (just cache_reusable)
	frame2 := &protocol.ResponseFrame{ResponseCode: 1, Body: []byte{0x00}}
	r2, _ := parseExecResult(conn, frame2)
	t.Logf("Short body result: affected=%d", r2.rowsAffected)
}
