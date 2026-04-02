package cubrid

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

// --- Helper: build full column metadata bytes (same as PREPARE response) ---

func buildColumnMetaBytes(colType protocol.CubridType, name string) []byte {
	var buf bytes.Buffer
	protocol.WriteByte(&buf, byte(colType))
	protocol.WriteShort(&buf, 0) // scale
	protocol.WriteInt(&buf, 10)  // precision
	protocol.WriteNullTermString(&buf, name)
	protocol.WriteNullTermString(&buf, name)  // realname
	protocol.WriteNullTermString(&buf, "tbl") // table
	protocol.WriteByte(&buf, 0x01)            // nullable
	protocol.WriteNullTermString(&buf, "")    // default
	protocol.WriteByte(&buf, 0x00)            // auto_inc
	protocol.WriteByte(&buf, 0x00)            // unique
	protocol.WriteByte(&buf, 0x00)            // primary
	protocol.WriteByte(&buf, 0x00)            // reverse_idx
	protocol.WriteByte(&buf, 0x00)            // reverse_unique
	protocol.WriteByte(&buf, 0x00)            // foreign
	protocol.WriteByte(&buf, 0x00)            // shared
	return buf.Bytes()
}

// buildTupleBytes creates a single tuple with the given column values.
func buildTupleBytes(values ...[]byte) []byte {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 0)            // row index
	buf.Write(make([]byte, 8))            // OID
	for _, v := range values {
		if v == nil {
			protocol.WriteInt(&buf, 0) // NULL
		} else {
			protocol.WriteInt(&buf, int32(len(v)))
			buf.Write(v)
		}
	}
	return buf.Bytes()
}

// --- OID parseOidGetResponse tests ---

func TestParseOidGetResponseFull(t *testing.T) {
	// Build a frame with 2 columns (INT "id", VARCHAR "name") and 1 tuple.
	var body bytes.Buffer
	body.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
	body.Write(buildColumnMetaBytes(protocol.CubridTypeString, "name"))

	// Tuple: id=42, name="hello"
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, 42)
	nameBytes := append([]byte("hello"), 0)
	body.Write(buildTupleBytes(idBytes, nameBytes))

	frame := &protocol.ResponseFrame{ResponseCode: 2, Body: body.Bytes()}
	result, err := parseOidGetResponse(frame, []string{"id", "name"})
	if err != nil {
		t.Fatalf("parseOidGetResponse: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 attrs, got %d", len(result))
	}
	if result["id"] != int32(42) {
		t.Errorf("id = %v, want 42", result["id"])
	}
	nameVal, ok := result["name"].(string)
	if !ok || nameVal != "hello" {
		t.Errorf("name = %v, want 'hello'", result["name"])
	}
}

func TestParseOidGetResponseEmpty(t *testing.T) {
	frame := &protocol.ResponseFrame{ResponseCode: 0, Body: nil}
	result, err := parseOidGetResponse(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty, got %v", result)
	}
}

func TestParseOidGetResponseEmptyBody(t *testing.T) {
	frame := &protocol.ResponseFrame{ResponseCode: 2, Body: []byte{}}
	result, err := parseOidGetResponse(frame, []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty, got %v", result)
	}
}

func TestParseOidGetResponseNullColumn(t *testing.T) {
	var body bytes.Buffer
	body.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "val"))
	body.Write(buildTupleBytes(nil)) // NULL value

	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	result, err := parseOidGetResponse(frame, []string{"val"})
	if err != nil {
		t.Fatal(err)
	}
	if result["val"] != nil {
		t.Errorf("expected nil, got %v", result["val"])
	}
}

// --- toDriverValue comprehensive tests ---

func TestToDriverValueAllTypes(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name string
		in   interface{}
		want interface{}
	}{
		{"nil", nil, nil},
		{"int16", int16(10), int64(10)},
		{"int32", int32(20), int64(20)},
		{"uint16", uint16(30), int64(30)},
		{"uint32", uint32(40), int64(40)},
		{"uint64", uint64(50), int64(50)},
		{"float32", float32(1.5), float64(1.5)},
		{"CubridNumeric", &CubridNumeric{value: "3.14"}, "3.14"},
		{"CubridEnum", &CubridEnum{Name: "RED"}, "RED"},
		{"CubridMonetary", &CubridMonetary{Amount: 9.99}, 9.99},
		{"CubridJson", &CubridJson{value: `{"a":1}`}, `{"a":1}`},
		{"CubridTimestampTz", &CubridTimestampTz{Time: now}, now},
		{"CubridTimestampLtz", &CubridTimestampLtz{Time: now}, now},
		{"CubridDateTimeTz", &CubridDateTimeTz{Time: now}, now},
		{"CubridDateTimeLtz", &CubridDateTimeLtz{Time: now}, now},
		{"string", "hello", "hello"},
		{"int64", int64(100), int64(100)},
		{"float64", float64(2.5), float64(2.5)},
		{"bytes", []byte{1, 2}, []byte{1, 2}},
		{"time", now, now},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toDriverValue(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toDriverValue(%T) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// --- cubridRows tests ---

func TestCubridRowsClose(t *testing.T) {
	r := &cubridRows{
		columns: []ColumnMeta{{Name: "a"}},
		buffer:  [][]interface{}{{"val"}},
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !r.closed {
		t.Fatal("should be closed")
	}
	if r.buffer != nil {
		t.Fatal("buffer should be nil")
	}
	// Double close is safe.
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCubridRowsColumns(t *testing.T) {
	r := &cubridRows{
		columns: []ColumnMeta{{Name: "id"}, {Name: "name"}},
	}
	cols := r.Columns()
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Fatalf("Columns = %v", cols)
	}
}

func TestCubridRowsNext(t *testing.T) {
	r := &cubridRows{
		columns: []ColumnMeta{
			{Name: "id", Type: protocol.CubridTypeInt},
			{Name: "name", Type: protocol.CubridTypeString},
		},
		buffer:      [][]interface{}{{int32(1), "hello"}, {int32(2), "world"}},
		bufferIdx:   0,
		totalTuples: 2,
		done:        true,
	}

	dest := make([]driver.Value, 2)
	if err := r.Next(dest); err != nil {
		t.Fatal(err)
	}
	if dest[0] != int64(1) || dest[1] != "hello" {
		t.Errorf("row 1: %v", dest)
	}

	if err := r.Next(dest); err != nil {
		t.Fatal(err)
	}
	if dest[0] != int64(2) || dest[1] != "world" {
		t.Errorf("row 2: %v", dest)
	}

	// Third call should return EOF.
	if err := r.Next(dest); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestCubridRowsNextClosed(t *testing.T) {
	r := &cubridRows{closed: true}
	if err := r.Next(nil); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestCubridRowsNextEmptyBuffer(t *testing.T) {
	r := &cubridRows{
		buffer:    nil,
		bufferIdx: 0,
		done:      true,
	}
	dest := make([]driver.Value, 0)
	if err := r.Next(dest); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// --- cubridRows ColumnType* tests ---

func TestColumnTypeLengthVariableTypes(t *testing.T) {
	r := &cubridRows{
		columns: []ColumnMeta{
			{Name: "c", Type: protocol.CubridTypeChar, Precision: 100},
			{Name: "v", Type: protocol.CubridTypeString, Precision: 200},
			{Name: "n", Type: protocol.CubridTypeNChar, Precision: 50},
			{Name: "vn", Type: protocol.CubridTypeVarNChar, Precision: 150},
			{Name: "b", Type: protocol.CubridTypeBit, Precision: 32},
			{Name: "vb", Type: protocol.CubridTypeVarBit, Precision: 64},
			{Name: "i", Type: protocol.CubridTypeInt, Precision: 10},
		},
	}
	for i, expected := range []struct {
		length int64
		ok     bool
	}{
		{100, true}, {200, true}, {50, true}, {150, true}, {32, true}, {64, true}, {0, false},
	} {
		length, ok := r.ColumnTypeLength(i)
		if ok != expected.ok || length != expected.length {
			t.Errorf("col %d: length=%d ok=%v, want %d %v", i, length, ok, expected.length, expected.ok)
		}
	}
}

func TestColumnTypePrecisionScaleNumeric(t *testing.T) {
	r := &cubridRows{
		columns: []ColumnMeta{
			{Name: "n", Type: protocol.CubridTypeNumeric, Precision: 10, Scale: 2},
			{Name: "i", Type: protocol.CubridTypeInt, Precision: 10},
		},
	}
	p, s, ok := r.ColumnTypePrecisionScale(0)
	if !ok || p != 10 || s != 2 {
		t.Errorf("NUMERIC: p=%d s=%d ok=%v", p, s, ok)
	}
	_, _, ok = r.ColumnTypePrecisionScale(1)
	if ok {
		t.Error("INT should not have precision/scale")
	}
}

func TestColumnTypeScanTypeAll(t *testing.T) {
	types := []struct {
		ct   protocol.CubridType
		want reflect.Type
	}{
		{protocol.CubridTypeShort, reflect.TypeOf(int16(0))},
		{protocol.CubridTypeInt, reflect.TypeOf(int32(0))},
		{protocol.CubridTypeBigInt, reflect.TypeOf(int64(0))},
		{protocol.CubridTypeFloat, reflect.TypeOf(float32(0))},
		{protocol.CubridTypeDouble, reflect.TypeOf(float64(0))},
		{protocol.CubridTypeString, reflect.TypeOf("")},
		{protocol.CubridTypeDate, reflect.TypeOf(time.Time{})},
		{protocol.CubridTypeBlob, reflect.TypeOf(&CubridLobHandle{})},
		{protocol.CubridTypeEnum, reflect.TypeOf(&CubridEnum{})},
		{protocol.CubridTypeJSON, reflect.TypeOf(&CubridJson{})},
		{protocol.CubridTypeMonetary, reflect.TypeOf(&CubridMonetary{})},
		{protocol.CubridTypeNumeric, reflect.TypeOf(&CubridNumeric{})},
		{protocol.CubridTypeSet, reflect.TypeOf(&CubridSet{})},
		{protocol.CubridTypeObject, reflect.TypeOf(&CubridOid{})},
		{protocol.CubridTypeTsTz, reflect.TypeOf(&CubridTimestampTz{})},
		{protocol.CubridTypeDtTz, reflect.TypeOf(&CubridDateTimeTz{})},
		{protocol.CubridTypeTsLtz, reflect.TypeOf(&CubridTimestampLtz{})},
		{protocol.CubridTypeDtLtz, reflect.TypeOf(&CubridDateTimeLtz{})},
		{protocol.CubridTypeUShort, reflect.TypeOf(uint16(0))},
		{protocol.CubridTypeUInt, reflect.TypeOf(uint32(0))},
		{protocol.CubridTypeUBigInt, reflect.TypeOf(uint64(0))},
		{protocol.CubridTypeMultiSet, reflect.TypeOf(&CubridMultiSet{})},
		{protocol.CubridTypeSequence, reflect.TypeOf(&CubridSequence{})},
		{protocol.CubridTypeBit, reflect.TypeOf([]byte{})},
		{protocol.CubridTypeClob, reflect.TypeOf(&CubridLobHandle{})},
		{protocol.CubridTypeChar, reflect.TypeOf("")},
		{protocol.CubridTypeTime, reflect.TypeOf(time.Time{})},
		{protocol.CubridTypeTimestamp, reflect.TypeOf(time.Time{})},
		{protocol.CubridTypeDatetime, reflect.TypeOf(time.Time{})},
		{protocol.CubridType(99), reflect.TypeOf([]byte{})}, // unknown
	}
	for _, tt := range types {
		got := scanTypeForCubridType(tt.ct)
		if got != tt.want {
			t.Errorf("scanTypeForCubridType(%v) = %v, want %v", tt.ct, got, tt.want)
		}
	}
}

// --- cubridResult tests ---

func TestCubridResult(t *testing.T) {
	r := &cubridResult{lastInsertID: 42, rowsAffected: 5}
	id, err := r.LastInsertId()
	if err != nil || id != 42 {
		t.Errorf("LastInsertId = %d, %v", id, err)
	}
	ra, err := r.RowsAffected()
	if err != nil || ra != 5 {
		t.Errorf("RowsAffected = %d, %v", ra, err)
	}
}

// --- cubridStmt tests ---

func TestCubridStmtCloseClosed(t *testing.T) {
	s := &cubridStmt{closed: true, conn: &cubridConn{}}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCubridStmtCloseConnClosed(t *testing.T) {
	s := &cubridStmt{
		conn: &cubridConn{closed: true, netConn: &mockConn{}},
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !s.closed {
		t.Fatal("should be closed")
	}
}

func TestCubridStmtCloseWithMockConn(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	s := &cubridStmt{
		handle: 1,
		conn: &cubridConn{
			netConn:    mc,
			casInfo:    protocol.NewCASInfo(),
			autoCommit: true,
			dsn:        DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
		},
	}
	// Will fail on sendRequest (reconnect fails), but Close shouldn't panic.
	_ = s.Close()
	if !s.closed {
		t.Fatal("should be closed")
	}
}

func TestCubridStmtNumInput(t *testing.T) {
	s := &cubridStmt{bindCount: 3, conn: &cubridConn{}}
	if s.NumInput() != 3 {
		t.Errorf("NumInput = %d", s.NumInput())
	}
}

func TestCubridStmtExecClosed(t *testing.T) {
	s := &cubridStmt{closed: true, conn: &cubridConn{}}
	_, err := s.Exec(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCubridStmtQueryClosed(t *testing.T) {
	s := &cubridStmt{closed: true, conn: &cubridConn{}}
	_, err := s.Query(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- cubridTx tests ---

func TestCubridTxCommitNotInTx(t *testing.T) {
	tx := &cubridTx{conn: &cubridConn{inTx: false}}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCubridTxRollbackNotInTx(t *testing.T) {
	tx := &cubridTx{conn: &cubridConn{inTx: false}}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestCubridTxCommitWithMock(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		inTx:    true,
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	tx := &cubridTx{conn: c}
	err := tx.Commit()
	if err == nil {
		t.Fatal("expected error from mock")
	}
	if c.inTx {
		t.Fatal("inTx should be false after commit attempt")
	}
}

func TestCubridTxRollbackWithMock(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		inTx:    true,
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	tx := &cubridTx{conn: c}
	err := tx.Rollback()
	if err == nil {
		t.Fatal("expected error from mock")
	}
	if c.inTx {
		t.Fatal("inTx should be false after rollback attempt")
	}
}

// --- conn tests ---

func TestConnClose(t *testing.T) {
	mc := &mockConn{}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if !c.closed {
		t.Fatal("should be closed")
	}
	if !mc.closed {
		t.Fatal("net conn should be closed")
	}
	// Double close is safe.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnBeginBasic(t *testing.T) {
	c := &cubridConn{netConn: &mockConn{}}
	tx, err := c.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil {
		t.Fatal("tx should not be nil")
	}
	if !c.inTx {
		t.Fatal("should be in tx")
	}
}

func TestConnPrepareCtxConnClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.Prepare("SELECT 1")
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

func TestConnResetSessionNotInTx(t *testing.T) {
	c := &cubridConn{
		netConn:    &mockConn{},
		autoCommit: false,
		dsn:        DSN{AutoCommit: true},
	}
	err := c.ResetSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !c.autoCommit {
		t.Fatal("autoCommit should be restored")
	}
}

func TestConnResetSessionClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

func TestConnResetSessionInTx(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		inTx:    true,
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.ResetSession(context.Background())
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

func TestConnCheckNamedValue(t *testing.T) {
	c := &cubridConn{}

	tests := []struct {
		name string
		val  interface{}
	}{
		{"*CubridEnum", &CubridEnum{Name: "A"}},
		{"CubridEnum", CubridEnum{Name: "B"}},
		{"*CubridMonetary", &CubridMonetary{Amount: 1.0}},
		{"CubridMonetary", CubridMonetary{Amount: 2.0}},
		{"*CubridNumeric", &CubridNumeric{value: "3.14"}},
		{"CubridNumeric", CubridNumeric{value: "2.71"}},
		{"*CubridJson", &CubridJson{value: "{}"}},
		{"CubridJson", CubridJson{value: "[]"}},
		{"*CubridOid", &CubridOid{PageID: 1, SlotID: 2, VolID: 3}},
		{"*CubridSet", &CubridSet{Elements: []interface{}{1}}},
		{"*CubridMultiSet", &CubridMultiSet{Elements: []interface{}{2}}},
		{"*CubridSequence", &CubridSequence{Elements: []interface{}{3}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nv := &driver.NamedValue{Ordinal: 1, Value: tt.val}
			err := c.CheckNamedValue(nv)
			if err != nil {
				t.Errorf("CheckNamedValue(%T) = %v", tt.val, err)
			}
		})
	}

	// Unknown type should return ErrSkip.
	nv := &driver.NamedValue{Ordinal: 1, Value: 42}
	if err := c.CheckNamedValue(nv); err != driver.ErrSkip {
		t.Errorf("expected ErrSkip, got %v", err)
	}
}

func TestConnEndTransaction(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.endTransaction(context.Background(), protocol.TranCommit)
	if err == nil {
		t.Fatal("expected error from mock")
	}
}

func TestConnPingWithMock(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.Ping(context.Background())
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

// --- Driver.Open test ---

func TestDriverOpen(t *testing.T) {
	d := &CubridDriver{}
	// Invalid DSN.
	_, err := d.Open("invalid")
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
	// Valid DSN but can't connect.
	_, err = d.Open("cubrid://dba:@192.0.2.1:33000/test?connect_timeout=50ms")
	if err == nil {
		t.Fatal("expected connection error")
	}
}

// --- SchemaRows tests ---

func TestSchemaRowsColumns(t *testing.T) {
	sr := &SchemaRows{
		columns: []ColumnMeta{{Name: "TABLE_NAME"}, {Name: "TABLE_TYPE"}},
	}
	cols := sr.Columns()
	if len(cols) != 2 || cols[0] != "TABLE_NAME" {
		t.Fatalf("Columns = %v", cols)
	}
}

func TestSchemaRowsClose(t *testing.T) {
	sr := &SchemaRows{
		buffer: [][]interface{}{{"test"}},
	}
	if err := sr.Close(); err != nil {
		t.Fatal(err)
	}
	if !sr.closed {
		t.Fatal("should be closed")
	}
	if sr.buffer != nil {
		t.Fatal("buffer should be nil")
	}
	// Double close is safe.
	if err := sr.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaRowsNextWithData(t *testing.T) {
	sr := &SchemaRows{
		buffer:    [][]interface{}{{"table1"}, {"table2"}},
		bufferIdx: 0,
		done:      true,
	}
	row, err := sr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if row[0] != "table1" {
		t.Errorf("row 1: %v", row)
	}
	row, err = sr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if row[0] != "table2" {
		t.Errorf("row 2: %v", row)
	}
	_, err = sr.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// --- parseSchemaInfoResponse test ---

func TestParseSchemaInfoResponse(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteInt(&body, 2)  // num_tuple
	protocol.WriteInt(&body, 1)  // col_count = 1
	// Schema column (minimal: type + scale + precision + name)
	protocol.WriteByte(&body, byte(protocol.CubridTypeString))
	protocol.WriteShort(&body, 0)
	protocol.WriteInt(&body, 255)
	protocol.WriteNullTermString(&body, "TABLE_NAME")

	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV7}}
	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	sr, err := parseSchemaInfoResponse(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(sr.columns) != 1 {
		t.Fatalf("columns: got %d, want 1", len(sr.columns))
	}
	if sr.columns[0].Name != "TABLE_NAME" {
		t.Errorf("column name = %q", sr.columns[0].Name)
	}
	if sr.totalTuples != 2 {
		t.Errorf("totalTuples = %d, want 2", sr.totalTuples)
	}
}

// --- LobHandle tests ---

func TestLobHandleValue(t *testing.T) {
	h := &CubridLobHandle{LobType: LobBlob, Size: 100, Locator: "loc1"}
	val, err := h.Value()
	if err != nil {
		t.Fatal(err)
	}
	b, ok := val.([]byte)
	if !ok {
		t.Fatal("expected []byte")
	}
	// Verify round-trip.
	h2, err := decodeLobHandle(b)
	if err != nil {
		t.Fatal(err)
	}
	if h2.LobType != LobBlob || h2.Size != 100 || h2.Locator != "loc1" {
		t.Errorf("round-trip mismatch: %+v", h2)
	}
}

func TestLobTypeString(t *testing.T) {
	if LobBlob.String() != "BLOB" {
		t.Errorf("BLOB string = %q", LobBlob.String())
	}
	if LobClob.String() != "CLOB" {
		t.Errorf("CLOB string = %q", LobClob.String())
	}
	if LobType(99).String() != "LOB(99)" {
		t.Errorf("unknown LOB string = %q", LobType(99).String())
	}
}

func TestDecodeLobHandleErrors(t *testing.T) {
	// Too short.
	_, err := decodeLobHandle([]byte{0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 100})
	if err == nil {
		t.Fatal("expected error for short data")
	}

	// Invalid type.
	data := make([]byte, 17)
	binary.BigEndian.PutUint32(data[0:4], 99)
	binary.BigEndian.PutUint32(data[12:16], 1)
	_, err = decodeLobHandle(data)
	if err == nil {
		t.Fatal("expected error for invalid type")
	}

	// Negative locator size.
	data2 := make([]byte, 17)
	binary.BigEndian.PutUint32(data2[0:4], 23)
	binary.BigEndian.PutUint32(data2[12:16], uint32(0xFFFFFFFF)) // -1
	_, err = decodeLobHandle(data2)
	if err == nil {
		t.Fatal("expected error for negative locator")
	}
}

func TestDecodeLobHandleAlternateTypeCodes(t *testing.T) {
	// Type code 33 should decode as BLOB, 34 as CLOB.
	for _, tc := range []struct {
		code    uint32
		lobType LobType
	}{
		{33, LobBlob},
		{34, LobClob},
	} {
		data := make([]byte, 18)
		binary.BigEndian.PutUint32(data[0:4], tc.code)
		binary.BigEndian.PutUint64(data[4:12], 50)
		binary.BigEndian.PutUint32(data[12:16], 2)
		copy(data[16:], "x\x00")
		h, err := decodeLobHandle(data)
		if err != nil {
			t.Fatalf("code %d: %v", tc.code, err)
		}
		if h.LobType != tc.lobType {
			t.Errorf("code %d: type = %v, want %v", tc.code, h.LobType, tc.lobType)
		}
	}
}

// --- LOB streaming with closed conn ---

func TestLobNewClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.lobNew(context.Background(), LobBlob)
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

func TestLobWriteClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.lobWrite(context.Background(), &CubridLobHandle{}, 0, []byte("data"))
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

func TestLobReadClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.lobRead(context.Background(), &CubridLobHandle{}, 0, 100)
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

// --- CubridJson tests ---

func TestCubridJsonDriverValue(t *testing.T) {
	j := &CubridJson{value: `{"key":"val"}`}
	v, err := j.DriverValue()
	if err != nil || v != `{"key":"val"}` {
		t.Errorf("DriverValue = %v, %v", v, err)
	}
}

func TestCubridJsonScanAllTypes(t *testing.T) {
	var j CubridJson
	j.Scan("test")
	if j.value != "test" {
		t.Errorf("Scan string: %q", j.value)
	}
	j.Scan([]byte("raw"))
	if j.value != "raw" {
		t.Errorf("Scan bytes: %q", j.value)
	}
	j.Scan(nil)
	if j.value != "" {
		t.Errorf("Scan nil: %q", j.value)
	}
	other := &CubridJson{value: "other"}
	j.Scan(other)
	if j.value != "other" {
		t.Errorf("Scan *CubridJson: %q", j.value)
	}
}

func TestMarshalCubridJsonError(t *testing.T) {
	// Channels can't be marshaled.
	_, err := MarshalCubridJson(make(chan int))
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- CubridNumeric tests ---

func TestCubridNumericDriverValue(t *testing.T) {
	n := &CubridNumeric{value: "12345.67"}
	v, err := n.DriverValue()
	if err != nil || v != "12345.67" {
		t.Errorf("DriverValue = %v, %v", v, err)
	}
}

func TestCubridNumericScanAllTypes(t *testing.T) {
	var n CubridNumeric
	n.Scan("3.14")
	if n.value != "3.14" {
		t.Errorf("Scan string: %q", n.value)
	}
	n.Scan([]byte("2.71"))
	if n.value != "2.71" {
		t.Errorf("Scan bytes: %q", n.value)
	}
	n.Scan(nil)
	if n.value != "" {
		t.Errorf("Scan nil: %q", n.value)
	}
	other := &CubridNumeric{value: "99"}
	n.Scan(other)
	if n.value != "99" {
		t.Errorf("Scan *CubridNumeric: %q", n.value)
	}
}

// --- OID Scan additional paths ---

func TestCubridOidScanNil(t *testing.T) {
	o := &CubridOid{PageID: 1, SlotID: 2, VolID: 3}
	o.Scan(nil)
	if o.PageID != 0 || o.SlotID != 0 || o.VolID != 0 {
		t.Errorf("after nil scan: %v", o)
	}
}

func TestCubridOidScanBytes(t *testing.T) {
	data := NewCubridOid(100, 5, 2).Encode()
	var o CubridOid
	if err := o.Scan(data); err != nil {
		t.Fatal(err)
	}
	if o.PageID != 100 || o.SlotID != 5 || o.VolID != 2 {
		t.Errorf("scan bytes: %v", o)
	}
}

func TestCubridOidScanBytesTooShort(t *testing.T) {
	var o CubridOid
	if err := o.Scan([]byte{0, 0}); err == nil {
		t.Fatal("expected error")
	}
}

// --- Temporal TZ Scan additional paths ---

func TestTemporalTzScanAllPaths(t *testing.T) {
	now := time.Now()

	// CubridTimestampTz
	var ts CubridTimestampTz
	ts.Scan(now)
	if ts.Time != now {
		t.Error("TimestampTz scan time")
	}
	ts2 := &CubridTimestampTz{Time: now, Timezone: "UTC"}
	ts.Scan(ts2)
	if ts.Timezone != "UTC" {
		t.Error("TimestampTz scan *CubridTimestampTz")
	}
	ts.Scan(nil)
	if !ts.Time.IsZero() {
		t.Error("TimestampTz scan nil")
	}

	// CubridTimestampLtz
	var tsl CubridTimestampLtz
	tsl.Scan(now)
	tsl2 := &CubridTimestampLtz{Time: now, Timezone: "KST"}
	tsl.Scan(tsl2)
	tsl.Scan(nil)

	// CubridDateTimeTz
	var dt CubridDateTimeTz
	dt.Scan(now)
	dt2 := &CubridDateTimeTz{Time: now, Timezone: "PST"}
	dt.Scan(dt2)
	dt.Scan(nil)

	// CubridDateTimeLtz
	var dtl CubridDateTimeLtz
	dtl.Scan(now)
	dtl2 := &CubridDateTimeLtz{Time: now, Timezone: "EST"}
	dtl.Scan(dtl2)
	dtl.Scan(nil)
}

func TestTemporalTzNewAndString(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tsl := NewCubridTimestampLtz(now, "UTC")
	if tsl.String() == "" {
		t.Error("empty string")
	}

	dtl := NewCubridDateTimeLtz(now, "UTC")
	if dtl.String() == "" {
		t.Error("empty string")
	}
}

// --- Collection Scan additional paths ---

func TestCollectionScanSliceTypes(t *testing.T) {
	// CubridSet from []interface{}
	var s CubridSet
	s.Scan([]interface{}{1, 2, 3})
	if len(s.Elements) != 3 {
		t.Fatalf("set elements: %d", len(s.Elements))
	}

	// CubridMultiSet from []interface{}
	var ms CubridMultiSet
	ms.Scan([]interface{}{"a", "b"})
	if len(ms.Elements) != 2 {
		t.Fatalf("multiset elements: %d", len(ms.Elements))
	}

	// CubridSequence from []interface{}
	var seq CubridSequence
	seq.Scan([]interface{}{true})
	if len(seq.Elements) != 1 {
		t.Fatalf("sequence elements: %d", len(seq.Elements))
	}
}

// --- DecodeValue comprehensive tests ---

func TestDecodeValueAllTypes(t *testing.T) {
	tests := []struct {
		name    string
		cubType protocol.CubridType
		data    []byte
		check   func(interface{}) bool
	}{
		{"NULL type", protocol.CubridTypeNull, nil, func(v interface{}) bool { return v == nil }},
		{"SHORT", protocol.CubridTypeShort, []byte{0, 42}, func(v interface{}) bool { return v == int16(42) }},
		{"INT", protocol.CubridTypeInt, []byte{0, 0, 0, 10}, func(v interface{}) bool { return v == int32(10) }},
		{"BIGINT", protocol.CubridTypeBigInt, []byte{0, 0, 0, 0, 0, 0, 0, 5}, func(v interface{}) bool { return v == int64(5) }},
		{"FLOAT", protocol.CubridTypeFloat, func() []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, math.Float32bits(1.5))
			return b
		}(), func(v interface{}) bool { return v == float32(1.5) }},
		{"DOUBLE", protocol.CubridTypeDouble, func() []byte {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, math.Float64bits(2.5))
			return b
		}(), func(v interface{}) bool { return v == float64(2.5) }},
		{"MONETARY", protocol.CubridTypeMonetary, func() []byte {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, math.Float64bits(9.99))
			return b
		}(), func(v interface{}) bool { m, ok := v.(*CubridMonetary); return ok && m.Amount == 9.99 }},
		{"STRING", protocol.CubridTypeString, []byte("hello\x00"), func(v interface{}) bool { return v == "hello" }},
		{"CHAR", protocol.CubridTypeChar, []byte("abc"), func(v interface{}) bool { return v == "abc" }},
		{"NUMERIC", protocol.CubridTypeNumeric, []byte("3.14\x00"), func(v interface{}) bool {
			n, ok := v.(*CubridNumeric)
			return ok && n.value == "3.14"
		}},
		{"DATE", protocol.CubridTypeDate, func() []byte {
			b := make([]byte, 6)
			binary.BigEndian.PutUint16(b[0:2], 2024)
			binary.BigEndian.PutUint16(b[2:4], 3)
			binary.BigEndian.PutUint16(b[4:6], 15)
			return b
		}(), func(v interface{}) bool {
			tt, ok := v.(time.Time)
			return ok && tt.Year() == 2024 && tt.Month() == 3 && tt.Day() == 15
		}},
		{"TIME", protocol.CubridTypeTime, func() []byte {
			b := make([]byte, 6)
			binary.BigEndian.PutUint16(b[0:2], 14)
			binary.BigEndian.PutUint16(b[2:4], 30)
			binary.BigEndian.PutUint16(b[4:6], 0)
			return b
		}(), func(v interface{}) bool {
			tt, ok := v.(time.Time)
			return ok && tt.Hour() == 14 && tt.Minute() == 30
		}},
		{"TIMESTAMP", protocol.CubridTypeTimestamp, func() []byte {
			b := make([]byte, 12)
			binary.BigEndian.PutUint16(b[0:2], 2024)
			binary.BigEndian.PutUint16(b[2:4], 6)
			binary.BigEndian.PutUint16(b[4:6], 1)
			binary.BigEndian.PutUint16(b[6:8], 12)
			binary.BigEndian.PutUint16(b[8:10], 0)
			binary.BigEndian.PutUint16(b[10:12], 0)
			return b
		}(), func(v interface{}) bool {
			tt, ok := v.(time.Time)
			return ok && tt.Year() == 2024 && tt.Month() == 6
		}},
		{"DATETIME", protocol.CubridTypeDatetime, func() []byte {
			b := make([]byte, 14)
			binary.BigEndian.PutUint16(b[0:2], 2024)
			binary.BigEndian.PutUint16(b[2:4], 1)
			binary.BigEndian.PutUint16(b[4:6], 1)
			binary.BigEndian.PutUint16(b[6:8], 0)
			binary.BigEndian.PutUint16(b[8:10], 0)
			binary.BigEndian.PutUint16(b[10:12], 0)
			binary.BigEndian.PutUint16(b[12:14], 500)
			return b
		}(), func(v interface{}) bool {
			tt, ok := v.(time.Time)
			return ok && tt.Nanosecond() == 500_000_000
		}},
		{"BIT", protocol.CubridTypeBit, []byte{0xFF}, func(v interface{}) bool {
			b, ok := v.([]byte)
			return ok && len(b) == 1 && b[0] == 0xFF
		}},
		{"ENUM", protocol.CubridTypeEnum, []byte("RED\x00"), func(v interface{}) bool {
			e, ok := v.(*CubridEnum)
			return ok && e.Name == "RED"
		}},
		{"JSON", protocol.CubridTypeJSON, []byte(`{"a":1}` + "\x00"), func(v interface{}) bool {
			j, ok := v.(*CubridJson)
			return ok && j.value == `{"a":1}`
		}},
		{"OBJECT", protocol.CubridTypeObject, make([]byte, 8), func(v interface{}) bool {
			_, ok := v.(*CubridOid)
			return ok
		}},
		{"unknown", protocol.CubridType(99), []byte{1, 2, 3}, func(v interface{}) bool {
			b, ok := v.([]byte)
			return ok && len(b) == 3
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := decodeValue(tt.cubType, tt.data)
			if err != nil {
				t.Fatalf("decodeValue(%v): %v", tt.cubType, err)
			}
			if !tt.check(val) {
				t.Errorf("decodeValue(%v) = %v (%T)", tt.cubType, val, val)
			}
		})
	}
}

func TestDecodeValueShortData(t *testing.T) {
	shortTests := []struct {
		ct   protocol.CubridType
		data []byte
	}{
		{protocol.CubridTypeShort, []byte{0}},
		{protocol.CubridTypeInt, []byte{0, 0}},
		{protocol.CubridTypeBigInt, []byte{0, 0, 0, 0}},
		{protocol.CubridTypeFloat, []byte{0, 0}},
		{protocol.CubridTypeDouble, []byte{0, 0, 0, 0}},
		{protocol.CubridTypeMonetary, []byte{0, 0, 0}},
		{protocol.CubridTypeDate, []byte{0, 0, 0}},
		{protocol.CubridTypeTime, []byte{0, 0}},
		{protocol.CubridTypeTimestamp, []byte{0, 0, 0, 0, 0}},
		{protocol.CubridTypeDatetime, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{protocol.CubridTypeUShort, []byte{0}},
		{protocol.CubridTypeUInt, []byte{0, 0}},
		{protocol.CubridTypeUBigInt, []byte{0, 0, 0, 0}},
	}
	for _, tt := range shortTests {
		_, err := decodeValue(tt.ct, tt.data)
		if err == nil {
			t.Errorf("decodeValue(%v, short data) expected error", tt.ct)
		}
	}
}

// --- EncodeBindValue comprehensive tests ---

func TestEncodeBindValueAllTypes(t *testing.T) {
	tests := []struct {
		name string
		val  interface{}
	}{
		{"nil", nil},
		{"int64", int64(42)},
		{"int32", int32(10)},
		{"int16", int16(5)},
		{"int", 7},
		{"float64", float64(1.5)},
		{"float32", float32(2.5)},
		{"bool true", true},
		{"bool false", false},
		{"string", "hello"},
		{"bytes", []byte{1, 2, 3}},
		{"time", time.Now()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := encodeBindValue(tt.val)
			if err != nil {
				t.Errorf("encodeBindValue(%T): %v", tt.val, err)
			}
		})
	}
}

// --- PrepareAndExecRows tests ---

func TestPrepareAndExecRowsColumns(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{
			columns: []ColumnMeta{{Name: "id"}, {Name: "val"}},
		},
	}
	cols := r.Columns()
	if len(cols) != 2 || cols[0] != "id" {
		t.Fatalf("Columns = %v", cols)
	}
}

func TestPrepareAndExecRowsClose(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer: [][]interface{}{{"test"}},
		},
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !r.inner.closed {
		t.Fatal("should be closed")
	}
}

func TestPrepareAndExecRowsNext(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer:      [][]interface{}{{int64(1)}, {int64(2)}},
			bufferIdx:   0,
			totalTuples: 2,
			done:        true,
		},
	}
	if !r.Next() {
		t.Fatal("expected true")
	}
	if !r.Next() {
		t.Fatal("expected true for row 2")
	}
	// After consuming both, buffer exhausted + done = false.
	var id interface{}
	r.Scan(&id) // consume row 1
	r.Scan(&id) // consume row 2
}

func TestPrepareAndExecRowsNextDone(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer:    nil,
			bufferIdx: 0,
			done:      true,
			closed:    false,
		},
	}
	if r.Next() {
		t.Fatal("expected false")
	}
}

func TestPrepareAndExecRowsNextClosed(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{closed: true},
	}
	if r.Next() {
		t.Fatal("expected false")
	}
}

// --- parseTuples tests ---

func TestParseTuplesMultiRow(t *testing.T) {
	columns := []ColumnMeta{
		{Name: "id", Type: protocol.CubridTypeInt},
		{Name: "name", Type: protocol.CubridTypeString},
	}

	var buf bytes.Buffer
	// Tuple 1: id=1, name="a"
	protocol.WriteInt(&buf, 0) // row index
	buf.Write(make([]byte, 8)) // OID
	protocol.WriteInt(&buf, 4)
	binary.Write(&buf, binary.BigEndian, int32(1))
	nameData := []byte("a\x00")
	protocol.WriteInt(&buf, int32(len(nameData)))
	buf.Write(nameData)

	// Tuple 2: id=2, name=NULL
	protocol.WriteInt(&buf, 1) // row index
	buf.Write(make([]byte, 8)) // OID
	protocol.WriteInt(&buf, 4)
	binary.Write(&buf, binary.BigEndian, int32(2))
	protocol.WriteInt(&buf, 0) // NULL

	tuples, err := parseTuples(bytes.NewReader(buf.Bytes()), 2, columns)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 2 {
		t.Fatalf("tuples: got %d, want 2", len(tuples))
	}
	if tuples[0][0] != int32(1) {
		t.Errorf("tuple 0, col 0 = %v", tuples[0][0])
	}
	if tuples[1][1] != nil {
		t.Errorf("tuple 1, col 1 should be nil, got %v", tuples[1][1])
	}
}

// --- fetchMore tests ---

func TestFetchMorePastTotal(t *testing.T) {
	r := &cubridRows{
		fetchPos:    100,
		totalTuples: 10,
	}
	err := r.fetchMore()
	if err != nil {
		t.Fatal(err)
	}
	if !r.done {
		t.Fatal("should be done")
	}
}

func TestFetchMoreClosedConn2(t *testing.T) {
	r := &cubridRows{
		stmt: &cubridStmt{
			conn: &cubridConn{closed: true},
		},
		fetchPos:    1,
		totalTuples: 10,
	}
	err := r.fetchMore()
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

// --- parseColumnMeta with collections ---

func TestParseColumnMetaCollectionFull(t *testing.T) {
	for _, tc := range []struct {
		flag byte
		want protocol.CubridType
	}{
		{protocol.CollectionSet, protocol.CubridTypeSet},
		{protocol.CollectionMultiSet, protocol.CubridTypeMultiSet},
		{protocol.CollectionSequence, protocol.CubridTypeSequence},
		{0x00, protocol.CubridTypeInt}, // no collection, just element type
	} {
		var buf bytes.Buffer
		protocol.WriteByte(&buf, protocol.CollectionMarker|tc.flag)
		protocol.WriteByte(&buf, byte(protocol.CubridTypeInt))
		protocol.WriteShort(&buf, 0)                       // scale
		protocol.WriteInt(&buf, 10)                        // precision
		protocol.WriteNullTermString(&buf, "col")          // name
		protocol.WriteNullTermString(&buf, "col")          // realname
		protocol.WriteNullTermString(&buf, "tbl")          // table
		protocol.WriteByte(&buf, 0x01)                     // nullable
		protocol.WriteNullTermString(&buf, "")             // default
		protocol.WriteByte(&buf, 0x00)                     // auto_inc
		protocol.WriteByte(&buf, 0x00)                     // unique
		protocol.WriteByte(&buf, 0x00)                     // primary
		protocol.WriteByte(&buf, 0x00)                     // reverse_idx
		protocol.WriteByte(&buf, 0x00)                     // reverse_unique
		protocol.WriteByte(&buf, 0x00)                     // foreign
		protocol.WriteByte(&buf, 0x00)                     // shared

		col, err := parseColumnMeta(bytes.NewReader(buf.Bytes()), protocol.ProtocolV7)
		if err != nil {
			t.Fatalf("flag %x: %v", tc.flag, err)
		}
		if col.Type != tc.want {
			t.Errorf("flag %x: type = %v, want %v", tc.flag, col.Type, tc.want)
		}
	}
}

func TestParseColumnMetaNonCollection(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteByte(&buf, byte(protocol.CubridTypeDouble))
	protocol.WriteShort(&buf, 2)
	protocol.WriteInt(&buf, 15)
	protocol.WriteNullTermString(&buf, "amount")
	protocol.WriteNullTermString(&buf, "amount")
	protocol.WriteNullTermString(&buf, "orders")
	protocol.WriteByte(&buf, 0x00) // not nullable
	protocol.WriteNullTermString(&buf, "0.0")
	protocol.WriteByte(&buf, 0x00) // auto_inc
	protocol.WriteByte(&buf, 0x01) // unique
	protocol.WriteByte(&buf, 0x01) // primary
	protocol.WriteByte(&buf, 0x00) // reverse_idx
	protocol.WriteByte(&buf, 0x00) // reverse_unique
	protocol.WriteByte(&buf, 0x01) // foreign
	protocol.WriteByte(&buf, 0x00) // shared

	col, err := parseColumnMeta(bytes.NewReader(buf.Bytes()), protocol.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	if col.Type != protocol.CubridTypeDouble {
		t.Errorf("type = %v", col.Type)
	}
	if !col.UniqueKey || !col.PrimaryKey || !col.ForeignKey {
		t.Error("expected unique, pk, fk")
	}
	if col.Nullable || col.AutoIncrement {
		t.Error("should not be nullable or auto_inc")
	}
	if col.DefaultValue != "0.0" {
		t.Errorf("default = %q", col.DefaultValue)
	}
}

// --- parseSchemaColumnMeta tests ---

func TestParseSchemaColumnMetaAllCollections(t *testing.T) {
	for _, tc := range []struct {
		flag byte
		want protocol.CubridType
	}{
		{protocol.CollectionMultiSet, protocol.CubridTypeMultiSet},
		{protocol.CollectionSequence, protocol.CubridTypeSequence},
	} {
		var buf bytes.Buffer
		protocol.WriteByte(&buf, protocol.CollectionMarker|tc.flag)
		protocol.WriteByte(&buf, byte(protocol.CubridTypeString))
		protocol.WriteShort(&buf, 0)
		protocol.WriteInt(&buf, 255)
		protocol.WriteNullTermString(&buf, "col")

		col, err := parseSchemaColumnMeta(bytes.NewReader(buf.Bytes()), protocol.ProtocolV7)
		if err != nil {
			t.Fatal(err)
		}
		if col.Type != tc.want {
			t.Errorf("flag %x: type = %v, want %v", tc.flag, col.Type, tc.want)
		}
	}
}

// --- DecodeCollectionValue tests ---

func TestDecodeCollectionValueEmpty(t *testing.T) {
	// Empty data.
	val, err := decodeCollectionValue(protocol.CubridTypeSet, protocol.CubridTypeInt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if val == nil {
		t.Fatal("expected non-nil (empty set)")
	}
}

// --- Error tests ---

func TestParseErrorResponseMalformed(t *testing.T) {
	err := parseErrorResponse([]byte{0, 0})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCubridErrorIsMatching(t *testing.T) {
	e := &CubridError{Code: -1000, Message: "test"}
	if !e.Is(ErrDBMSError) {
		t.Error("should match ErrDBMSError")
	}
	if e.Is(ErrCASInternal) {
		t.Error("should not match ErrCASInternal")
	}
	if e.Is(io.EOF) {
		t.Error("should not match io.EOF")
	}
}

func TestCubridErrorString(t *testing.T) {
	e := &CubridError{Code: -1000, Message: ""}
	if e.Error() != "cubrid: error -1000" {
		t.Errorf("Error() = %q", e.Error())
	}
	e2 := &CubridError{Code: -1000, Message: "test"}
	if e2.Error() != "cubrid: error -1000: test" {
		t.Errorf("Error() = %q", e2.Error())
	}
}

// --- XID tests ---

func TestXIDRoundTrip(t *testing.T) {
	xid := &XID{
		FormatID:            42,
		GlobalTransactionID: []byte("global-txn-id"),
		BranchQualifier:     []byte("branch-qual"),
	}
	data := xid.encode()
	decoded, err := decodeXID(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.FormatID != 42 {
		t.Errorf("formatID = %d", decoded.FormatID)
	}
	if string(decoded.GlobalTransactionID) != "global-txn-id" {
		t.Errorf("gtrid = %q", decoded.GlobalTransactionID)
	}
	if string(decoded.BranchQualifier) != "branch-qual" {
		t.Errorf("bqual = %q", decoded.BranchQualifier)
	}
}

func TestDecodeXIDTooShort(t *testing.T) {
	_, err := decodeXID([]byte{0, 0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Batch/Savepoint/DBParam internal methods ---

func TestExecuteBatchAutocommitFlag(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn:    mc,
		casInfo:    protocol.NewCASInfo(),
		autoCommit: true,
		dsn:        DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	// Should attempt to send and fail from EOF.
	err := c.executeBatch(context.Background(), []string{"INSERT INTO t VALUES(1)"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSavepointCreate(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.savepoint(context.Background(), savepointCreate, "sp1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSavepointRollback(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.savepoint(context.Background(), savepointRollback, "sp1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetDBParameterSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.getDBParameter(context.Background(), ParamIsolationLevel)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSetDBParameterSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.setDBParameter(context.Background(), ParamIsolationLevel, 4)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Advanced operations internal methods ---

func TestNextResultSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.nextResult(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCursorUpdateSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.cursorUpdate(context.Background(), 1, 1, []interface{}{42, "test"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCursorUpdateWithNullValue(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.cursorUpdate(context.Background(), 1, 1, []interface{}{nil})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetGeneratedKeysSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.getGeneratedKeys(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetRowCountSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.getRowCount(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetLastInsertIDSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.getLastInsertID(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCursorCloseSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.cursorClose(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- XA internal methods ---

func TestXaPrepareRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	xid := &XID{FormatID: 1, GlobalTransactionID: []byte("g"), BranchQualifier: []byte("b")}
	err := c.xaPrepare(context.Background(), xid)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestXaRecoverRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.xaRecover(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestXaEndTranRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	xid := &XID{FormatID: 1, GlobalTransactionID: []byte("g"), BranchQualifier: []byte("b")}
	err := c.xaEndTran(context.Background(), xid, XaCommit)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- prepareAndExecute/prepareAndQuery internal ---

func TestPrepareAndExecuteSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn:    mc,
		casInfo:    protocol.NewCASInfo(),
		autoCommit: true,
		dsn:        DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond, QueryTimeout: 5 * time.Second},
	}
	_, err := c.prepareAndExecute(context.Background(), "INSERT INTO t VALUES(?)", []interface{}{42}, false)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepareAndQuerySendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn:    mc,
		casInfo:    protocol.NewCASInfo(),
		autoCommit: false,
		inTx:       true,
		dsn:        DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.prepareAndQuery(context.Background(), "SELECT * FROM t", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- OidGet/OidPut closed ---

func TestOidGetClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := c.oidGet(context.Background(), &CubridOid{PageID: 1}, []string{"id"})
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

func TestOidPutClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	err := c.oidPut(context.Background(), &CubridOid{PageID: 1}, map[string]interface{}{"name": "test"})
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

func TestOidPutWithNullValue(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	err := c.oidPut(context.Background(), &CubridOid{PageID: 1}, map[string]interface{}{"name": nil})
	if err == nil {
		t.Fatal("expected error from mock")
	}
}

func TestOidGetSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
	}
	_, err := c.oidGet(context.Background(), &CubridOid{PageID: 1, SlotID: 2, VolID: 3}, []string{"id", "name"})
	if err == nil {
		t.Fatal("expected error from mock")
	}
}

// --- SchemaInfo closed ---

func TestSchemaInfoClosed(t *testing.T) {
	c := &cubridConn{closed: true}
	_, err := schemaInfo(context.Background(), c, SchemaClass, "", "", SchemaFlagExact)
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

// --- execute method tests ---

func TestStmtExecuteSendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	s := &cubridStmt{
		handle: 1,
		conn: &cubridConn{
			netConn:    mc,
			casInfo:    protocol.NewCASInfo(),
			autoCommit: true,
			dsn:        DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond, QueryTimeout: 10 * time.Second},
		},
	}
	_, err := s.ExecContext(context.Background(), []driver.NamedValue{
		{Ordinal: 1, Value: int64(42)},
		{Ordinal: 2, Value: nil},
	})
	if err == nil {
		t.Fatal("expected error from mock")
	}
}

func TestStmtQuerySendsRequest(t *testing.T) {
	mc := &mockConn{readErr: io.EOF}
	s := &cubridStmt{
		handle: 1,
		conn: &cubridConn{
			netConn:    mc,
			casInfo:    protocol.NewCASInfo(),
			autoCommit: false,
			inTx:       true,
			dsn:        DSN{Host: "192.0.2.1", Port: 33000, ConnectTimeout: 50 * time.Millisecond},
		},
	}
	_, err := s.QueryContext(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error from mock")
	}
}

// --- Timezone decode tests ---

func TestDecodeTimestampTzFull(t *testing.T) {
	// Build timestamp tz data: 12 bytes timestamp + TZ string
	data := make([]byte, 20)
	binary.BigEndian.PutUint16(data[0:2], 2024)
	binary.BigEndian.PutUint16(data[2:4], 6)
	binary.BigEndian.PutUint16(data[4:6], 15)
	binary.BigEndian.PutUint16(data[6:8], 14)
	binary.BigEndian.PutUint16(data[8:10], 30)
	binary.BigEndian.PutUint16(data[10:12], 45)
	copy(data[12:], "UTC\x00")

	val, err := decodeValue(protocol.CubridTypeTsTz, data)
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := val.(*CubridTimestampTz)
	if !ok {
		t.Fatalf("expected *CubridTimestampTz, got %T", val)
	}
	if ts.Timezone != "UTC" {
		t.Errorf("timezone = %q", ts.Timezone)
	}
}

func TestDecodeDatetimeTzFull(t *testing.T) {
	data := make([]byte, 22)
	binary.BigEndian.PutUint16(data[0:2], 2024)
	binary.BigEndian.PutUint16(data[2:4], 1)
	binary.BigEndian.PutUint16(data[4:6], 1)
	binary.BigEndian.PutUint16(data[6:8], 0)
	binary.BigEndian.PutUint16(data[8:10], 0)
	binary.BigEndian.PutUint16(data[10:12], 0)
	binary.BigEndian.PutUint16(data[12:14], 500)
	copy(data[14:], "+09:00\x00")

	val, err := decodeValue(protocol.CubridTypeDtTz, data)
	if err != nil {
		t.Fatal(err)
	}
	dt, ok := val.(*CubridDateTimeTz)
	if !ok {
		t.Fatalf("expected *CubridDateTimeTz, got %T", val)
	}
	if dt.Timezone != "+09:00" {
		t.Errorf("timezone = %q", dt.Timezone)
	}
}

func TestDecodeTimestampLtz(t *testing.T) {
	data := make([]byte, 20)
	binary.BigEndian.PutUint16(data[0:2], 2024)
	binary.BigEndian.PutUint16(data[2:4], 3)
	binary.BigEndian.PutUint16(data[4:6], 1)
	binary.BigEndian.PutUint16(data[6:8], 12)
	binary.BigEndian.PutUint16(data[8:10], 0)
	binary.BigEndian.PutUint16(data[10:12], 0)
	copy(data[12:], "UTC\x00")

	val, err := decodeValue(protocol.CubridTypeTsLtz, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := val.(*CubridTimestampLtz); !ok {
		t.Fatalf("expected *CubridTimestampLtz, got %T", val)
	}
}

func TestDecodeDatetimeLtz(t *testing.T) {
	data := make([]byte, 22)
	binary.BigEndian.PutUint16(data[0:2], 2024)
	binary.BigEndian.PutUint16(data[2:4], 6)
	binary.BigEndian.PutUint16(data[4:6], 1)
	binary.BigEndian.PutUint16(data[6:8], 0)
	binary.BigEndian.PutUint16(data[8:10], 0)
	binary.BigEndian.PutUint16(data[10:12], 0)
	binary.BigEndian.PutUint16(data[12:14], 0)
	copy(data[14:], "UTC\x00")

	val, err := decodeValue(protocol.CubridTypeDtLtz, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := val.(*CubridDateTimeLtz); !ok {
		t.Fatalf("expected *CubridDateTimeLtz, got %T", val)
	}
}

// --- LOB decode in DecodeValue ---

func TestDecodeValueBlob(t *testing.T) {
	// Build a LOB handle.
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 23) // BLOB
	protocol.WriteLong(&buf, 1024)
	loc := "blob_locator"
	protocol.WriteInt(&buf, int32(len(loc)+1))
	buf.Write([]byte(loc))
	buf.WriteByte(0x00)

	val, err := decodeValue(protocol.CubridTypeBlob, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	h, ok := val.(*CubridLobHandle)
	if !ok {
		t.Fatalf("expected *CubridLobHandle, got %T", val)
	}
	if h.LobType != LobBlob || h.Size != 1024 {
		t.Errorf("handle = %+v", h)
	}
}

func TestDecodeValueBlobSmall(t *testing.T) {
	// Small data (not a LOB handle).
	val, err := decodeValue(protocol.CubridTypeBlob, []byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	b, ok := val.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", val)
	}
	if len(b) != 4 {
		t.Errorf("len = %d", len(b))
	}
}

// --- readOpenDatabaseResponse test with mock data ---

func TestReadOpenDatabaseResponseShortBody(t *testing.T) {
	// Build a valid-looking response: length + casinfo + short body
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 6) // length (includes 4-byte casinfo)
	buf.Write([]byte{0, 0, 0, 0}) // casinfo
	buf.Write([]byte{0, 0})       // body too short (< 4 bytes)

	mc := &readableMockConn{Reader: bytes.NewReader(buf.Bytes())}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
	}
	_, err := c.readOpenDatabaseResponse()
	if err == nil {
		t.Fatal("expected error for short body")
	}
}

func TestReadOpenDatabaseResponseNegativePID(t *testing.T) {
	// Simulate error response: cas_pid is negative.
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 16)                  // length
	buf.Write([]byte{0, 0, 0, 0})               // casinfo
	binary.Write(&buf, binary.BigEndian, int32(-1)) // negative pid (error)
	// Error body: code + message
	binary.Write(&buf, binary.BigEndian, int32(-1000))
	buf.Write([]byte("auth failed\x00"))

	mc := &readableMockConn{Reader: bytes.NewReader(buf.Bytes())}
	c := &cubridConn{
		netConn: mc,
		casInfo: protocol.NewCASInfo(),
	}
	_, err := c.readOpenDatabaseResponse()
	if err == nil {
		t.Fatal("expected error")
	}
}

// readableMockConn is a mock net.Conn that reads from a bytes.Reader.
type readableMockConn struct {
	mockConn
	Reader *bytes.Reader
}

func (m *readableMockConn) Read(b []byte) (int, error) {
	return m.Reader.Read(b)
}

// --- parsePrepareResponse with full column metadata ---

func TestParsePrepareResponseWithColumns(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteInt(&body, 0)                           // cache_lifetime
	protocol.WriteByte(&body, byte(protocol.StmtSelect))  // stmt_type
	protocol.WriteInt(&body, 2)                            // bind_count
	protocol.WriteByte(&body, 0x00)                        // updatable

	// 2 columns.
	protocol.WriteInt(&body, 2)
	body.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
	body.Write(buildColumnMetaBytes(protocol.CubridTypeString, "name"))

	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV7}}
	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	stmt, err := parsePrepareResponse(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.bindCount != 2 {
		t.Errorf("bindCount = %d", stmt.bindCount)
	}
	if len(stmt.columns) != 2 {
		t.Errorf("columns = %d", len(stmt.columns))
	}
	if stmt.columns[0].Name != "id" || stmt.columns[1].Name != "name" {
		t.Errorf("columns: %v, %v", stmt.columns[0].Name, stmt.columns[1].Name)
	}
}

// --- parseQueryResult with inline tuples ---

func TestParseQueryResultWithInlineTuples(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00) // cache_reusable
	protocol.WriteInt(&body, 1)     // result_count
	protocol.WriteByte(&body, byte(protocol.StmtSelect))
	protocol.WriteInt(&body, 2)     // affected/tuple count
	body.Write(make([]byte, 8))     // OID
	protocol.WriteInt(&body, 0)     // cache sec
	protocol.WriteInt(&body, 0)     // cache usec
	protocol.WriteByte(&body, 0x00) // include_column_info = 0
	protocol.WriteInt(&body, -1)    // shard_id

	// Inline fetch data.
	protocol.WriteInt(&body, 0) // fetch_code
	protocol.WriteInt(&body, 1) // tuple_count = 1

	// One tuple with one INT column.
	protocol.WriteInt(&body, 0) // row index
	body.Write(make([]byte, 8)) // OID
	protocol.WriteInt(&body, 4) // col value size
	binary.Write(&body, binary.BigEndian, int32(99))

	cols := []ColumnMeta{{Name: "id", Type: protocol.CubridTypeInt}}
	stmt := &cubridStmt{
		conn:    &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}},
		columns: cols,
	}
	frame := &protocol.ResponseFrame{ResponseCode: 2, Body: body.Bytes()}
	rows, err := parseQueryResult(stmt, frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.buffer) != 1 {
		t.Fatalf("inline buffer: %d", len(rows.buffer))
	}
	if rows.buffer[0][0] != int32(99) {
		t.Errorf("value = %v", rows.buffer[0][0])
	}
	if rows.fetchPos != 2 {
		t.Errorf("fetchPos = %d", rows.fetchPos)
	}
}

// --- DSN String coverage ---

func TestDSNStringAllParams(t *testing.T) {
	dsn := DSN{
		Host:           "localhost",
		Port:           33000,
		Database:       "testdb",
		User:           "dba",
		Password:       "secret",
		AutoCommit:     false,
		Charset:        "utf-8",
		ConnectTimeout: 5 * time.Second,
		QueryTimeout:   30 * time.Second,
		IsolationLevel: 6,
		LockTimeout:    10,
	}
	s := dsn.String()
	if s == "" {
		t.Fatal("empty string")
	}
}
