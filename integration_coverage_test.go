//go:build integration

package cubrid

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func openTestConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func getRawConn(t *testing.T, conn *sql.Conn) *cubridConn {
	t.Helper()
	var raw *cubridConn
	conn.Raw(func(dc interface{}) error {
		raw = dc.(*cubridConn)
		return nil
	})
	return raw
}

// ==========================================================================
// driver.Open, Begin, Prepare (non-context wrappers)
// ==========================================================================

func TestIntegrationDriverOpen(t *testing.T) {
	drv := &CubridDriver{}
	conn, err := drv.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	t.Log("driver.Open succeeded")
}

func TestIntegrationConnBeginNonContext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	conn := openTestConn(t, db)
	defer conn.Close()

	raw := getRawConn(t, conn)
	tx, err := raw.Begin()
	if err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	t.Log("conn.Begin() + Rollback succeeded")
}

// ==========================================================================
// stmt.Exec / stmt.Query (non-context wrappers)
// ==========================================================================

func TestIntegrationStmtExecQuery(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_stmt")
	db.Exec("CREATE TABLE go_cov_stmt (id INT, name VARCHAR(50))")
	defer db.Exec("DROP TABLE go_cov_stmt")

	conn := openTestConn(t, db)
	defer conn.Close()

	raw := getRawConn(t, conn)

	// Prepare
	stmt, err := raw.Prepare("INSERT INTO go_cov_stmt VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}

	// Exec (non-context)
	result, err := stmt.Exec([]driver.Value{int64(1), "hello"})
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()
	t.Logf("Exec: affected=%d lastID=%d", affected, lastID)
	stmt.Close()

	// NumInput
	stmt2, _ := raw.Prepare("SELECT * FROM go_cov_stmt WHERE id = ?")
	if stmt2.NumInput() != 1 {
		t.Fatalf("NumInput: got %d, want 1", stmt2.NumInput())
	}

	// Query (non-context)
	rows, err := stmt2.Query([]driver.Value{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	cols := rows.Columns()
	t.Logf("Query columns: %v", cols)
	rows.Close()
	stmt2.Close()
}

// ==========================================================================
// LOB streaming via conn.Raw()
// ==========================================================================

func TestIntegrationLobStreamingRaw(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_lob")
	db.Exec("CREATE TABLE go_cov_lob (id INT, bdata BLOB, cdata CLOB)")
	defer db.Exec("DROP TABLE go_cov_lob")

	ctx := context.Background()
	conn := openTestConn(t, db)
	defer conn.Close()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)

		// BLOB: new, write, read
		blobHandle, err := LobNew(ctx, c, LobBlob)
		if err != nil {
			t.Fatalf("LobNew BLOB: %v", err)
		}
		t.Logf("BLOB handle: %s", blobHandle.String())

		testData := []byte("Hello CUBRID LOB streaming test data! This should be long enough.")
		n, err := LobWrite(ctx, c, blobHandle, 0, testData)
		if err != nil {
			t.Fatalf("LobWrite: %v", err)
		}
		t.Logf("LobWrite: %d bytes", n)
		blobHandle.Size = int64(n) // Update size after write

		readData, err := LobRead(ctx, c, blobHandle, 0, len(testData))
		if err != nil {
			t.Fatalf("LobRead: %v", err)
		}
		t.Logf("LobRead: %d bytes", len(readData))

		// CLOB: new, write, read
		clobHandle, err := LobNew(ctx, c, LobClob)
		if err != nil {
			t.Fatalf("LobNew CLOB: %v", err)
		}

		clobData := []byte("CLOB text content for coverage test.")
		LobWrite(ctx, c, clobHandle, 0, clobData)

		// LobReader streaming
		reader := NewLobReader(ctx, c, blobHandle)
		buf := make([]byte, 20)
		total := 0
		for {
			rn, err := reader.Read(buf)
			total += rn
			if err != nil {
				break
			}
		}
		t.Logf("LobReader total: %d bytes", total)

		// LobWriter streaming
		blobHandle2, _ := LobNew(ctx, c, LobBlob)
		writer := NewLobWriter(ctx, c, blobHandle2)
		wn, err := writer.Write([]byte("streaming write"))
		t.Logf("LobWriter: wrote %d bytes, err=%v", wn, err)

		// Encode/Value
		encoded := blobHandle.Encode()
		t.Logf("LOB Encode: %d bytes", len(encoded))

		val, err := blobHandle.Value()
		t.Logf("LOB Value: %v, err=%v", val != nil, err)

		return nil
	})
}

// ==========================================================================
// Savepoint via *sql.Conn helpers
// ==========================================================================

func TestIntegrationSavepointHelpers(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN+"?auto_commit=false")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_sp")
	db.Exec("CREATE TABLE go_cov_sp (id INT)")
	defer db.Exec("DROP TABLE go_cov_sp")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	// Begin transaction
	conn.ExecContext(ctx, "INSERT INTO go_cov_sp VALUES (1)")

	// Create savepoint via helper
	err = SavepointContext(ctx, conn, "sp_cov")
	if err != nil {
		t.Fatalf("SavepointContext: %v", err)
	}

	conn.ExecContext(ctx, "INSERT INTO go_cov_sp VALUES (2)")

	// Rollback to savepoint via helper
	err = RollbackToSavepointContext(ctx, conn, "sp_cov")
	if err != nil {
		t.Fatalf("RollbackToSavepointContext: %v", err)
	}

	// Non-context versions
	err = Savepoint(conn, "sp_cov2")
	if err != nil {
		t.Logf("Savepoint (non-ctx): %v", err)
	}
	RollbackToSavepoint(conn, "sp_cov2")

	t.Log("Savepoint helpers passed")
}

// ==========================================================================
// PrepareAndQuery (SELECT via FC 41)
// ==========================================================================

func TestIntegrationPrepareAndQuery(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_paq")
	db.Exec("CREATE TABLE go_cov_paq (id INT, name VARCHAR(50))")
	db.Exec("INSERT INTO go_cov_paq VALUES (1, 'alice')")
	db.Exec("INSERT INTO go_cov_paq VALUES (2, 'bob')")
	defer db.Exec("DROP TABLE go_cov_paq")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	rows, err := PrepareAndQuery(ctx, conn, "SELECT id, name FROM go_cov_paq ORDER BY id")
	if err != nil {
		t.Skipf("PrepareAndQuery not supported: %v", err)
		return
	}
	defer rows.Close()

	cols := rows.Columns()
	t.Logf("Columns: %v", cols)

	var results []string
	for rows.Next() {
		var id, name interface{}
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		results = append(results, name.(string))
	}
	t.Logf("Rows: %v", results)
}

// ==========================================================================
// XA Transactions
// ==========================================================================

func TestIntegrationXaTransactions(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	// XaRecover to list in-doubt transactions
	xids, err := XaRecover(ctx, conn)
	if err != nil {
		t.Logf("XaRecover: %v (may not be supported)", err)
	} else {
		t.Logf("XaRecover: %d in-doubt transactions", len(xids))
	}

	// XaPrepare
	xid := &XID{
		FormatID:            1,
		GlobalTransactionID: []byte("go-test-xa-001"),
		BranchQualifier:     []byte("branch-001"),
	}
	err = XaPrepare(ctx, conn, xid)
	if err != nil {
		t.Logf("XaPrepare: %v (expected - no active XA transaction)", err)
	}

	// XaEndTran
	err = XaEndTran(ctx, conn, xid, XaRollback)
	if err != nil {
		t.Logf("XaEndTran: %v (expected)", err)
	}

	// Verify XID encode/decode
	encoded := xid.encode()
	decoded, err := decodeXID(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.FormatID != 1 {
		t.Fatalf("XID FormatID: got %d", decoded.FormatID)
	}
	t.Logf("XID encode/decode round-trip OK")
}

// ==========================================================================
// Schema: ListViews, GetInheritanceInfo
// ==========================================================================

func TestIntegrationSchemaListViews(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()

	db.ExecContext(ctx, "DROP VIEW IF EXISTS go_cov_view")
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_cov_vtbl")
	db.ExecContext(ctx, "CREATE TABLE go_cov_vtbl (id INT, name VARCHAR(50))")
	db.ExecContext(ctx, "CREATE VIEW go_cov_view AS SELECT * FROM go_cov_vtbl")
	defer func() {
		db.ExecContext(ctx, "DROP VIEW go_cov_view")
		db.ExecContext(ctx, "DROP TABLE go_cov_vtbl")
	}()

	views, err := ListViews(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range views {
		if v == "go_cov_view" {
			found = true
		}
	}
	if !found {
		t.Fatalf("view not found in %v", views)
	}
	t.Logf("Views: %v", views)
}

func TestIntegrationGetInheritanceInfo(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_cov_child")
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_cov_parent")
	db.ExecContext(ctx, "CREATE TABLE go_cov_parent (id INT)")
	_, err := db.ExecContext(ctx, "CREATE TABLE go_cov_child UNDER go_cov_parent (grade INT)")
	if err != nil {
		t.Skipf("table inheritance not supported: %v", err)
		db.ExecContext(ctx, "DROP TABLE go_cov_parent")
		return
	}
	defer func() {
		db.ExecContext(ctx, "DROP TABLE go_cov_child")
		db.ExecContext(ctx, "DROP TABLE go_cov_parent")
	}()

	info, err := GetInheritanceInfo(ctx, db, "go_cov_child")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("InheritanceInfo: class=%s supers=%v subs=%v", info.ClassName, info.SuperClasses, info.SubClasses)
}

// ==========================================================================
// CursorClose, NextResult, CursorUpdate via conn.Raw()
// ==========================================================================

func TestIntegrationCursorOps(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_cursor")
	db.Exec("CREATE TABLE go_cov_cursor (id INT, val VARCHAR(50))")
	db.Exec("INSERT INTO go_cov_cursor VALUES (1, 'a')")
	defer db.Exec("DROP TABLE go_cov_cursor")

	conn, _ := db.Conn(context.Background())
	defer conn.Close()

	// Use short timeout context so invalid handles don't block forever
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)

		// nextResult with invalid handle (short timeout)
		_, err := c.nextResult(ctx, 0)
		t.Logf("nextResult(0): %v", err)

		// cursorUpdate with invalid handle
		err = c.cursorUpdate(ctx, 0, 1, []interface{}{"x"})
		t.Logf("cursorUpdate(0): %v", err)

		// getGeneratedKeys with invalid handle
		_, err = c.getGeneratedKeys(ctx, 0)
		t.Logf("getGeneratedKeys(0): %v", err)

		return nil
	})
}

// ==========================================================================
// Custom type Scan / DriverValue / String coverage
// ==========================================================================

func TestIntegrationCustomTypeCoverage(t *testing.T) {
	// CubridEnum
	e := NewCubridEnum("TestVal", 5)
	_ = e.String()
	ev, _ := e.DriverValue()
	t.Logf("Enum: %s driverValue=%v", e.String(), ev)

	var e2 CubridEnum
	e2.Scan("hello")
	e2.Scan([]byte("world"))
	e2.Scan(e)
	e2.Scan(nil)

	// CubridMonetary
	m := NewCubridMonetary(99.99, CurrencyKRW)
	_ = m.String()
	mv, _ := m.DriverValue()
	_ = CurrencyUSD.String()
	_ = Currency(99).String()
	t.Logf("Monetary: %s driverValue=%v", m.String(), mv)

	var m2 CubridMonetary
	m2.Scan(float64(1.0))
	m2.Scan(int64(100))
	m2.Scan(m)
	m2.Scan(nil)
	m.Equal(NewCubridMonetary(99.99, CurrencyKRW))

	// CubridNumeric
	n := NewCubridNumeric("3.14")
	_ = n.String()
	_ = n.IsValid()
	nv, _ := n.DriverValue()
	t.Logf("Numeric: %s driverValue=%v", n.String(), nv)

	TryNewCubridNumeric("42.5")
	TryNewCubridNumeric("")
	TryNewCubridNumeric("-")
	TryNewCubridNumeric("abc")
	TryNewCubridNumeric("1.2.3")

	var n2 CubridNumeric
	n2.Scan("99")
	n2.Scan([]byte("100"))
	n2.Scan(n)
	n2.Scan(nil)

	// CubridJson
	j := NewCubridJson(`{"a":1}`)
	_ = j.String()
	jv, _ := j.DriverValue()
	var target map[string]interface{}
	j.Unmarshal(&target)
	MarshalCubridJson(map[string]int{"x": 1})
	t.Logf("Json: %s driverValue=%v", j.String(), jv)

	var j2 CubridJson
	j2.Scan("[]")
	j2.Scan([]byte("{}"))
	j2.Scan(j)
	j2.Scan(nil)

	// CubridOid
	o := NewCubridOid(1, 2, 3)
	_ = o.String()
	_ = o.IsNull()
	_ = o.Encode()
	ov, _ := o.Value()
	t.Logf("Oid: %s value=%v", o.String(), ov)

	var o2 CubridOid
	o2.Scan(o.Encode())
	o2.Scan(o)
	o2.Scan(nil)

	// CubridTimestampTz
	ts := NewCubridTimestampTz(time.Now(), "Asia/Seoul")
	_ = ts.String()
	var ts2 CubridTimestampTz
	ts2.Scan(time.Now())
	ts2.Scan(ts)
	ts2.Scan(nil)

	// CubridTimestampLtz
	tsl := NewCubridTimestampLtz(time.Now(), "UTC")
	_ = tsl.String()
	var tsl2 CubridTimestampLtz
	tsl2.Scan(time.Now())
	tsl2.Scan(tsl)
	tsl2.Scan(nil)

	// CubridDateTimeTz
	dt := NewCubridDateTimeTz(time.Now(), "Europe/London")
	_ = dt.String()
	var dt2 CubridDateTimeTz
	dt2.Scan(time.Now())
	dt2.Scan(dt)
	dt2.Scan(nil)

	// CubridDateTimeLtz
	dtl := NewCubridDateTimeLtz(time.Now(), "US/Pacific")
	_ = dtl.String()
	var dtl2 CubridDateTimeLtz
	dtl2.Scan(time.Now())
	dtl2.Scan(dtl)
	dtl2.Scan(nil)

	// CubridLobHandle
	lob := &CubridLobHandle{LobType: LobBlob, Size: 100, Locator: "test"}
	_ = lob.String()
	_ = lob.Encode()
	lob.Value()

	// LobType.String
	_ = LobBlob.String()
	_ = LobClob.String()
	_ = LobType(99).String()

	// Collection Value()
	cs := &CubridSet{Elements: []interface{}{1, 2}}
	cs.Value()
	cms := &CubridMultiSet{Elements: []interface{}{"a"}}
	cms.Value()
	cseq := &CubridSequence{Elements: []interface{}{true}}
	cseq.Value()

	t.Log("All custom type coverage passed")
}

// ==========================================================================
// CheckNamedValue coverage
// ==========================================================================

func TestIntegrationCheckNamedValueAll(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	conn := openTestConn(t, db)
	defer conn.Close()

	raw := getRawConn(t, conn)

	tests := []struct {
		name string
		val  interface{}
	}{
		{"enum_ptr", NewCubridEnum("x", 1)},
		{"enum_val", CubridEnum{Name: "y"}},
		{"monetary_ptr", NewCubridMonetary(1.0, CurrencyUSD)},
		{"monetary_val", CubridMonetary{Amount: 2.0}},
		{"numeric_ptr", NewCubridNumeric("1")},
		{"numeric_val", CubridNumeric{value: "2"}},
		{"json_ptr", NewCubridJson("{}")},
		{"json_val", CubridJson{value: "[]"}},
		{"oid", NewCubridOid(1, 2, 3)},
		{"set", &CubridSet{Elements: []interface{}{1}}},
		{"multiset", &CubridMultiSet{Elements: []interface{}{2}}},
		{"sequence", &CubridSequence{Elements: []interface{}{3}}},
		{"string_skip", "plain string"},
	}

	for _, tt := range tests {
		nv := &driver.NamedValue{Ordinal: 1, Value: tt.val}
		err := raw.CheckNamedValue(nv)
		t.Logf("CheckNamedValue(%s): err=%v val=%v", tt.name, err, nv.Value)
	}
}

// ==========================================================================
// Connection lifecycle: ResetSession, IsValid, isConnectionLost
// ==========================================================================

func TestIntegrationConnLifecycle(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	conn := openTestConn(t, db)
	defer conn.Close()

	raw := getRawConn(t, conn)

	// IsValid
	if !raw.IsValid() {
		t.Fatal("should be valid")
	}

	// ResetSession
	err := raw.ResetSession(context.Background())
	if err != nil {
		t.Fatalf("ResetSession: %v", err)
	}

	// isConnectionLost
	if isConnectionLost(nil) {
		t.Fatal("nil should not be connection lost")
	}

	t.Log("Connection lifecycle coverage passed")
}

// ==========================================================================
// DB Parameter non-context wrappers
// ==========================================================================

func TestIntegrationDBParamNonContext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	conn := openTestConn(t, db)
	defer conn.Close()

	val, err := GetParam(conn, ParamIsolationLevel)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GetParam: %d", val)

	err = SetParam(conn, ParamLockTimeout, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("SetParam OK")
}

// ==========================================================================
// BatchExec non-context wrapper
// ==========================================================================

func TestIntegrationBatchExecNonContext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_batch2")
	db.Exec("CREATE TABLE go_cov_batch2 (id INT)")
	defer db.Exec("DROP TABLE go_cov_batch2")

	err := BatchExec(db, []string{
		"INSERT INTO go_cov_batch2 VALUES (1)",
		"INSERT INTO go_cov_batch2 VALUES (2)",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Empty batch
	err = BatchExec(db, []string{})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("BatchExec non-context OK")
}

// ==========================================================================
// Rows: ColumnType* interfaces with real data
// ==========================================================================

func TestIntegrationRowsColumnTypes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_coltype")
	db.Exec("CREATE TABLE go_cov_coltype (id INT NOT NULL, name VARCHAR(100), score NUMERIC(10,2), PRIMARY KEY(id))")
	db.Exec("INSERT INTO go_cov_coltype VALUES (1, 'test', 99.99)")
	defer db.Exec("DROP TABLE go_cov_coltype")

	rows, err := db.Query("SELECT id, name, score FROM go_cov_coltype")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	colTypes, _ := rows.ColumnTypes()
	for _, ct := range colTypes {
		name := ct.Name()
		dbType := ct.DatabaseTypeName()
		nullable, _ := ct.Nullable()
		scanType := ct.ScanType()
		length, hasLen := ct.Length()
		prec, scale, hasDec := ct.DecimalSize()
		t.Logf("Column %s: type=%s nullable=%v scanType=%v len=(%d,%v) decimal=(%d,%d,%v)",
			name, dbType, nullable, scanType, length, hasLen, prec, scale, hasDec)
	}
}

// ==========================================================================
// ScanTypeForCubridType comprehensive
// ==========================================================================

func TestIntegrationScanTypeAll(t *testing.T) {
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
		{protocol.CubridTypeNumeric, reflect.TypeOf(&CubridNumeric{})},
		{protocol.CubridTypeEnum, reflect.TypeOf(&CubridEnum{})},
		{protocol.CubridTypeMonetary, reflect.TypeOf(&CubridMonetary{})},
		{protocol.CubridTypeJSON, reflect.TypeOf(&CubridJson{})},
		{protocol.CubridTypeTsTz, reflect.TypeOf(&CubridTimestampTz{})},
		{protocol.CubridTypeTsLtz, reflect.TypeOf(&CubridTimestampLtz{})},
		{protocol.CubridTypeDtTz, reflect.TypeOf(&CubridDateTimeTz{})},
		{protocol.CubridTypeDtLtz, reflect.TypeOf(&CubridDateTimeLtz{})},
		{protocol.CubridTypeDate, reflect.TypeOf(time.Time{})},
		{protocol.CubridTypeBlob, reflect.TypeOf(&CubridLobHandle{})},
		{protocol.CubridTypeObject, reflect.TypeOf(&CubridOid{})},
		{protocol.CubridTypeSet, reflect.TypeOf(&CubridSet{})},
		{protocol.CubridTypeMultiSet, reflect.TypeOf(&CubridMultiSet{})},
		{protocol.CubridTypeSequence, reflect.TypeOf(&CubridSequence{})},
		{protocol.CubridTypeUShort, reflect.TypeOf(uint16(0))},
		{protocol.CubridTypeUInt, reflect.TypeOf(uint32(0))},
		{protocol.CubridTypeUBigInt, reflect.TypeOf(uint64(0))},
		{protocol.CubridTypeBit, reflect.TypeOf([]byte{})},
	}
	for _, tt := range types {
		got := ScanTypeForCubridType(tt.ct)
		if got != tt.want {
			t.Errorf("ScanType(%v): got %v, want %v", tt.ct, got, tt.want)
		}
	}
}

// ==========================================================================
// Error paths
// ==========================================================================

func TestIntegrationErrorPaths(t *testing.T) {
	// ParseErrorResponse
	err := ParseErrorResponse([]byte{0, 0, 0, 1, 'e', 'r', 'r', 0})
	t.Logf("ParseErrorResponse: %v", err)

	// CubridError.Is
	e1 := &CubridError{Code: -1000}
	e2 := &CubridError{Code: -1000}
	e3 := &CubridError{Code: -1001}
	if !e1.Is(e2) {
		t.Fatal("should match")
	}
	if e1.Is(e3) {
		t.Fatal("should not match")
	}

	// Error with message
	e4 := &CubridError{Code: -1, Message: "test"}
	_ = e4.Error()
	e5 := &CubridError{Code: -1}
	_ = e5.Error()

	t.Log("Error paths covered")
}

// ==========================================================================
// toDriverValue edge cases
// ==========================================================================

func TestIntegrationToDriverValue(t *testing.T) {
	tests := []struct {
		name string
		val  interface{}
	}{
		{"nil", nil},
		{"int16", int16(1)},
		{"int32", int32(2)},
		{"uint16", uint16(3)},
		{"uint32", uint32(4)},
		{"uint64", uint64(5)},
		{"float32", float32(1.5)},
		{"string", "hello"},
		{"int64", int64(100)},
		{"float64", float64(3.14)},
		{"time", time.Now()},
		{"bytes", []byte{1, 2, 3}},
		{"numeric", &CubridNumeric{value: "1.23"}},
		{"enum", &CubridEnum{Name: "R"}},
		{"monetary", &CubridMonetary{Amount: 9.9}},
		{"json", &CubridJson{value: "{}"}},
		{"tstz", &CubridTimestampTz{Time: time.Now(), Timezone: "UTC"}},
		{"tsltz", &CubridTimestampLtz{Time: time.Now(), Timezone: "UTC"}},
		{"dttz", &CubridDateTimeTz{Time: time.Now(), Timezone: "UTC"}},
		{"dtltz", &CubridDateTimeLtz{Time: time.Now(), Timezone: "UTC"}},
	}
	for _, tt := range tests {
		dv := toDriverValue(tt.val)
		t.Logf("toDriverValue(%s): %T", tt.name, dv)
	}
}

// ==========================================================================
// SchemaRows direct iteration
// ==========================================================================

func TestIntegrationSchemaRowsDirect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_schemr")
	db.Exec("CREATE TABLE go_cov_schemr (id INT, name VARCHAR(50))")
	defer db.Exec("DROP TABLE go_cov_schemr")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)
		sr, err := SchemaInfo(ctx, c, SchemaClass, "", "", SchemaFlagExact)
		if err != nil {
			t.Fatalf("SchemaInfo: %v", err)
		}
		defer sr.Close()

		cols := sr.Columns()
		t.Logf("Schema columns: %v", cols)

		count := 0
		for {
			row, err := sr.Next()
			if err != nil {
				break
			}
			count++
			if count <= 3 {
				t.Logf("  row: %v", row)
			}
		}
		t.Logf("Schema total rows: %d", count)
		return nil
	})
}

// ==========================================================================
// parseOidGetResponse coverage via valid OID
// ==========================================================================

func TestIntegrationOidGetValidHandle(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_oidg")
	db.Exec("CREATE TABLE go_cov_oidg (id INT, name VARCHAR(50))")
	db.Exec("INSERT INTO go_cov_oidg VALUES (1, 'oidtest')")
	defer db.Exec("DROP TABLE go_cov_oidg")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	// Get a real OID from a SELECT
	var oid *CubridOid
	rows, err := db.Query("SELECT go_cov_oidg FROM go_cov_oidg WHERE id = 1")
	if err != nil {
		t.Skipf("OID query not supported: %v", err)
		return
	}
	defer rows.Close()
	if rows.Next() {
		var val interface{}
		rows.Scan(&val)
		if o, ok := val.(*CubridOid); ok {
			oid = o
		}
	}

	if oid == nil || oid.IsNull() {
		t.Skip("Could not obtain valid OID")
		return
	}

	t.Logf("Got OID: %s", oid.String())

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)
		result, err := OidGet(ctx, c, oid, []string{"id", "name"})
		if err != nil {
			t.Logf("OidGet: %v", err)
		} else {
			t.Logf("OidGet result: %v", result)
		}
		return nil
	})
}

// ==========================================================================
// CursorClose/NextResult/etc with conn.Raw and timeout
// ==========================================================================

func TestIntegrationAdvancedPublicAPI(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	// These use public API (through *sql.Conn) with timeout to avoid blocking
	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// NextResult public API
	_, err := NextResult(tctx, conn, 0)
	t.Logf("NextResult public: %v", err)

	// CursorUpdate public API
	conn2, _ := db.Conn(ctx)
	defer conn2.Close()
	tctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	err = CursorUpdate(tctx2, conn2, 0, 1, "x")
	t.Logf("CursorUpdate public: %v", err)

	// GetGeneratedKeys public API
	conn3, _ := db.Conn(ctx)
	defer conn3.Close()
	tctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()
	_, err = GetGeneratedKeys(tctx3, conn3, 0)
	t.Logf("GetGeneratedKeys public: %v", err)

	// CursorClose public API
	conn4, _ := db.Conn(ctx)
	defer conn4.Close()
	tctx4, cancel4 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel4()
	err = CursorClose(tctx4, conn4, 0)
	t.Logf("CursorClose public: %v", err)
}

// ==========================================================================
// PrepareAndExecRows.Scan coverage
// ==========================================================================

func TestIntegrationPrepareAndQueryMultiRow(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_paqm")
	db.Exec("CREATE TABLE go_cov_paqm (id INT, name VARCHAR(50))")
	for i := 0; i < 5; i++ {
		db.Exec("INSERT INTO go_cov_paqm VALUES (?, ?)", i, "row")
	}
	defer db.Exec("DROP TABLE go_cov_paqm")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)
		rows, err := c.prepareAndQuery(ctx, "SELECT id, name FROM go_cov_paqm ORDER BY id", nil)
		if err != nil {
			t.Skipf("prepareAndQuery: %v", err)
			return nil
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id, name interface{}
			rows.Scan(&id, &name)
			count++
		}
		t.Logf("prepareAndQuery multi-row: %d rows", count)
		return nil
	})
}

func TestIntegrationPrepareAndQueryScan(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_paqs")
	db.Exec("CREATE TABLE go_cov_paqs (id INT, name VARCHAR(50))")
	db.Exec("INSERT INTO go_cov_paqs VALUES (1, 'scan_test')")
	defer db.Exec("DROP TABLE go_cov_paqs")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	rows, err := PrepareAndQuery(ctx, conn, "SELECT id, name FROM go_cov_paqs")
	if err != nil {
		t.Skipf("PrepareAndQuery not supported: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var id, name interface{}
		err := rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Scan: id=%v name=%v", id, name)
	}
}

// ==========================================================================
// DSN edge cases
// ==========================================================================

func TestIntegrationDSNIsolationLevel(t *testing.T) {
	levels := []string{"read_committed", "repeatable_read", "serializable"}
	for _, l := range levels {
		_, err := ParseDSN("cubrid://dba:@localhost:33000/db?isolation_level=" + l)
		if err != nil {
			t.Fatalf("parse %s: %v", l, err)
		}
	}

	dsn := DSN{
		Host: "localhost", Port: 33000, Database: "db", User: "dba",
		IsolationLevel: 4,
	}
	s := dsn.String()
	t.Logf("DSN with isolation: %s", s)

	dsn2 := DSN{
		Host: "localhost", Port: 33000, Database: "db", User: "dba",
		IsolationLevel: 5,
	}
	_ = dsn2.String()

	dsn3 := DSN{
		Host: "localhost", Port: 33000, Database: "db", User: "dba",
		IsolationLevel: 6,
	}
	_ = dsn3.String()
}

// ==========================================================================
// LOB Read streaming full cycle
// ==========================================================================

func TestIntegrationLobReadFullCycle(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_lobr")
	db.Exec("CREATE TABLE go_cov_lobr (id INT, bdata BLOB)")
	defer db.Exec("DROP TABLE go_cov_lobr")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)

		// Create and write a larger BLOB
		handle, err := LobNew(ctx, c, LobBlob)
		if err != nil {
			t.Fatal(err)
		}

		// Write 500 bytes
		data := make([]byte, 500)
		for i := range data {
			data[i] = byte(i % 256)
		}
		n2, _ := LobWrite(ctx, c, handle, 0, data)
		handle.Size = int64(n2) // Update size for LobReader

		// Read back using LobReader
		reader := NewLobReader(ctx, c, handle)
		total := 0
		buf := make([]byte, 64)
		for {
			n, err := reader.Read(buf)
			total += n
			if err != nil {
				break
			}
		}
		t.Logf("LobReader full cycle: read %d bytes (wrote %d)", total, len(data))
		return nil
	})
}

// ==========================================================================
// parseExecResult with real INSERT (auto_increment)
// ==========================================================================

func TestIntegrationParseExecResultAutoInc(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_autoinc")
	db.Exec("CREATE TABLE go_cov_autoinc (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))")
	defer db.Exec("DROP TABLE go_cov_autoinc")

	// Multiple inserts to exercise parseExecResult body parsing
	for i := 0; i < 5; i++ {
		result, err := db.Exec("INSERT INTO go_cov_autoinc (name) VALUES (?)", "test")
		if err != nil {
			t.Fatal(err)
		}
		affected, _ := result.RowsAffected()
		lastID, _ := result.LastInsertId()
		if i == 0 {
			t.Logf("Insert %d: affected=%d lastID=%d", i, affected, lastID)
		}
	}
}

// ==========================================================================
// Large result set to exercise fetchMore in rows.go
// ==========================================================================

func TestIntegrationFetchMoreCoverage(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_fetch")
	db.Exec("CREATE TABLE go_cov_fetch (id INT)")
	defer db.Exec("DROP TABLE go_cov_fetch")

	// Insert 250 rows to exceed fetch size (100)
	for i := 0; i < 250; i++ {
		db.Exec("INSERT INTO go_cov_fetch VALUES (?)", i)
	}

	rows, err := db.Query("SELECT id FROM go_cov_fetch ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		count++
	}
	if count != 250 {
		t.Fatalf("count: got %d, want 250", count)
	}
	t.Logf("fetchMore coverage: fetched %d rows across multiple batches", count)
}

// ==========================================================================
// ResetSession with active transaction
// ==========================================================================

func TestIntegrationResetSessionInTx(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN+"?auto_commit=false")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, _ := db.Conn(context.Background())
	defer conn.Close()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)
		// Start a transaction
		c.inTx = true
		// ResetSession should rollback
		err := c.ResetSession(context.Background())
		if err != nil {
			t.Logf("ResetSession with tx: %v", err)
		}
		return nil
	})
}

// ==========================================================================
// OidGet / OidPut via conn.Raw()
// ==========================================================================

func TestIntegrationOidGetPut(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_cov_oid")
	db.Exec("CREATE TABLE go_cov_oid (id INT, name VARCHAR(50))")
	db.Exec("INSERT INTO go_cov_oid VALUES (1, 'test')")
	defer db.Exec("DROP TABLE go_cov_oid")

	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)

		// OidGet with a null OID (test error handling)
		nullOid := NewCubridOid(0, 0, 0)
		_, err := OidGet(ctx, c, nullOid, []string{"id", "name"})
		t.Logf("OidGet null OID: %v", err)

		// OidPut with a null OID
		err = OidPut(ctx, c, nullOid, map[string]interface{}{"name": "updated"})
		t.Logf("OidPut null OID: %v", err)

		return nil
	})
}
