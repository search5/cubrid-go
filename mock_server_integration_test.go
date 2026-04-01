package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

// helper: build exec result body for mock responses
func buildExecResultBody(affected int32) []byte {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00)                        // cache_reusable
	protocol.WriteInt(&body, 1)                             // result_count
	protocol.WriteByte(&body, byte(protocol.StmtInsert))   // stmt_type
	protocol.WriteInt(&body, affected)                      // affected
	body.Write(make([]byte, 8))                             // OID
	protocol.WriteInt(&body, 0)                             // cache sec
	protocol.WriteInt(&body, 0)                             // cache usec
	protocol.WriteByte(&body, 0x00)                         // include_column_info = 0
	protocol.WriteInt(&body, -1)                            // shard_id
	return body.Bytes()
}

// helper: build prepare response body
func buildPrepareBody(stmtType protocol.StmtType, bindCount int32) []byte {
	var body bytes.Buffer
	protocol.WriteInt(&body, 0)                      // cache_lifetime
	protocol.WriteByte(&body, byte(stmtType))        // stmt_type
	protocol.WriteInt(&body, bindCount)              // bind_count
	protocol.WriteByte(&body, 0x00)                  // updatable
	protocol.WriteInt(&body, 0)                      // col_count = 0
	return body.Bytes()
}

func openTestDB(t *testing.T, srv *mockCASServer) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("cubrid://dba:@127.0.0.1:%d/testdb?connect_timeout=5s", srv.port())
	db, err := sql.Open("cubrid", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func TestMockServerPing(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetDBVersion, func(body []byte) (int32, []byte) {
		return 0, []byte("11.4.0\x00")
	})

	db := openTestDB(t, srv)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestMockServerExec(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		return 1, buildPrepareBody(protocol.StmtInsert, 1)
	})
	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		return 1, buildExecResultBody(1)
	})
	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	result, err := db.ExecContext(context.Background(), "INSERT INTO t VALUES(?)", 42)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	ra, _ := result.RowsAffected()
	if ra != 1 {
		t.Errorf("RowsAffected = %d", ra)
	}
}

func TestMockServerBeginCommit(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeEndTran, func(body []byte) (int32, []byte) {
		return 0, nil
	})
	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		return 1, buildPrepareBody(protocol.StmtInsert, 0)
	})
	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		return 1, buildExecResultBody(1)
	})
	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	_, err = tx.Exec("INSERT INTO t VALUES(1)")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestMockServerBeginRollback(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeEndTran, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestMockServerClose(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	db := openTestDB(t, srv)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMockServerBatchExec(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeExecuteBatch, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	err := BatchExec(db, []string{
		"INSERT INTO t VALUES(1)",
		"INSERT INTO t VALUES(2)",
	})
	if err != nil {
		t.Fatalf("BatchExec: %v", err)
	}
}

func TestMockServerBatchExecEmpty(t *testing.T) {
	err := BatchExec(nil, nil)
	if err != nil {
		t.Fatalf("expected nil for empty batch, got %v", err)
	}
}

func TestMockServerSavepoint(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeSavepoint, func(body []byte) (int32, []byte) {
		return 0, nil
	})
	srv.setHandler(protocol.FuncCodeEndTran, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := Savepoint(conn, "sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}
	if err := SavepointContext(context.Background(), conn, "sp1"); err != nil {
		t.Fatalf("SavepointContext: %v", err)
	}
	if err := RollbackToSavepoint(conn, "sp1"); err != nil {
		t.Fatalf("RollbackToSavepoint: %v", err)
	}
	if err := RollbackToSavepointContext(context.Background(), conn, "sp1"); err != nil {
		t.Fatalf("RollbackToSavepointContext: %v", err)
	}
}

func TestMockServerGetSetParam(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetDBParameter, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 4) // isolation level value
		return 0, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeSetDBParameter, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	val, err := GetParam(conn, ParamIsolationLevel)
	if err != nil {
		t.Fatalf("GetParam: %v", err)
	}
	if val != 4 {
		t.Errorf("param value = %d, want 4", val)
	}

	val2, err := GetParamContext(context.Background(), conn, ParamLockTimeout)
	if err != nil {
		t.Fatalf("GetParamContext: %v", err)
	}
	_ = val2

	if err := SetParam(conn, ParamIsolationLevel, 6); err != nil {
		t.Fatalf("SetParam: %v", err)
	}
	if err := SetParamContext(context.Background(), conn, ParamLockTimeout, 10); err != nil {
		t.Fatalf("SetParamContext: %v", err)
	}
}

func TestMockServerAdvancedOps(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeNextResult, func(body []byte) (int32, []byte) {
		return 5, nil
	})
	srv.setHandler(protocol.FuncCodeCursorUpdate, func(body []byte) (int32, []byte) {
		return 0, nil
	})
	srv.setHandler(protocol.FuncCodeGetGeneratedKeys, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 100)
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeGetRowCount, func(body []byte) (int32, []byte) {
		return 10, nil
	})
	srv.setHandler(protocol.FuncCodeGetLastInsertID, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteNullTermString(&resp, "42")
		return 0, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeCursorClose, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	nr, err := NextResult(context.Background(), conn, 1)
	if err != nil {
		t.Fatalf("NextResult: %v", err)
	}
	if nr != 5 {
		t.Errorf("NextResult = %d", nr)
	}

	if err := CursorUpdate(context.Background(), conn, 1, 1, 42, "test"); err != nil {
		t.Fatalf("CursorUpdate: %v", err)
	}

	keys, err := GetGeneratedKeys(context.Background(), conn, 1)
	if err != nil {
		t.Fatalf("GetGeneratedKeys: %v", err)
	}
	if len(keys) != 1 || keys[0].Value != int64(100) {
		t.Errorf("keys = %v", keys)
	}

	rc, err := GetRowCount(context.Background(), conn)
	if err != nil {
		t.Fatalf("GetRowCount: %v", err)
	}
	if rc != 10 {
		t.Errorf("RowCount = %d", rc)
	}

	lid, err := GetLastInsertID(context.Background(), conn)
	if err != nil {
		t.Fatalf("GetLastInsertID: %v", err)
	}
	if lid != "42" {
		t.Errorf("LastInsertID = %q", lid)
	}

	if err := CursorClose(context.Background(), conn, 1); err != nil {
		t.Fatalf("CursorClose: %v", err)
	}
}

func TestMockServerXaOps(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeXaPrepare, func(body []byte) (int32, []byte) {
		return 0, nil
	})
	srv.setHandler(protocol.FuncCodeXaRecover, func(body []byte) (int32, []byte) {
		// Return 1 XID.
		var resp bytes.Buffer
		xid := &XID{FormatID: 1, GlobalTransactionID: []byte("g"), BranchQualifier: []byte("b")}
		xidData := xid.encode()
		protocol.WriteInt(&resp, int32(len(xidData)))
		resp.Write(xidData)
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeXaEndTran, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	xid := &XID{FormatID: 1, GlobalTransactionID: []byte("g"), BranchQualifier: []byte("b")}

	if err := XaPrepare(context.Background(), conn, xid); err != nil {
		t.Fatalf("XaPrepare: %v", err)
	}

	xids, err := XaRecover(context.Background(), conn)
	if err != nil {
		t.Fatalf("XaRecover: %v", err)
	}
	if len(xids) != 1 {
		t.Fatalf("XaRecover: %d xids", len(xids))
	}

	if err := XaEndTran(context.Background(), conn, xid, XaCommit); err != nil {
		t.Fatalf("XaEndTran: %v", err)
	}
}

func TestMockServerLobOps(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeLOBNew, func(body []byte) (int32, []byte) {
		h := &CubridLobHandle{LobType: LobBlob, Size: 0, Locator: "loc1"}
		return 0, h.Encode()
	})
	srv.setHandler(protocol.FuncCodeLOBWrite, func(body []byte) (int32, []byte) {
		return 5, nil // 5 bytes written
	})
	srv.setHandler(protocol.FuncCodeLOBRead, func(body []byte) (int32, []byte) {
		return 5, []byte("hello")
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

		h, err := LobNew(context.Background(), c, LobBlob)
		if err != nil {
			return fmt.Errorf("LobNew: %w", err)
		}
		if h.Locator != "loc1" {
			return fmt.Errorf("locator = %q", h.Locator)
		}

		n, err := LobWrite(context.Background(), c, h, 0, []byte("hello"))
		if err != nil {
			return fmt.Errorf("LobWrite: %w", err)
		}
		if n != 5 {
			return fmt.Errorf("LobWrite n = %d", n)
		}

		data, err := LobRead(context.Background(), c, h, 0, 5)
		if err != nil {
			return fmt.Errorf("LobRead: %w", err)
		}
		if string(data) != "hello" {
			return fmt.Errorf("LobRead data = %q", data)
		}

		// Test LobReader.
		h.Size = 5
		reader := NewLobReader(context.Background(), c, h)
		buf := make([]byte, 10)
		rn, err := reader.Read(buf)
		if rn != 5 {
			return fmt.Errorf("LobReader.Read n = %d", rn)
		}

		// Test LobWriter.
		h2 := &CubridLobHandle{LobType: LobBlob, Size: 0, Locator: "loc2"}
		writer := NewLobWriter(context.Background(), c, h2)
		wn, err := writer.Write([]byte("world"))
		if err != nil {
			return fmt.Errorf("LobWriter.Write: %w", err)
		}
		if wn != 5 {
			return fmt.Errorf("LobWriter.Write n = %d", wn)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMockServerOidOps(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeOidGet, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		resp.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
		resp.Write(buildTupleBytes(func() []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, 42)
			return b
		}()))
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeOidPut, func(body []byte) (int32, []byte) {
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
		oid := NewCubridOid(100, 5, 2)

		result, err := OidGet(context.Background(), c, oid, []string{"id"})
		if err != nil {
			return fmt.Errorf("OidGet: %w", err)
		}
		if result["id"] != int32(42) {
			return fmt.Errorf("id = %v", result["id"])
		}

		err = OidPut(context.Background(), c, oid, map[string]interface{}{"name": "test"})
		if err != nil {
			return fmt.Errorf("OidPut: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMockServerSchemaInfo(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeSchemaInfo, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 1) // num_tuple
		protocol.WriteInt(&resp, 1) // col_count
		protocol.WriteByte(&resp, byte(protocol.CubridTypeString))
		protocol.WriteShort(&resp, 0)
		protocol.WriteInt(&resp, 255)
		protocol.WriteNullTermString(&resp, "TABLE_NAME")
		return 1, resp.Bytes()
	})

	fetchCount := 0
	srv.setHandler(protocol.FuncCodeFetch, func(body []byte) (int32, []byte) {
		fetchCount++
		if fetchCount == 1 {
			// First fetch: return 1 row.
			var resp bytes.Buffer
			protocol.WriteInt(&resp, 1) // tuple_count
			protocol.WriteInt(&resp, 0) // row index
			resp.Write(make([]byte, 8)) // OID
			nameData := []byte("test_table\x00")
			protocol.WriteInt(&resp, int32(len(nameData)))
			resp.Write(nameData)
			return 0, resp.Bytes()
		}
		// Second fetch: no more data.
		return ErrCodeNoMoreData, nil
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
		sr, err := SchemaInfo(context.Background(), c, SchemaClass, "", "", SchemaFlagExact)
		if err != nil {
			return fmt.Errorf("SchemaInfo: %w", err)
		}
		defer sr.Close()

		cols := sr.Columns()
		if len(cols) != 1 || cols[0] != "TABLE_NAME" {
			return fmt.Errorf("columns = %v", cols)
		}

		row, err := sr.Next()
		if err != nil {
			return fmt.Errorf("Next: %w", err)
		}
		if row[0] != "test_table" {
			return fmt.Errorf("row = %v", row)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMockServerPrepareAndExec(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepareAndExec, func(body []byte) (int32, []byte) {
		// Build combined prepare+execute response.
		var resp bytes.Buffer
		// Prepare body: cache_lifetime(4) + stmt_type(1) + bind_count(4) + updatable(1) + col_count(4) + trailing(4) = 18 bytes
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtInsert))
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0) // trailing

		// Execute body.
		resp.Write(buildExecResultBody(3))
		return 1, resp.Bytes() // query handle = 1
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := PrepareAndExec(context.Background(), conn, "INSERT INTO t VALUES(1)")
	if err != nil {
		t.Fatalf("PrepareAndExec: %v", err)
	}
	ra, _ := result.RowsAffected()
	if ra != 3 {
		t.Errorf("RowsAffected = %d, want 3", ra)
	}
}

func TestMockServerResetSession(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeEndTran, func(body []byte) (int32, []byte) {
		return 0, nil
	})
	srv.setHandler(protocol.FuncCodeGetDBVersion, func(body []byte) (int32, []byte) {
		return 0, []byte("11.4\x00")
	})

	db := openTestDB(t, srv)
	defer db.Close()

	// Get a connection, start a transaction, then return to pool.
	// ResetSession should rollback.
	conn1, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn1.Raw(func(driverConn interface{}) error {
		c := driverConn.(*cubridConn)
		c.inTx = true
		return nil
	})
	conn1.Close()

	// Ping to reuse the pool connection.
	time.Sleep(10 * time.Millisecond)
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping after reset: %v", err)
	}
}

func TestMockServerGetGeneratedKeysNoKeys(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetGeneratedKeys, func(body []byte) (int32, []byte) {
		return 0, nil // no keys
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	keys, err := GetGeneratedKeys(context.Background(), conn, 1)
	if err != nil {
		t.Fatalf("GetGeneratedKeys: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestMockServerGetLastInsertIDFromResponseCode(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetLastInsertID, func(body []byte) (int32, []byte) {
		return 99, nil // no body, use response_code
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	lid, err := GetLastInsertID(context.Background(), conn)
	if err != nil {
		t.Fatalf("GetLastInsertID: %v", err)
	}
	if lid != "99" {
		t.Errorf("LastInsertID = %q, want '99'", lid)
	}
}

func TestMockServerXaRecoverEmpty(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeXaRecover, func(body []byte) (int32, []byte) {
		return 0, nil // no XIDs
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	xids, err := XaRecover(context.Background(), conn)
	if err != nil {
		t.Fatalf("XaRecover: %v", err)
	}
	if len(xids) != 0 {
		t.Errorf("expected 0 xids, got %d", len(xids))
	}
}

func TestMockServerQuery(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		// SELECT with 1 column.
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)                          // cache_lifetime
		protocol.WriteByte(&resp, byte(protocol.StmtSelect)) // stmt_type
		protocol.WriteInt(&resp, 0)                          // bind_count
		protocol.WriteByte(&resp, 0x00)                      // updatable
		protocol.WriteInt(&resp, 1)                          // col_count = 1
		resp.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		// Return 1 tuple inline.
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)                        // cache_reusable
		protocol.WriteInt(&resp, 1)                            // result_count
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1)                            // tuple count
		resp.Write(make([]byte, 8))                            // OID
		protocol.WriteInt(&resp, 0)                            // cache sec
		protocol.WriteInt(&resp, 0)                            // cache usec
		protocol.WriteByte(&resp, 0x00)                        // include_column_info = 0
		protocol.WriteInt(&resp, -1)                           // shard_id
		// Inline fetch data.
		protocol.WriteInt(&resp, 0)                            // fetch_code
		protocol.WriteInt(&resp, 1)                            // tuple_count = 1
		protocol.WriteInt(&resp, 0)                            // row index
		resp.Write(make([]byte, 8))                            // OID
		protocol.WriteInt(&resp, 4)                            // col size
		binary.Write(&resp, binary.BigEndian, int32(42))
		return 1, resp.Bytes()
	})
	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var id int64
	if err := rows.Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Errorf("id = %d", id)
	}
	if rows.Next() {
		t.Error("expected no more rows")
	}
}
