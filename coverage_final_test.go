package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

// --- Schema SQL helpers via mock server ---

func setupSchemaQueryServer(t *testing.T) (*mockCASServer, *sql.DB) {
	t.Helper()
	srv := newMockCASServer(t)

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)                          // cache_lifetime
		protocol.WriteByte(&resp, byte(protocol.StmtSelect)) // stmt_type
		protocol.WriteInt(&resp, -1)                         // bind_count = -1 (unknown, skip arg check)
		protocol.WriteByte(&resp, 0x00)                      // updatable
		protocol.WriteInt(&resp, 1)                          // col_count = 1
		resp.Write(buildColumnMetaBytes(protocol.CubridTypeString, "result"))
		return 1, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 2) // 2 tuples
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)

		// Inline fetch: 2 rows.
		protocol.WriteInt(&resp, 0) // fetch_code
		protocol.WriteInt(&resp, 2) // tuple_count

		// Row 1: "item_a"
		protocol.WriteInt(&resp, 0)
		resp.Write(make([]byte, 8))
		data1 := []byte("item_a\x00")
		protocol.WriteInt(&resp, int32(len(data1)))
		resp.Write(data1)

		// Row 2: "item_b"
		protocol.WriteInt(&resp, 1)
		resp.Write(make([]byte, 8))
		data2 := []byte("item_b\x00")
		protocol.WriteInt(&resp, int32(len(data2)))
		resp.Write(data2)

		return 2, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	return srv, db
}

func TestListTables(t *testing.T) {
	srv, db := setupSchemaQueryServer(t)
	defer srv.close()
	defer db.Close()

	tables, err := ListTables(context.Background(), db)
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
	if tables[0] != "item_a" {
		t.Errorf("tables[0] = %q", tables[0])
	}
}

func TestListViews(t *testing.T) {
	srv, db := setupSchemaQueryServer(t)
	defer srv.close()
	defer db.Close()

	views, err := ListViews(context.Background(), db)
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("expected 2 views, got %d", len(views))
	}
}

func TestListPrimaryKeys(t *testing.T) {
	srv, db := setupSchemaQueryServer(t)
	defer srv.close()
	defer db.Close()

	keys, err := ListPrimaryKeys(context.Background(), db, "test_table")
	if err != nil {
		t.Fatalf("ListPrimaryKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestListSuperClasses(t *testing.T) {
	srv, db := setupSchemaQueryServer(t)
	defer srv.close()
	defer db.Close()

	supers, err := ListSuperClasses(context.Background(), db, "child")
	if err != nil {
		t.Fatalf("ListSuperClasses: %v", err)
	}
	if len(supers) != 2 {
		t.Fatalf("expected 2, got %d", len(supers))
	}
}

func TestListSubClasses(t *testing.T) {
	srv, db := setupSchemaQueryServer(t)
	defer srv.close()
	defer db.Close()

	subs, err := ListSubClasses(context.Background(), db, "parent")
	if err != nil {
		t.Fatalf("ListSubClasses: %v", err)
	}
	if len(subs) != 2 {
		t.Fatalf("expected 2, got %d", len(subs))
	}
}

func TestGetInheritanceInfo(t *testing.T) {
	srv, db := setupSchemaQueryServer(t)
	defer srv.close()
	defer db.Close()

	info, err := GetInheritanceInfo(context.Background(), db, "child")
	if err != nil {
		t.Fatalf("GetInheritanceInfo: %v", err)
	}
	if info.ClassName != "child" {
		t.Errorf("ClassName = %q", info.ClassName)
	}
	if len(info.SuperClasses) != 2 || len(info.SubClasses) != 2 {
		t.Errorf("supers=%d subs=%d", len(info.SuperClasses), len(info.SubClasses))
	}
}

// ListColumns and ListConstraints need multi-column results.
func setupMultiColServer(t *testing.T, colCount int) (*mockCASServer, *sql.DB) {
	t.Helper()
	srv := newMockCASServer(t)

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, int32(colCount))
		for i := 0; i < colCount; i++ {
			resp.Write(buildColumnMetaBytes(protocol.CubridTypeString, fmt.Sprintf("c%d", i)))
		}
		return 1, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1)
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)

		// 1 row with colCount columns.
		protocol.WriteInt(&resp, 0) // fetch_code
		protocol.WriteInt(&resp, 1)
		protocol.WriteInt(&resp, 0) // row index
		resp.Write(make([]byte, 8))
		for i := 0; i < colCount; i++ {
			val := []byte(fmt.Sprintf("val%d\x00", i))
			protocol.WriteInt(&resp, int32(len(val)))
			resp.Write(val)
		}
		return 1, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	return srv, db
}

func TestListColumns(t *testing.T) {
	// ListColumns expects 6 columns: attr_name, data_type, prec, scale, is_nullable, default_value
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 6) // 6 columns
		for _, name := range []string{"attr_name", "data_type", "prec", "scale", "is_nullable", "default_value"} {
			if name == "prec" || name == "scale" {
				resp.Write(buildColumnMetaBytes(protocol.CubridTypeInt, name))
			} else {
				resp.Write(buildColumnMetaBytes(protocol.CubridTypeString, name))
			}
		}
		return 1, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1)
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)

		protocol.WriteInt(&resp, 0) // fetch_code
		protocol.WriteInt(&resp, 1) // 1 row
		protocol.WriteInt(&resp, 0)
		resp.Write(make([]byte, 8))

		// attr_name
		v1 := []byte("id\x00")
		protocol.WriteInt(&resp, int32(len(v1)))
		resp.Write(v1)
		// data_type
		v2 := []byte("INTEGER\x00")
		protocol.WriteInt(&resp, int32(len(v2)))
		resp.Write(v2)
		// prec (INT)
		protocol.WriteInt(&resp, 4)
		binary.Write(&resp, binary.BigEndian, int32(10))
		// scale (INT)
		protocol.WriteInt(&resp, 4)
		binary.Write(&resp, binary.BigEndian, int32(0))
		// is_nullable
		v5 := []byte("YES\x00")
		protocol.WriteInt(&resp, int32(len(v5)))
		resp.Write(v5)
		// default_value (NULL)
		protocol.WriteInt(&resp, 0)

		return 1, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	cols, err := ListColumns(context.Background(), db, "test_table")
	if err != nil {
		t.Fatalf("ListColumns: %v", err)
	}
	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d", len(cols))
	}
	if cols[0].Name != "id" || cols[0].DataType != "INTEGER" {
		t.Errorf("col = %+v", cols[0])
	}
	if !cols[0].IsNullable {
		t.Error("expected nullable")
	}
}

func TestListConstraints(t *testing.T) {
	// ListConstraints expects 4 columns: index_name, is_unique, is_primary_key, is_foreign_key
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepare, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 4)
		for _, name := range []string{"index_name", "is_unique", "is_primary_key", "is_foreign_key"} {
			resp.Write(buildColumnMetaBytes(protocol.CubridTypeString, name))
		}
		return 1, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeExecute, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1)
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)

		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 1)
		protocol.WriteInt(&resp, 0)
		resp.Write(make([]byte, 8))

		for _, v := range []string{"pk_test\x00", "YES\x00", "YES\x00", "NO\x00"} {
			protocol.WriteInt(&resp, int32(len(v)))
			resp.Write([]byte(v))
		}
		return 1, resp.Bytes()
	})

	srv.setHandler(protocol.FuncCodeCloseReqHandle, func(body []byte) (int32, []byte) {
		return 0, nil
	})

	db := openTestDB(t, srv)
	defer db.Close()

	constraints, err := ListConstraints(context.Background(), db, "test_table")
	if err != nil {
		t.Fatalf("ListConstraints: %v", err)
	}
	if len(constraints) != 1 {
		t.Fatalf("expected 1, got %d", len(constraints))
	}
	if constraints[0].Name != "pk_test" || !constraints[0].IsPrimaryKey {
		t.Errorf("constraint = %+v", constraints[0])
	}
}

// SchemaRows.Next with already-exhausted-and-done state.
func TestSchemaRowsNextExhaustedDone(t *testing.T) {
	sr := &SchemaRows{
		buffer:    [][]interface{}{{"a"}},
		bufferIdx: 1, // exhausted
		done:      true,
	}
	_, err := sr.Next()
	if err == nil {
		t.Fatal("expected EOF")
	}
}

// --- PrepareAndExecRows.Next with fetchMore needed ---

func TestPrepareAndExecRowsNextFetchNeeded(t *testing.T) {
	r := &PrepareAndExecRows{
		inner: &cubridRows{
			buffer:      [][]interface{}{{int64(1)}},
			bufferIdx:   1, // exhausted
			totalTuples: 1,
			done:        true,
		},
	}
	if r.Next() {
		t.Fatal("should be false (done + exhausted)")
	}
}

// --- DecodeCollectionValue default branch ---

func TestDecodeCollectionValueDefault(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteByte(&buf, byte(protocol.CubridTypeInt))
	protocol.WriteInt(&buf, 1)
	protocol.WriteInt(&buf, 4)
	binary.Write(&buf, binary.BigEndian, int32(42))

	// Use unknown collection type (not SET/MULTISET/SEQUENCE).
	val, err := DecodeCollectionValue(protocol.CubridType(99), protocol.CubridTypeInt, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	// Default branch returns *CubridSequence.
	if _, ok := val.(*CubridSequence); !ok {
		t.Fatalf("expected *CubridSequence, got %T", val)
	}
}

// --- Stmt.Exec and Stmt.Query (non-context wrappers) ---

func TestMockServerStmtExecQuery(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

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

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Test via conn.Raw to access the stmt directly.
	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*cubridConn)
		stmt, err := c.Prepare("INSERT INTO t VALUES(1)")
		if err != nil {
			return err
		}
		defer stmt.Close()

		result, err := stmt.Exec(nil)
		if err != nil {
			return err
		}
		ra, _ := result.RowsAffected()
		lid, _ := result.LastInsertId()
		_ = ra
		_ = lid
		_ = stmt.NumInput()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- ColumnTypeScanType out of bounds ---

func TestColumnTypeScanTypeOutOfBounds(t *testing.T) {
	r := &cubridRows{columns: []ColumnMeta{}}
	st := r.ColumnTypeScanType(0)
	if st == nil {
		t.Fatal("expected non-nil default type")
	}
}
