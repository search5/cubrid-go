package cubrid

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"github.com/search5/cubrid-go/protocol"
)

// --- Monetary Scan from []byte ---

func TestCubridMonetaryScanBytes(t *testing.T) {
	var m CubridMonetary
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, 42)
	m.Scan(b)
}

// --- DecodeCollectionValue more cases ---

func TestDecodeCollectionValueFull(t *testing.T) {
	// Build SET of INT: [1-byte type_code][4-byte count][elements]
	var buf bytes.Buffer
	protocol.WriteByte(&buf, byte(protocol.CubridTypeInt)) // element type
	protocol.WriteInt(&buf, 2)  // element count
	protocol.WriteInt(&buf, 4)  // elem 1 size
	binary.Write(&buf, binary.BigEndian, int32(10))
	protocol.WriteInt(&buf, 4)  // elem 2 size
	binary.Write(&buf, binary.BigEndian, int32(20))

	val, err := decodeCollectionValue(protocol.CubridTypeSet, protocol.CubridTypeInt, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	set, ok := val.(*CubridSet)
	if !ok {
		t.Fatalf("expected *CubridSet, got %T", val)
	}
	if len(set.Elements) != 2 {
		t.Fatalf("elements: %d", len(set.Elements))
	}

	// MULTISET
	val2, err := decodeCollectionValue(protocol.CubridTypeMultiSet, protocol.CubridTypeInt, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := val2.(*CubridMultiSet); !ok {
		t.Fatalf("expected *CubridMultiSet, got %T", val2)
	}

	// SEQUENCE
	val3, err := decodeCollectionValue(protocol.CubridTypeSequence, protocol.CubridTypeInt, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := val3.(*CubridSequence); !ok {
		t.Fatalf("expected *CubridSequence, got %T", val3)
	}
}

func TestDecodeCollectionValueWithNull(t *testing.T) {
	// SET with NULL element.
	var buf bytes.Buffer
	protocol.WriteByte(&buf, byte(protocol.CubridTypeInt)) // element type
	protocol.WriteInt(&buf, 1)  // element count
	protocol.WriteInt(&buf, 0)  // elem size = 0 (NULL)

	val, err := decodeCollectionValue(protocol.CubridTypeSet, protocol.CubridTypeInt, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	set := val.(*CubridSet)
	if len(set.Elements) != 1 || set.Elements[0] != nil {
		t.Errorf("elements: %v", set.Elements)
	}
}

// --- PrepareAndQuery via mock server ---

func TestMockServerPrepareAndQuery(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodePrepareAndExec, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		// Prepare body: cache_lifetime(4) + stmt_type(1) + bind_count(4) + updatable(1) + col_count(4)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 0) // bind_count
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, 1) // col_count = 1
		resp.Write(buildColumnMetaBytes(protocol.CubridTypeInt, "id"))
		protocol.WriteInt(&resp, 0) // trailing

		// Execute body.
		protocol.WriteByte(&resp, 0x00) // cache_reusable
		protocol.WriteInt(&resp, 1)     // result_count
		protocol.WriteByte(&resp, byte(protocol.StmtSelect))
		protocol.WriteInt(&resp, 1) // tuple count
		resp.Write(make([]byte, 8)) // OID
		protocol.WriteInt(&resp, 0) // cache sec
		protocol.WriteInt(&resp, 0) // cache usec
		protocol.WriteByte(&resp, 0x00) // include_column_info = 0
		protocol.WriteInt(&resp, -1)    // shard_id

		// Inline fetch.
		protocol.WriteInt(&resp, 0)     // fetch_code
		protocol.WriteInt(&resp, 1)     // tuple_count = 1
		protocol.WriteInt(&resp, 0)     // row index
		resp.Write(make([]byte, 8))     // OID
		protocol.WriteInt(&resp, 4)     // col size
		binary.Write(&resp, binary.BigEndian, int32(99))

		return 1, resp.Bytes()
	})

	db := openTestDB(t, srv)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rows, err := PrepareAndQuery(context.Background(), conn, "SELECT id FROM t")
	if err != nil {
		t.Fatalf("PrepareAndQuery: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var id interface{}
	if err := rows.Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != int32(99) {
		t.Errorf("id = %v", id)
	}
}

// --- getDBParameter body parsing ---

func TestGetDBParameterFullPath(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetDBParameter, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 42)
		return 0, resp.Bytes()
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
		val, err := c.getDBParameter(context.Background(), ParamIsolationLevel)
		if err != nil {
			return err
		}
		if val != 42 {
			return fmt.Errorf("val = %d, want 42", val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- getLastInsertID body parsing ---

func TestGetLastInsertIDBodyParsing(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetLastInsertID, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteNullTermString(&resp, "12345")
		return 0, resp.Bytes()
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
		val, err := c.getLastInsertID(context.Background())
		if err != nil {
			return err
		}
		if val != "12345" {
			return fmt.Errorf("val = %q", val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- getGeneratedKeys full path ---

func TestGetGeneratedKeysFullParsing(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeGetGeneratedKeys, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		protocol.WriteInt(&resp, 100)
		protocol.WriteInt(&resp, 200)
		return 2, resp.Bytes()
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
		keys, err := c.getGeneratedKeys(context.Background(), 1)
		if err != nil {
			return err
		}
		if len(keys) != 2 {
			return fmt.Errorf("keys: %d", len(keys))
		}
		if keys[0].Value != int64(100) || keys[1].Value != int64(200) {
			return fmt.Errorf("keys: %v", keys)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- xaRecover body parsing ---

func TestXaRecoverBodyParsing(t *testing.T) {
	srv := newMockCASServer(t)
	defer srv.close()

	srv.setHandler(protocol.FuncCodeXaRecover, func(body []byte) (int32, []byte) {
		var resp bytes.Buffer
		xid1 := &XID{FormatID: 1, GlobalTransactionID: []byte("g1"), BranchQualifier: []byte("b1")}
		xid2 := &XID{FormatID: 2, GlobalTransactionID: []byte("g2"), BranchQualifier: []byte("b2")}
		for _, xid := range []*XID{xid1, xid2} {
			data := xid.encode()
			protocol.WriteInt(&resp, int32(len(data)))
			resp.Write(data)
		}
		return 2, resp.Bytes()
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
		xids, err := c.xaRecover(context.Background())
		if err != nil {
			return err
		}
		if len(xids) != 2 {
			return fmt.Errorf("xids: %d", len(xids))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- parseExecResult with INSERT OID ---

func TestParseExecResultInsertOID(t *testing.T) {
	var body bytes.Buffer
	protocol.WriteByte(&body, 0x00)                        // cache_reusable
	protocol.WriteInt(&body, 1)                             // result_count
	protocol.WriteByte(&body, byte(protocol.StmtInsert))   // stmt_type INSERT
	protocol.WriteInt(&body, 1)                             // affected
	// OID with non-zero page ID
	oid := make([]byte, 8)
	binary.BigEndian.PutUint32(oid[0:4], 42) // pageID = 42
	body.Write(oid)
	protocol.WriteInt(&body, 0) // cache sec
	protocol.WriteInt(&body, 0) // cache usec
	protocol.WriteByte(&body, 0x00) // include_column_info = 0
	protocol.WriteInt(&body, -1)    // shard_id

	frame := &protocol.ResponseFrame{ResponseCode: 1, Body: body.Bytes()}
	conn := &cubridConn{broker: protocol.BrokerInfo{ProtocolVersion: protocol.ProtocolV5}}
	r, err := parseExecResult(conn, frame)
	if err != nil {
		t.Fatal(err)
	}
	// CCI does not provide last insert ID in EXECUTE response.
	// OID pageID is not a valid last insert ID — use FC 40 instead.
	if r.lastInsertID != 0 {
		t.Errorf("lastInsertID = %d, want 0 (OID pageID should not be used)", r.lastInsertID)
	}
}

// --- Fetch with no-more-data ---

func TestMockServerFetchNoMoreData(t *testing.T) {
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
		protocol.WriteInt(&resp, 1) // 1 tuple total
		resp.Write(make([]byte, 8))
		protocol.WriteInt(&resp, 0)
		protocol.WriteInt(&resp, 0)
		protocol.WriteByte(&resp, 0x00)
		protocol.WriteInt(&resp, -1)
		// NO inline tuples (need to fetch).
		return 1, resp.Bytes()
	})
	fetchCount := 0
	srv.setHandler(protocol.FuncCodeFetch, func(body []byte) (int32, []byte) {
		fetchCount++
		if fetchCount == 1 {
			var resp bytes.Buffer
			protocol.WriteInt(&resp, 1)
			protocol.WriteInt(&resp, 0)
			resp.Write(make([]byte, 8))
			protocol.WriteInt(&resp, 4)
			binary.Write(&resp, binary.BigEndian, int32(77))
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

	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var id int64
	rows.Scan(&id)
	if id != 77 {
		t.Errorf("id = %d", id)
	}
	if rows.Next() {
		t.Error("expected no more rows")
	}
}
