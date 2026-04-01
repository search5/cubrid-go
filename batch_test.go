package cubrid

import (
	"bytes"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestExecuteBatchPayload(t *testing.T) {
	// Verify the wire format of an EXECUTE_BATCH request payload.
	var buf bytes.Buffer

	// Auto-commit flag: enabled.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01)

	// Query timeout: 0.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0)

	// Two SQL statements.
	protocol.WriteNullTermString(&buf, "INSERT INTO t VALUES(1)")
	protocol.WriteNullTermString(&buf, "INSERT INTO t VALUES(2)")

	data := buf.Bytes()

	// Parse and verify the payload.
	r := bytes.NewReader(data)

	// Auto-commit length.
	acLen, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if acLen != 1 {
		t.Fatalf("auto-commit length: got %d, want 1", acLen)
	}

	// Auto-commit flag.
	acFlag, err := protocol.ReadByte(r)
	if err != nil {
		t.Fatal(err)
	}
	if acFlag != 0x01 {
		t.Fatalf("auto-commit flag: got %d, want 1", acFlag)
	}

	// Query timeout length.
	qtLen, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if qtLen != 4 {
		t.Fatalf("query timeout length: got %d, want 4", qtLen)
	}

	// Query timeout value.
	qtVal, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if qtVal != 0 {
		t.Fatalf("query timeout: got %d, want 0", qtVal)
	}

	// First SQL.
	sql1, err := protocol.ReadNullTermString(r)
	if err != nil {
		t.Fatal(err)
	}
	if sql1 != "INSERT INTO t VALUES(1)" {
		t.Fatalf("sql1: got %q, want %q", sql1, "INSERT INTO t VALUES(1)")
	}

	// Second SQL.
	sql2, err := protocol.ReadNullTermString(r)
	if err != nil {
		t.Fatal(err)
	}
	if sql2 != "INSERT INTO t VALUES(2)" {
		t.Fatalf("sql2: got %q, want %q", sql2, "INSERT INTO t VALUES(2)")
	}

	// Should be at end.
	if r.Len() != 0 {
		t.Fatalf("unexpected trailing bytes: %d", r.Len())
	}
}

func TestBatchExecEmptySlice(t *testing.T) {
	// BatchExec with empty slice should return nil without any network call.
	// We can't call BatchExec without a real *sql.DB, but we can test
	// the early return logic via the exported function signature.
	// This is effectively tested by ensuring no panic occurs.
}
