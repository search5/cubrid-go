package cubrid

import (
	"bytes"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestPrepareAndExecPayload(t *testing.T) {
	// Build a PREPARE_AND_EXECUTE payload manually and verify structure.
	var buf bytes.Buffer

	query := "SELECT * FROM t WHERE id = ?"

	// Prepare phase.
	protocol.WriteNullTermString(&buf, query)
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x00) // prepare flag
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01) // auto-commit

	// Execute phase.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x00) // execute flag
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0) // max column size
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0) // max row size
	protocol.WriteInt(&buf, 0) // NULL reserved
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01) // fetch flag (SELECT)
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01) // auto-commit
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01) // forward-only
	protocol.WriteInt(&buf, 8)
	protocol.WriteInt(&buf, 0)
	protocol.WriteInt(&buf, 0) // cache time
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0) // query timeout

	// One bind parameter (int 42).
	data, cubType, _ := encodeBindValue(int32(42))
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, byte(cubType))
	protocol.WriteInt(&buf, int32(len(data)))
	buf.Write(data)

	// Parse back.
	r := bytes.NewReader(buf.Bytes())

	// Query string.
	sql, err := protocol.ReadNullTermString(r)
	if err != nil {
		t.Fatal(err)
	}
	if sql != query {
		t.Fatalf("query: got %q, want %q", sql, query)
	}

	// Prepare flag.
	pfLen, _ := protocol.ReadInt(r)
	if pfLen != 1 {
		t.Fatalf("prepare flag len: got %d, want 1", pfLen)
	}
	pf, _ := protocol.ReadByte(r)
	if pf != 0x00 {
		t.Fatalf("prepare flag: got %d, want 0", pf)
	}

	// Auto-commit.
	acLen, _ := protocol.ReadInt(r)
	if acLen != 1 {
		t.Fatalf("auto-commit len: got %d, want 1", acLen)
	}
	ac, _ := protocol.ReadByte(r)
	if ac != 0x01 {
		t.Fatalf("auto-commit: got %d, want 1", ac)
	}

	// Execute flag.
	efLen, _ := protocol.ReadInt(r)
	if efLen != 1 {
		t.Fatalf("exec flag len: got %d, want 1", efLen)
	}
	ef, _ := protocol.ReadByte(r)
	if ef != 0x00 {
		t.Fatalf("exec flag: got %d, want 0", ef)
	}

	// Max column size.
	mcsLen, _ := protocol.ReadInt(r)
	if mcsLen != 4 {
		t.Fatalf("max col size len: got %d, want 4", mcsLen)
	}
	mcs, _ := protocol.ReadInt(r)
	if mcs != 0 {
		t.Fatalf("max col size: got %d, want 0", mcs)
	}

	// Max row size.
	mrsLen, _ := protocol.ReadInt(r)
	if mrsLen != 4 {
		t.Fatalf("max row size len: got %d, want 4", mrsLen)
	}
	mrs, _ := protocol.ReadInt(r)
	if mrs != 0 {
		t.Fatalf("max row size: got %d, want 0", mrs)
	}

	// Reserved NULL.
	resLen, _ := protocol.ReadInt(r)
	if resLen != 0 {
		t.Fatalf("reserved len: got %d, want 0", resLen)
	}

	// Fetch flag.
	ffLen, _ := protocol.ReadInt(r)
	if ffLen != 1 {
		t.Fatalf("fetch flag len: got %d, want 1", ffLen)
	}
	ff, _ := protocol.ReadByte(r)
	if ff != 0x01 {
		t.Fatalf("fetch flag: got %d, want 1", ff)
	}

	// Auto-commit (execute phase).
	ac2Len, _ := protocol.ReadInt(r)
	if ac2Len != 1 {
		t.Fatalf("exec auto-commit len: got %d, want 1", ac2Len)
	}
	ac2, _ := protocol.ReadByte(r)
	if ac2 != 0x01 {
		t.Fatalf("exec auto-commit: got %d, want 1", ac2)
	}

	// Forward-only.
	foLen, _ := protocol.ReadInt(r)
	if foLen != 1 {
		t.Fatalf("forward-only len: got %d, want 1", foLen)
	}
	fo, _ := protocol.ReadByte(r)
	if fo != 0x01 {
		t.Fatalf("forward-only: got %d, want 1", fo)
	}

	// Cache time.
	ctLen, _ := protocol.ReadInt(r)
	if ctLen != 8 {
		t.Fatalf("cache time len: got %d, want 8", ctLen)
	}
	ct1, _ := protocol.ReadInt(r)
	ct2, _ := protocol.ReadInt(r)
	if ct1 != 0 || ct2 != 0 {
		t.Fatalf("cache time: got %d/%d, want 0/0", ct1, ct2)
	}

	// Query timeout.
	qtLen, _ := protocol.ReadInt(r)
	if qtLen != 4 {
		t.Fatalf("timeout len: got %d, want 4", qtLen)
	}
	qt, _ := protocol.ReadInt(r)
	if qt != 0 {
		t.Fatalf("timeout: got %d, want 0", qt)
	}

	// Bind parameter type.
	btLen, _ := protocol.ReadInt(r)
	if btLen != 1 {
		t.Fatalf("bind type len: got %d, want 1", btLen)
	}
	bt, _ := protocol.ReadByte(r)
	if bt != byte(protocol.CubridTypeInt) {
		t.Fatalf("bind type: got %d, want %d", bt, protocol.CubridTypeInt)
	}

	// Bind parameter value.
	bvLen, _ := protocol.ReadInt(r)
	if bvLen != 4 {
		t.Fatalf("bind val len: got %d, want 4", bvLen)
	}
	bv, _ := protocol.ReadInt(r)
	if bv != 42 {
		t.Fatalf("bind val: got %d, want 42", bv)
	}

	if r.Len() != 0 {
		t.Fatalf("trailing bytes: %d", r.Len())
	}
}

func TestPrepareAndExecFuncCode(t *testing.T) {
	if byte(protocol.FuncCodePrepareAndExec) != 41 {
		t.Fatalf("FuncCodePrepareAndExec: got %d, want 41", protocol.FuncCodePrepareAndExec)
	}
}
