package cubrid

import (
	"bytes"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestXIDEncode(t *testing.T) {
	xid := &XID{
		FormatID:            1,
		GlobalTransactionID: []byte("gtrid-123"),
		BranchQualifier:     []byte("bqual-456"),
	}

	data := xid.encode()
	r := bytes.NewReader(data)

	formatID, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if formatID != 1 {
		t.Fatalf("FormatID: got %d, want 1", formatID)
	}

	gtridLen, _ := protocol.ReadInt(r)
	if gtridLen != 9 {
		t.Fatalf("gtrid len: got %d, want 9", gtridLen)
	}

	bqualLen, _ := protocol.ReadInt(r)
	if bqualLen != 9 {
		t.Fatalf("bqual len: got %d, want 9", bqualLen)
	}

	gtrid, _ := protocol.ReadBytes(r, int(gtridLen))
	if string(gtrid) != "gtrid-123" {
		t.Fatalf("gtrid: got %q, want %q", gtrid, "gtrid-123")
	}

	bqual, _ := protocol.ReadBytes(r, int(bqualLen))
	if string(bqual) != "bqual-456" {
		t.Fatalf("bqual: got %q, want %q", bqual, "bqual-456")
	}

	if r.Len() != 0 {
		t.Fatalf("trailing bytes: %d", r.Len())
	}
}

func TestXIDDecode(t *testing.T) {
	xid := &XID{
		FormatID:            42,
		GlobalTransactionID: []byte("global"),
		BranchQualifier:     []byte("branch"),
	}

	data := xid.encode()
	decoded, err := decodeXID(data)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.FormatID != 42 {
		t.Fatalf("FormatID: got %d, want 42", decoded.FormatID)
	}
	if string(decoded.GlobalTransactionID) != "global" {
		t.Fatalf("gtrid: got %q, want %q", decoded.GlobalTransactionID, "global")
	}
	if string(decoded.BranchQualifier) != "branch" {
		t.Fatalf("bqual: got %q, want %q", decoded.BranchQualifier, "branch")
	}
}

func TestXIDDecodeError(t *testing.T) {
	_, err := decodeXID([]byte{0, 0, 0})
	if err == nil {
		t.Fatal("expected error for short XID data")
	}
}

func TestXaPreparePayload(t *testing.T) {
	xid := &XID{
		FormatID:            1,
		GlobalTransactionID: []byte("tx1"),
		BranchQualifier:     []byte("b1"),
	}

	var buf bytes.Buffer
	xidData := xid.encode()
	protocol.WriteInt(&buf, int32(len(xidData)))
	buf.Write(xidData)

	r := bytes.NewReader(buf.Bytes())

	// XID length prefix.
	xidLen, _ := protocol.ReadInt(r)
	if xidLen != int32(len(xidData)) {
		t.Fatalf("xid len: got %d, want %d", xidLen, len(xidData))
	}

	// Decode the XID back.
	raw, _ := protocol.ReadBytes(r, int(xidLen))
	decoded, err := decodeXID(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.FormatID != 1 {
		t.Fatalf("FormatID: got %d, want 1", decoded.FormatID)
	}
}

func TestXaEndTranPayload(t *testing.T) {
	xid := &XID{
		FormatID:            1,
		GlobalTransactionID: []byte("tx1"),
		BranchQualifier:     []byte("b1"),
	}

	var buf bytes.Buffer
	xidData := xid.encode()
	protocol.WriteInt(&buf, int32(len(xidData)))
	buf.Write(xidData)

	// Operation.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, byte(XaCommit))

	r := bytes.NewReader(buf.Bytes())

	// Skip XID.
	xidLen, _ := protocol.ReadInt(r)
	protocol.ReadBytes(r, int(xidLen))

	// Op.
	opLen, _ := protocol.ReadInt(r)
	if opLen != 1 {
		t.Fatalf("op len: got %d, want 1", opLen)
	}
	op, _ := protocol.ReadByte(r)
	if op != byte(XaCommit) {
		t.Fatalf("op: got %d, want %d", op, XaCommit)
	}

	if r.Len() != 0 {
		t.Fatalf("trailing bytes: %d", r.Len())
	}
}

func TestXaEndTranOpConstants(t *testing.T) {
	if XaCommit != 1 {
		t.Fatalf("XaCommit: got %d, want 1", XaCommit)
	}
	if XaRollback != 2 {
		t.Fatalf("XaRollback: got %d, want 2", XaRollback)
	}
}
