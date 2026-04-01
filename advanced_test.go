package cubrid

import (
	"bytes"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestNextResultPayload(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 42) // query handle

	r := bytes.NewReader(buf.Bytes())

	l, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if l != 4 {
		t.Fatalf("length: got %d, want 4", l)
	}
	handle, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if handle != 42 {
		t.Fatalf("handle: got %d, want 42", handle)
	}
	if r.Len() != 0 {
		t.Fatalf("trailing bytes: %d", r.Len())
	}
}

func TestCursorUpdatePayload(t *testing.T) {
	var buf bytes.Buffer

	// Query handle.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 10)

	// Cursor position.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 3)

	// One string value.
	data, cubType, _ := EncodeBindValue("hello")
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, byte(cubType))
	protocol.WriteInt(&buf, int32(len(data)))
	buf.Write(data)

	r := bytes.NewReader(buf.Bytes())

	// Handle.
	protocol.ReadInt(r) // length
	handle, _ := protocol.ReadInt(r)
	if handle != 10 {
		t.Fatalf("handle: got %d, want 10", handle)
	}

	// Position.
	protocol.ReadInt(r) // length
	pos, _ := protocol.ReadInt(r)
	if pos != 3 {
		t.Fatalf("pos: got %d, want 3", pos)
	}

	// Type field.
	typeLen, _ := protocol.ReadInt(r)
	if typeLen != 1 {
		t.Fatalf("type len: got %d, want 1", typeLen)
	}
	typeByte, _ := protocol.ReadByte(r)
	if typeByte != byte(protocol.CubridTypeString) {
		t.Fatalf("type: got %d, want %d", typeByte, protocol.CubridTypeString)
	}

	// Value field.
	valLen, _ := protocol.ReadInt(r)
	valData, _ := protocol.ReadBytes(r, int(valLen))
	val := string(bytes.TrimRight(valData, "\x00"))
	if val != "hello" {
		t.Fatalf("value: got %q, want %q", val, "hello")
	}
}

func TestGetGeneratedKeysPayload(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 7) // query handle

	r := bytes.NewReader(buf.Bytes())
	l, _ := protocol.ReadInt(r)
	if l != 4 {
		t.Fatalf("length: got %d, want 4", l)
	}
	handle, _ := protocol.ReadInt(r)
	if handle != 7 {
		t.Fatalf("handle: got %d, want 7", handle)
	}
}

func TestCursorClosePayload(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 99) // query handle

	r := bytes.NewReader(buf.Bytes())
	l, _ := protocol.ReadInt(r)
	if l != 4 {
		t.Fatalf("length: got %d, want 4", l)
	}
	handle, _ := protocol.ReadInt(r)
	if handle != 99 {
		t.Fatalf("handle: got %d, want 99", handle)
	}
}

func TestFuncCodeConstants(t *testing.T) {
	tests := []struct {
		name string
		code protocol.FuncCode
		want byte
	}{
		{"NextResult", protocol.FuncCodeNextResult, 19},
		{"CursorUpdate", protocol.FuncCodeCursorUpdate, 22},
		{"XaPrepare", protocol.FuncCodeXaPrepare, 28},
		{"XaRecover", protocol.FuncCodeXaRecover, 29},
		{"XaEndTran", protocol.FuncCodeXaEndTran, 30},
		{"GetGeneratedKeys", protocol.FuncCodeGetGeneratedKeys, 34},
		{"GetRowCount", protocol.FuncCodeGetRowCount, 39},
		{"GetLastInsertID", protocol.FuncCodeGetLastInsertID, 40},
		{"PrepareAndExec", protocol.FuncCodePrepareAndExec, 41},
		{"CursorClose", protocol.FuncCodeCursorClose, 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if byte(tt.code) != tt.want {
				t.Fatalf("%s: got %d, want %d", tt.name, tt.code, tt.want)
			}
		})
	}
}
