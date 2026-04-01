package cubrid

import (
	"bytes"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestGetDBParameterPayload(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(ParamIsolationLevel))

	r := bytes.NewReader(buf.Bytes())

	// Length prefix.
	length, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if length != 4 {
		t.Fatalf("param length: got %d, want 4", length)
	}

	// Param ID.
	paramID, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if paramID != 1 {
		t.Fatalf("param id: got %d, want 1", paramID)
	}

	if r.Len() != 0 {
		t.Fatalf("unexpected trailing bytes: %d", r.Len())
	}
}

func TestSetDBParameterPayload(t *testing.T) {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(ParamLockTimeout))
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 30)

	r := bytes.NewReader(buf.Bytes())

	// Param ID length.
	l1, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if l1 != 4 {
		t.Fatalf("param id length: got %d, want 4", l1)
	}

	// Param ID.
	paramID, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if paramID != int32(ParamLockTimeout) {
		t.Fatalf("param id: got %d, want %d", paramID, ParamLockTimeout)
	}

	// Value length.
	l2, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if l2 != 4 {
		t.Fatalf("value length: got %d, want 4", l2)
	}

	// Value.
	val, err := protocol.ReadInt(r)
	if err != nil {
		t.Fatal(err)
	}
	if val != 30 {
		t.Fatalf("value: got %d, want 30", val)
	}

	if r.Len() != 0 {
		t.Fatalf("unexpected trailing bytes: %d", r.Len())
	}
}

func TestDBParamConstants(t *testing.T) {
	if ParamIsolationLevel != 1 {
		t.Fatalf("ParamIsolationLevel: got %d, want 1", ParamIsolationLevel)
	}
	if ParamLockTimeout != 2 {
		t.Fatalf("ParamLockTimeout: got %d, want 2", ParamLockTimeout)
	}
	if ParamMaxStringLength != 3 {
		t.Fatalf("ParamMaxStringLength: got %d, want 3", ParamMaxStringLength)
	}
	if ParamAutoCommit != 4 {
		t.Fatalf("ParamAutoCommit: got %d, want 4", ParamAutoCommit)
	}
}
