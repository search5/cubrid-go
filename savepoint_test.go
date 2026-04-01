package cubrid

import (
	"bytes"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestSavepointPayload(t *testing.T) {
	tests := []struct {
		name string
		op   byte
		sp   string
	}{
		{"create", savepointCreate, "sp1"},
		{"rollback", savepointRollback, "my_savepoint"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Op field.
			protocol.WriteInt(&buf, 1)
			protocol.WriteByte(&buf, tt.op)

			// Savepoint name.
			protocol.WriteNullTermString(&buf, tt.sp)

			data := buf.Bytes()
			r := bytes.NewReader(data)

			// Op length.
			opLen, err := protocol.ReadInt(r)
			if err != nil {
				t.Fatal(err)
			}
			if opLen != 1 {
				t.Fatalf("op length: got %d, want 1", opLen)
			}

			// Op value.
			opVal, err := protocol.ReadByte(r)
			if err != nil {
				t.Fatal(err)
			}
			if opVal != tt.op {
				t.Fatalf("op: got %d, want %d", opVal, tt.op)
			}

			// Savepoint name.
			spName, err := protocol.ReadNullTermString(r)
			if err != nil {
				t.Fatal(err)
			}
			if spName != tt.sp {
				t.Fatalf("savepoint name: got %q, want %q", spName, tt.sp)
			}

			if r.Len() != 0 {
				t.Fatalf("unexpected trailing bytes: %d", r.Len())
			}
		})
	}
}
