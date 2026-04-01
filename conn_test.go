package cubrid

import (
	"bytes"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestParseColumnMeta(t *testing.T) {
	var buf bytes.Buffer

	// Type: VARCHAR (2).
	protocol.WriteByte(&buf, byte(protocol.CubridTypeString))
	// Scale.
	protocol.WriteShort(&buf, 0)
	// Precision.
	protocol.WriteInt(&buf, 255)
	// Column name.
	protocol.WriteNullTermString(&buf, "name")
	// Real name.
	protocol.WriteNullTermString(&buf, "name")
	// Table name.
	protocol.WriteNullTermString(&buf, "users")
	// Nullable.
	protocol.WriteByte(&buf, 1)
	// Default value.
	protocol.WriteNullTermString(&buf, "")
	// Auto-increment.
	protocol.WriteByte(&buf, 0)
	// Unique key.
	protocol.WriteByte(&buf, 0)
	// Primary key.
	protocol.WriteByte(&buf, 0)
	// Reverse index.
	protocol.WriteByte(&buf, 0)
	// Reverse unique.
	protocol.WriteByte(&buf, 0)
	// Foreign key.
	protocol.WriteByte(&buf, 0)
	// Shared.
	protocol.WriteByte(&buf, 0)

	r := bytes.NewReader(buf.Bytes())
	col, err := parseColumnMeta(r, protocol.ProtocolV12)
	if err != nil {
		t.Fatal(err)
	}

	if col.Type != protocol.CubridTypeString {
		t.Errorf("Type = %v, want VARCHAR", col.Type)
	}
	if col.Precision != 255 {
		t.Errorf("Precision = %d, want 255", col.Precision)
	}
	if col.Name != "name" {
		t.Errorf("Name = %q, want %q", col.Name, "name")
	}
	if col.TableName != "users" {
		t.Errorf("TableName = %q, want %q", col.TableName, "users")
	}
	if !col.Nullable {
		t.Error("expected Nullable = true")
	}
}

func TestParseColumnMetaWithCollection(t *testing.T) {
	var buf bytes.Buffer

	// Collection type: SET marker (0x80 | 0x20 = 0xA0).
	protocol.WriteByte(&buf, 0xA0)
	// Element type: INT.
	protocol.WriteByte(&buf, byte(protocol.CubridTypeInt))
	// Scale.
	protocol.WriteShort(&buf, 0)
	// Precision.
	protocol.WriteInt(&buf, 0)
	// Column name.
	protocol.WriteNullTermString(&buf, "tags")
	// Real name.
	protocol.WriteNullTermString(&buf, "tags")
	// Table name.
	protocol.WriteNullTermString(&buf, "items")
	// Nullable.
	protocol.WriteByte(&buf, 0)
	// Default value.
	protocol.WriteNullTermString(&buf, "")
	// Auto-increment.
	protocol.WriteByte(&buf, 0)
	// Unique key.
	protocol.WriteByte(&buf, 0)
	// Primary key.
	protocol.WriteByte(&buf, 0)
	// Reverse index.
	protocol.WriteByte(&buf, 0)
	// Reverse unique.
	protocol.WriteByte(&buf, 0)
	// Foreign key.
	protocol.WriteByte(&buf, 0)
	// Shared.
	protocol.WriteByte(&buf, 0)

	r := bytes.NewReader(buf.Bytes())
	col, err := parseColumnMeta(r, protocol.ProtocolV7)
	if err != nil {
		t.Fatal(err)
	}

	if col.Type != protocol.CubridTypeSet {
		t.Errorf("Type = %v, want SET", col.Type)
	}
	if col.Name != "tags" {
		t.Errorf("Name = %q, want %q", col.Name, "tags")
	}
}

func TestParseTuples(t *testing.T) {
	columns := []ColumnMeta{
		{Type: protocol.CubridTypeInt, Name: "id"},
		{Type: protocol.CubridTypeString, Name: "name"},
	}

	var buf bytes.Buffer
	// Row 1.
	protocol.WriteInt(&buf, 0)           // row index
	buf.Write(make([]byte, 8))           // OID
	protocol.WriteInt(&buf, 4)           // size of int
	buf.Write([]byte{0x00, 0x00, 0x00, 0x2A}) // 42
	protocol.WriteInt(&buf, 6)           // size of string "hello\0"
	buf.Write([]byte{'h', 'e', 'l', 'l', 'o', 0x00})

	// Row 2 with NULL.
	protocol.WriteInt(&buf, 1)           // row index
	buf.Write(make([]byte, 8))           // OID
	protocol.WriteInt(&buf, 4)           // size of int
	buf.Write([]byte{0x00, 0x00, 0x00, 0x07}) // 7
	protocol.WriteInt(&buf, 0)           // NULL

	tuples, err := parseTuples(bytes.NewReader(buf.Bytes()), 2, columns)
	if err != nil {
		t.Fatal(err)
	}

	if len(tuples) != 2 {
		t.Fatalf("got %d tuples, want 2", len(tuples))
	}

	// Row 1.
	if tuples[0][0] != int32(42) {
		t.Errorf("row 0 col 0 = %v, want 42", tuples[0][0])
	}
	if tuples[0][1] != "hello" {
		t.Errorf("row 0 col 1 = %v, want %q", tuples[0][1], "hello")
	}

	// Row 2.
	if tuples[1][0] != int32(7) {
		t.Errorf("row 1 col 0 = %v, want 7", tuples[1][0])
	}
	if tuples[1][1] != nil {
		t.Errorf("row 1 col 1 = %v, want nil", tuples[1][1])
	}
}

func TestRowsColumns(t *testing.T) {
	r := &cubridRows{
		columns: []ColumnMeta{
			{Name: "id", Type: protocol.CubridTypeInt},
			{Name: "name", Type: protocol.CubridTypeString, Precision: 100, Nullable: true},
			{Name: "score", Type: protocol.CubridTypeNumeric, Precision: 10, Scale: 2},
		},
	}

	cols := r.Columns()
	if len(cols) != 3 {
		t.Fatalf("got %d columns, want 3", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" || cols[2] != "score" {
		t.Errorf("columns = %v", cols)
	}

	// ColumnTypeDatabaseTypeName.
	if name := r.ColumnTypeDatabaseTypeName(0); name != "INT" {
		t.Errorf("TypeName(0) = %q, want INT", name)
	}
	if name := r.ColumnTypeDatabaseTypeName(1); name != "VARCHAR" {
		t.Errorf("TypeName(1) = %q, want VARCHAR", name)
	}

	// ColumnTypeNullable.
	nullable, ok := r.ColumnTypeNullable(0)
	if !ok || nullable {
		t.Errorf("Nullable(0) = %v, %v; want false, true", nullable, ok)
	}
	nullable, ok = r.ColumnTypeNullable(1)
	if !ok || !nullable {
		t.Errorf("Nullable(1) = %v, %v; want true, true", nullable, ok)
	}

	// ColumnTypeLength.
	length, ok := r.ColumnTypeLength(1)
	if !ok || length != 100 {
		t.Errorf("Length(1) = %d, %v; want 100, true", length, ok)
	}
	_, ok = r.ColumnTypeLength(0)
	if ok {
		t.Error("Length(0) should return ok=false for INT")
	}

	// ColumnTypePrecisionScale.
	prec, scale, ok := r.ColumnTypePrecisionScale(2)
	if !ok || prec != 10 || scale != 2 {
		t.Errorf("PrecisionScale(2) = %d, %d, %v; want 10, 2, true", prec, scale, ok)
	}
}
