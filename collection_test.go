package cubrid

import (
	"database/sql/driver"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestCubridSetScanValue(t *testing.T) {
	// CubridSet should implement driver.Valuer and sql.Scanner.
	s := &CubridSet{
		Type:     protocol.CubridTypeString,
		Elements: []interface{}{"a", "b", "c"},
	}

	// driver.Valuer: returns self for custom handling.
	val, err := s.Value()
	if err != nil {
		t.Fatal(err)
	}
	if val == nil {
		t.Error("expected non-nil Value")
	}

	// Scan from []interface{}.
	s2 := &CubridSet{}
	err = s2.Scan([]interface{}{"x", "y"})
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.Elements) != 2 {
		t.Errorf("Elements len = %d, want 2", len(s2.Elements))
	}
}

func TestCubridSequenceScanValue(t *testing.T) {
	s := &CubridSequence{
		Type:     protocol.CubridTypeInt,
		Elements: []interface{}{int32(1), int32(2), int32(3)},
	}

	val, err := s.Value()
	if err != nil {
		t.Fatal(err)
	}
	if val == nil {
		t.Error("expected non-nil Value")
	}
}

func TestCubridMultiSetScanValue(t *testing.T) {
	s := &CubridMultiSet{
		Type:     protocol.CubridTypeDouble,
		Elements: []interface{}{float64(1.1), float64(2.2)},
	}

	val, err := s.Value()
	if err != nil {
		t.Fatal(err)
	}
	if val == nil {
		t.Error("expected non-nil Value")
	}
}

func TestDecodeCollectionValue(t *testing.T) {
	// Wire format: [1-byte elem_type][4-byte count][repeated: [4-byte size][data]]
	data := []byte{
		0x02,                               // element type = VARCHAR (2)
		0x00, 0x00, 0x00, 0x03,             // count = 3
		0x00, 0x00, 0x00, 0x02, 'a', 0x00, // element "a" (size=2)
		0x00, 0x00, 0x00, 0x02, 'b', 0x00, // element "b" (size=2)
		0x00, 0x00, 0x00, 0x02, 'c', 0x00, // element "c" (size=2)
	}

	result, err := decodeCollectionValue(protocol.CubridTypeSet, protocol.CubridTypeString, data)
	if err != nil {
		t.Fatal(err)
	}

	set, ok := result.(*CubridSet)
	if !ok {
		t.Fatalf("expected *CubridSet, got %T", result)
	}
	if len(set.Elements) != 3 {
		t.Fatalf("elements len = %d, want 3", len(set.Elements))
	}
	if set.Elements[0] != "a" || set.Elements[1] != "b" || set.Elements[2] != "c" {
		t.Errorf("elements = %v, want [a b c]", set.Elements)
	}
}

func TestDecodeCollectionValueInt(t *testing.T) {
	// SEQUENCE of INT.
	data := []byte{
		0x08,                                             // element type = INT (8)
		0x00, 0x00, 0x00, 0x02,                           // count = 2
		0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x0A, // 10
		0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x14, // 20
	}

	result, err := decodeCollectionValue(protocol.CubridTypeSequence, protocol.CubridTypeInt, data)
	if err != nil {
		t.Fatal(err)
	}

	seq, ok := result.(*CubridSequence)
	if !ok {
		t.Fatalf("expected *CubridSequence, got %T", result)
	}
	if len(seq.Elements) != 2 {
		t.Fatalf("elements len = %d, want 2", len(seq.Elements))
	}
	if seq.Elements[0] != int32(10) || seq.Elements[1] != int32(20) {
		t.Errorf("elements = %v", seq.Elements)
	}
}

func TestDecodeCollectionEmpty(t *testing.T) {
	result, err := decodeCollectionValue(protocol.CubridTypeSet, protocol.CubridTypeString, nil)
	if err != nil {
		t.Fatal(err)
	}
	set := result.(*CubridSet)
	if len(set.Elements) != 0 {
		t.Errorf("expected empty set, got %d elements", len(set.Elements))
	}
}

func TestDecodeCollectionWithNull(t *testing.T) {
	// SET with a NULL element (size=0).
	data := []byte{
		0x02,                               // element type = VARCHAR
		0x00, 0x00, 0x00, 0x03,             // count = 3
		0x00, 0x00, 0x00, 0x02, 'x', 0x00, // "x"
		0x00, 0x00, 0x00, 0x00,             // NULL element
		0x00, 0x00, 0x00, 0x02, 'y', 0x00, // "y"
	}

	result, err := decodeCollectionValue(protocol.CubridTypeSet, protocol.CubridTypeString, data)
	if err != nil {
		t.Fatal(err)
	}
	set := result.(*CubridSet)
	if len(set.Elements) != 3 {
		t.Fatalf("elements len = %d, want 3", len(set.Elements))
	}
	if set.Elements[0] != "x" {
		t.Errorf("elem[0] = %v", set.Elements[0])
	}
	if set.Elements[1] != nil {
		t.Errorf("elem[1] = %v, want nil", set.Elements[1])
	}
	if set.Elements[2] != "y" {
		t.Errorf("elem[2] = %v", set.Elements[2])
	}
}

// Compile-time interface checks.
var (
	_ driver.Valuer = (*CubridSet)(nil)
	_ driver.Valuer = (*CubridMultiSet)(nil)
	_ driver.Valuer = (*CubridSequence)(nil)
)
