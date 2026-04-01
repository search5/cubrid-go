package cubrid

import (
	"reflect"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestCubridEnumNew(t *testing.T) {
	e := NewCubridEnum("Red", 1)
	if e.Name != "Red" {
		t.Fatalf("Name: got %q, want %q", e.Name, "Red")
	}
	if e.Value != 1 {
		t.Fatalf("Value: got %d, want 1", e.Value)
	}
}

func TestCubridEnumString(t *testing.T) {
	e := NewCubridEnum("Green", 3)
	got := e.String()
	want := "Green(3)"
	if got != want {
		t.Fatalf("String(): got %q, want %q", got, want)
	}
}

func TestCubridEnumDriverValue(t *testing.T) {
	e := NewCubridEnum("Blue", 2)
	v, err := e.DriverValue()
	if err != nil {
		t.Fatal(err)
	}
	if v != "Blue" {
		t.Fatalf("DriverValue: got %v, want %q", v, "Blue")
	}
}

func TestCubridEnumScan(t *testing.T) {
	tests := []struct {
		name  string
		src   interface{}
		want  string
	}{
		{"string", "Large", "Large"},
		{"bytes", []byte("Small"), "Small"},
		{"enum_ptr", NewCubridEnum("Medium", 2), "Medium"},
		{"nil", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e CubridEnum
			if err := e.Scan(tt.src); err != nil {
				t.Fatal(err)
			}
			if e.Name != tt.want {
				t.Fatalf("Name: got %q, want %q", e.Name, tt.want)
			}
		})
	}
}

func TestCubridEnumScanError(t *testing.T) {
	var e CubridEnum
	if err := e.Scan(123); err == nil {
		t.Fatal("expected error scanning int into CubridEnum")
	}
}

func TestDecodeValueEnum(t *testing.T) {
	data := []byte("Active\x00")
	val, err := DecodeValue(protocol.CubridTypeEnum, data)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := val.(*CubridEnum)
	if !ok {
		t.Fatalf("expected *CubridEnum, got %T", val)
	}
	if e.Name != "Active" {
		t.Fatalf("Name: got %q, want %q", e.Name, "Active")
	}
	if e.Value != 0 {
		t.Fatalf("Value: got %d, want 0 (default from wire)", e.Value)
	}
}

func TestEncodeBindValueEnum(t *testing.T) {
	e := NewCubridEnum("Red", 1)
	data, cubType, err := EncodeBindValue(e)
	if err != nil {
		t.Fatal(err)
	}
	if cubType != protocol.CubridTypeString {
		t.Fatalf("type: got %d, want %d", cubType, protocol.CubridTypeString)
	}
	want := "Red\x00"
	if string(data) != want {
		t.Fatalf("data: got %q, want %q", data, want)
	}
}

func TestEncodeBindValueEnumByValue(t *testing.T) {
	e := CubridEnum{Name: "Blue", Value: 2}
	data, cubType, err := EncodeBindValue(e)
	if err != nil {
		t.Fatal(err)
	}
	if cubType != protocol.CubridTypeString {
		t.Fatalf("type: got %d, want %d", cubType, protocol.CubridTypeString)
	}
	want := "Blue\x00"
	if string(data) != want {
		t.Fatalf("data: got %q, want %q", data, want)
	}
}

func TestScanTypeForEnum(t *testing.T) {
	got := ScanTypeForCubridType(protocol.CubridTypeEnum)
	want := reflect.TypeOf(&CubridEnum{})
	if got != want {
		t.Fatalf("ScanType: got %v, want %v", got, want)
	}
}
