package cubrid

import (
	"reflect"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestCubridNumericNew(t *testing.T) {
	n := NewCubridNumeric("3.14")
	if n.String() != "3.14" {
		t.Fatalf("got %q, want %q", n.String(), "3.14")
	}
}

func TestTryNewCubridNumericValid(t *testing.T) {
	valid := []string{"0", "1", "-1", "3.14", "-100.5", "007", "  123  "}
	for _, s := range valid {
		n, err := TryNewCubridNumeric(s)
		if err != nil {
			t.Fatalf("TryNewCubridNumeric(%q) error: %v", s, err)
		}
		if !n.IsValid() {
			t.Fatalf("%q should be valid", s)
		}
	}
}

func TestTryNewCubridNumericInvalid(t *testing.T) {
	invalid := []string{"", "   ", "-", "NaN", "abc", "1.2.3", "123.", ".123", "1,234"}
	for _, s := range invalid {
		_, err := TryNewCubridNumeric(s)
		if err == nil {
			t.Fatalf("TryNewCubridNumeric(%q) should error", s)
		}
	}
}

func TestCubridNumericIsValid(t *testing.T) {
	if !NewCubridNumeric("3.14").IsValid() {
		t.Fatal("3.14 should be valid")
	}
	if NewCubridNumeric("NaN").IsValid() {
		t.Fatal("NaN should not be valid")
	}
}

func TestCubridNumericScan(t *testing.T) {
	var n CubridNumeric
	if err := n.Scan("42.5"); err != nil {
		t.Fatal(err)
	}
	if n.String() != "42.5" {
		t.Fatalf("got %q, want %q", n.String(), "42.5")
	}
}

func TestDecodeValueNumeric(t *testing.T) {
	data := []byte("99.99\x00")
	val, err := decodeValue(protocol.CubridTypeNumeric, data)
	if err != nil {
		t.Fatal(err)
	}
	n, ok := val.(*CubridNumeric)
	if !ok {
		t.Fatalf("expected *CubridNumeric, got %T", val)
	}
	if n.String() != "99.99" {
		t.Fatalf("got %q, want %q", n.String(), "99.99")
	}
}

func TestEncodeBindValueNumeric(t *testing.T) {
	n := NewCubridNumeric("3.14")
	data, cubType, err := encodeBindValue(n)
	if err != nil {
		t.Fatal(err)
	}
	if cubType != protocol.CubridTypeString {
		t.Fatalf("type: got %d, want %d", cubType, protocol.CubridTypeString)
	}
	if string(data) != "3.14\x00" {
		t.Fatalf("data: got %q, want %q", data, "3.14\x00")
	}
}

func TestScanTypeForNumeric(t *testing.T) {
	got := scanTypeForCubridType(protocol.CubridTypeNumeric)
	want := reflect.TypeOf(&CubridNumeric{})
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
