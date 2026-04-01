package cubrid

import (
	"database/sql/driver"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestCubridOidBasic(t *testing.T) {
	oid := NewCubridOid(12345, 67, 89)
	if oid.PageID != 12345 || oid.SlotID != 67 || oid.VolID != 89 {
		t.Errorf("oid = %v", oid)
	}
	if oid.IsNull() {
		t.Error("expected non-null OID")
	}
}

func TestCubridOidNull(t *testing.T) {
	oid := NewCubridOid(0, 0, 0)
	if !oid.IsNull() {
		t.Error("expected null OID")
	}
}

func TestCubridOidString(t *testing.T) {
	oid := NewCubridOid(100, 5, 2)
	s := oid.String()
	if s != "OID(100, 5, 2)" {
		t.Errorf("String() = %q", s)
	}
}

func TestCubridOidScanValue(t *testing.T) {
	oid := NewCubridOid(1, 2, 3)

	// driver.Valuer
	val, err := oid.Value()
	if err != nil {
		t.Fatal(err)
	}
	if val == nil {
		t.Error("expected non-nil")
	}

	// sql.Scanner from *CubridOid
	oid2 := &CubridOid{}
	err = oid2.Scan(oid)
	if err != nil {
		t.Fatal(err)
	}
	if oid2.PageID != 1 || oid2.SlotID != 2 || oid2.VolID != 3 {
		t.Errorf("scanned oid = %v", oid2)
	}

	// Scanner from protocol.OID
	oid3 := &CubridOid{}
	err = oid3.Scan(protocol.OID{PageID: 10, SlotID: 20, VolID: 30})
	if err != nil {
		t.Fatal(err)
	}
	if oid3.PageID != 10 {
		t.Errorf("scanned from protocol.OID: %v", oid3)
	}
}

func TestCubridOidEncode(t *testing.T) {
	oid := NewCubridOid(1, 2, 3)
	data := oid.Encode()
	if len(data) != 8 {
		t.Fatalf("encode len = %d", len(data))
	}
	// page_id=1 big-endian: 00 00 00 01
	if data[3] != 1 {
		t.Errorf("page_id byte = %x", data[3])
	}
	// slot_id=2: 00 02
	if data[5] != 2 {
		t.Errorf("slot_id byte = %x", data[5])
	}
	// vol_id=3: 00 03
	if data[7] != 3 {
		t.Errorf("vol_id byte = %x", data[7])
	}
}

func TestCubridOidDecode(t *testing.T) {
	data := []byte{0x00, 0x00, 0x30, 0x39, 0x00, 0x43, 0x00, 0x59} // 12345, 67, 89
	oid, err := DecodeCubridOid(data)
	if err != nil {
		t.Fatal(err)
	}
	if oid.PageID != 12345 || oid.SlotID != 67 || oid.VolID != 89 {
		t.Errorf("decoded = %v", oid)
	}
}

// Compile-time interface checks.
var (
	_ driver.Valuer = (*CubridOid)(nil)
)
