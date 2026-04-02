package cubrid

import (
	"encoding/binary"
	"testing"
)

func TestLobHandleDecode(t *testing.T) {
	// Wire format: db_type(4) + lobSize(8) + locatorSize(4) + locator(null-terminated)
	locator := "file:/data/blob_001"
	locatorSize := len(locator) + 1 // includes null

	buf := make([]byte, 4+8+4+locatorSize)
	binary.BigEndian.PutUint32(buf[0:4], 23)                // BLOB
	binary.BigEndian.PutUint64(buf[4:12], 1024)              // size
	binary.BigEndian.PutUint32(buf[12:16], uint32(locatorSize))
	copy(buf[16:], locator)
	buf[16+len(locator)] = 0x00

	h, err := decodeLobHandle(buf)
	if err != nil {
		t.Fatal(err)
	}
	if h.LobType != LobBlob {
		t.Errorf("LobType = %v, want BLOB", h.LobType)
	}
	if h.Size != 1024 {
		t.Errorf("Size = %d, want 1024", h.Size)
	}
	if h.Locator != locator {
		t.Errorf("Locator = %q, want %q", h.Locator, locator)
	}
}

func TestLobHandleDecodeClob(t *testing.T) {
	locator := "file:/clob_xyz"
	locatorSize := len(locator) + 1

	buf := make([]byte, 4+8+4+locatorSize)
	binary.BigEndian.PutUint32(buf[0:4], 24) // CLOB
	binary.BigEndian.PutUint64(buf[4:12], 55555)
	binary.BigEndian.PutUint32(buf[12:16], uint32(locatorSize))
	copy(buf[16:], locator)
	buf[16+len(locator)] = 0x00

	h, err := decodeLobHandle(buf)
	if err != nil {
		t.Fatal(err)
	}
	if h.LobType != LobClob {
		t.Errorf("LobType = %v, want CLOB", h.LobType)
	}
	if h.Size != 55555 {
		t.Errorf("Size = %d", h.Size)
	}
}

func TestLobHandleEncode(t *testing.T) {
	h := &CubridLobHandle{
		LobType: LobBlob,
		Size:    512,
		Locator: "loc_abc",
	}
	data := h.Encode()
	// db_type(4) + size(8) + locatorSize(4) + "loc_abc\0"(8) = 24
	if len(data) != 24 {
		t.Fatalf("encode len = %d, want 24", len(data))
	}

	h2, err := decodeLobHandle(data)
	if err != nil {
		t.Fatal(err)
	}
	if h2.LobType != h.LobType || h2.Size != h.Size || h2.Locator != h.Locator {
		t.Errorf("round-trip mismatch: %v", h2)
	}
}

func TestLobHandleDecodeTooShort(t *testing.T) {
	_, err := decodeLobHandle([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if err == nil {
		t.Error("expected error for short data")
	}
}

func TestLobHandleString(t *testing.T) {
	h := &CubridLobHandle{LobType: LobBlob, Size: 100, Locator: "loc"}
	s := h.String()
	if s != "BLOB(100 bytes, loc)" {
		t.Errorf("String() = %q", s)
	}
}
