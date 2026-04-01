package protocol

import (
	"bytes"
	"testing"
)

func TestWriteInt(t *testing.T) {
	tests := []struct {
		name string
		val  int32
		want []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"positive", 256, []byte{0x00, 0x00, 0x01, 0x00}},
		{"negative", -1, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"max", 2147483647, []byte{0x7F, 0xFF, 0xFF, 0xFF}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			WriteInt(&buf, tt.val)
			if !bytes.Equal(buf.Bytes(), tt.want) {
				t.Errorf("WriteInt(%d) = %x, want %x", tt.val, buf.Bytes(), tt.want)
			}
		})
	}
}

func TestWriteShort(t *testing.T) {
	tests := []struct {
		name string
		val  int16
		want []byte
	}{
		{"zero", 0, []byte{0x00, 0x00}},
		{"positive", 2024, []byte{0x07, 0xE8}},
		{"negative", -1, []byte{0xFF, 0xFF}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			WriteShort(&buf, tt.val)
			if !bytes.Equal(buf.Bytes(), tt.want) {
				t.Errorf("WriteShort(%d) = %x, want %x", tt.val, buf.Bytes(), tt.want)
			}
		})
	}
}

func TestWriteLong(t *testing.T) {
	var buf bytes.Buffer
	WriteLong(&buf, 1234567890123456789)
	want := []byte{0x11, 0x22, 0x10, 0xF4, 0x7D, 0xE9, 0x81, 0x15}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("WriteLong = %x, want %x", buf.Bytes(), want)
	}
}

func TestWriteNullTermString(t *testing.T) {
	var buf bytes.Buffer
	WriteNullTermString(&buf, "hello")
	// 4-byte length (6 = 5 chars + 1 null) + "hello" + 0x00
	want := []byte{0x00, 0x00, 0x00, 0x06, 'h', 'e', 'l', 'l', 'o', 0x00}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("got %x, want %x", buf.Bytes(), want)
	}
}

func TestWriteNullTermStringEmpty(t *testing.T) {
	var buf bytes.Buffer
	WriteNullTermString(&buf, "")
	// 4-byte length (1 = just null) + 0x00
	want := []byte{0x00, 0x00, 0x00, 0x01, 0x00}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("got %x, want %x", buf.Bytes(), want)
	}
}

func TestWriteFixedString(t *testing.T) {
	var buf bytes.Buffer
	WriteFixedString(&buf, "dba", 32)
	got := buf.Bytes()
	if len(got) != 32 {
		t.Fatalf("len = %d, want 32", len(got))
	}
	if string(got[:3]) != "dba" {
		t.Errorf("prefix = %q, want %q", got[:3], "dba")
	}
	for i := 3; i < 32; i++ {
		if got[i] != 0 {
			t.Fatalf("byte[%d] = %x, want 0x00", i, got[i])
		}
	}
}

func TestReadInt(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want int32
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0},
		{"positive", []byte{0x00, 0x00, 0x01, 0x00}, 256},
		{"negative", []byte{0xFF, 0xFF, 0xFF, 0xFF}, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			got, err := ReadInt(r)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("ReadInt = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReadShort(t *testing.T) {
	r := bytes.NewReader([]byte{0x07, 0xE8})
	got, err := ReadShort(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != 2024 {
		t.Errorf("ReadShort = %d, want 2024", got)
	}
}

func TestReadLong(t *testing.T) {
	r := bytes.NewReader([]byte{0x11, 0x22, 0x10, 0xF4, 0x7D, 0xE9, 0x81, 0x15})
	got, err := ReadLong(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1234567890123456789 {
		t.Errorf("ReadLong = %d, want 1234567890123456789", got)
	}
}

func TestReadNullTermString(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x06, 'h', 'e', 'l', 'l', 'o', 0x00}
	r := bytes.NewReader(data)
	got, err := ReadNullTermString(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

func TestReadFixedString(t *testing.T) {
	data := make([]byte, 32)
	copy(data, "dba")
	r := bytes.NewReader(data)
	got, err := ReadFixedString(r, 32)
	if err != nil {
		t.Fatal(err)
	}
	if got != "dba" {
		t.Errorf("got %q, want %q", got, "dba")
	}
}

func TestReadByte(t *testing.T) {
	r := bytes.NewReader([]byte{0x42})
	got, err := ReadByte(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0x42 {
		t.Errorf("got %x, want 0x42", got)
	}
}

func TestReadFloat(t *testing.T) {
	// IEEE 754: 3.14 ≈ 0x4048F5C3
	data := []byte{0x40, 0x48, 0xF5, 0xC3}
	r := bytes.NewReader(data)
	got, err := ReadFloat(r)
	if err != nil {
		t.Fatal(err)
	}
	if got < 3.13 || got > 3.15 {
		t.Errorf("ReadFloat = %f, want ~3.14", got)
	}
}

func TestReadDouble(t *testing.T) {
	// IEEE 754: 3.14 ≈ 0x40091EB851EB851F
	data := []byte{0x40, 0x09, 0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F}
	r := bytes.NewReader(data)
	got, err := ReadDouble(r)
	if err != nil {
		t.Fatal(err)
	}
	if got < 3.13 || got > 3.15 {
		t.Errorf("ReadDouble = %f, want ~3.14", got)
	}
}

func TestCASInfoTracking(t *testing.T) {
	info := NewCASInfo()
	if info[0] != 0x00 || info[1] != 0xFF || info[2] != 0xFF || info[3] != 0xFF {
		t.Errorf("initial CASInfo = %x, want [00 FF FF FF]", info)
	}

	info.SetAutoCommit(true)
	if info[0]&CASInfoAutoCommit == 0 {
		t.Error("autocommit flag not set")
	}

	info.SetAutoCommit(false)
	if info[0]&CASInfoAutoCommit != 0 {
		t.Error("autocommit flag should be cleared")
	}
}

func TestBuildRequestMessage(t *testing.T) {
	casInfo := CASInfo{0x01, 0xFF, 0xFF, 0xFF}
	payload := []byte{0x01, 0x02, 0x03}
	msg := BuildRequestMessage(casInfo, FuncCodePrepare, payload)

	// 4-byte length + 4-byte CAS info + 1-byte func code + 3-byte payload = 12
	if len(msg) != 12 {
		t.Fatalf("message length = %d, want 12", len(msg))
	}

	// Length field = 1 (func code) + 3 (payload) = 4 (excludes CAS info)
	length := int32(msg[0])<<24 | int32(msg[1])<<16 | int32(msg[2])<<8 | int32(msg[3])
	if length != 4 {
		t.Errorf("length field = %d, want 4", length)
	}

	// CAS info
	if !bytes.Equal(msg[4:8], casInfo[:]) {
		t.Errorf("CAS info = %x, want %x", msg[4:8], casInfo[:])
	}

	// Function code
	if msg[8] != byte(FuncCodePrepare) {
		t.Errorf("func code = %d, want %d", msg[8], FuncCodePrepare)
	}

	// Payload
	if !bytes.Equal(msg[9:], payload) {
		t.Errorf("payload = %x, want %x", msg[9:], payload)
	}
}

func TestParseResponseFrame(t *testing.T) {
	// Standard response: [payload_len][CAS info][payload]
	// payload_len = responseCode(4) + body(4) = 8 (excludes CAS info)
	var buf bytes.Buffer
	WriteInt(&buf, 8) // payload_len: responseCode(4) + body(4)
	buf.Write([]byte{0x01, 0xFF, 0xFF, 0xFF}) // CAS info
	WriteInt(&buf, 0)                          // response code (success)
	WriteInt(&buf, 42)                         // body data

	frame, err := ParseResponseFrame(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if frame.ResponseCode != 0 {
		t.Errorf("ResponseCode = %d, want 0", frame.ResponseCode)
	}
	if frame.CASInfo[0] != 0x01 {
		t.Errorf("CASInfo[0] = %x, want 0x01", frame.CASInfo[0])
	}
	if len(frame.Body) != 4 {
		t.Errorf("body length = %d, want 4", len(frame.Body))
	}
}

func TestParseResponseFrameError(t *testing.T) {
	var buf bytes.Buffer
	// payload_len = responseCode(4) + errorCode(4) + errorMsg + null
	errMsg := "table not found"
	payloadLen := 4 + 4 + len(errMsg) + 1 // responseCode + errCode + msg + null
	WriteInt(&buf, int32(payloadLen))
	buf.Write([]byte{0x00, 0xFF, 0xFF, 0xFF})
	WriteInt(&buf, -1)    // response code (CAS error)
	WriteInt(&buf, -1008) // error code
	buf.Write([]byte(errMsg))
	buf.WriteByte(0x00)

	frame, err := ParseResponseFrame(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if frame.ResponseCode != -1 {
		t.Errorf("ResponseCode = %d, want -1", frame.ResponseCode)
	}
}

func TestParseResponseFrameZeroPadding(t *testing.T) {
	// Zero-length packets should be skipped.
	var buf bytes.Buffer
	WriteInt(&buf, 0) // zero padding
	WriteInt(&buf, 0) // zero padding
	WriteInt(&buf, 4) // payload_len = responseCode(4) only
	buf.Write([]byte{0x01, 0xFF, 0xFF, 0xFF})
	WriteInt(&buf, 99) // response code

	frame, err := ParseResponseFrame(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if frame.ResponseCode != 99 {
		t.Errorf("ResponseCode = %d, want 99", frame.ResponseCode)
	}
}
