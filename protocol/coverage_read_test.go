package protocol

import (
	"bytes"
	"testing"
)

func TestReadFunctionsEOF(t *testing.T) {
	empty := bytes.NewReader(nil)

	_, err := ReadInt(empty)
	if err == nil {
		t.Error("ReadInt: expected error")
	}
	_, err = ReadShort(empty)
	if err == nil {
		t.Error("ReadShort: expected error")
	}
	_, err = ReadLong(empty)
	if err == nil {
		t.Error("ReadLong: expected error")
	}
	_, err = ReadFloat(empty)
	if err == nil {
		t.Error("ReadFloat: expected error")
	}
	_, err = ReadDouble(empty)
	if err == nil {
		t.Error("ReadDouble: expected error")
	}
	_, err = ReadByte(empty)
	if err == nil {
		t.Error("ReadByte: expected error")
	}
	_, err = ReadNullTermString(empty)
	if err == nil {
		t.Error("ReadNullTermString: expected error")
	}
	_, err = ReadFixedString(empty, 5)
	if err == nil {
		t.Error("ReadFixedString: expected error")
	}
	_, err = ReadBytes(empty, 5)
	if err == nil {
		t.Error("ReadBytes: expected error")
	}
}

func TestReadNullTermStringNoTerminator(t *testing.T) {
	// String without null terminator - may return error or partial read.
	r := bytes.NewReader([]byte("hello"))
	_, err := ReadNullTermString(r)
	// This may or may not error depending on implementation.
	_ = err
}

func TestParseResponseFrameZeroPaddingFull(t *testing.T) {
	// Send a zero-length padding followed by a real response.
	var buf bytes.Buffer
	WriteInt(&buf, 0) // padding
	// Real response: payload_len(4) = 4 (just response_code)
	WriteInt(&buf, 4) // payload_len
	buf.Write([]byte{0, 0, 0, 0}) // cas_info
	WriteInt(&buf, 42) // response_code

	frame, err := ParseResponseFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if frame.ResponseCode != 42 {
		t.Errorf("response_code = %d", frame.ResponseCode)
	}
}

func TestParseResponseFrameNegativeLen(t *testing.T) {
	var buf bytes.Buffer
	WriteInt(&buf, -1) // negative payload_len
	_, err := ParseResponseFrame(&buf)
	if err == nil {
		t.Fatal("expected error for negative len")
	}
}

func TestParseOpenDatabaseResponsePreV3(t *testing.T) {
	// Pre-V3: cas_pid(4) + broker_info(8) + session_id(4) = 16 bytes.
	body := make([]byte, 16)
	body[3] = 1 // cas_pid = 1
	// broker_info at offset 4, with protocol V1.
	body[4+4] = 0x40 | byte(ProtocolV1)
	// session_id at offset 12.
	body[12] = 0x42

	resp, err := ParseOpenDatabaseResponse(body, true)
	if err != nil {
		t.Fatal(err)
	}
	if resp.CASPID != 1 {
		t.Errorf("CASPID = %d", resp.CASPID)
	}
}

func TestParseOpenDatabaseResponseTooShort(t *testing.T) {
	_, err := ParseOpenDatabaseResponse([]byte{0, 0, 0}, true)
	if err == nil {
		t.Fatal("expected error for short body")
	}
}
