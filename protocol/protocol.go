// Package protocol implements the CUBRID CCI binary protocol encoding and decoding.
// All multi-byte integers are big-endian (network byte order).
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
)

// FuncCode represents a CAS function code.
type FuncCode byte

const (
	FuncCodeEndTran          FuncCode = 1
	FuncCodePrepare          FuncCode = 2
	FuncCodeExecute          FuncCode = 3
	FuncCodeGetDBParameter   FuncCode = 4
	FuncCodeSetDBParameter   FuncCode = 5
	FuncCodeCloseReqHandle   FuncCode = 6
	FuncCodeCursor           FuncCode = 7
	FuncCodeFetch            FuncCode = 8
	FuncCodeSchemaInfo       FuncCode = 9
	FuncCodeGetDBVersion     FuncCode = 15
	FuncCodeCollection       FuncCode = 18
	FuncCodeOidGet           FuncCode = 10
	FuncCodeOidPut           FuncCode = 11
	FuncCodeOidCmd           FuncCode = 17
	FuncCodeExecuteBatch     FuncCode = 20
	FuncCodeNextResult       FuncCode = 19
	FuncCodeCursorUpdate     FuncCode = 22
	FuncCodeSavepoint        FuncCode = 26
	FuncCodeXaPrepare        FuncCode = 28
	FuncCodeXaRecover        FuncCode = 29
	FuncCodeXaEndTran        FuncCode = 30
	FuncCodeConClose         FuncCode = 31
	FuncCodeGetGeneratedKeys FuncCode = 34
	FuncCodeLOBNew           FuncCode = 35
	FuncCodeLOBWrite         FuncCode = 36
	FuncCodeLOBRead          FuncCode = 37
	FuncCodeEndSession       FuncCode = 38
	FuncCodeGetRowCount      FuncCode = 39
	FuncCodeGetLastInsertID  FuncCode = 40
	FuncCodePrepareAndExec   FuncCode = 41
	FuncCodeCursorClose      FuncCode = 42
)

// CAS info flags.
const (
	CASInfoAutoCommit   byte = 0x01
	CASInfoForceOutTran byte = 0x02
	CASInfoNewSessionID byte = 0x04
)

// CASInfo holds the 4-byte session state exchanged with every request/response.
type CASInfo [4]byte

// NewCASInfo returns the initial CAS info value for a new connection.
func NewCASInfo() CASInfo {
	return CASInfo{0x00, 0xFF, 0xFF, 0xFF}
}

// SetAutoCommit sets or clears the auto-commit flag.
func (c *CASInfo) SetAutoCommit(on bool) {
	if on {
		c[0] |= CASInfoAutoCommit
	} else {
		c[0] &^= CASInfoAutoCommit
	}
}

// AutoCommit returns whether auto-commit is enabled.
func (c CASInfo) AutoCommit() bool {
	return c[0]&CASInfoAutoCommit != 0
}

// --- Write primitives (big-endian) ---

// WriteInt writes a 32-bit big-endian integer.
func WriteInt(w io.Writer, v int32) {
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], uint32(v))
	w.Write(b[:])
}

// WriteShort writes a 16-bit big-endian integer.
func WriteShort(w io.Writer, v int16) {
	b := [2]byte{}
	binary.BigEndian.PutUint16(b[:], uint16(v))
	w.Write(b[:])
}

// WriteLong writes a 64-bit big-endian integer.
func WriteLong(w io.Writer, v int64) {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.Write(b[:])
}

// WriteFloat writes a 32-bit IEEE 754 float in big-endian.
func WriteFloat(w io.Writer, v float32) {
	WriteInt(w, int32(math.Float32bits(v)))
}

// WriteDouble writes a 64-bit IEEE 754 double in big-endian.
func WriteDouble(w io.Writer, v float64) {
	WriteLong(w, int64(math.Float64bits(v)))
}

// WriteByte writes a single byte.
func WriteByte(w io.Writer, v byte) {
	w.Write([]byte{v})
}

// WriteNullTermString writes a length-prefixed, null-terminated string.
// Format: [4-byte length including null] [string bytes] [0x00]
func WriteNullTermString(w io.Writer, s string) {
	WriteInt(w, int32(len(s)+1))
	w.Write([]byte(s))
	w.Write([]byte{0x00})
}

// WriteFixedString writes a string zero-padded to exactly n bytes.
func WriteFixedString(w io.Writer, s string, n int) {
	b := make([]byte, n)
	copy(b, s)
	w.Write(b)
}

// WriteBytes writes a length-prefixed byte slice.
func WriteBytes(w io.Writer, data []byte) {
	WriteInt(w, int32(len(data)))
	w.Write(data)
}

// --- Read primitives (big-endian) ---

// ReadInt reads a 32-bit big-endian integer.
func ReadInt(r io.Reader) (int32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b[:])), nil
}

// ReadShort reads a 16-bit big-endian integer.
func ReadShort(r io.Reader) (int16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b[:])), nil
}

// ReadLong reads a 64-bit big-endian integer.
func ReadLong(r io.Reader) (int64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b[:])), nil
}

// ReadFloat reads a 32-bit IEEE 754 float in big-endian.
func ReadFloat(r io.Reader) (float32, error) {
	v, err := ReadInt(r)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(uint32(v)), nil
}

// ReadDouble reads a 64-bit IEEE 754 double in big-endian.
func ReadDouble(r io.Reader) (float64, error) {
	v, err := ReadLong(r)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(v)), nil
}

// ReadByte reads a single byte.
func ReadByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadNullTermString reads a length-prefixed, null-terminated string.
func ReadNullTermString(r io.Reader) (string, error) {
	length, err := ReadInt(r)
	if err != nil {
		return "", err
	}
	if length <= 0 {
		return "", nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	// Strip trailing null terminator.
	return strings.TrimRight(string(buf), "\x00"), nil
}

// ReadFixedString reads n bytes and returns the string up to the first null.
func ReadFixedString(r io.Reader, n int) (string, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	idx := 0
	for idx < n && buf[idx] != 0 {
		idx++
	}
	return string(buf[:idx]), nil
}

// ReadBytes reads exactly n bytes.
func ReadBytes(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// --- Message framing ---

// BuildRequestMessage constructs a complete CCI request message.
//
// Wire format:
//
//	[4 bytes] payload_len — size of func_code(1) + payload, EXCLUDING CAS info
//	[4 bytes] CAS info
//	[1 byte]  func code
//	[N bytes] payload
func BuildRequestMessage(casInfo CASInfo, fc FuncCode, payload []byte) []byte {
	payloadLen := 1 + len(payload) // func code + payload (excludes CAS info)
	msg := make([]byte, 4+4+payloadLen)
	binary.BigEndian.PutUint32(msg[0:4], uint32(payloadLen))
	copy(msg[4:8], casInfo[:])
	msg[8] = byte(fc)
	copy(msg[9:], payload)
	return msg
}

// ResponseFrame holds a parsed CCI response.
type ResponseFrame struct {
	CASInfo      CASInfo
	ResponseCode int32
	Body         []byte
}

// ParseResponseFrame reads and parses a CCI response from the reader.
//
// Standard CAS response framing (NOT used for OpenDatabase):
//
//	[4 bytes] payload_len — size of payload EXCLUDING CAS info
//	[4 bytes] CAS info
//	[payload_len bytes] payload (starts with response_code)
//
// Total bytes after length field = CAS info(4) + payload_len.
func ParseResponseFrame(r io.Reader) (*ResponseFrame, error) {
	// Skip zero-length padding packets that CAS may send between responses.
	var payloadLen int32
	for i := 0; i < 100; i++ {
		var err error
		payloadLen, err = ReadInt(r)
		if err != nil {
			return nil, fmt.Errorf("read response length: %w", err)
		}
		if payloadLen != 0 {
			break
		}
	}
	if payloadLen < 4 {
		return nil, fmt.Errorf("response payload too short: %d bytes", payloadLen)
	}

	// Read CAS info (4 bytes, not counted in payloadLen).
	var frame ResponseFrame
	if _, err := io.ReadFull(r, frame.CASInfo[:]); err != nil {
		return nil, fmt.Errorf("read CAS info: %w", err)
	}

	// Read response code (first 4 bytes of payload).
	var err error
	frame.ResponseCode, err = ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("read response code: %w", err)
	}

	// Remaining payload after response code.
	bodyLen := payloadLen - 4
	if bodyLen > 0 {
		frame.Body = make([]byte, bodyLen)
		if _, err := io.ReadFull(r, frame.Body); err != nil {
			return nil, fmt.Errorf("read response body: %w", err)
		}
	}

	return &frame, nil
}
