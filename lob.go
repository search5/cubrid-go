package cubrid

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/search5/cubrid-go/protocol"
)

// LobType discriminates between BLOB and CLOB handles.
type LobType int32

const (
	LobBlob LobType = 23
	LobClob LobType = 24
)

func (t LobType) String() string {
	switch t {
	case LobBlob:
		return "BLOB"
	case LobClob:
		return "CLOB"
	default:
		return fmt.Sprintf("LOB(%d)", t)
	}
}

// CubridLobHandle is a locator-based handle for a CUBRID BLOB or CLOB value.
// LOB data is NOT transferred inline. The server returns a locator handle
// that must be used with LobRead/LobWrite to stream the actual content.
type CubridLobHandle struct {
	LobType LobType
	Size    int64
	Locator string
}

// String returns a human-readable representation.
func (h *CubridLobHandle) String() string {
	return fmt.Sprintf("%s(%d bytes, %s)", h.LobType, h.Size, h.Locator)
}

// Encode serializes the LOB handle into wire format.
// Wire: db_type(4) + lobSize(8) + locatorSize(4) + locator(null-terminated)
func (h *CubridLobHandle) Encode() []byte {
	locatorLen := len(h.Locator) + 1 // includes null terminator
	buf := make([]byte, 4+8+4+locatorLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(h.LobType))
	binary.BigEndian.PutUint64(buf[4:12], uint64(h.Size))
	binary.BigEndian.PutUint32(buf[12:16], uint32(locatorLen))
	copy(buf[16:], h.Locator)
	buf[16+len(h.Locator)] = 0x00
	return buf
}

// DecodeLobHandle deserializes a LOB handle from wire data.
func DecodeLobHandle(data []byte) (*CubridLobHandle, error) {
	if len(data) < 17 {
		return nil, fmt.Errorf("cubrid: LOB handle requires at least 17 bytes, got %d", len(data))
	}
	typeCode := int32(binary.BigEndian.Uint32(data[0:4]))
	size := int64(binary.BigEndian.Uint64(data[4:12]))
	locatorSize := int32(binary.BigEndian.Uint32(data[12:16]))
	if locatorSize < 0 {
		return nil, fmt.Errorf("cubrid: negative LOB locator size: %d", locatorSize)
	}
	if int(16+locatorSize) > len(data) {
		return nil, fmt.Errorf("cubrid: LOB handle truncated: locator needs %d bytes", locatorSize)
	}
	locator := strings.TrimRight(string(data[16:16+locatorSize]), "\x00")

	// LOB handles on the wire may use either the CubridDataType codes (23/24)
	// or an internal encoding with a +10 offset (33/34). Accept both.
	var lobType LobType
	switch typeCode {
	case 23, 33:
		lobType = LobBlob
	case 24, 34:
		lobType = LobClob
	default:
		return nil, fmt.Errorf("cubrid: invalid LOB type code: %d", typeCode)
	}

	return &CubridLobHandle{
		LobType: lobType,
		Size:    size,
		Locator: locator,
	}, nil
}

// Value implements driver.Valuer — returns the encoded handle bytes.
func (h *CubridLobHandle) Value() (driver.Value, error) {
	return h.Encode(), nil
}

// Scan implements sql.Scanner.
func (h *CubridLobHandle) Scan(src interface{}) error {
	switch v := src.(type) {
	case *CubridLobHandle:
		*h = *v
		return nil
	case []byte:
		decoded, err := DecodeLobHandle(v)
		if err != nil {
			return err
		}
		*h = *decoded
		return nil
	case nil:
		*h = CubridLobHandle{}
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridLobHandle", src)
	}
}

// --- LOB streaming operations ---

const lobChunkSize = 1024 * 1024 // 1 MB per read/write chunk

// LobNew creates a new empty LOB on the server and returns its handle.
func LobNew(ctx context.Context, conn *cubridConn, lobType LobType) (*CubridLobHandle, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed {
		return nil, driver.ErrBadConn
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(lobType))

	frame, err := conn.sendRequestCtx(ctx, protocol.FuncCodeLOBNew, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if err := conn.checkError(frame); err != nil {
		return nil, err
	}

	// Response body contains the LOB handle.
	return DecodeLobHandle(frame.Body)
}

// LobWrite writes data to a LOB at the given offset.
// Returns the number of bytes written.
func LobWrite(ctx context.Context, conn *cubridConn, handle *CubridLobHandle, offset int64, data []byte) (int, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed {
		return 0, driver.ErrBadConn
	}

	handleBytes := handle.Encode()
	var buf bytes.Buffer
	// LOB handle (length-prefixed).
	protocol.WriteInt(&buf, int32(len(handleBytes)))
	buf.Write(handleBytes)
	// Offset.
	protocol.WriteInt(&buf, 8)
	protocol.WriteLong(&buf, offset)
	// Data (length-prefixed).
	protocol.WriteInt(&buf, int32(len(data)))
	buf.Write(data)

	frame, err := conn.sendRequestCtx(ctx, protocol.FuncCodeLOBWrite, buf.Bytes())
	if err != nil {
		return 0, err
	}
	if err := conn.checkError(frame); err != nil {
		return 0, err
	}

	return int(frame.ResponseCode), nil
}

// LobRead reads data from a LOB at the given offset.
// Returns the data read (up to length bytes).
func LobRead(ctx context.Context, conn *cubridConn, handle *CubridLobHandle, offset int64, length int) ([]byte, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed {
		return nil, driver.ErrBadConn
	}

	handleBytes := handle.Encode()
	var buf bytes.Buffer
	// LOB handle (length-prefixed).
	protocol.WriteInt(&buf, int32(len(handleBytes)))
	buf.Write(handleBytes)
	// Offset.
	protocol.WriteInt(&buf, 8)
	protocol.WriteLong(&buf, offset)
	// Length.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(length))

	frame, err := conn.sendRequestCtx(ctx, protocol.FuncCodeLOBRead, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if err := conn.checkError(frame); err != nil {
		return nil, err
	}

	return frame.Body, nil
}

// --- io.Reader / io.Writer wrappers ---

// LobReader provides an io.Reader interface for reading LOB data.
type LobReader struct {
	ctx    context.Context
	conn   *cubridConn
	handle *CubridLobHandle
	offset int64
}

// NewLobReader creates a reader that streams LOB data from the server.
func NewLobReader(ctx context.Context, conn *cubridConn, handle *CubridLobHandle) *LobReader {
	return &LobReader{ctx: ctx, conn: conn, handle: handle, offset: 0}
}

// Read implements io.Reader.
func (r *LobReader) Read(p []byte) (int, error) {
	if r.offset >= r.handle.Size {
		return 0, io.EOF
	}

	readLen := len(p)
	remaining := int(r.handle.Size - r.offset)
	if readLen > remaining {
		readLen = remaining
	}
	if readLen > lobChunkSize {
		readLen = lobChunkSize
	}

	data, err := LobRead(r.ctx, r.conn, r.handle, r.offset, readLen)
	if err != nil {
		return 0, err
	}

	n := copy(p, data)
	r.offset += int64(n)

	if r.offset >= r.handle.Size {
		return n, io.EOF
	}
	return n, nil
}

// LobWriter provides an io.Writer interface for writing LOB data.
type LobWriter struct {
	ctx    context.Context
	conn   *cubridConn
	handle *CubridLobHandle
	offset int64
}

// NewLobWriter creates a writer that streams LOB data to the server.
func NewLobWriter(ctx context.Context, conn *cubridConn, handle *CubridLobHandle) *LobWriter {
	return &LobWriter{ctx: ctx, conn: conn, handle: handle, offset: 0}
}

// Write implements io.Writer.
func (w *LobWriter) Write(p []byte) (int, error) {
	total := 0
	for total < len(p) {
		chunk := p[total:]
		if len(chunk) > lobChunkSize {
			chunk = chunk[:lobChunkSize]
		}

		n, err := LobWrite(w.ctx, w.conn, w.handle, w.offset, chunk)
		if err != nil {
			return total, err
		}

		total += n
		w.offset += int64(n)
		w.handle.Size = w.offset // Update handle size.
	}
	return total, nil
}
