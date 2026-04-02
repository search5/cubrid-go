package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/search5/cubrid-go/protocol"
)

// CubridOid represents a CUBRID Object Identifier.
// Every persistent object in CUBRID is identified by a triple of
// (PageID, SlotID, VolID) packed into 8 bytes on the wire.
type CubridOid struct {
	PageID int32
	SlotID int16
	VolID  int16
}

// NewCubridOid creates a new OID.
func NewCubridOid(pageID int32, slotID, volID int16) *CubridOid {
	return &CubridOid{PageID: pageID, SlotID: slotID, VolID: volID}
}

// IsNull returns true if this is the zero (null) OID.
func (o *CubridOid) IsNull() bool {
	return o.PageID == 0 && o.SlotID == 0 && o.VolID == 0
}

// String returns a human-readable representation.
func (o *CubridOid) String() string {
	return fmt.Sprintf("OID(%d, %d, %d)", o.PageID, o.SlotID, o.VolID)
}

// Encode serializes the OID into 8 bytes (big-endian).
func (o *CubridOid) Encode() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], uint32(o.PageID))
	binary.BigEndian.PutUint16(b[4:6], uint16(o.SlotID))
	binary.BigEndian.PutUint16(b[6:8], uint16(o.VolID))
	return b
}

// DecodeCubridOid deserializes an OID from 8 bytes.
func decodeCubridOid(data []byte) (*CubridOid, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("cubrid: OID requires 8 bytes, got %d", len(data))
	}
	return &CubridOid{
		PageID: int32(binary.BigEndian.Uint32(data[0:4])),
		SlotID: int16(binary.BigEndian.Uint16(data[4:6])),
		VolID:  int16(binary.BigEndian.Uint16(data[6:8])),
	}, nil
}

// Value implements driver.Valuer.
func (o *CubridOid) Value() (driver.Value, error) {
	return o.Encode(), nil
}

// Scan implements sql.Scanner.
func (o *CubridOid) Scan(src interface{}) error {
	switch v := src.(type) {
	case *CubridOid:
		*o = *v
		return nil
	case protocol.OID:
		o.PageID = v.PageID
		o.SlotID = v.SlotID
		o.VolID = v.VolID
		return nil
	case []byte:
		decoded, err := decodeCubridOid(v)
		if err != nil {
			return err
		}
		*o = *decoded
		return nil
	case nil:
		*o = CubridOid{}
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridOid", src)
	}
}

// --- OID-based operations ---

// OidGet reads attribute values from an object identified by OID.
// The attrs parameter specifies which attributes to read (empty = all).
// Returns a map of attribute name → value.
func OidGet(ctx context.Context, conn *sql.Conn, oid *CubridOid, attrs []string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: OidGet requires a cubrid connection")
		}
		r, err := c.oidGet(ctx, oid, attrs)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	return result, err
}

func (c *cubridConn) oidGet(ctx context.Context, oid *CubridOid, attrs []string) (map[string]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	var buf bytes.Buffer
	// OID (length-prefixed 8 bytes).
	protocol.WriteInt(&buf, 8)
	buf.Write(oid.Encode())
	// Attribute names as comma-separated null-terminated string.
	attrsStr := strings.Join(attrs, ",")
	protocol.WriteNullTermString(&buf, attrsStr)

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeOidGet, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if err := c.checkError(frame); err != nil {
		return nil, err
	}

	return parseOidGetResponse(frame, attrs)
}

// OidPut updates attribute values on an object identified by OID.
func OidPut(ctx context.Context, conn *sql.Conn, oid *CubridOid, attrs map[string]interface{}) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: OidPut requires a cubrid connection")
		}
		return c.oidPut(ctx, oid, attrs)
	})
}

func (c *cubridConn) oidPut(ctx context.Context, oid *CubridOid, attrs map[string]interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	var buf bytes.Buffer
	// OID (length-prefixed 8 bytes).
	protocol.WriteInt(&buf, 8)
	buf.Write(oid.Encode())
	// Attribute count.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(len(attrs)))
	// Attribute names.
	for name := range attrs {
		protocol.WriteNullTermString(&buf, name)
	}
	// Attribute values (type + value pairs, same as bind params).
	for _, val := range attrs {
		data, cubType, err := encodeBindValue(val)
		if err != nil {
			return fmt.Errorf("cubrid: encode OID attr value: %w", err)
		}
		if cubType == protocol.CubridTypeNull {
			protocol.WriteInt(&buf, 1)
			protocol.WriteByte(&buf, byte(protocol.CubridTypeNull))
			protocol.WriteInt(&buf, 0)
		} else {
			protocol.WriteInt(&buf, 1)
			protocol.WriteByte(&buf, byte(cubType))
			protocol.WriteInt(&buf, int32(len(data)))
			buf.Write(data)
		}
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeOidPut, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}

func parseOidGetResponse(frame *protocol.ResponseFrame, attrs []string) (map[string]interface{}, error) {
	// OID_GET response has the same format as a single-row result set.
	// ResponseCode is column count, body contains column metadata + one tuple.
	colCount := int(frame.ResponseCode)
	if colCount <= 0 {
		return map[string]interface{}{}, nil
	}

	if len(frame.Body) == 0 {
		return map[string]interface{}{}, nil
	}

	r := bytes.NewReader(frame.Body)

	// Parse column metadata (same format as PREPARE response).
	columns := make([]ColumnMeta, colCount)
	for i := 0; i < colCount; i++ {
		col, err := parseColumnMeta(r, protocol.ProtocolLatest)
		if err != nil {
			return nil, fmt.Errorf("cubrid: OID_GET parse column %d: %w", i, err)
		}
		columns[i] = col
	}

	// Parse exactly one tuple.
	tuples, err := parseTuples(r, 1, columns)
	if err != nil {
		return nil, fmt.Errorf("cubrid: OID_GET parse tuple: %w", err)
	}

	if len(tuples) == 0 {
		return map[string]interface{}{}, nil
	}

	// Build result map using column names as keys.
	result := make(map[string]interface{}, colCount)
	row := tuples[0]
	for i, col := range columns {
		if i < len(row) {
			result[col.Name] = row[i]
		}
	}
	return result, nil
}
