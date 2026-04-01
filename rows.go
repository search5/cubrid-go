package cubrid

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"

	"github.com/search5/cubrid-go/protocol"
)

// cubridRows implements driver.Rows and ColumnType* interfaces.
type cubridRows struct {
	stmt        *cubridStmt
	columns     []ColumnMeta
	buffer      [][]interface{} // Buffered rows from fetch.
	bufferIdx   int             // Current index into buffer.
	totalTuples int             // Total rows reported by server.
	fetchPos    int             // Next row position to fetch (1-based).
	fetchSize   int             // Number of rows per fetch request.
	done        bool            // No more rows to fetch.
	closed      bool
}

// Columns returns the column names.
func (r *cubridRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, c := range r.columns {
		names[i] = c.Name
	}
	return names
}

// Close releases the cursor.
func (r *cubridRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.buffer = nil
	return nil
}

// Next fetches the next row of data.
func (r *cubridRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	// If buffer is exhausted, fetch more rows.
	if r.bufferIdx >= len(r.buffer) {
		if r.done {
			return io.EOF
		}
		if err := r.fetchMore(); err != nil {
			return err
		}
		if len(r.buffer) == 0 {
			r.done = true
			return io.EOF
		}
	}

	row := r.buffer[r.bufferIdx]
	r.bufferIdx++

	for i, v := range row {
		dest[i] = toDriverValue(v)
	}
	return nil
}

// toDriverValue converts custom CUBRID types to driver.Value compatible types.
// driver.Value must be one of: nil, int64, float64, bool, []byte, string, time.Time.
func toDriverValue(v interface{}) driver.Value {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case *CubridNumeric:
		return val.value
	case *CubridEnum:
		return val.Name
	case *CubridMonetary:
		return val.Amount
	case *CubridJson:
		return val.value
	case *CubridTimestampTz:
		return val.Time
	case *CubridTimestampLtz:
		return val.Time
	case *CubridDateTimeTz:
		return val.Time
	case *CubridDateTimeLtz:
		return val.Time
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return float64(val)
	default:
		return v
	}
}

// fetchMore sends a FETCH request to get more rows.
func (r *cubridRows) fetchMore() error {
	r.buffer = nil
	r.bufferIdx = 0

	if r.fetchPos > r.totalTuples {
		r.done = true
		return nil
	}

	r.stmt.conn.mu.Lock()
	defer r.stmt.conn.mu.Unlock()

	if r.stmt.conn.closed {
		return driver.ErrBadConn
	}

	var buf bytes.Buffer
	// Query handle.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, r.stmt.handle)

	// Start position (1-based).
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(r.fetchPos))

	// Fetch size.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(r.fetchSize))

	// Case-sensitive flag.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x00)

	// Result set index.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0)

	frame, err := r.stmt.conn.sendRequest(protocol.FuncCodeFetch, buf.Bytes())
	if err != nil {
		return fmt.Errorf("cubrid: fetch: %w", err)
	}

	if frame.ResponseCode < 0 {
		// -1012 (no more data) is normal end of result set.
		if frame.ResponseCode == ErrCodeNoMoreData {
			r.done = true
			return nil
		}
		return r.stmt.conn.checkError(frame)
	}

	// Parse fetch response.
	reader := bytes.NewReader(frame.Body)
	tupleCount, err := protocol.ReadInt(reader)
	if err != nil {
		return fmt.Errorf("cubrid: read tuple count: %w", err)
	}

	if tupleCount <= 0 {
		r.done = true
		return nil
	}

	r.buffer, err = parseTuples(reader, int(tupleCount), r.columns)
	if err != nil {
		return fmt.Errorf("cubrid: parse fetched tuples: %w", err)
	}

	r.fetchPos += int(tupleCount)
	return nil
}

// --- ColumnType interfaces for GORM, Atlas, sqlx compatibility ---

// ColumnTypeDatabaseTypeName returns the CUBRID type name.
func (r *cubridRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return r.columns[index].Type.String()
}

// ColumnTypeNullable reports whether the column is nullable.
func (r *cubridRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return false, false
	}
	return r.columns[index].Nullable, true
}

// ColumnTypeScanType returns the Go type suitable for scanning.
func (r *cubridRows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return reflect.TypeOf([]byte{})
	}
	return ScanTypeForCubridType(r.columns[index].Type)
}

// ColumnTypeLength returns the column length (for variable-length types).
func (r *cubridRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, false
	}
	col := r.columns[index]
	switch col.Type {
	case protocol.CubridTypeChar, protocol.CubridTypeString, protocol.CubridTypeNChar,
		protocol.CubridTypeVarNChar, protocol.CubridTypeBit, protocol.CubridTypeVarBit:
		return int64(col.Precision), true
	default:
		return 0, false
	}
}

// ColumnTypePrecisionScale returns the precision and scale for NUMERIC types.
func (r *cubridRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, 0, false
	}
	col := r.columns[index]
	if col.Type == protocol.CubridTypeNumeric {
		return int64(col.Precision), int64(col.Scale), true
	}
	return 0, 0, false
}
