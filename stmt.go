package cubrid

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/search5/cubrid-go/protocol"
)

// cubridStmt implements driver.Stmt and related interfaces.
type cubridStmt struct {
	conn      *cubridConn
	handle    int32
	stmtType  protocol.StmtType
	bindCount int
	columns   []ColumnMeta
	closed    bool
}

// Close releases the prepared statement on the server.
func (s *cubridStmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if s.conn.closed {
		return nil
	}

	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, s.handle)
	// Auto-commit flag (required by protocol).
	protocol.WriteInt(&buf, 1)
	if s.conn.autoCommit && !s.conn.inTx {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	_, err := s.conn.sendRequest(protocol.FuncCodeCloseReqHandle, buf.Bytes())
	return err
}

// NumInput returns the number of bind parameters.
func (s *cubridStmt) NumInput() int {
	return s.bindCount
}

// Exec executes a non-query statement.
func (s *cubridStmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), named)
}

// Query executes a query that returns rows.
func (s *cubridStmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), named)
}

// ExecContext executes a non-query statement with context.
func (s *cubridStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if s.closed || s.conn.closed {
		return nil, driver.ErrBadConn
	}

	frame, err := s.execute(ctx, args, false)
	if err != nil {
		return nil, err
	}
	if err := s.conn.checkError(frame); err != nil {
		return nil, err
	}

	return parseExecResult(s.conn, frame)
}

// QueryContext executes a query that returns rows with context.
func (s *cubridStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if s.closed || s.conn.closed {
		return nil, driver.ErrBadConn
	}

	frame, err := s.execute(ctx, args, true)
	if err != nil {
		return nil, err
	}
	if err := s.conn.checkError(frame); err != nil {
		return nil, err
	}

	return parseQueryResult(s, frame)
}

// execute builds and sends an EXECUTE request.
func (s *cubridStmt) execute(ctx context.Context, args []driver.NamedValue, isQuery bool) (*protocol.ResponseFrame, error) {
	var buf bytes.Buffer

	// Query handle.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, s.handle)

	// Execute flag: 0x00 normal.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x00)

	// Max column size (0 = unlimited).
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0)

	// Max row size (0 = unlimited).
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0)

	// NULL reserved.
	protocol.WriteInt(&buf, 0)

	// Fetch flag: 1 for SELECT, 0 otherwise.
	protocol.WriteInt(&buf, 1)
	if isQuery {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	// Auto-commit.
	protocol.WriteInt(&buf, 1)
	if s.conn.autoCommit && !s.conn.inTx {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	// Forward-only cursor.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x01)

	// Cache time: seconds(4) + microseconds(4).
	protocol.WriteInt(&buf, 8)
	protocol.WriteInt(&buf, 0)
	protocol.WriteInt(&buf, 0)

	// Query timeout (milliseconds).
	protocol.WriteInt(&buf, 4)
	if s.conn.dsn.QueryTimeout > 0 {
		protocol.WriteInt(&buf, int32(s.conn.dsn.QueryTimeout.Milliseconds()))
	} else {
		protocol.WriteInt(&buf, 0)
	}

	// Bind parameters.
	// CUBRID wire format: type and value are SEPARATE length-prefixed fields.
	// [4-byte len=1][1-byte type_code] [4-byte value_len][N-byte value]
	// For NULL: [4-byte len=1][1-byte type_code] [4-byte len=0]
	for _, arg := range args {
		data, cubType, err := encodeBindValue(arg.Value)
		if err != nil {
			return nil, fmt.Errorf("cubrid: encode param %d: %w", arg.Ordinal, err)
		}

		if cubType == protocol.CubridTypeNull {
			// Type field: length=1 + NULL type code.
			protocol.WriteInt(&buf, 1)
			protocol.WriteByte(&buf, byte(protocol.CubridTypeNull))
			// Value field: length=0 (NULL).
			protocol.WriteInt(&buf, 0)
		} else {
			// Type field: length=1 + type code.
			protocol.WriteInt(&buf, 1)
			protocol.WriteByte(&buf, byte(cubType))
			// Value field: length + data.
			protocol.WriteInt(&buf, int32(len(data)))
			buf.Write(data)
		}
	}

	return s.conn.sendRequestCtx(ctx, protocol.FuncCodeExecute, buf.Bytes())
}

// --- Result parsing ---

type cubridResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r *cubridResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *cubridResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func parseExecResult(conn *cubridConn, frame *protocol.ResponseFrame) (*cubridResult, error) {
	result := &cubridResult{
		rowsAffected: int64(frame.ResponseCode),
	}

	if len(frame.Body) < 1 {
		return result, nil
	}

	r := bytes.NewReader(frame.Body)

	// Cache reusable.
	if _, err := protocol.ReadByte(r); err != nil {
		return result, nil
	}

	// Result count.
	resultCount, err := protocol.ReadInt(r)
	if err != nil {
		return result, nil
	}

	// Parse each result entry (same format as parseQueryResult).
	for i := int32(0); i < resultCount; i++ {
		// Statement type.
		if _, err := protocol.ReadByte(r); err != nil {
			return result, nil
		}
		// Rows affected.
		affected, err := protocol.ReadInt(r)
		if err != nil {
			return result, nil
		}
		if i == 0 {
			result.rowsAffected = int64(affected)
		}
		// OID (8 bytes) — read and discard.
		// Note: CCI does NOT provide last insert ID in the EXECUTE response.
		// Use GetLastInsertID() (FC 40) or SELECT LAST_INSERT_ID() instead.
		if _, err := protocol.ReadBytes(r, 8); err != nil {
			return result, nil
		}
		// Cache time.
		if _, err := protocol.ReadInt(r); err != nil {
			return result, nil
		}
		if _, err := protocol.ReadInt(r); err != nil {
			return result, nil
		}
	}

	// include_column_info flag (PROTOCOL_V2+).
	if conn.broker.ProtocolVersion >= protocol.ProtocolV2 {
		includeColInfo, err := protocol.ReadByte(r)
		if err != nil {
			return result, nil
		}
		if includeColInfo == 1 {
			protocol.ReadInt(r)  // result_cache_lifetime
			protocol.ReadByte(r) // stmt_type
			protocol.ReadInt(r)  // num_markers
			protocol.ReadByte(r) // updatable_flag
			colCount, _ := protocol.ReadInt(r)
			if colCount > 0 {
				for j := int32(0); j < colCount; j++ {
					parseColumnMeta(r, conn.broker.ProtocolVersion)
				}
			}
		}
	}

	// Shard ID (PROTOCOL_V5+).
	if conn.broker.ProtocolVersion >= protocol.ProtocolV5 {
		protocol.ReadInt(r)
	}

	return result, nil
}

func parseQueryResult(stmt *cubridStmt, frame *protocol.ResponseFrame) (*cubridRows, error) {
	totalTuples := frame.ResponseCode
	r := bytes.NewReader(frame.Body)

	// Cache reusable.
	if _, err := protocol.ReadByte(r); err != nil {
		return nil, fmt.Errorf("cubrid: read cache reusable: %w", err)
	}

	// Result count.
	resultCount, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read result count: %w", err)
	}

	for i := int32(0); i < resultCount; i++ {
		// Statement type.
		if _, err := protocol.ReadByte(r); err != nil {
			return nil, fmt.Errorf("cubrid: read stmt type: %w", err)
		}
		// Result count.
		if _, err := protocol.ReadInt(r); err != nil {
			return nil, fmt.Errorf("cubrid: read affected count: %w", err)
		}
		// OID.
		if _, err := protocol.ReadBytes(r, 8); err != nil {
			return nil, fmt.Errorf("cubrid: read OID: %w", err)
		}
		// Cache time.
		if _, err := protocol.ReadInt(r); err != nil {
			return nil, fmt.Errorf("cubrid: read cache time sec: %w", err)
		}
		if _, err := protocol.ReadInt(r); err != nil {
			return nil, fmt.Errorf("cubrid: read cache time usec: %w", err)
		}
	}

	// include_column_info flag (PROTOCOL_V2+).
	// When set to 1 (multi-query), server includes full column metadata.
	// We must consume it to keep the cursor aligned.
	if stmt.conn.broker.ProtocolVersion >= protocol.ProtocolV2 {
		includeColInfo, err := protocol.ReadByte(r)
		if err != nil {
			return nil, fmt.Errorf("cubrid: read include_column_info: %w", err)
		}
		if includeColInfo == 1 {
			// result_cache_lifetime(4) + stmt_type(1) + num_markers(4)
			protocol.ReadInt(r)  // result_cache_lifetime
			protocol.ReadByte(r) // stmt_type
			protocol.ReadInt(r)  // num_markers
			protocol.ReadByte(r) // updatable_flag
			colCount, _ := protocol.ReadInt(r)
			if colCount > 0 {
				// Skip column metadata (same format as prepare).
				for i := int32(0); i < colCount; i++ {
					parseColumnMeta(r, stmt.conn.broker.ProtocolVersion)
				}
			}
		}
	}

	// Shard ID (PROTOCOL_V5+).
	if stmt.conn.broker.ProtocolVersion >= protocol.ProtocolV5 {
		if _, err := protocol.ReadInt(r); err != nil {
			return nil, fmt.Errorf("cubrid: read shard id: %w", err)
		}
	}

	rows := &cubridRows{
		stmt:        stmt,
		columns:     stmt.columns,
		totalTuples: int(totalTuples),
		fetchPos:    1,
		fetchSize:   100,
	}

	// Parse inline fetch data if present.
	// The Rust implementation reads (and ignores) the fetch_code, then always
	// reads tuple_count. If tupleCount > 0, inline tuple data follows.
	if r.Len() >= 8 {
		// fetch_code is read but not used (consistent with Rust).
		if _, err := protocol.ReadInt(r); err == nil {
			tupleCount, err := protocol.ReadInt(r)
			if err == nil && tupleCount > 0 {
				rows.buffer, err = parseTuples(r, int(tupleCount), stmt.columns)
				if err != nil {
					return nil, fmt.Errorf("cubrid: parse inline tuples: %w", err)
				}
				rows.fetchPos += int(tupleCount)
				if int(tupleCount) >= rows.totalTuples {
					rows.done = true
				}
			}
		}
	}

	return rows, nil
}

// parseTuples reads tuple data from the reader.
func parseTuples(r io.Reader, count int, columns []ColumnMeta) ([][]interface{}, error) {
	tuples := make([][]interface{}, 0, count)

	for i := 0; i < count; i++ {
		// Row index.
		if _, err := protocol.ReadInt(r); err != nil {
			return tuples, err
		}
		// OID.
		if _, err := protocol.ReadBytes(r, 8); err != nil {
			return tuples, err
		}

		row := make([]interface{}, len(columns))
		for j := range columns {
			size, err := protocol.ReadInt(r)
			if err != nil {
				return tuples, err
			}

			if size <= 0 {
				row[j] = nil
				continue
			}

			data, err := protocol.ReadBytes(r, int(size))
			if err != nil {
				return tuples, err
			}

			col := columns[j]
			var val interface{}
			switch col.Type {
			case protocol.CubridTypeSet, protocol.CubridTypeMultiSet, protocol.CubridTypeSequence:
				val, err = decodeCollectionValue(col.Type, col.ElementType, data)
			default:
				val, err = decodeValue(col.Type, data)
			}
			if err != nil {
				return tuples, fmt.Errorf("column %d (%s): %w", j, col.Name, err)
			}
			row[j] = val
		}
		tuples = append(tuples, row)
	}

	return tuples, nil
}
