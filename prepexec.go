package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/search5/cubrid-go/protocol"
)

// PrepareAndExec combines PREPARE and EXECUTE into a single network round-trip.
// This is more efficient for one-shot queries that don't need to be re-executed.
// For queries, use PrepareAndQuery instead.
func PrepareAndExec(ctx context.Context, conn *sql.Conn, query string, args ...interface{}) (sql.Result, error) {
	var result sql.Result
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: PrepareAndExec requires a cubrid connection")
		}
		r, err := c.prepareAndExecute(ctx, query, args, false)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	return result, err
}

// PrepareAndQuery combines PREPARE and EXECUTE into a single network round-trip
// for SELECT queries. Returns rows that must be closed by the caller.
func PrepareAndQuery(ctx context.Context, conn *sql.Conn, query string, args ...interface{}) (*PrepareAndExecRows, error) {
	var rows *PrepareAndExecRows
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: PrepareAndQuery requires a cubrid connection")
		}
		r, err := c.prepareAndQuery(ctx, query, args)
		if err != nil {
			return err
		}
		rows = r
		return nil
	})
	return rows, err
}

// PrepareAndExecRows wraps cubridRows for public access from PrepareAndQuery.
type PrepareAndExecRows struct {
	inner *cubridRows
}

// Next advances to the next row. Returns false when done.
func (r *PrepareAndExecRows) Next() bool {
	return r.inner.hasNext()
}

// Scan copies the current row values into dest.
func (r *PrepareAndExecRows) Scan(dest ...interface{}) error {
	return r.inner.scanRow(dest...)
}

// Columns returns the column names.
func (r *PrepareAndExecRows) Columns() []string {
	return r.inner.Columns()
}

// Close releases the result set.
func (r *PrepareAndExecRows) Close() error {
	return r.inner.Close()
}

// prepareAndExecute sends a PREPARE_AND_EXECUTE request for non-query statements.
func (c *cubridConn) prepareAndExecute(ctx context.Context, query string, args []interface{}, isQuery bool) (*cubridResult, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	frame, err := c.sendPrepareAndExec(ctx, query, args, isQuery)
	if err != nil {
		return nil, err
	}
	if err := c.checkError(frame); err != nil {
		return nil, err
	}

	return parsePrepareAndExecResult(frame)
}

// parsePrepareAndExecResult parses the combined prepare+execute response.
// The response_code is the query handle (NOT affected rows).
// Body layout: prepare_body(18 bytes for DML) + execute_body.
// Execute body: cache_reusable(1) + result_count(4) + [stmt_type(1) + affected(4) + oid(8) + cache(8)] * N
func parsePrepareAndExecResult(frame *protocol.ResponseFrame) (*cubridResult, error) {
	result := &cubridResult{}
	body := frame.Body
	execOffset := 18 // prepare body size for DML (no columns)
	if len(body) > execOffset+1+4+1+4 {
		resultCount := int32(body[execOffset+1])<<24 | int32(body[execOffset+2])<<16 |
			int32(body[execOffset+3])<<8 | int32(body[execOffset+4])
		if resultCount > 0 {
			affected := int32(body[execOffset+6])<<24 | int32(body[execOffset+7])<<16 |
				int32(body[execOffset+8])<<8 | int32(body[execOffset+9])
			result.rowsAffected = int64(affected)
		}
	}
	return result, nil
}

// prepareAndQuery sends a PREPARE_AND_EXECUTE request for SELECT queries.
//
// FC 41 response layout:
//
//	response_code = query handle
//	body = prepare_body + execute_body
//
// Prepare body:
//
//	cache_lifetime(4) + stmt_type(1) + bind_count(4) + updatable(1) + col_count(4) + column_meta[]*
//
// Execute body (same as standalone EXECUTE response):
//
//	cache_reusable(1) + result_count(4) + result_info[]* + include_column_info(1)?
//	+ shard_id(4)? + fetch_code(4)? + tuple_count(4)? + tuples[]*
func (c *cubridConn) prepareAndQuery(ctx context.Context, query string, args []interface{}) (*PrepareAndExecRows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	frame, err := c.sendPrepareAndExec(ctx, query, args, true)
	if err != nil {
		return nil, err
	}
	if err := c.checkError(frame); err != nil {
		return nil, err
	}

	stmt := &cubridStmt{
		conn:   c,
		handle: frame.ResponseCode,
	}

	r := bytes.NewReader(frame.Body)

	// --- Phase 1: Parse prepare body ---
	if _, err := protocol.ReadInt(r); err != nil { // cache_lifetime
		return nil, fmt.Errorf("cubrid: pae read cache_lifetime: %w", err)
	}
	stmtTypeByte, err := protocol.ReadByte(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: pae read stmt_type: %w", err)
	}
	stmt.stmtType = protocol.StmtType(stmtTypeByte)

	bindCount, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: pae read bind_count: %w", err)
	}
	stmt.bindCount = int(bindCount)

	if _, err := protocol.ReadByte(r); err != nil { // updatable
		return nil, fmt.Errorf("cubrid: pae read updatable: %w", err)
	}

	colCount, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: pae read col_count: %w", err)
	}

	cols := make([]ColumnMeta, colCount)
	for j := int32(0); j < colCount; j++ {
		col, err := parseColumnMeta(r, c.broker.ProtocolVersion)
		if err != nil {
			return nil, fmt.Errorf("cubrid: pae parse column %d: %w", j, err)
		}
		cols[j] = col
	}
	stmt.columns = cols

	// Prepare body trailing field (4 bytes, present in both DML and SELECT).
	// This is the same field that makes DML prepare body 18 bytes (14 header + 4 trailing).
	protocol.ReadInt(r)

	// --- Phase 2: Parse execute body ---
	// At this point, r.Len() bytes remain for the execute body.
	// cache_reusable
	protocol.ReadByte(r)
	// result_count
	resultCount, _ := protocol.ReadInt(r)
	totalTuples := int32(0)
	for i := int32(0); i < resultCount; i++ {
		protocol.ReadByte(r)      // stmt type
		affected, _ := protocol.ReadInt(r) // affected/tuple count
		if i == 0 {
			totalTuples = affected
		}
		protocol.ReadBytes(r, 8)  // OID
		protocol.ReadInt(r)       // cache time sec
		protocol.ReadInt(r)       // cache time usec
	}

	// include_column_info (PROTOCOL_V2+) — skip if present
	if c.broker.ProtocolVersion >= protocol.ProtocolV2 && r.Len() >= 1 {
		includeColInfo, _ := protocol.ReadByte(r)
		if includeColInfo == 1 {
			protocol.ReadInt(r)  // result_cache_lifetime
			protocol.ReadByte(r) // stmt_type
			protocol.ReadInt(r)  // num_markers
			protocol.ReadByte(r) // updatable_flag
			execColCount, _ := protocol.ReadInt(r)
			for j := int32(0); j < execColCount; j++ {
				parseColumnMeta(r, c.broker.ProtocolVersion)
			}
		}
	}

	// Shard ID (PROTOCOL_V5+)
	if c.broker.ProtocolVersion >= protocol.ProtocolV5 && r.Len() >= 4 {
		protocol.ReadInt(r)
	}

	rows := &cubridRows{
		stmt:        stmt,
		columns:     stmt.columns,
		totalTuples: int(totalTuples),
		fetchPos:    1,
		fetchSize:   100,
	}

	// Parse inline fetch data if present.
	if r.Len() >= 8 {
		if _, err := protocol.ReadInt(r); err == nil { // fetch_code
			tupleCount, err := protocol.ReadInt(r)
			if err == nil && tupleCount > 0 && len(stmt.columns) > 0 {
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

	return &PrepareAndExecRows{inner: rows}, nil
}

// sendPrepareAndExec builds and sends the PREPARE_AND_EXECUTE request.
//
// CAS server wire format (from cas_function.c fn_prepare_and_execute):
//   argv[0]: prepare_argc_count (int) — number of prepare-phase arguments
//   argv[1..prepare_argc_count]: prepare args (sql, flag, auto_commit)
//   argv[prepare_argc_count+1..]: execute args (10 fixed + bind params)
func (c *cubridConn) sendPrepareAndExec(ctx context.Context, query string, args []interface{}, isQuery bool) (*protocol.ResponseFrame, error) {
	var buf bytes.Buffer

	// argv[0]: prepare_argc_count = 3 (sql + flag + auto_commit).
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 3)

	// Prepare phase (3 arguments).
	// argv[1]: SQL string.
	protocol.WriteNullTermString(&buf, query)

	// argv[2]: prepare flag: 0x00 (normal).
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x00)

	// argv[3]: auto-commit flag (for prepare phase).
	protocol.WriteInt(&buf, 1)
	if c.autoCommit && !c.inTx {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	// Execute phase (10 fixed arguments, then bind params).
	// argv[4]: execute flag: 0x00 (normal).
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

	// Fetch flag.
	protocol.WriteInt(&buf, 1)
	if isQuery {
		protocol.WriteByte(&buf, 0x01)
	} else {
		protocol.WriteByte(&buf, 0x00)
	}

	// Auto-commit (for execute phase).
	protocol.WriteInt(&buf, 1)
	if c.autoCommit && !c.inTx {
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

	// Query timeout.
	protocol.WriteInt(&buf, 4)
	if c.dsn.QueryTimeout > 0 {
		protocol.WriteInt(&buf, int32(c.dsn.QueryTimeout.Milliseconds()))
	} else {
		protocol.WriteInt(&buf, 0)
	}

	// Bind parameters.
	for i, arg := range args {
		data, cubType, err := encodeBindValue(arg)
		if err != nil {
			return nil, fmt.Errorf("cubrid: encode param %d: %w", i+1, err)
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

	return c.sendRequestCtx(ctx, protocol.FuncCodePrepareAndExec, buf.Bytes())
}
