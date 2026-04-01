package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"

	"github.com/search5/cubrid-go/protocol"
)

// SchemaType represents a CUBRID SCHEMA_INFO request type.
type SchemaType int32

const (
	SchemaClass        SchemaType = 1  // List tables (classes).
	SchemaVClass       SchemaType = 2  // List views (virtual classes).
	SchemaAttribute    SchemaType = 4  // Column attributes.
	SchemaConstraint   SchemaType = 11 // Constraints.
	SchemaPrimaryKey   SchemaType = 16 // Primary key columns.
	SchemaImportedKeys SchemaType = 17 // Imported foreign keys (FK targets).
	SchemaExportedKeys SchemaType = 18 // Exported foreign keys (FK sources).
)

// SchemaPatternFlag controls how table/column name filters are matched.
type SchemaPatternFlag byte

const (
	SchemaFlagExact          SchemaPatternFlag = 0 // Exact match.
	SchemaFlagClassPattern   SchemaPatternFlag = 1 // Class name pattern (LIKE).
	SchemaFlagAttrPattern    SchemaPatternFlag = 2 // Attribute name pattern (LIKE).
	SchemaFlagBothPattern    SchemaPatternFlag = 3 // Both patterns.
)

// SchemaInfoResult holds the parsed SCHEMA_INFO response metadata.
type SchemaInfoResult struct {
	Handle   int32        // Query handle for FETCH.
	NumTuple int32        // Total rows available.
	Columns  []ColumnMeta // Schema result column metadata.
}

// SchemaInfo queries CUBRID system catalog metadata.
//
// The response is a result set that must be fetched row by row, similar to
// a SELECT query. Use the returned SchemaInfoResult.Handle with fetchMore
// or iterate via SchemaRows.
//
// Examples:
//
//	SchemaInfo(ctx, conn, SchemaClass, "", "", SchemaFlagExact)        // All tables
//	SchemaInfo(ctx, conn, SchemaAttribute, "my_table", "", SchemaFlagExact) // Columns of my_table
//	SchemaInfo(ctx, conn, SchemaPrimaryKey, "my_table", "", SchemaFlagExact) // PK of my_table
func SchemaInfo(ctx context.Context, conn *cubridConn, schemaType SchemaType, tableName, columnName string, flag SchemaPatternFlag) (*SchemaRows, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.closed {
		return nil, driver.ErrBadConn
	}

	var buf bytes.Buffer
	// Schema type.
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(schemaType))
	// Table name filter.
	protocol.WriteNullTermString(&buf, tableName)
	// Column name filter.
	protocol.WriteNullTermString(&buf, columnName)
	// Pattern match flag.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, byte(flag))

	frame, err := conn.sendRequestCtx(ctx, protocol.FuncCodeSchemaInfo, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if err := conn.checkError(frame); err != nil {
		return nil, err
	}

	sr, err := parseSchemaInfoResponse(conn, frame)
	if err != nil {
		return nil, err
	}

	// Eagerly fetch all rows while we still hold the lock and the
	// query handle is valid. Under KEEP_CONNECTION=AUTO the CAS may
	// close the socket after a request in autocommit mode, which
	// would invalidate the handle before a deferred FETCH.
	if err := sr.fetchAllLocked(); err != nil {
		return nil, err
	}

	return sr, nil
}

// parseSchemaInfoResponse parses the SCHEMA_INFO response.
// Format: response_code = query_handle, body = num_tuple(4) + column_count(4) + column_meta[]
// Schema column metadata is minimal: type + scale + precision + name only.
func parseSchemaInfoResponse(conn *cubridConn, frame *protocol.ResponseFrame) (*SchemaRows, error) {
	r := bytes.NewReader(frame.Body)

	numTuple, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read schema num_tuple: %w", err)
	}

	colCount, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read schema column_count: %w", err)
	}

	columns := make([]ColumnMeta, colCount)
	for i := int32(0); i < colCount; i++ {
		col, err := parseSchemaColumnMeta(r, conn.broker.ProtocolVersion)
		if err != nil {
			return nil, fmt.Errorf("cubrid: parse schema column %d: %w", i, err)
		}
		columns[i] = col
	}

	return &SchemaRows{
		conn:        conn,
		handle:      frame.ResponseCode,
		columns:     columns,
		totalTuples: int(numTuple),
		fetchPos:    1,
		fetchSize:   100,
	}, nil
}

// parseSchemaColumnMeta reads a single schema column metadata entry.
// Minimal format: type(1) [+ elem_type(1) if collection] + scale(2) + precision(4) + name(string).
// No realName, tableName, nullable, or key flags (unlike full PREPARE column metadata).
func parseSchemaColumnMeta(r io.Reader, pv protocol.ProtocolVersion) (ColumnMeta, error) {
	var col ColumnMeta

	typeByte, err := protocol.ReadByte(r)
	if err != nil {
		return col, err
	}

	if typeByte&protocol.CollectionMarker != 0 && pv >= protocol.ProtocolV7 {
		elemTypeByte, err := protocol.ReadByte(r)
		if err != nil {
			return col, err
		}
		elemType := protocol.CubridType(elemTypeByte)
		collCode := typeByte & protocol.CollectionFlagMask
		switch collCode {
		case protocol.CollectionSet:
			col.Type = protocol.CubridTypeSet
			col.ElementType = elemType
		case protocol.CollectionMultiSet:
			col.Type = protocol.CubridTypeMultiSet
			col.ElementType = elemType
		case protocol.CollectionSequence:
			col.Type = protocol.CubridTypeSequence
			col.ElementType = elemType
		default:
			col.Type = elemType
		}
	} else {
		col.Type = protocol.CubridType(typeByte)
	}

	col.Scale, err = protocol.ReadShort(r)
	if err != nil {
		return col, err
	}
	col.Precision, err = protocol.ReadInt(r)
	if err != nil {
		return col, err
	}
	col.Name, err = protocol.ReadNullTermString(r)
	if err != nil {
		return col, err
	}

	col.Nullable = true // Schema columns are always nullable by convention.
	return col, nil
}

// SchemaRows provides iteration over SCHEMA_INFO result rows.
type SchemaRows struct {
	conn        *cubridConn
	handle      int32
	columns     []ColumnMeta
	buffer      [][]interface{}
	bufferIdx   int
	totalTuples int
	fetchPos    int
	fetchSize   int
	done        bool
	closed      bool
}

// Columns returns the schema result column names.
func (r *SchemaRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, c := range r.columns {
		names[i] = c.Name
	}
	return names
}

// Next fetches the next row. Returns io.EOF when done.
func (r *SchemaRows) Next() ([]interface{}, error) {
	if r.closed {
		return nil, io.EOF
	}

	if r.bufferIdx >= len(r.buffer) {
		if r.done {
			return nil, io.EOF
		}
		if err := r.fetchMore(); err != nil {
			return nil, err
		}
		if len(r.buffer) == 0 {
			r.done = true
			return nil, io.EOF
		}
	}

	row := r.buffer[r.bufferIdx]
	r.bufferIdx++
	return row, nil
}

// Close releases the schema query handle.
func (r *SchemaRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.buffer = nil
	return nil
}

// fetchAllLocked fetches all remaining rows. Caller must hold conn.mu.
// CUBRID may report numTuple=0 in SCHEMA_INFO response even when rows exist,
// so we fetch until the server returns no-more-data or an empty batch.
func (r *SchemaRows) fetchAllLocked() error {
	for {
		if err := r.fetchOneLocked(); err != nil {
			return err
		}
		if r.done {
			break
		}
	}
	return nil
}

// fetchOneLocked performs a single FETCH request. Caller must hold conn.mu.
func (r *SchemaRows) fetchOneLocked() error {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, r.handle)
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(r.fetchPos))
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, int32(r.fetchSize))
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, 0x00)
	protocol.WriteInt(&buf, 4)
	protocol.WriteInt(&buf, 0)

	frame, err := r.conn.sendRequest(protocol.FuncCodeFetch, buf.Bytes())
	if err != nil {
		return err
	}

	if frame.ResponseCode < 0 {
		if frame.ResponseCode == ErrCodeNoMoreData {
			r.done = true
			return nil
		}
		return r.conn.checkError(frame)
	}

	reader := bytes.NewReader(frame.Body)
	tupleCount, err := protocol.ReadInt(reader)
	if err != nil || tupleCount <= 0 {
		r.done = true
		return nil
	}

	rows, err := parseTuples(reader, int(tupleCount), r.columns)
	if err != nil {
		return fmt.Errorf("cubrid: parse schema tuples: %w", err)
	}
	r.buffer = append(r.buffer, rows...)
	r.fetchPos += int(tupleCount)
	return nil
}

// fetchMore is for lazy fetching (not used when fetchAllLocked pre-loaded).
func (r *SchemaRows) fetchMore() error {
	// All data is pre-fetched by fetchAllLocked; this should not be called.
	r.done = true
	return nil
}

// --- Convenience helpers using SQL catalog queries ---
//
// These use SQL queries against CUBRID's system catalog tables (db_class,
// db_attribute, etc.) instead of the SCHEMA_INFO protocol, because
// SCHEMA_INFO responses cannot be FETCHed reliably under KEEP_CONNECTION=AUTO
// (the CAS closes the socket between SCHEMA_INFO and FETCH in autocommit mode).

// ListTables returns user table names via db_class catalog.
func ListTables(ctx context.Context, db interface{ QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) }) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT class_name FROM db_class WHERE is_system_class = 'NO' AND class_type = 'CLASS' ORDER BY class_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return tables, err
		}
		tables = append(tables, strings.TrimSpace(name))
	}
	return tables, rows.Err()
}

// ListViews returns view names via db_class catalog.
func ListViews(ctx context.Context, db interface{ QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) }) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT class_name FROM db_class WHERE is_system_class = 'NO' AND class_type = 'VCLASS' ORDER BY class_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return views, err
		}
		views = append(views, strings.TrimSpace(name))
	}
	return views, rows.Err()
}

// SchemaColumnInfo holds column metadata from db_attribute catalog.
type SchemaColumnInfo struct {
	Name         string
	DataType     string
	Precision    int32
	Scale        int32
	IsNullable   bool
	DefaultValue string
	IsPrimaryKey bool
}

// ListColumns returns column metadata for a table via db_attribute catalog.
func ListColumns(ctx context.Context, db interface{ QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) }, tableName string) ([]SchemaColumnInfo, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT attr_name, data_type, prec, scale, is_nullable, default_value "+
			"FROM db_attribute WHERE class_name = ? AND attr_type = 'INSTANCE' ORDER BY def_order",
		tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []SchemaColumnInfo
	for rows.Next() {
		var col SchemaColumnInfo
		var nullable string
		var defVal sql.NullString
		if err := rows.Scan(&col.Name, &col.DataType, &col.Precision, &col.Scale, &nullable, &defVal); err != nil {
			return columns, err
		}
		col.Name = strings.TrimSpace(col.Name)
		col.DataType = strings.TrimSpace(col.DataType)
		col.IsNullable = nullable == "YES"
		if defVal.Valid {
			col.DefaultValue = defVal.String
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// ListPrimaryKeys returns the primary key column names for a table.
func ListPrimaryKeys(ctx context.Context, db interface{ QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) }, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT k.key_attr_name FROM db_index i, db_index_key k "+
			"WHERE i.class_name = ? AND i.is_primary_key = 'YES' "+
			"AND k.class_name = i.class_name AND k.index_name = i.index_name "+
			"ORDER BY k.key_order",
		tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return keys, err
		}
		keys = append(keys, strings.TrimSpace(name))
	}
	return keys, rows.Err()
}

// ListConstraints returns constraint info for a table.
func ListConstraints(ctx context.Context, db interface{ QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) }, tableName string) ([]SchemaConstraintInfo, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT index_name, is_unique, is_primary_key, is_foreign_key "+
			"FROM db_index WHERE class_name = ? ORDER BY index_name",
		tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []SchemaConstraintInfo
	for rows.Next() {
		var c SchemaConstraintInfo
		var unique, pk, fk string
		if err := rows.Scan(&c.Name, &unique, &pk, &fk); err != nil {
			return constraints, err
		}
		c.Name = strings.TrimSpace(c.Name)
		c.IsUnique = unique == "YES"
		c.IsPrimaryKey = pk == "YES"
		c.IsForeignKey = fk == "YES"
		constraints = append(constraints, c)
	}
	return constraints, rows.Err()
}

// SchemaConstraintInfo holds constraint metadata.
type SchemaConstraintInfo struct {
	Name         string
	IsUnique     bool
	IsPrimaryKey bool
	IsForeignKey bool
}

// --- Table inheritance helpers ---

// InheritanceInfo holds super/sub class relationships for a table.
type InheritanceInfo struct {
	ClassName    string   // The table name.
	SuperClasses []string // Parent tables (direct supers).
	SubClasses   []string // Child tables (direct subs).
}

// ListSuperClasses returns the direct parent classes of the given table.
// CUBRID supports class inheritance via CREATE TABLE child UNDER parent.
func ListSuperClasses(ctx context.Context, db interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT super_class_name FROM db_direct_super_class WHERE class_name = ? ORDER BY super_class_name",
		tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var supers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return supers, err
		}
		supers = append(supers, strings.TrimSpace(name))
	}
	return supers, rows.Err()
}

// ListSubClasses returns the direct child classes of the given table.
func ListSubClasses(ctx context.Context, db interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT class_name FROM db_direct_super_class WHERE super_class_name = ? ORDER BY class_name",
		tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return subs, err
		}
		subs = append(subs, strings.TrimSpace(name))
	}
	return subs, rows.Err()
}

// GetInheritanceInfo returns the full inheritance info for a table,
// including both its parent and child classes.
func GetInheritanceInfo(ctx context.Context, db interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}, tableName string) (*InheritanceInfo, error) {
	info := &InheritanceInfo{ClassName: tableName}

	supers, err := ListSuperClasses(ctx, db, tableName)
	if err != nil {
		return nil, fmt.Errorf("cubrid: list super classes: %w", err)
	}
	info.SuperClasses = supers

	subs, err := ListSubClasses(ctx, db, tableName)
	if err != nil {
		return nil, fmt.Errorf("cubrid: list sub classes: %w", err)
	}
	info.SubClasses = subs

	return info, nil
}

// SchemaTypeSuperClass is the SCHEMA_INFO type for querying super class relationships.
const SchemaTypeSuperClass SchemaType = 3
