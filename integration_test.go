//go:build integration

package cubrid

import (
	"database/sql"
	"testing"
	"time"
)

const testDSN = "cubrid://dba:@localhost:33100/cubdb"

func TestIntegrationConnect(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	t.Log("connected and pinged successfully")
}

func TestIntegrationCreateTableAndCRUD(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Drop table if exists.
	db.Exec("DROP TABLE IF EXISTS go_test")

	// CREATE TABLE.
	_, err = db.Exec("CREATE TABLE go_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), score DOUBLE, created DATETIME)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE go_test")

	// INSERT.
	now := time.Now().Truncate(time.Millisecond)
	result, err := db.Exec("INSERT INTO go_test (name, score, created) VALUES (?, ?, ?)",
		"Alice", 95.5, now)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if affected != 1 {
		t.Errorf("RowsAffected = %d, want 1", affected)
	}

	// INSERT more rows.
	_, err = db.Exec("INSERT INTO go_test (name, score, created) VALUES (?, ?, ?)",
		"Bob", 87.3, now)
	if err != nil {
		t.Fatalf("INSERT Bob: %v", err)
	}

	// SELECT.
	rows, err := db.Query("SELECT id, name, score FROM go_test ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type row struct {
		id    int32
		name  string
		score float64
	}
	var results []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name, &r.score); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("got %d rows, want 2", len(results))
	}
	if results[0].name != "Alice" || results[1].name != "Bob" {
		t.Errorf("names = %q, %q", results[0].name, results[1].name)
	}

	// UPDATE.
	result, err = db.Exec("UPDATE go_test SET score = ? WHERE name = ?", 99.9, "Alice")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	affected, _ = result.RowsAffected()
	if affected != 1 {
		t.Errorf("UPDATE affected = %d, want 1", affected)
	}

	// DELETE.
	result, err = db.Exec("DELETE FROM go_test WHERE name = ?", "Bob")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	affected, _ = result.RowsAffected()
	if affected != 1 {
		t.Errorf("DELETE affected = %d, want 1", affected)
	}

	// Verify final state.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM go_test").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if count != 1 {
		t.Errorf("final count = %d, want 1", count)
	}
}

func TestIntegrationTransaction(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_tx")
	_, err = db.Exec("CREATE TABLE go_test_tx (id INT, val VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_tx")

	// Test commit.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec("INSERT INTO go_test_tx VALUES (1, 'committed')")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var val string
	err = db.QueryRow("SELECT val FROM go_test_tx WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query after commit: %v", err)
	}
	if val != "committed" {
		t.Errorf("val = %q, want %q", val, "committed")
	}

	// Test rollback.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec("INSERT INTO go_test_tx VALUES (2, 'rolled_back')")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM go_test_tx WHERE id = 2").Scan(&count)
	if err != nil {
		t.Fatalf("query after rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("count after rollback = %d, want 0", count)
	}
}

func TestIntegrationNullValues(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_null")
	_, err = db.Exec("CREATE TABLE go_test_null (id INT, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_null")

	// Insert with NULL.
	_, err = db.Exec("INSERT INTO go_test_null VALUES (1, NULL)")
	if err != nil {
		t.Fatal(err)
	}

	var id int32
	var name sql.NullString
	err = db.QueryRow("SELECT id, name FROM go_test_null WHERE id = 1").Scan(&id, &name)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if name.Valid {
		t.Errorf("expected NULL name, got %q", name.String)
	}
}

func TestIntegrationDataTypes(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_types")
	_, err = db.Exec(`CREATE TABLE go_test_types (
		col_short SHORT,
		col_int INT,
		col_bigint BIGINT,
		col_float FLOAT,
		col_double DOUBLE,
		col_numeric NUMERIC(10,2),
		col_char CHAR(10),
		col_varchar VARCHAR(100),
		col_date DATE,
		col_time TIME,
		col_datetime DATETIME,
		col_timestamp TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE go_test_types")

	// Insert with typed data.
	_, err = db.Exec(`INSERT INTO go_test_types VALUES (
		7, 42, 9999999999, 3.14, 2.718281828,
		'12345.67', 'hello', 'world',
		DATE'2024-03-15', TIME'10:30:45',
		DATETIME'2024-03-15 10:30:45.500',
		TIMESTAMP'2024-03-15 10:30:45'
	)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	row := db.QueryRow("SELECT * FROM go_test_types")

	var (
		colShort     int16
		colInt       int32
		colBigint    int64
		colFloat     float32
		colDouble    float64
		colNumeric   string
		colChar      string
		colVarchar   string
		colDate      time.Time
		colTime      time.Time
		colDatetime  time.Time
		colTimestamp time.Time
	)

	err = row.Scan(&colShort, &colInt, &colBigint, &colFloat, &colDouble,
		&colNumeric, &colChar, &colVarchar,
		&colDate, &colTime, &colDatetime, &colTimestamp)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if colShort != 7 {
		t.Errorf("SHORT = %d, want 7", colShort)
	}
	if colInt != 42 {
		t.Errorf("INT = %d, want 42", colInt)
	}
	if colBigint != 9999999999 {
		t.Errorf("BIGINT = %d, want 9999999999", colBigint)
	}
	if colFloat < 3.13 || colFloat > 3.15 {
		t.Errorf("FLOAT = %f, want ~3.14", colFloat)
	}
	if colDouble < 2.71 || colDouble > 2.72 {
		t.Errorf("DOUBLE = %f, want ~2.718", colDouble)
	}
	if colVarchar != "world" {
		t.Errorf("VARCHAR = %q, want %q", colVarchar, "world")
	}
	if colDate.Year() != 2024 || colDate.Month() != 3 || colDate.Day() != 15 {
		t.Errorf("DATE = %v", colDate)
	}

	t.Logf("All types verified: short=%d int=%d bigint=%d float=%f double=%f numeric=%s char=%q varchar=%q date=%v time=%v datetime=%v timestamp=%v",
		colShort, colInt, colBigint, colFloat, colDouble, colNumeric, colChar, colVarchar, colDate, colTime, colDatetime, colTimestamp)
}

func TestIntegrationColumnMetadata(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_meta")
	_, err = db.Exec("CREATE TABLE go_test_meta (id INT NOT NULL, name VARCHAR(200), score NUMERIC(10,2))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_meta")

	_, err = db.Exec("INSERT INTO go_test_meta VALUES (1, 'test', '99.99')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT id, name, score FROM go_test_meta")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	if len(colTypes) != 3 {
		t.Fatalf("got %d column types, want 3", len(colTypes))
	}

	// id: INT, NOT NULL
	if colTypes[0].DatabaseTypeName() != "INT" {
		t.Errorf("col 0 type = %q, want INT", colTypes[0].DatabaseTypeName())
	}

	// name: VARCHAR(200), nullable
	if colTypes[1].DatabaseTypeName() != "VARCHAR" {
		t.Errorf("col 1 type = %q, want VARCHAR", colTypes[1].DatabaseTypeName())
	}
	if length, ok := colTypes[1].Length(); ok {
		if length != 200 {
			t.Errorf("col 1 length = %d, want 200", length)
		}
	}

	// score: NUMERIC(10,2)
	if colTypes[2].DatabaseTypeName() != "NUMERIC" {
		t.Errorf("col 2 type = %q, want NUMERIC", colTypes[2].DatabaseTypeName())
	}
	if prec, scale, ok := colTypes[2].DecimalSize(); ok {
		if prec != 10 || scale != 2 {
			t.Errorf("col 2 decimal = (%d, %d), want (10, 2)", prec, scale)
		}
	}
}

func TestIntegrationMultipleConnections(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)

	for i := 0; i < 5; i++ {
		if err := db.Ping(); err != nil {
			t.Fatalf("ping %d: %v", i, err)
		}
	}
}
