//go:build integration

package cubrid

import (
	"database/sql"
	"strings"
	"testing"
)

// Test sequential Exec then Query on the same connection to verify
// that parseExecResult fully consumes the response body.
func TestIntegrationExecThenQuery(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // Force single connection reuse.

	db.Exec("DROP TABLE IF EXISTS go_test_eq")
	_, err = db.Exec("CREATE TABLE go_test_eq (id INT, val VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_eq")

	// Multiple Execs followed by a Query on the same connection.
	for i := 0; i < 5; i++ {
		_, err = db.Exec("INSERT INTO go_test_eq VALUES (?, ?)", i, "row")
		if err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM go_test_eq").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}

	// Now do a full SELECT.
	rows, err := db.Query("SELECT id, val FROM go_test_eq ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var id int32
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("Scan row %d: %v", n, err)
		}
		if int(id) != n {
			t.Errorf("row %d: id=%d", n, id)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if n != 5 {
		t.Errorf("got %d rows, want 5", n)
	}
}

// Test bind parameters with various Go types.
func TestIntegrationBindParamTypes(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_bind")
	_, err = db.Exec(`CREATE TABLE go_test_bind (
		col_int INT,
		col_bigint BIGINT,
		col_float DOUBLE,
		col_str VARCHAR(200),
		col_bool SHORT
	)`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_bind")

	// Insert with Go native types.
	_, err = db.Exec("INSERT INTO go_test_bind VALUES (?, ?, ?, ?, ?)",
		int32(42), int64(9876543210), float64(3.14159), "hello world", true)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var colInt int32
	var colBigint int64
	var colFloat float64
	var colStr string
	var colBool int16
	err = db.QueryRow("SELECT * FROM go_test_bind").Scan(
		&colInt, &colBigint, &colFloat, &colStr, &colBool)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if colInt != 42 {
		t.Errorf("int = %d", colInt)
	}
	if colBigint != 9876543210 {
		t.Errorf("bigint = %d", colBigint)
	}
	if colFloat < 3.14 || colFloat > 3.15 {
		t.Errorf("float = %f", colFloat)
	}
	if colStr != "hello world" {
		t.Errorf("str = %q", colStr)
	}
	if colBool != 1 {
		t.Errorf("bool = %d, want 1", colBool)
	}
}

// Test large result set that requires multiple FETCH requests.
func TestIntegrationLargeResultSet(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_large")
	_, err = db.Exec("CREATE TABLE go_test_large (id INT, val VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_large")

	// Insert 200 rows (exceeds default fetchSize=100).
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		_, err = tx.Exec("INSERT INTO go_test_large VALUES (?, ?)", i, "data")
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows, err := db.Query("SELECT id, val FROM go_test_large ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int32
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("Scan row %d: %v", count, err)
		}
		if int(id) != count {
			t.Errorf("row %d: id=%d", count, id)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 200 {
		t.Errorf("got %d rows, want 200", count)
	}
}

// Test empty result set.
func TestIntegrationEmptyResultSet(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_empty")
	_, err = db.Exec("CREATE TABLE go_test_empty (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_empty")

	rows, err := db.Query("SELECT * FROM go_test_empty")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Error("expected no rows")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}

// Test multiple transactions in sequence.
func TestIntegrationSequentialTransactions(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	db.Exec("DROP TABLE IF EXISTS go_test_stx")
	_, err = db.Exec("CREATE TABLE go_test_stx (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_stx")

	for i := 0; i < 5; i++ {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin %d: %v", i, err)
		}
		_, err = tx.Exec("INSERT INTO go_test_stx VALUES (?)", i)
		if err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
		if i%2 == 0 {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM go_test_stx").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	// Committed: i=0,2,4 → 3 rows
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

// Test long strings and special characters.
func TestIntegrationSpecialStrings(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_str")
	_, err = db.Exec("CREATE TABLE go_test_str (id INT, val VARCHAR(4000))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_str")

	tests := []struct {
		name string
		val  string
	}{
		{"empty", ""},
		{"unicode", "한글 테스트 🎉"},
		{"quotes", `He said "hello" and 'goodbye'`},
		{"long", strings.Repeat("abcdefghij", 100)}, // 1000 chars
		{"newlines", "line1\nline2\nline3"},
		{"backslash", `path\to\file`},
	}

	for i, tt := range tests {
		_, err = db.Exec("INSERT INTO go_test_str VALUES (?, ?)", i, tt.val)
		if err != nil {
			t.Fatalf("INSERT %q: %v", tt.name, err)
		}
	}

	for i, tt := range tests {
		var val string
		err = db.QueryRow("SELECT val FROM go_test_str WHERE id = ?", i).Scan(&val)
		if err != nil {
			t.Fatalf("SELECT %q: %v", tt.name, err)
		}
		if val != tt.val {
			t.Errorf("%q: got len=%d, want len=%d", tt.name, len(val), len(tt.val))
		}
	}
}

// Test that QueryRow with no results returns sql.ErrNoRows.
func TestIntegrationQueryRowNoResult(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_norow")
	_, err = db.Exec("CREATE TABLE go_test_norow (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_norow")

	var id int
	err = db.QueryRow("SELECT id FROM go_test_norow WHERE id = 999").Scan(&id)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows, got %v", err)
	}
}

// Test UPDATE/DELETE returning RowsAffected=0 for no matching rows.
func TestIntegrationZeroAffected(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_za")
	_, err = db.Exec("CREATE TABLE go_test_za (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_za")

	result, err := db.Exec("DELETE FROM go_test_za WHERE id = 999")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 0 {
		t.Errorf("affected = %d, want 0", affected)
	}
}

// Test rapid Prepare/Close cycles for statement handle leaks.
func TestIntegrationPrepareClose(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	db.Exec("DROP TABLE IF EXISTS go_test_pc")
	db.Exec("CREATE TABLE go_test_pc (id INT)")
	defer db.Exec("DROP TABLE go_test_pc")

	for i := 0; i < 50; i++ {
		stmt, err := db.Prepare("SELECT COUNT(*) FROM go_test_pc")
		if err != nil {
			t.Fatalf("Prepare %d: %v", i, err)
		}
		var count int
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			t.Fatalf("QueryRow %d: %v", i, err)
		}
		stmt.Close()
	}
}
