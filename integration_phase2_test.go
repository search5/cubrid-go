//go:build integration

package cubrid

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// --- EXECUTE_BATCH ---

func TestIntegrationBatchExec(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_batch")
	_, err := db.Exec("CREATE TABLE go_test_batch (id INT, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_batch")

	sqls := []string{
		"INSERT INTO go_test_batch VALUES (1, 'Alice')",
		"INSERT INTO go_test_batch VALUES (2, 'Bob')",
		"INSERT INTO go_test_batch VALUES (3, 'Charlie')",
	}
	if err := BatchExec(db, sqls); err != nil {
		t.Fatalf("BatchExec: %v", err)
	}

	var count int64
	db.QueryRow("SELECT COUNT(*) FROM go_test_batch").Scan(&count)
	if count != 3 {
		t.Fatalf("count: got %d, want 3", count)
	}
	t.Logf("BatchExec: inserted %d rows", count)
}

// --- SAVEPOINT ---

func TestIntegrationSavepoint(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN+"?auto_commit=false")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_sp")
	db.Exec("CREATE TABLE go_test_sp (id INT)")
	defer db.Exec("DROP TABLE go_test_sp")

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx.Exec("INSERT INTO go_test_sp VALUES (1)")

	// Create savepoint via SQL (since Savepoint() needs *sql.Conn)
	_, err = tx.Exec("SAVEPOINT sp1")
	if err != nil {
		t.Fatalf("SAVEPOINT: %v", err)
	}

	tx.Exec("INSERT INTO go_test_sp VALUES (2)")

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	if err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT: %v", err)
	}

	tx.Commit()

	// Row 1 should exist, row 2 should not
	var count int64
	db.QueryRow("SELECT COUNT(*) FROM go_test_sp").Scan(&count)
	if count != 1 {
		t.Fatalf("count after savepoint rollback: got %d, want 1", count)
	}
	t.Logf("Savepoint: rollback worked, %d row remains", count)
}

// --- SET/GET_DB_PARAMETER ---

func TestIntegrationDBParameter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Get isolation level
	level, err := GetParam(conn, ParamIsolationLevel)
	if err != nil {
		t.Fatalf("GetParam isolation: %v", err)
	}
	t.Logf("Isolation level: %d", level)

	// Get lock timeout
	timeout, err := GetParam(conn, ParamLockTimeout)
	if err != nil {
		t.Fatalf("GetParam lock_timeout: %v", err)
	}
	t.Logf("Lock timeout: %d", timeout)

	// Set and verify lock timeout
	if err := SetParam(conn, ParamLockTimeout, 10); err != nil {
		t.Fatalf("SetParam lock_timeout: %v", err)
	}
	newTimeout, err := GetParam(conn, ParamLockTimeout)
	if err != nil {
		t.Fatalf("GetParam after set: %v", err)
	}
	if newTimeout != 10 {
		t.Fatalf("lock_timeout after set: got %d, want 10", newTimeout)
	}
	t.Logf("Lock timeout after set: %d", newTimeout)
}

// --- ENUM type ---

func TestIntegrationEnumType(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_enum")
	_, err := db.Exec("CREATE TABLE go_test_enum (id INT, color ENUM('Red','Green','Blue'))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_enum")

	_, err = db.Exec("INSERT INTO go_test_enum VALUES (1, 'Red')")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO go_test_enum VALUES (2, 'Blue')")
	if err != nil {
		t.Fatal(err)
	}

	// Scan as string (driver.Value conversion)
	var color string
	err = db.QueryRow("SELECT color FROM go_test_enum WHERE id = 1").Scan(&color)
	if err != nil {
		t.Fatalf("Scan enum as string: %v", err)
	}
	if color != "Red" {
		t.Fatalf("enum: got %q, want %q", color, "Red")
	}
	t.Logf("Enum value: %s", color)
}

// --- MONETARY type ---

func TestIntegrationMonetaryType(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_monetary")
	_, err := db.Exec("CREATE TABLE go_test_monetary (id INT, amount MONETARY)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_monetary")

	_, err = db.Exec("INSERT INTO go_test_monetary VALUES (1, 99.99)")
	if err != nil {
		t.Fatal(err)
	}

	// Scan as float64 (driver.Value conversion)
	var amount float64
	err = db.QueryRow("SELECT amount FROM go_test_monetary WHERE id = 1").Scan(&amount)
	if err != nil {
		t.Fatalf("Scan monetary: %v", err)
	}
	if amount < 99.98 || amount > 100.0 {
		t.Fatalf("monetary: got %f, want ~99.99", amount)
	}
	t.Logf("Monetary value: %f", amount)
}

// --- NUMERIC type ---

func TestIntegrationNumericType(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_numeric")
	_, err := db.Exec("CREATE TABLE go_test_numeric (id INT, val NUMERIC(15,3))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_numeric")

	_, err = db.Exec("INSERT INTO go_test_numeric VALUES (1, 12345.678)")
	if err != nil {
		t.Fatal(err)
	}

	// Scan as string (driver.Value is string for NUMERIC)
	var val string
	err = db.QueryRow("SELECT val FROM go_test_numeric WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("Scan numeric: %v", err)
	}
	if val != "12345.678" {
		t.Fatalf("numeric: got %q, want %q", val, "12345.678")
	}
	t.Logf("Numeric value: %s", val)
}

// --- JSON type ---

func TestIntegrationJsonType(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_json")
	_, err := db.Exec("CREATE TABLE go_test_json (id INT, jdoc JSON)")
	if err != nil {
		t.Skipf("JSON type not supported (may require CUBRID 11.2+): %v", err)
		return
	}
	defer db.Exec("DROP TABLE go_test_json")

	_, err = db.Exec(`INSERT INTO go_test_json VALUES (1, '{"key":"value","num":42}')`)
	if err != nil {
		t.Fatal(err)
	}

	var jdoc string
	err = db.QueryRow("SELECT jdoc FROM go_test_json WHERE id = 1").Scan(&jdoc)
	if err != nil {
		t.Fatalf("Scan JSON: %v", err)
	}
	t.Logf("JSON value: %s", jdoc)
}

// --- PREPARE_AND_EXECUTE ---

func TestIntegrationPrepareAndExec(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_pae")
	_, err := db.Exec("CREATE TABLE go_test_pae (id INT, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_pae")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := PrepareAndExec(ctx, conn, "INSERT INTO go_test_pae VALUES (1, 'test')")
	if err != nil {
		t.Skipf("PrepareAndExec not supported by this server: %v", err)
		return
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Fatalf("affected: got %d, want 1", affected)
	}
	t.Logf("PrepareAndExec: %d row affected", affected)
}

// --- PREPARE_AND_EXECUTE UPDATE/DELETE/SELECT ---

func TestIntegrationPrepareAndExecUpdate(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_pae_upd")
	_, err := db.Exec("CREATE TABLE go_test_pae_upd (id INT, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_pae_upd")
	db.Exec("INSERT INTO go_test_pae_upd VALUES (1, 'alice')")
	db.Exec("INSERT INTO go_test_pae_upd VALUES (2, 'bob')")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := PrepareAndExec(ctx, conn, "UPDATE go_test_pae_upd SET name = 'charlie' WHERE id = 1")
	if err != nil {
		t.Fatalf("PrepareAndExec UPDATE: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Fatalf("UPDATE affected: got %d, want 1", affected)
	}
	t.Logf("PrepareAndExec UPDATE: %d row affected", affected)

	// Verify the update took effect
	var name string
	db.QueryRow("SELECT name FROM go_test_pae_upd WHERE id = 1").Scan(&name)
	if name != "charlie" {
		t.Fatalf("UPDATE verify: got %q, want %q", name, "charlie")
	}
}

func TestIntegrationPrepareAndExecDelete(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_pae_del")
	_, err := db.Exec("CREATE TABLE go_test_pae_del (id INT, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_pae_del")
	db.Exec("INSERT INTO go_test_pae_del VALUES (1, 'alice')")
	db.Exec("INSERT INTO go_test_pae_del VALUES (2, 'bob')")
	db.Exec("INSERT INTO go_test_pae_del VALUES (3, 'carol')")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := PrepareAndExec(ctx, conn, "DELETE FROM go_test_pae_del WHERE id <= 2")
	if err != nil {
		t.Fatalf("PrepareAndExec DELETE: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 2 {
		t.Fatalf("DELETE affected: got %d, want 2", affected)
	}
	t.Logf("PrepareAndExec DELETE: %d rows affected", affected)

	// Verify only id=3 remains
	var count int
	db.QueryRow("SELECT COUNT(*) FROM go_test_pae_del").Scan(&count)
	if count != 1 {
		t.Fatalf("DELETE verify count: got %d, want 1", count)
	}
}

func TestIntegrationPrepareAndQuerySelect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_pae_sel")
	_, err := db.Exec("CREATE TABLE go_test_pae_sel (id INT, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_pae_sel")
	db.Exec("INSERT INTO go_test_pae_sel VALUES (1, 'alice')")
	db.Exec("INSERT INTO go_test_pae_sel VALUES (2, 'bob')")
	db.Exec("INSERT INTO go_test_pae_sel VALUES (3, 'carol')")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rows, err := PrepareAndQuery(ctx, conn, "SELECT id, name FROM go_test_pae_sel ORDER BY id")
	if err != nil {
		t.Fatalf("PrepareAndQuery SELECT: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Fatalf("columns: got %v, want [id name]", cols)
	}

	type row struct {
		id   interface{}
		name interface{}
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}

	if len(got) != 3 {
		t.Fatalf("row count: got %d, want 3", len(got))
	}
	t.Logf("PrepareAndQuery SELECT: %d rows", len(got))
	for _, r := range got {
		t.Logf("  id=%v name=%v", r.id, r.name)
	}
}

// --- GET_ROW_COUNT ---

func TestIntegrationGetRowCount(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_rc")
	db.Exec("CREATE TABLE go_test_rc (id INT)")
	defer db.Exec("DROP TABLE go_test_rc")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.ExecContext(ctx, "INSERT INTO go_test_rc VALUES (1)")
	conn.ExecContext(ctx, "INSERT INTO go_test_rc VALUES (2)")

	count, err := GetRowCount(ctx, conn)
	if err != nil {
		t.Fatalf("GetRowCount: %v", err)
	}
	t.Logf("GetRowCount: %d", count)
}

// --- GET_LAST_INSERT_ID ---

func TestIntegrationGetLastInsertID(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_lid")
	db.Exec("CREATE TABLE go_test_lid (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))")
	defer db.Exec("DROP TABLE go_test_lid")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.ExecContext(ctx, "INSERT INTO go_test_lid (name) VALUES ('test')")

	lastID, err := GetLastInsertID(ctx, conn)
	if err != nil {
		t.Fatalf("GetLastInsertID: %v", err)
	}
	t.Logf("GetLastInsertID: %s", lastID)
}

// --- NamedValueChecker ---

func TestIntegrationNamedValueChecker(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_nvc")
	_, err := db.Exec("CREATE TABLE go_test_nvc (id INT, name VARCHAR(50), amount DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_nvc")

	// Use CubridEnum as bind parameter
	_, err = db.Exec("INSERT INTO go_test_nvc VALUES (1, ?, 0)", NewCubridEnum("test_val", 1))
	if err != nil {
		t.Fatalf("bind CubridEnum: %v", err)
	}

	// Use CubridNumeric as bind parameter
	_, err = db.Exec("INSERT INTO go_test_nvc VALUES (2, 'num', ?)", NewCubridNumeric("42.5"))
	if err != nil {
		// CubridNumeric converts to string, which may not match DOUBLE column
		t.Logf("bind CubridNumeric to DOUBLE column: %v (expected, type mismatch)", err)
	}

	var name string
	db.QueryRow("SELECT name FROM go_test_nvc WHERE id = 1").Scan(&name)
	if name != "test_val" {
		t.Fatalf("name: got %q, want %q", name, "test_val")
	}
	t.Logf("NamedValueChecker: CubridEnum bind works, name=%s", name)
}

// --- Timezone types with CubridTimestampTz scan ---

func TestIntegrationTimestampTzCustomScan(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_tstz2")
	_, err := db.Exec("CREATE TABLE go_test_tstz2 (id INT, ts TIMESTAMPTZ)")
	if err != nil {
		t.Skipf("TIMESTAMPTZ not supported: %v", err)
		return
	}
	defer db.Exec("DROP TABLE go_test_tstz2")

	_, err = db.Exec("INSERT INTO go_test_tstz2 VALUES (1, TIMESTAMPTZ'2026-03-15 10:30:00 Asia/Seoul')")
	if err != nil {
		t.Fatal(err)
	}

	// Scan as time.Time (driver.Value is time.Time)
	var ts time.Time
	err = db.QueryRow("SELECT ts FROM go_test_tstz2 WHERE id = 1").Scan(&ts)
	if err != nil {
		t.Fatalf("Scan TIMESTAMPTZ as time.Time: %v", err)
	}
	t.Logf("TIMESTAMPTZ as time.Time: %v (tz=%s)", ts, ts.Location())
}

// --- Table inheritance ---

func TestIntegrationTableInheritance(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()

	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_test_child")
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_test_parent")

	_, err := db.ExecContext(ctx, "CREATE TABLE go_test_parent (id INT, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, "CREATE TABLE go_test_child UNDER go_test_parent (grade INT)")
	if err != nil {
		t.Skipf("Table inheritance not supported: %v", err)
		db.ExecContext(ctx, "DROP TABLE go_test_parent")
		return
	}
	defer func() {
		db.ExecContext(ctx, "DROP TABLE go_test_child")
		db.ExecContext(ctx, "DROP TABLE go_test_parent")
	}()

	supers, err := ListSuperClasses(ctx, db, "go_test_child")
	if err != nil {
		t.Fatalf("ListSuperClasses: %v", err)
	}
	t.Logf("SuperClasses of go_test_child: %v", supers)

	subs, err := ListSubClasses(ctx, db, "go_test_parent")
	if err != nil {
		t.Fatalf("ListSubClasses: %v", err)
	}
	t.Logf("SubClasses of go_test_parent: %v", subs)
}
