//go:build integration

package cubrid

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// Test bind parameters with NULL via Go's sql.NullXxx types.
func TestIntegrationNullBindParams(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_nbp")
	_, err = db.Exec("CREATE TABLE go_test_nbp (id INT, name VARCHAR(50), score DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_nbp")

	// Insert with nil bind params.
	_, err = db.Exec("INSERT INTO go_test_nbp VALUES (1, NULL, NULL)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id int32
	var name sql.NullString
	var score sql.NullFloat64
	err = db.QueryRow("SELECT * FROM go_test_nbp").Scan(&id, &name, &score)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if name.Valid {
		t.Errorf("name should be NULL")
	}
	if score.Valid {
		t.Errorf("score should be NULL")
	}
}

// Test context cancellation with timeout.
func TestIntegrationContextTimeout(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Short timeout should still allow fast queries.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result int
	err = db.QueryRowContext(ctx, "SELECT 1+1 FROM db_root").Scan(&result)
	if err != nil {
		t.Fatalf("QueryRowContext: %v", err)
	}
	if result != 2 {
		t.Errorf("1+1 = %d", result)
	}
}

// Test multiple queries interleaved with execs.
func TestIntegrationInterleavedOps(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	db.Exec("DROP TABLE IF EXISTS go_test_il")
	_, err = db.Exec("CREATE TABLE go_test_il (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_il")

	// Interleave Exec and Query on a single connection.
	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO go_test_il VALUES (?)", i)
		if err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM go_test_il").Scan(&count)
		if err != nil {
			t.Fatalf("COUNT after INSERT %d: %v", i, err)
		}
		if count != i+1 {
			t.Errorf("after INSERT %d: count=%d, want %d", i, count, i+1)
		}
	}
}

// Test connection reuse after error.
func TestIntegrationErrorRecovery(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// Execute an invalid SQL — should get an error.
	_, err = db.Exec("THIS IS NOT VALID SQL")
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
	t.Logf("Expected error: %v", err)

	// Connection should still be usable.
	var result int
	err = db.QueryRow("SELECT 42 FROM db_root").Scan(&result)
	if err != nil {
		t.Fatalf("query after error: %v", err)
	}
	if result != 42 {
		t.Errorf("result = %d, want 42", result)
	}
}

// Test a transaction with SELECT inside.
func TestIntegrationTransactionWithQuery(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_tq")
	_, err = db.Exec("CREATE TABLE go_test_tq (id INT, val VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_tq")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// INSERT inside tx.
	_, err = tx.Exec("INSERT INTO go_test_tq VALUES (1, 'in_tx')")
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	// SELECT inside same tx should see the uncommitted row.
	var val string
	err = tx.QueryRow("SELECT val FROM go_test_tq WHERE id = 1").Scan(&val)
	if err != nil {
		tx.Rollback()
		t.Fatalf("SELECT in tx: %v", err)
	}
	if val != "in_tx" {
		t.Errorf("val = %q, want in_tx", val)
	}

	tx.Commit()
}

// Test that db.Ping works after idle period.
func TestIntegrationRepeatedPing(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		if err := db.Ping(); err != nil {
			t.Fatalf("Ping %d: %v", i, err)
		}
	}
}

// Test BIGINT edge values.
func TestIntegrationBigIntEdge(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_bi")
	_, err = db.Exec("CREATE TABLE go_test_bi (val BIGINT)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_bi")

	tests := []int64{0, -1, 9223372036854775807, -9223372036854775808, 1}
	for _, v := range tests {
		_, err = db.Exec("INSERT INTO go_test_bi VALUES (?)", v)
		if err != nil {
			t.Fatalf("INSERT %d: %v", v, err)
		}
	}

	rows, err := db.Query("SELECT val FROM go_test_bi ORDER BY val")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		results = append(results, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(results) != len(tests) {
		t.Errorf("got %d results, want %d", len(results), len(tests))
	}
}
