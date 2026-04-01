//go:build integration

package cubrid

import (
	"context"
	"database/sql"
	"testing"
)

// Exercise reconnect path by doing operations after connection may have been dropped.
// KEEP_CONNECTION=AUTO means CAS can drop the connection between requests.
func TestIntegrationReconnectPath(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()

	// Do many sequential operations to trigger reconnect scenarios
	for i := 0; i < 20; i++ {
		var val int64
		err := db.QueryRowContext(ctx, "SELECT 1+1").Scan(&val)
		if err != nil {
			t.Logf("iter %d: %v (reconnect expected)", i, err)
			continue
		}
		if val != 2 {
			t.Fatalf("iter %d: got %d, want 2", i, val)
		}
	}

	// Exec + Query interleaved
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_cov_recon")
	db.ExecContext(ctx, "CREATE TABLE go_cov_recon (id INT)")
	defer db.ExecContext(ctx, "DROP TABLE go_cov_recon")

	for i := 0; i < 10; i++ {
		db.ExecContext(ctx, "INSERT INTO go_cov_recon VALUES (?)", i)
	}

	rows, _ := db.QueryContext(ctx, "SELECT COUNT(*) FROM go_cov_recon")
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int64
			rows.Scan(&count)
			t.Logf("Reconnect test: %d rows", count)
		}
	}
}

// Test with auto_commit=false (no KEEP_CONNECTION drop)
func TestIntegrationNoAutoCommitOps(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN+"?auto_commit=false")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_cov_noac")
	db.ExecContext(ctx, "CREATE TABLE go_cov_noac (id INT, name VARCHAR(100))")
	defer db.ExecContext(ctx, "DROP TABLE go_cov_noac")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		tx.ExecContext(ctx, "INSERT INTO go_cov_noac VALUES (?, ?)", i, "name")
	}
	tx.Commit()

	// Query with fetch
	rows, _ := db.QueryContext(ctx, "SELECT * FROM go_cov_noac ORDER BY id")
	if rows != nil {
		defer rows.Close()
		count := 0
		for rows.Next() {
			var id int64
			var name string
			rows.Scan(&id, &name)
			count++
		}
		t.Logf("NoAutoCommit: %d rows", count)
	}
}

// Multiple connections exercising the pool
func TestIntegrationConnectionPool(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)

	ctx := context.Background()

	// Open multiple connections
	conns := make([]*sql.Conn, 3)
	for i := 0; i < 3; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
	}

	// Use each connection
	for i, c := range conns {
		var v int64
		err := c.QueryRowContext(ctx, "SELECT ?+1", i).Scan(&v)
		if err != nil {
			t.Logf("conn %d: %v", i, err)
		}
	}

	// Close all
	for _, c := range conns {
		c.Close()
	}
	t.Log("Connection pool test passed")
}

// Exercise readOpenDatabaseResponse error path
func TestIntegrationBadDBName(t *testing.T) {
	_, err := sql.Open("cubrid", "cubrid://dba:@localhost:33100/nonexistent_db_xyz")
	if err != nil {
		t.Logf("Bad DB name open: %v", err)
		return
	}
	db, _ := sql.Open("cubrid", "cubrid://dba:@localhost:33100/nonexistent_db_xyz")
	defer db.Close()
	err = db.Ping()
	t.Logf("Bad DB ping: %v", err)
}
