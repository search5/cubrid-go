//go:build integration

package cubrid

import (
	"database/sql"
	"strings"
	"testing"
)

func TestIntegrationLobBlobInsertAndRead(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_blob")
	_, err = db.Exec("CREATE TABLE go_test_blob (id INT, content BLOB)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_blob")

	// Insert a BLOB using SQL literal.
	_, err = db.Exec("INSERT INTO go_test_blob VALUES (1, X'DEADBEEF')")
	if err != nil {
		t.Fatalf("INSERT BLOB: %v", err)
	}

	// Read it back — should get a LOB handle.
	row := db.QueryRow("SELECT id, content FROM go_test_blob WHERE id = 1")
	var id int32
	var lobHandle CubridLobHandle
	err = row.Scan(&id, &lobHandle)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("BLOB handle: type=%v size=%d locator=%q", lobHandle.LobType, lobHandle.Size, lobHandle.Locator)

	if lobHandle.LobType != LobBlob {
		t.Errorf("LobType = %v, want BLOB", lobHandle.LobType)
	}
	if lobHandle.Size != 4 { // DEADBEEF = 4 bytes
		t.Errorf("Size = %d, want 4", lobHandle.Size)
	}
}

func TestIntegrationLobClobInsertAndRead(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_clob")
	_, err = db.Exec("CREATE TABLE go_test_clob (id INT, content CLOB)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_clob")

	// Insert a CLOB using SQL.
	_, err = db.Exec("INSERT INTO go_test_clob VALUES (1, CHAR_TO_CLOB('hello world'))")
	if err != nil {
		t.Fatalf("INSERT CLOB: %v", err)
	}

	row := db.QueryRow("SELECT id, content FROM go_test_clob WHERE id = 1")
	var id int32
	var lobHandle CubridLobHandle
	err = row.Scan(&id, &lobHandle)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("CLOB handle: type=%v size=%d locator=%q", lobHandle.LobType, lobHandle.Size, lobHandle.Locator)

	if lobHandle.LobType != LobClob {
		t.Errorf("LobType = %v, want CLOB", lobHandle.LobType)
	}
	if lobHandle.Size != 11 { // "hello world" = 11 bytes
		t.Errorf("Size = %d, want 11", lobHandle.Size)
	}
}

func TestIntegrationLobReadStreaming(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_lobread")
	_, err = db.Exec("CREATE TABLE go_test_lobread (id INT, content CLOB)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_lobread")

	testData := strings.Repeat("CUBRID LOB test data. ", 100) // ~2200 bytes
	_, err = db.Exec("INSERT INTO go_test_lobread VALUES (1, CHAR_TO_CLOB(?))", testData)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Get the LOB handle.
	row := db.QueryRow("SELECT content FROM go_test_lobread WHERE id = 1")
	var lobHandle CubridLobHandle
	err = row.Scan(&lobHandle)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("LOB handle: size=%d", lobHandle.Size)
	if lobHandle.Size != int64(len(testData)) {
		t.Errorf("Size = %d, want %d", lobHandle.Size, len(testData))
	}
}

func TestIntegrationLobColumnMeta(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_lobmeta")
	_, err = db.Exec("CREATE TABLE go_test_lobmeta (id INT, b BLOB, c CLOB)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_lobmeta")

	_, err = db.Exec("INSERT INTO go_test_lobmeta VALUES (1, X'01', CHAR_TO_CLOB('x'))")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT id, b, c FROM go_test_lobmeta")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	if colTypes[1].DatabaseTypeName() != "BLOB" {
		t.Errorf("col 1 = %q, want BLOB", colTypes[1].DatabaseTypeName())
	}
	if colTypes[2].DatabaseTypeName() != "CLOB" {
		t.Errorf("col 2 = %q, want CLOB", colTypes[2].DatabaseTypeName())
	}
}
