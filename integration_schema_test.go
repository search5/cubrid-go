//go:build integration

package cubrid

import (
	"context"
	"database/sql"
	"testing"
)

func TestIntegrationSchemaListTables(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_tbl1")
	_, err = db.Exec("CREATE TABLE go_test_tbl1 (id INT)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_tbl1")

	tables, err := ListTables(context.Background(), db)
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}

	t.Logf("Tables (%d): %v", len(tables), tables)

	found := false
	for _, name := range tables {
		if name == "go_test_tbl1" {
			found = true
		}
	}
	if !found {
		t.Error("go_test_tbl1 not found in table list")
	}
}

func TestIntegrationSchemaListColumns(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_cols")
	_, err = db.Exec("CREATE TABLE go_test_cols (id INT NOT NULL, name VARCHAR(100), score DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_cols")

	columns, err := ListColumns(context.Background(), db, "go_test_cols")
	if err != nil {
		t.Fatalf("ListColumns: %v", err)
	}

	t.Logf("Columns (%d):", len(columns))
	for _, c := range columns {
		t.Logf("  %s type=%s prec=%d scale=%d nullable=%v default=%q",
			c.Name, c.DataType, c.Precision, c.Scale, c.IsNullable, c.DefaultValue)
	}

	if len(columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(columns))
	}
	if len(columns) >= 1 && columns[0].Name != "id" {
		t.Errorf("col[0].Name = %q, want id", columns[0].Name)
	}
	if len(columns) >= 1 && columns[0].IsNullable {
		t.Errorf("col[0] should not be nullable")
	}
}

func TestIntegrationSchemaPrimaryKey(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_pk2")
	_, err = db.Exec("CREATE TABLE go_test_pk2 (id INT PRIMARY KEY, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_pk2")

	keys, err := ListPrimaryKeys(context.Background(), db, "go_test_pk2")
	if err != nil {
		t.Fatalf("ListPrimaryKeys: %v", err)
	}

	t.Logf("PK columns: %v", keys)
	if len(keys) != 1 || keys[0] != "id" {
		t.Errorf("expected [id], got %v", keys)
	}
}

func TestIntegrationSchemaConstraints(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_cst2")
	_, err = db.Exec("CREATE TABLE go_test_cst2 (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_cst2")

	constraints, err := ListConstraints(context.Background(), db, "go_test_cst2")
	if err != nil {
		t.Fatalf("ListConstraints: %v", err)
	}

	t.Logf("Constraints (%d):", len(constraints))
	for _, c := range constraints {
		t.Logf("  %s pk=%v unique=%v fk=%v", c.Name, c.IsPrimaryKey, c.IsUnique, c.IsForeignKey)
	}

	hasPK := false
	hasUnique := false
	for _, c := range constraints {
		if c.IsPrimaryKey {
			hasPK = true
		}
		if c.IsUnique && !c.IsPrimaryKey {
			hasUnique = true
		}
	}
	if !hasPK {
		t.Error("missing PK constraint")
	}
	if !hasUnique {
		t.Error("missing UNIQUE constraint")
	}
}

func TestIntegrationSchemaInfoProtocol(t *testing.T) {
	// Verify the low-level SCHEMA_INFO protocol still works for parsing.
	dsn, _ := ParseDSN(testDSN)
	conn, err := connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rows, err := SchemaInfo(context.Background(), conn, SchemaClass, "", "", SchemaFlagExact)
	if err != nil {
		t.Fatalf("SchemaInfo: %v", err)
	}
	defer rows.Close()

	t.Logf("Schema columns: %v, buffer=%d", rows.Columns(), len(rows.buffer))
}
