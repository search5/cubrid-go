//go:build integration

package cubrid

import (
	"database/sql"
	"testing"
)

func TestIntegrationCollectionSetVarchar(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_coll_set")
	_, err = db.Exec("CREATE TABLE go_test_coll_set (id INT, tags SET(VARCHAR(50)))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_coll_set")

	_, err = db.Exec("INSERT INTO go_test_coll_set VALUES (1, {'alpha','beta','gamma'})")
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow("SELECT id, tags FROM go_test_coll_set WHERE id = 1")
	var id int32
	var tags CubridSet
	err = row.Scan(&id, &tags)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if id != 1 {
		t.Errorf("id = %d", id)
	}
	t.Logf("SET elements: %v (count=%d)", tags.Elements, len(tags.Elements))
	if len(tags.Elements) != 3 {
		t.Errorf("expected 3 elements, got %d", len(tags.Elements))
	}
}

func TestIntegrationCollectionSequenceInt(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_coll_seq")
	_, err = db.Exec("CREATE TABLE go_test_coll_seq (id INT, scores SEQUENCE(INT))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_coll_seq")

	_, err = db.Exec("INSERT INTO go_test_coll_seq VALUES (1, {10, 20, 30})")
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow("SELECT id, scores FROM go_test_coll_seq WHERE id = 1")
	var id int32
	var scores CubridSequence
	err = row.Scan(&id, &scores)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("SEQUENCE elements: %v (count=%d)", scores.Elements, len(scores.Elements))
	if len(scores.Elements) != 3 {
		t.Errorf("expected 3 elements, got %d", len(scores.Elements))
	}
}

func TestIntegrationCollectionMultiSet(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_coll_ms")
	_, err = db.Exec("CREATE TABLE go_test_coll_ms (id INT, vals MULTISET(DOUBLE))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_coll_ms")

	_, err = db.Exec("INSERT INTO go_test_coll_ms VALUES (1, {1.1, 2.2, 1.1})")
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow("SELECT id, vals FROM go_test_coll_ms WHERE id = 1")
	var id int32
	var vals CubridMultiSet
	err = row.Scan(&id, &vals)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("MULTISET elements: %v (count=%d)", vals.Elements, len(vals.Elements))
	if len(vals.Elements) != 3 {
		t.Errorf("expected 3 elements, got %d", len(vals.Elements))
	}
}

func TestIntegrationCollectionEmpty(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_coll_empty")
	_, err = db.Exec("CREATE TABLE go_test_coll_empty (id INT, tags SET(VARCHAR(50)))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_coll_empty")

	_, err = db.Exec("INSERT INTO go_test_coll_empty VALUES (1, {})")
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow("SELECT id, tags FROM go_test_coll_empty WHERE id = 1")
	var id int32
	var tags CubridSet
	err = row.Scan(&id, &tags)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("Empty SET elements: %v (count=%d)", tags.Elements, len(tags.Elements))
	if len(tags.Elements) != 0 {
		t.Errorf("expected 0 elements, got %d", len(tags.Elements))
	}
}

func TestIntegrationCollectionColumnMeta(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_coll_meta")
	_, err = db.Exec("CREATE TABLE go_test_coll_meta (id INT, tags SET(VARCHAR(50)))")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE go_test_coll_meta")

	_, err = db.Exec("INSERT INTO go_test_coll_meta VALUES (1, {'x'})")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT id, tags FROM go_test_coll_meta")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	if colTypes[0].DatabaseTypeName() != "INT" {
		t.Errorf("col 0 type = %q", colTypes[0].DatabaseTypeName())
	}
	if colTypes[1].DatabaseTypeName() != "SET" {
		t.Errorf("col 1 type = %q, want SET", colTypes[1].DatabaseTypeName())
	}
}
