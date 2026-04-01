//go:build integration

package cubrid

import (
	"database/sql"
	"testing"
	"time"
)

func TestIntegrationTimestampTz(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_tstz")
	_, err = db.Exec("CREATE TABLE go_test_tstz (id INT, ts TIMESTAMPTZ)")
	if err != nil {
		t.Skipf("TIMESTAMPTZ not supported: %v", err)
	}
	defer db.Exec("DROP TABLE go_test_tstz")

	_, err = db.Exec("INSERT INTO go_test_tstz VALUES (1, TIMESTAMPTZ'2026-03-15 10:30:00 Asia/Seoul')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id int32
	var ts time.Time
	err = db.QueryRow("SELECT id, ts FROM go_test_tstz WHERE id = 1").Scan(&id, &ts)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("TIMESTAMPTZ: %v (location=%s)", ts, ts.Location())
	if ts.Year() != 2026 || ts.Month() != 3 || ts.Day() != 15 {
		t.Errorf("date = %d-%d-%d", ts.Year(), ts.Month(), ts.Day())
	}
	if ts.Hour() != 10 || ts.Minute() != 30 {
		t.Errorf("time = %d:%d", ts.Hour(), ts.Minute())
	}
}

func TestIntegrationDatetimeTz(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_dttz")
	_, err = db.Exec("CREATE TABLE go_test_dttz (id INT, dt DATETIMETZ)")
	if err != nil {
		t.Skipf("DATETIMETZ not supported: %v", err)
	}
	defer db.Exec("DROP TABLE go_test_dttz")

	_, err = db.Exec("INSERT INTO go_test_dttz VALUES (1, DATETIMETZ'2026-12-31 23:59:59.500 UTC')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id int32
	var dt time.Time
	err = db.QueryRow("SELECT id, dt FROM go_test_dttz WHERE id = 1").Scan(&id, &dt)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("DATETIMETZ: %v (location=%s)", dt, dt.Location())
	if dt.Year() != 2026 || dt.Month() != 12 || dt.Day() != 31 {
		t.Errorf("date = %d-%d-%d", dt.Year(), dt.Month(), dt.Day())
	}
	if dt.Hour() != 23 || dt.Minute() != 59 || dt.Second() != 59 {
		t.Errorf("time = %d:%d:%d", dt.Hour(), dt.Minute(), dt.Second())
	}
	if dt.Nanosecond() != 500_000_000 {
		t.Errorf("nanosecond = %d, want 500000000", dt.Nanosecond())
	}
}

func TestIntegrationTimestampLtz(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_tsltz")
	_, err = db.Exec("CREATE TABLE go_test_tsltz (id INT, ts TIMESTAMPLTZ)")
	if err != nil {
		t.Skipf("TIMESTAMPLTZ not supported: %v", err)
	}
	defer db.Exec("DROP TABLE go_test_tsltz")

	_, err = db.Exec("INSERT INTO go_test_tsltz VALUES (1, TIMESTAMPLTZ'2026-06-15 12:00:00 UTC')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id int32
	var ts time.Time
	err = db.QueryRow("SELECT id, ts FROM go_test_tsltz WHERE id = 1").Scan(&id, &ts)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("TIMESTAMPLTZ: %v (location=%s)", ts, ts.Location())
	// LTZ converts to server's local timezone; just verify it scans successfully.
	if ts.Year() != 2026 {
		t.Errorf("year = %d", ts.Year())
	}
}

func TestIntegrationDatetimeLtz(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_dtltz")
	_, err = db.Exec("CREATE TABLE go_test_dtltz (id INT, dt DATETIMELTZ)")
	if err != nil {
		t.Skipf("DATETIMELTZ not supported: %v", err)
	}
	defer db.Exec("DROP TABLE go_test_dtltz")

	_, err = db.Exec("INSERT INTO go_test_dtltz VALUES (1, DATETIMELTZ'2026-06-15 12:00:00.123 UTC')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id int32
	var dt time.Time
	err = db.QueryRow("SELECT id, dt FROM go_test_dtltz WHERE id = 1").Scan(&id, &dt)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("DATETIMELTZ: %v (location=%s)", dt, dt.Location())
	if dt.Year() != 2026 {
		t.Errorf("year = %d", dt.Year())
	}
}

func TestIntegrationTzColumnMeta(t *testing.T) {
	db, err := sql.Open("cubrid", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_test_tzmeta")
	_, err = db.Exec("CREATE TABLE go_test_tzmeta (a TIMESTAMPTZ, b DATETIMETZ)")
	if err != nil {
		t.Skipf("TZ types not supported: %v", err)
	}
	defer db.Exec("DROP TABLE go_test_tzmeta")

	_, err = db.Exec("INSERT INTO go_test_tzmeta VALUES (TIMESTAMPTZ'2026-01-01 00:00:00 UTC', DATETIMETZ'2026-01-01 00:00:00.000 UTC')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT a, b FROM go_test_tzmeta")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	if colTypes[0].DatabaseTypeName() != "TIMESTAMPTZ" {
		t.Errorf("col 0 = %q, want TIMESTAMPTZ", colTypes[0].DatabaseTypeName())
	}
	if colTypes[1].DatabaseTypeName() != "DATETIMETZ" {
		t.Errorf("col 1 = %q, want DATETIMETZ", colTypes[1].DatabaseTypeName())
	}
}
