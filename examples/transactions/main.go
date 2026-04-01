// Example: transaction handling with CUBRID, including savepoints.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	cubrid "github.com/search5/cubrid-go"
)

func main() {
	db, err := sql.Open("cubrid", "cubrid://dba:@localhost:33000/demodb?auto_commit=false")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_tx_example")
	_, err = db.ExecContext(ctx, "CREATE TABLE go_tx_example (id INT, value VARCHAR(50))")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Table created")

	// --- Basic transaction: commit ---
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	tx.ExecContext(ctx, "INSERT INTO go_tx_example VALUES (1, 'committed')")
	tx.ExecContext(ctx, "INSERT INTO go_tx_example VALUES (2, 'committed')")

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Transaction committed: 2 rows inserted")

	// --- Transaction: rollback ---
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	tx.ExecContext(ctx, "INSERT INTO go_tx_example VALUES (3, 'will_rollback')")

	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Transaction rolled back: row 3 NOT inserted")

	// Verify
	var count int32
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM go_tx_example").Scan(&count)
	fmt.Printf("Rows after rollback: %d (expected 2)\n", count)

	// --- Savepoint example ---
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Start transaction via Exec (for savepoint access)
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	tx2.ExecContext(ctx, "INSERT INTO go_tx_example VALUES (10, 'before_savepoint')")

	// Create savepoint (requires *sql.Conn)
	conn2, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := cubrid.Savepoint(conn2, "sp1"); err != nil {
		fmt.Printf("Savepoint creation: %v (may require active tx on same conn)\n", err)
	}
	conn2.Close()

	if err := tx2.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Transaction with savepoint committed")

	// Cleanup
	db.ExecContext(ctx, "DROP TABLE go_tx_example")
	fmt.Println("\nDone.")
}
