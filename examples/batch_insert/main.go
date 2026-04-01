// Example: batch insert using EXECUTE_BATCH and prepared statement batching.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	cubrid "github.com/search5/cubrid-go"
)

func main() {
	db, err := sql.Open("cubrid", "cubrid://dba:@localhost:33000/demodb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_batch_example")
	_, err = db.ExecContext(ctx, "CREATE TABLE go_batch_example (id INT, name VARCHAR(50))")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Table created")

	// --- Method 1: EXECUTE_BATCH (multiple SQL in one round-trip) ---
	sqls := []string{
		"INSERT INTO go_batch_example VALUES (1, 'Alice')",
		"INSERT INTO go_batch_example VALUES (2, 'Bob')",
		"INSERT INTO go_batch_example VALUES (3, 'Charlie')",
		"INSERT INTO go_batch_example VALUES (4, 'Diana')",
		"INSERT INTO go_batch_example VALUES (5, 'Eve')",
	}

	if err := cubrid.BatchExec(db, sqls); err != nil {
		log.Fatal("BatchExec failed:", err)
	}
	fmt.Printf("BatchExec: inserted %d rows in one round-trip\n", len(sqls))

	// --- Method 2: Prepared statement batch (parameterized) ---
	stmt, err := db.PrepareContext(ctx, "INSERT INTO go_batch_example VALUES (?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	names := []string{"Frank", "Grace", "Heidi", "Ivan", "Judy"}
	for i, name := range names {
		_, err := stmt.ExecContext(ctx, i+100, name)
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("Prepared batch: inserted %d rows with parameters\n", len(names))

	// Verify
	var count int32
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM go_batch_example").Scan(&count)
	fmt.Printf("Total rows: %d\n", count)

	// Cleanup
	db.ExecContext(ctx, "DROP TABLE go_batch_example")
	fmt.Println("Done.")
}
