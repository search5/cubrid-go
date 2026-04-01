// Example: BLOB and CLOB handling with CUBRID.
//
// CUBRID LOB operations use the CAS protocol's LOB_NEW, LOB_WRITE,
// and LOB_READ function codes. The driver's LOB API operates on the
// raw driver connection via conn.Raw().
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/search5/cubrid-go"
)

func main() {
	db, err := sql.Open("cubrid", "cubrid://dba:@localhost:33000/demodb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup table with LOB columns.
	db.ExecContext(ctx, "DROP TABLE IF EXISTS go_lob_example")
	_, err = db.ExecContext(ctx, `CREATE TABLE go_lob_example (
		id INT PRIMARY KEY,
		bin_data BLOB,
		text_data CLOB
	)`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Table created with BLOB and CLOB columns")

	// --- Insert LOB data via SQL ---
	// CUBRID supports inserting BLOB/CLOB via BIT_TO_BLOB() and CHAR_TO_CLOB().
	_, err = db.ExecContext(ctx,
		"INSERT INTO go_lob_example (id, text_data) VALUES (1, CHAR_TO_CLOB('Hello from CLOB'))")
	if err != nil {
		log.Fatal("insert CLOB:", err)
	}
	fmt.Println("Inserted row with CLOB data")

	// --- Read LOB data back ---
	// CLOB_TO_CHAR() converts CLOB column to string for reading.
	var textData string
	err = db.QueryRowContext(ctx,
		"SELECT CLOB_TO_CHAR(text_data) FROM go_lob_example WHERE id = 1").Scan(&textData)
	if err != nil {
		log.Fatal("read CLOB:", err)
	}
	fmt.Printf("CLOB data: %q\n", textData)

	// --- Working with BLOB via SQL ---
	_, err = db.ExecContext(ctx,
		"INSERT INTO go_lob_example (id) VALUES (2)")
	if err != nil {
		log.Fatal("insert row 2:", err)
	}

	// --- Query LOB handle from result set ---
	// When selecting a BLOB/CLOB column directly, the driver returns
	// a *CubridLobHandle that can be used with the LOB streaming API.
	rows, err := db.QueryContext(ctx, "SELECT id, text_data FROM go_lob_example WHERE id = 1")
	if err != nil {
		log.Fatal("query LOB:", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int32
		var lobVal interface{}
		if err := rows.Scan(&id, &lobVal); err != nil {
			fmt.Printf("Scan LOB handle: %v\n", err)
		} else {
			fmt.Printf("Row id=%d, LOB value type: %T\n", id, lobVal)
		}
	}

	// --- Column type metadata ---
	rows2, err := db.QueryContext(ctx, "SELECT bin_data, text_data FROM go_lob_example LIMIT 1")
	if err != nil {
		log.Fatal(err)
	}
	defer rows2.Close()

	colTypes, _ := rows2.ColumnTypes()
	for _, ct := range colTypes {
		fmt.Printf("Column %q: DB type = %s\n", ct.Name(), ct.DatabaseTypeName())
	}

	// Cleanup
	db.ExecContext(ctx, "DROP TABLE go_lob_example")
	fmt.Println("\nDone.")
}
