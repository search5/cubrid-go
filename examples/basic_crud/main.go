// Example: basic CRUD operations with CUBRID.
package main

import (
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

	if err := db.Ping(); err != nil {
		log.Fatal("ping failed:", err)
	}
	fmt.Println("Connected to CUBRID")

	// CREATE
	_, err = db.Exec("DROP TABLE IF EXISTS go_example")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE go_example (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), score DOUBLE)")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Table created")

	// INSERT
	result, err := db.Exec("INSERT INTO go_example (name, score) VALUES (?, ?)", "Alice", 95.5)
	if err != nil {
		log.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	fmt.Printf("Inserted %d row(s)\n", affected)

	_, err = db.Exec("INSERT INTO go_example (name, score) VALUES (?, ?)", "Bob", 87.3)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO go_example (name, score) VALUES (?, ?)", "Charlie", 92.1)
	if err != nil {
		log.Fatal(err)
	}

	// SELECT
	rows, err := db.Query("SELECT id, name, score FROM go_example ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("\nAll rows:")
	for rows.Next() {
		var id int32
		var name string
		var score float64
		if err := rows.Scan(&id, &name, &score); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  id=%d name=%s score=%.1f\n", id, name, score)
	}

	// UPDATE
	result, err = db.Exec("UPDATE go_example SET score = ? WHERE name = ?", 99.9, "Alice")
	if err != nil {
		log.Fatal(err)
	}
	affected, _ = result.RowsAffected()
	fmt.Printf("\nUpdated %d row(s)\n", affected)

	// SELECT single row
	var score float64
	err = db.QueryRow("SELECT score FROM go_example WHERE name = ?", "Alice").Scan(&score)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice's new score: %.1f\n", score)

	// DELETE
	result, err = db.Exec("DELETE FROM go_example WHERE name = ?", "Bob")
	if err != nil {
		log.Fatal(err)
	}
	affected, _ = result.RowsAffected()
	fmt.Printf("\nDeleted %d row(s)\n", affected)

	// COUNT
	var count int32
	err = db.QueryRow("SELECT COUNT(*) FROM go_example").Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Remaining rows: %d\n", count)

	// Cleanup
	_, err = db.Exec("DROP TABLE go_example")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nTable dropped. Done.")
}
