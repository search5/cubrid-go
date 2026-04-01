//go:build integration

package cubrid

import (
	"context"
	"fmt"
	"testing"
)

func TestDebugPrepareAndQuerySelect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS go_dbg_paq")
	db.Exec("CREATE TABLE go_dbg_paq (id INT, name VARCHAR(50))")
	db.Exec("INSERT INTO go_dbg_paq VALUES (1, 'alice')")
	db.Exec("INSERT INTO go_dbg_paq VALUES (2, 'bob')")
	defer db.Exec("DROP TABLE go_dbg_paq")

	ctx := context.Background()
	conn := openTestConn(t, db)
	defer conn.Close()

	conn.Raw(func(dc interface{}) error {
		c := dc.(*cubridConn)

		rows, err := c.prepareAndQuery(ctx, "SELECT id, name FROM go_dbg_paq ORDER BY id", nil)
		if err != nil {
			t.Fatalf("prepareAndQuery: %v", err)
		}
		defer rows.Close()

		fmt.Printf("columns: %v\n", rows.Columns())
		fmt.Printf("totalTuples: %d\n", rows.inner.totalTuples)
		fmt.Printf("buffer len: %d\n", len(rows.inner.buffer))
		fmt.Printf("done: %v\n", rows.inner.done)

		count := 0
		for rows.Next() {
			var id, name interface{}
			rows.Scan(&id, &name)
			fmt.Printf("  row: id=%v name=%v\n", id, name)
			count++
		}
		fmt.Printf("total rows: %d\n", count)
		return nil
	})
}
