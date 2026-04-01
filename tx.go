package cubrid

import (
	"context"

	"github.com/search5/cubrid-go/protocol"
)

// cubridTx implements driver.Tx.
type cubridTx struct {
	conn *cubridConn
}

// Commit commits the transaction.
func (tx *cubridTx) Commit() error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if !tx.conn.inTx {
		return nil
	}

	err := tx.conn.endTransaction(context.Background(), protocol.TranCommit)
	tx.conn.inTx = false
	// autoCommit is restored automatically — request builders check c.inTx.

	return err
}

// Rollback aborts the transaction.
func (tx *cubridTx) Rollback() error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if !tx.conn.inTx {
		return nil
	}

	err := tx.conn.endTransaction(context.Background(), protocol.TranRollback)
	tx.conn.inTx = false
	// autoCommit is restored automatically — request builders check c.inTx.

	return err
}
