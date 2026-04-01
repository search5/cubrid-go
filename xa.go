package cubrid

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/search5/cubrid-go/protocol"
)

// XID represents an XA transaction identifier following the X/Open XA specification.
type XID struct {
	// FormatID identifies the format of the GlobalTransactionID and BranchQualifier.
	FormatID int32
	// GlobalTransactionID is the global transaction identifier (max 64 bytes).
	GlobalTransactionID []byte
	// BranchQualifier is the branch qualifier (max 64 bytes).
	BranchQualifier []byte
}

// encodeXID serializes an XID into the wire format:
// format_id(4) + gtrid_length(4) + bqual_length(4) + data(gtrid + bqual)
func (x *XID) encode() []byte {
	var buf bytes.Buffer
	protocol.WriteInt(&buf, x.FormatID)
	protocol.WriteInt(&buf, int32(len(x.GlobalTransactionID)))
	protocol.WriteInt(&buf, int32(len(x.BranchQualifier)))
	buf.Write(x.GlobalTransactionID)
	buf.Write(x.BranchQualifier)
	return buf.Bytes()
}

// decodeXID deserializes an XID from the wire format.
func decodeXID(data []byte) (*XID, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("cubrid: XID data too short: %d bytes", len(data))
	}
	r := bytes.NewReader(data)
	formatID, err := protocol.ReadInt(r)
	if err != nil {
		return nil, err
	}
	gtridLen, err := protocol.ReadInt(r)
	if err != nil {
		return nil, err
	}
	bqualLen, err := protocol.ReadInt(r)
	if err != nil {
		return nil, err
	}

	gtrid, err := protocol.ReadBytes(r, int(gtridLen))
	if err != nil {
		return nil, err
	}
	bqual, err := protocol.ReadBytes(r, int(bqualLen))
	if err != nil {
		return nil, err
	}

	return &XID{
		FormatID:            formatID,
		GlobalTransactionID: gtrid,
		BranchQualifier:     bqual,
	}, nil
}

// XaPrepare prepares an XA transaction for commit (first phase of 2PC).
func XaPrepare(ctx context.Context, conn *sql.Conn, xid *XID) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: XaPrepare requires a cubrid connection")
		}
		return c.xaPrepare(ctx, xid)
	})
}

func (c *cubridConn) xaPrepare(ctx context.Context, xid *XID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer
	xidData := xid.encode()
	protocol.WriteInt(&buf, int32(len(xidData)))
	buf.Write(xidData)

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeXaPrepare, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}

// XaRecover retrieves a list of prepared (in-doubt) XA transactions.
func XaRecover(ctx context.Context, conn *sql.Conn) ([]*XID, error) {
	var xids []*XID
	err := conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: XaRecover requires a cubrid connection")
		}
		result, err := c.xaRecover(ctx)
		if err != nil {
			return err
		}
		xids = result
		return nil
	})
	return xids, err
}

func (c *cubridConn) xaRecover(ctx context.Context) ([]*XID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("cubrid: connection is closed")
	}

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeXaRecover, nil)
	if err != nil {
		return nil, err
	}
	if err := c.checkError(frame); err != nil {
		return nil, err
	}

	xidCount := int(frame.ResponseCode)
	if xidCount <= 0 {
		return nil, nil
	}

	xids := make([]*XID, 0, xidCount)
	r := bytes.NewReader(frame.Body)
	for i := 0; i < xidCount; i++ {
		// Each XID is length-prefixed.
		xidLen, err := protocol.ReadInt(r)
		if err != nil {
			break
		}
		xidData, err := protocol.ReadBytes(r, int(xidLen))
		if err != nil {
			break
		}
		xid, err := decodeXID(xidData)
		if err != nil {
			break
		}
		xids = append(xids, xid)
	}
	return xids, nil
}

// XaEndTranOp represents an XA end transaction operation.
type XaEndTranOp byte

const (
	// XaCommit commits a prepared XA transaction.
	XaCommit XaEndTranOp = 1
	// XaRollback rolls back a prepared XA transaction.
	XaRollback XaEndTranOp = 2
)

// XaEndTran commits or rolls back a prepared XA transaction (second phase of 2PC).
func XaEndTran(ctx context.Context, conn *sql.Conn, xid *XID, op XaEndTranOp) error {
	return conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*cubridConn)
		if !ok {
			return fmt.Errorf("cubrid: XaEndTran requires a cubrid connection")
		}
		return c.xaEndTran(ctx, xid, op)
	})
}

func (c *cubridConn) xaEndTran(ctx context.Context, xid *XID, op XaEndTranOp) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cubrid: connection is closed")
	}

	var buf bytes.Buffer

	// XID data.
	xidData := xid.encode()
	protocol.WriteInt(&buf, int32(len(xidData)))
	buf.Write(xidData)

	// Operation: commit or rollback.
	protocol.WriteInt(&buf, 1)
	protocol.WriteByte(&buf, byte(op))

	frame, err := c.sendRequestCtx(ctx, protocol.FuncCodeXaEndTran, buf.Bytes())
	if err != nil {
		return err
	}
	return c.checkError(frame)
}
