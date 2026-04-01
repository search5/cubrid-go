package cubrid

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

// mockConn simulates a net.Conn that fails on read/write for testing.
type mockConn struct {
	readErr  error
	writeErr error
	closed   bool
}

func (m *mockConn) Read(b []byte) (int, error)  { return 0, m.readErr }
func (m *mockConn) Write(b []byte) (int, error) { return len(b), m.writeErr }
func (m *mockConn) Close() error                { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr         { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr        { return &net.TCPAddr{} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error   { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error  { return nil }

// Test sendRequest with write failure (non-connection-lost)
func TestSendRequestWriteError(t *testing.T) {
	c := &cubridConn{
		netConn: &mockConn{writeErr: io.ErrClosedPipe},
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "localhost", Port: 33100, Database: "test"},
	}
	_, err := c.sendRequest(protocol.FuncCodeGetDBVersion, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	t.Logf("sendRequest write error: %v", err)
}

// Test sendRequest with read failure (connection lost -> reconnect)
func TestSendRequestReadEOF(t *testing.T) {
	c := &cubridConn{
		netConn: &mockConn{readErr: io.EOF},
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, Database: "test", ConnectTimeout: 100 * time.Millisecond},
	}
	_, err := c.sendRequest(protocol.FuncCodeGetDBVersion, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	t.Logf("sendRequest EOF -> reconnect exhausted: %v", err)
}

// Test reconnectAndRetry directly
func TestReconnectAndRetryFail(t *testing.T) {
	c := &cubridConn{
		netConn: &mockConn{},
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, Database: "test", ConnectTimeout: 100 * time.Millisecond},
	}
	_, err := c.reconnectAndRetry(protocol.FuncCodeGetDBVersion, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	t.Logf("reconnectAndRetry: %v", err)
	if !c.closed {
		t.Fatal("connection should be marked closed")
	}
}

// Test sendRequestCtx with cancelled context
func TestSendRequestCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	c := &cubridConn{
		netConn: &mockConn{readErr: io.EOF},
		casInfo: protocol.NewCASInfo(),
		dsn:     DSN{Host: "192.0.2.1", Port: 33000, Database: "test", ConnectTimeout: 100 * time.Millisecond},
	}
	_, err := c.sendRequestCtx(ctx, protocol.FuncCodeGetDBVersion, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	t.Logf("cancelled context: %v", err)
}

// Test checkError paths
func TestCheckErrorPaths(t *testing.T) {
	c := &cubridConn{}

	// Positive response code (no error)
	frame := &protocol.ResponseFrame{ResponseCode: 1}
	if err := c.checkError(frame); err != nil {
		t.Fatal(err)
	}

	// Negative with body
	frame2 := &protocol.ResponseFrame{
		ResponseCode: -1000,
		Body:         []byte{0xFF, 0xFF, 0xFC, 0x18, 't', 'e', 's', 't', 0},
	}
	err := c.checkError(frame2)
	if err == nil {
		t.Fatal("expected error")
	}
	t.Logf("checkError: %v", err)

	// Negative without body
	frame3 := &protocol.ResponseFrame{ResponseCode: -1}
	err = c.checkError(frame3)
	if err == nil {
		t.Fatal("expected error")
	}
}

// Test readOpenDatabaseResponse error path
func TestReadOpenDatabaseResponseError(t *testing.T) {
	c := &cubridConn{
		netConn: &mockConn{readErr: io.EOF},
		casInfo: protocol.NewCASInfo(),
	}
	_, err := c.readOpenDatabaseResponse()
	if err == nil {
		t.Fatal("expected error")
	}
	t.Logf("readOpenDatabaseResponse error: %v", err)
}
