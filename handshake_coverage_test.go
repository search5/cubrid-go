package cubrid

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

// mockListener creates a local TCP server that sends a specific broker response.
func mockBrokerServer(t *testing.T, brokerResponse int32) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read client info exchange (ignore)
		buf := make([]byte, 256)
		conn.Read(buf)
		// Send broker response
		resp := make([]byte, 4)
		binary.BigEndian.PutUint32(resp, uint32(brokerResponse))
		conn.Write(resp)
	}()
	return ln
}

// Test handshake with broker refusing connection (negative response)
func TestHandshakeBrokerRefused(t *testing.T) {
	ln := mockBrokerServer(t, -1)
	defer ln.Close()

	addr := ln.Addr().(*net.TCPAddr)
	c := &cubridConn{
		casInfo: protocol.NewCASInfo(),
		dsn: DSN{
			Host:           "127.0.0.1",
			Port:           addr.Port,
			Database:       "test",
			User:           "dba",
			ConnectTimeout: 2 * time.Second,
		},
	}

	netConn, _ := net.DialTimeout("tcp", addr.String(), 2*time.Second)
	c.netConn = netConn

	err := c.handshake(context.Background())
	if err == nil {
		t.Fatal("expected error for refused connection")
	}
	t.Logf("Broker refused: %v", err)
}

// Test handshake with CAS port redirect (positive response > 0)
func TestHandshakeCASPortRedirect(t *testing.T) {
	// Broker returns new port number
	ln := mockBrokerServer(t, 44444) // redirect to port 44444
	defer ln.Close()

	addr := ln.Addr().(*net.TCPAddr)
	c := &cubridConn{
		casInfo: protocol.NewCASInfo(),
		dsn: DSN{
			Host:           "127.0.0.1",
			Port:           addr.Port,
			Database:       "test",
			User:           "dba",
			ConnectTimeout: 500 * time.Millisecond,
		},
	}

	netConn, _ := net.DialTimeout("tcp", addr.String(), 2*time.Second)
	c.netConn = netConn

	err := c.handshake(context.Background())
	// Will fail to connect to port 44444 (nothing listening), but exercises the redirect path
	if err == nil {
		t.Fatal("expected error for redirect to non-existent port")
	}
	t.Logf("CAS redirect error: %v", err)
}

// Test handshake with 0 response (reuse current connection) but bad auth
func TestHandshakeReuseConnectionBadAuth(t *testing.T) {
	// Broker returns 0 (reuse), then client sends OpenDatabase
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read client info exchange
		buf := make([]byte, 256)
		conn.Read(buf)
		// Send broker response = 0 (reuse)
		resp := make([]byte, 4)
		conn.Write(resp)
		// Read OpenDatabase request
		conn.Read(buf)
		// Send error response (negative cas_pid)
		var errResp bytes.Buffer
		// response_length (includes 4-byte CAS info)
		binary.Write(&errResp, binary.BigEndian, int32(4+4+4+5)) // casinfo + caspid + errorcode + msg
		// CAS info
		errResp.Write([]byte{0, 0xFF, 0xFF, 0xFF})
		// cas_pid = -1 (error)
		binary.Write(&errResp, binary.BigEndian, int32(-1))
		// error code
		binary.Write(&errResp, binary.BigEndian, int32(-1003))
		// error message
		errResp.Write([]byte("fail\x00"))
		conn.Write(errResp.Bytes())
	}()

	addr := ln.Addr().(*net.TCPAddr)
	c := &cubridConn{
		casInfo: protocol.NewCASInfo(),
		dsn: DSN{
			Host:           "127.0.0.1",
			Port:           addr.Port,
			Database:       "test",
			User:           "dba",
			ConnectTimeout: 2 * time.Second,
		},
	}

	netConn, _ := net.DialTimeout("tcp", addr.String(), 2*time.Second)
	c.netConn = netConn

	err = c.handshake(context.Background())
	if err == nil {
		t.Fatal("expected auth error")
	}
	t.Logf("Auth error: %v", err)
}
