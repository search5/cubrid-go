package cubrid

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

// mockCASServer simulates a minimal CUBRID CAS server for unit testing.
// It handles the two-phase handshake and responds to CAS requests.
type mockCASServer struct {
	listener net.Listener
	mu       sync.Mutex
	handlers map[protocol.FuncCode]func(body []byte) (int32, []byte) // response_code, body
	t        *testing.T
	closed   bool
}

func newMockCASServer(t *testing.T) *mockCASServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &mockCASServer{
		listener: ln,
		handlers: make(map[protocol.FuncCode]func(body []byte) (int32, []byte)),
		t:        t,
	}
	go s.serve()
	return s
}

func (s *mockCASServer) addr() string {
	return s.listener.Addr().String()
}

func (s *mockCASServer) port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

func (s *mockCASServer) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.listener.Close()
}

func (s *mockCASServer) setHandler(fc protocol.FuncCode, h func(body []byte) (int32, []byte)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[fc] = h
}

func (s *mockCASServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *mockCASServer) handleConn(conn net.Conn) {
	defer conn.Close()

	// Phase 1: Read client info exchange (10 bytes).
	clientInfo := make([]byte, 10)
	if _, err := io.ReadFull(conn, clientInfo); err != nil {
		return
	}

	// Phase 1 response: CAS port = 0 (reuse connection).
	var resp1 [4]byte
	binary.BigEndian.PutUint32(resp1[:], 0)
	conn.Write(resp1[:])

	// Phase 2: Read open database (628 bytes).
	openDB := make([]byte, 628)
	if _, err := io.ReadFull(conn, openDB); err != nil {
		return
	}

	// Phase 2 response: Build OpenDatabase response.
	// Format: response_length(4) + cas_info(4) + body
	// Body: cas_pid(4) + cas_id(4) + broker_info(8) + session_id(20) = 36 bytes
	var body2 bytes.Buffer
	protocol.WriteInt(&body2, 12345)       // cas_pid
	protocol.WriteInt(&body2, 0)            // cas_id
	// broker_info: 8 bytes
	bi := [8]byte{
		0x02, // dbms_type
		0x01, // keep_connection
		0x01, // statement_pooling
		0x01, // cci_permanent
		0x40 | byte(protocol.ProtocolV5), // protocol version
		0xC0, // function flags
		0x00, 0x00,
	}
	body2.Write(bi[:])
	body2.Write(make([]byte, 20)) // session_id

	bodyBytes := body2.Bytes()
	var resp2 bytes.Buffer
	protocol.WriteInt(&resp2, int32(len(bodyBytes)+4)) // length includes cas_info
	resp2.Write([]byte{0, 0, 0, 0})                   // cas_info
	resp2.Write(bodyBytes)
	conn.Write(resp2.Bytes())

	// Now handle CAS requests.
	for {
		frame, fc, payload, err := s.readRequest(conn)
		if err != nil {
			return
		}
		_ = frame

		s.mu.Lock()
		handler, ok := s.handlers[fc]
		s.mu.Unlock()

		var respCode int32
		var respBody []byte
		if ok {
			respCode, respBody = handler(payload)
		} else {
			// Default: success with empty body.
			respCode = 0
			respBody = nil
		}

		s.writeResponse(conn, frame, respCode, respBody)
	}
}

func (s *mockCASServer) readRequest(conn net.Conn) (casInfo [4]byte, fc protocol.FuncCode, payload []byte, err error) {
	// Request format: payload_len(4) + cas_info(4) + func_code(1) + payload
	// payload_len = 1 + len(payload) (excludes cas_info)
	lenBuf := make([]byte, 4)
	if _, err = io.ReadFull(conn, lenBuf); err != nil {
		return
	}
	payloadLen := int(binary.BigEndian.Uint32(lenBuf))

	// Read cas_info (4 bytes, not included in payloadLen).
	if _, err = io.ReadFull(conn, casInfo[:]); err != nil {
		return
	}

	// Read payload (payloadLen bytes: func_code(1) + actual payload).
	data := make([]byte, payloadLen)
	if _, err = io.ReadFull(conn, data); err != nil {
		return
	}

	if len(data) >= 1 {
		fc = protocol.FuncCode(data[0])
		payload = data[1:]
	}
	return
}

func (s *mockCASServer) writeResponse(conn net.Conn, casInfo [4]byte, responseCode int32, body []byte) {
	// Standard CAS response framing:
	// [4 bytes] payload_len — size of payload EXCLUDING CAS info
	// [4 bytes] CAS info
	// [payload_len bytes] payload (response_code(4) + body)
	payloadLen := 4 + len(body) // response_code + body
	var resp bytes.Buffer
	binary.Write(&resp, binary.BigEndian, int32(payloadLen))
	resp.Write(casInfo[:])
	binary.Write(&resp, binary.BigEndian, responseCode)
	if len(body) > 0 {
		resp.Write(body)
	}
	conn.Write(resp.Bytes())
}
