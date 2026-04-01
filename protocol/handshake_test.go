package protocol

import (
	"bytes"
	"testing"
)

func TestBuildClientInfoExchange(t *testing.T) {
	pkt := BuildClientInfoExchange(ProtocolV12, false)

	if len(pkt) != 10 {
		t.Fatalf("packet length = %d, want 10", len(pkt))
	}

	// Magic string "CUBRK"
	if string(pkt[:5]) != "CUBRK" {
		t.Errorf("magic = %q, want %q", pkt[:5], "CUBRK")
	}

	// Client type = JDBC (3)
	if pkt[5] != ClientTypeJDBC {
		t.Errorf("client type = %d, want %d", pkt[5], ClientTypeJDBC)
	}

	// Protocol version byte: 0x40 | 12 = 0x4C
	if pkt[6] != 0x4C {
		t.Errorf("protocol version byte = %x, want 0x4C", pkt[6])
	}

	// Function flags: BROKER_RENEWED_ERROR_CODE | BROKER_SUPPORT_HOLDABLE_RESULT = 0xC0
	if pkt[7] != 0xC0 {
		t.Errorf("function flags = %x, want 0xC0", pkt[7])
	}

	// Reserved bytes
	if pkt[8] != 0 || pkt[9] != 0 {
		t.Errorf("reserved = [%x, %x], want [0, 0]", pkt[8], pkt[9])
	}
}

func TestBuildClientInfoExchangeSSL(t *testing.T) {
	pkt := BuildClientInfoExchange(ProtocolV10, true)
	if string(pkt[:5]) != "CUBRS" {
		t.Errorf("magic = %q, want %q for SSL", pkt[:5], "CUBRS")
	}
}

func TestParseBrokerResponse(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		wantPort int32
	}{
		{"new port", []byte{0x00, 0x00, 0x81, 0x11}, 33041},
		{"reuse", []byte{0x00, 0x00, 0x00, 0x00}, 0},
		{"error", []byte{0xFF, 0xFF, 0xFF, 0xFF}, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			port, err := ParseBrokerResponse(r)
			if err != nil {
				t.Fatal(err)
			}
			if port != tt.wantPort {
				t.Errorf("port = %d, want %d", port, tt.wantPort)
			}
		})
	}
}

func TestBuildOpenDatabase(t *testing.T) {
	pkt := BuildOpenDatabase("demodb", "dba", "secret")

	if len(pkt) != 628 {
		t.Fatalf("packet length = %d, want 628", len(pkt))
	}

	// Database name at offset 0-31
	dbName := string(bytes.TrimRight(pkt[0:32], "\x00"))
	if dbName != "demodb" {
		t.Errorf("db = %q, want %q", dbName, "demodb")
	}

	// User at offset 32-63
	user := string(bytes.TrimRight(pkt[32:64], "\x00"))
	if user != "dba" {
		t.Errorf("user = %q, want %q", user, "dba")
	}

	// Password at offset 64-95
	pass := string(bytes.TrimRight(pkt[64:96], "\x00"))
	if pass != "secret" {
		t.Errorf("password = %q, want %q", pass, "secret")
	}

	// Extended info (96-607) should be zeros
	for i := 96; i < 608; i++ {
		if pkt[i] != 0 {
			t.Fatalf("extended info byte[%d] = %x, want 0", i, pkt[i])
		}
	}

	// Session ID (608-627) should be zeros for new connection
	for i := 608; i < 628; i++ {
		if pkt[i] != 0 {
			t.Fatalf("session id byte[%d] = %x, want 0", i, pkt[i])
		}
	}
}

func TestParseOpenDatabaseResponseV4(t *testing.T) {
	// Build a V4+ response: cas_pid(4) + cas_id(4) + broker_info(8) + session_id(20) = 36 bytes
	var body bytes.Buffer
	WriteInt(&body, 1234)  // cas_pid
	WriteInt(&body, 5)     // cas_id
	// broker_info: DBMS=1, keepConn=1, stmtPool=1, cciPerm=0, protoVer=0x4C, flags=0xC0, res, res
	body.Write([]byte{0x01, 0x01, 0x01, 0x00, 0x4C, 0xC0, 0x00, 0x00})
	body.Write(make([]byte, 20)) // session_id

	resp, err := ParseOpenDatabaseResponse(body.Bytes(), true)
	if err != nil {
		t.Fatal(err)
	}
	if resp.CASPID != 1234 {
		t.Errorf("CASPID = %d, want 1234", resp.CASPID)
	}
	if resp.CASIndex != 5 {
		t.Errorf("CASIndex = %d, want 5", resp.CASIndex)
	}
	if resp.Broker.ProtocolVersion != ProtocolV12 {
		t.Errorf("ProtocolVersion = %d, want %d", resp.Broker.ProtocolVersion, ProtocolV12)
	}
	if resp.Broker.StatementPooling != 1 {
		t.Errorf("StatementPooling = %d, want 1", resp.Broker.StatementPooling)
	}
}

func TestParseOpenDatabaseResponseV3(t *testing.T) {
	// V3 response: cas_pid(4) + broker_info(8) + session_id(20) = 32 bytes
	var body bytes.Buffer
	WriteInt(&body, 999) // cas_pid
	body.Write([]byte{0x01, 0x01, 0x01, 0x00, 0x43, 0xC0, 0x00, 0x00}) // proto V3
	body.Write(make([]byte, 20))

	resp, err := ParseOpenDatabaseResponse(body.Bytes(), true)
	if err != nil {
		t.Fatal(err)
	}
	if resp.CASPID != 999 {
		t.Errorf("CASPID = %d, want 999", resp.CASPID)
	}
	if resp.Broker.ProtocolVersion != ProtocolV3 {
		t.Errorf("ProtocolVersion = %d, want %d", resp.Broker.ProtocolVersion, ProtocolV3)
	}
}

func TestParseBrokerInfo(t *testing.T) {
	data := [8]byte{0x01, 0x01, 0x01, 0x00, 0x4C, 0xC0, 0x00, 0x00}
	bi := ParseBrokerInfo(data)

	if bi.DBMSType != 1 {
		t.Errorf("DBMSType = %d, want 1", bi.DBMSType)
	}
	if bi.ProtocolVersion != ProtocolV12 {
		t.Errorf("ProtocolVersion = %d, want %d", bi.ProtocolVersion, ProtocolV12)
	}
	if bi.FunctionFlags != 0xC0 {
		t.Errorf("FunctionFlags = %x, want 0xC0", bi.FunctionFlags)
	}
}
