package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BuildClientInfoExchange builds the 10-byte broker negotiation packet (Phase 1).
//
// Wire format:
//
//	[0-4]  Magic: "CUBRK" (TCP) or "CUBRS" (SSL)
//	[5]    Client type: 3 (JDBC-compatible)
//	[6]    Protocol version: 0x40 | version_number
//	[7]    Function flags: 0xC0
//	[8-9]  Reserved: 0x00
func BuildClientInfoExchange(version ProtocolVersion, ssl bool) []byte {
	pkt := make([]byte, 10)
	if ssl {
		copy(pkt[:5], "CUBRS")
	} else {
		copy(pkt[:5], "CUBRK")
	}
	pkt[5] = ClientTypeJDBC
	pkt[6] = 0x40 | byte(version)
	pkt[7] = BrokerRenewedErrorCode | BrokerSupportHoldableResult
	return pkt
}

// ParseBrokerResponse reads the 4-byte broker response (Phase 1).
// Returns:
//
//	> 0: new CAS port to reconnect to
//	  0: reuse current connection
//	< 0: connection refused (error code)
func ParseBrokerResponse(r io.Reader) (int32, error) {
	return ReadInt(r)
}

// BuildOpenDatabase builds the 628-byte database authentication packet (Phase 2).
//
// Wire format:
//
//	[0-31]    Database name (32 bytes, zero-padded)
//	[32-63]   User name (32 bytes, zero-padded)
//	[64-95]   Password (32 bytes, zero-padded, PLAINTEXT)
//	[96-607]  Extended info (512 bytes, reserved)
//	[608-627] Session ID (20 bytes, zeros for new connection)
func BuildOpenDatabase(database, user, password string) []byte {
	pkt := make([]byte, 628)
	copy(pkt[0:32], database)
	copy(pkt[32:64], user)
	copy(pkt[64:96], password)
	// Extended info and session ID remain zero.
	return pkt
}

// OpenDatabaseResponse holds the parsed Phase 2 handshake response.
type OpenDatabaseResponse struct {
	CASPID    int32
	CASIndex  int32       // Only present in PROTOCOL_V4+
	Broker    BrokerInfo
	SessionID [20]byte
}

// ParseOpenDatabaseResponse parses the body of an OpenDatabase response.
// The body format depends on the negotiated protocol version:
//
//	Pre-V3:  cas_pid(4) + broker_info(8) + session_id(4) = 16 bytes
//	V3:      cas_pid(4) + broker_info(8) + session_id(20) = 32 bytes
//	V4+:     cas_pid(4) + cas_id(4) + broker_info(8) + session_id(20) = 36 bytes
//
// The renewedErrorCode flag indicates whether the server uses the modern error format.
func ParseOpenDatabaseResponse(body []byte, renewedErrorCode bool) (*OpenDatabaseResponse, error) {
	if len(body) < 16 {
		return nil, fmt.Errorf("open database response too short: %d bytes", len(body))
	}

	resp := &OpenDatabaseResponse{}
	resp.CASPID = int32(binary.BigEndian.Uint32(body[0:4]))

	var brokerOffset int

	switch {
	case len(body) >= 36:
		// V4+: has cas_id
		resp.CASIndex = int32(binary.BigEndian.Uint32(body[4:8]))
		brokerOffset = 8
	default:
		// V3 or earlier: no cas_id
		brokerOffset = 4
	}

	var bi [8]byte
	copy(bi[:], body[brokerOffset:brokerOffset+8])
	resp.Broker = ParseBrokerInfo(bi)

	sessionOffset := brokerOffset + 8
	if resp.Broker.ProtocolVersion >= ProtocolV3 && sessionOffset+20 <= len(body) {
		copy(resp.SessionID[:], body[sessionOffset:sessionOffset+20])
	} else if sessionOffset+4 <= len(body) {
		// Pre-V3: only 4-byte session ID
		copy(resp.SessionID[:4], body[sessionOffset:sessionOffset+4])
	}

	return resp, nil
}
