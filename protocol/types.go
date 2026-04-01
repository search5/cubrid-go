package protocol

// CubridType represents a CUBRID wire-level data type code.
type CubridType byte

const (
	CubridTypeNull      CubridType = 0
	CubridTypeChar      CubridType = 1
	CubridTypeString    CubridType = 2  // VARCHAR
	CubridTypeNChar     CubridType = 3
	CubridTypeVarNChar  CubridType = 4
	CubridTypeBit       CubridType = 5
	CubridTypeVarBit    CubridType = 6
	CubridTypeNumeric   CubridType = 7  // DECIMAL
	CubridTypeInt       CubridType = 8
	CubridTypeShort     CubridType = 9
	CubridTypeMonetary  CubridType = 10
	CubridTypeFloat     CubridType = 11
	CubridTypeDouble    CubridType = 12
	CubridTypeDate      CubridType = 13
	CubridTypeTime      CubridType = 14
	CubridTypeTimestamp  CubridType = 15
	CubridTypeSet       CubridType = 16
	CubridTypeMultiSet  CubridType = 17
	CubridTypeSequence  CubridType = 18
	CubridTypeObject    CubridType = 19 // OID
	CubridTypeResultSet CubridType = 20
	CubridTypeBigInt    CubridType = 21
	CubridTypeDatetime  CubridType = 22
	CubridTypeBlob      CubridType = 23
	CubridTypeClob      CubridType = 24
	CubridTypeEnum      CubridType = 25
	CubridTypeUShort    CubridType = 26 // PROTOCOL_V6+
	CubridTypeUInt      CubridType = 27 // PROTOCOL_V6+
	CubridTypeUBigInt   CubridType = 28 // PROTOCOL_V6+
	CubridTypeTsTz      CubridType = 29 // PROTOCOL_V7+
	CubridTypeTsLtz     CubridType = 30 // PROTOCOL_V7+
	CubridTypeDtTz      CubridType = 31 // PROTOCOL_V7+
	CubridTypeDtLtz     CubridType = 32 // PROTOCOL_V7+
	CubridTypeJSON      CubridType = 34 // PROTOCOL_V8+
)

// String returns the SQL type name for a CUBRID type code.
func (t CubridType) String() string {
	switch t {
	case CubridTypeNull:
		return "NULL"
	case CubridTypeChar:
		return "CHAR"
	case CubridTypeString:
		return "VARCHAR"
	case CubridTypeNChar:
		return "NCHAR"
	case CubridTypeVarNChar:
		return "VARNCHAR"
	case CubridTypeBit:
		return "BIT"
	case CubridTypeVarBit:
		return "VARBIT"
	case CubridTypeNumeric:
		return "NUMERIC"
	case CubridTypeInt:
		return "INT"
	case CubridTypeShort:
		return "SHORT"
	case CubridTypeMonetary:
		return "MONETARY"
	case CubridTypeFloat:
		return "FLOAT"
	case CubridTypeDouble:
		return "DOUBLE"
	case CubridTypeDate:
		return "DATE"
	case CubridTypeTime:
		return "TIME"
	case CubridTypeTimestamp:
		return "TIMESTAMP"
	case CubridTypeSet:
		return "SET"
	case CubridTypeMultiSet:
		return "MULTISET"
	case CubridTypeSequence:
		return "SEQUENCE"
	case CubridTypeObject:
		return "OBJECT"
	case CubridTypeResultSet:
		return "RESULTSET"
	case CubridTypeBigInt:
		return "BIGINT"
	case CubridTypeDatetime:
		return "DATETIME"
	case CubridTypeBlob:
		return "BLOB"
	case CubridTypeClob:
		return "CLOB"
	case CubridTypeEnum:
		return "ENUM"
	case CubridTypeUShort:
		return "USHORT"
	case CubridTypeUInt:
		return "UINT"
	case CubridTypeUBigInt:
		return "UBIGINT"
	case CubridTypeTsTz:
		return "TIMESTAMPTZ"
	case CubridTypeTsLtz:
		return "TIMESTAMPLTZ"
	case CubridTypeDtTz:
		return "DATETIMETZ"
	case CubridTypeDtLtz:
		return "DATETIMELTZ"
	case CubridTypeJSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}

// StmtType represents a CUBRID statement type returned by PREPARE.
type StmtType byte

const (
	StmtAlterTable   StmtType = 0
	StmtAlterSerial  StmtType = 1
	StmtCreateTable  StmtType = 4
	StmtCreateIndex  StmtType = 5
	StmtCreateSerial StmtType = 7
	StmtDropTable    StmtType = 9
	StmtDropIndex    StmtType = 10
	StmtDropSerial   StmtType = 13
	StmtEvaluate     StmtType = 14
	StmtInsert       StmtType = 20
	StmtSelect       StmtType = 21
	StmtUpdate       StmtType = 22
	StmtDelete       StmtType = 23
	StmtCall         StmtType = 24
	StmtGetIsoLvl    StmtType = 25
	StmtGetTimeout   StmtType = 26
	StmtGetOptLvl    StmtType = 27
	StmtGetTrigger   StmtType = 30
	StmtGetLdb       StmtType = 39
	StmtGetStats     StmtType = 41
	StmtSelectUpdate StmtType = 54
	StmtMerge        StmtType = 57
	StmtCallSp       StmtType = 0x7E
)

// IsQuery returns true if the statement type produces a result set.
func (s StmtType) IsQuery() bool {
	switch s {
	case StmtSelect, StmtCall, StmtCallSp, StmtEvaluate,
		StmtSelectUpdate, StmtGetStats, StmtGetIsoLvl,
		StmtGetTimeout, StmtGetOptLvl, StmtGetTrigger, StmtGetLdb:
		return true
	default:
		return false
	}
}

// ProtocolVersion represents a CCI protocol version.
type ProtocolVersion int

const (
	ProtocolV1  ProtocolVersion = 1  // Query timeout
	ProtocolV2  ProtocolVersion = 2  // Column metadata with result
	ProtocolV3  ProtocolVersion = 3  // 20-byte session ID
	ProtocolV4  ProtocolVersion = 4  // CAS index
	ProtocolV5  ProtocolVersion = 5  // Shard feature, fetch end flag
	ProtocolV6  ProtocolVersion = 6  // Unsigned integer types
	ProtocolV7  ProtocolVersion = 7  // Timezone types, collection encoding
	ProtocolV8  ProtocolVersion = 8  // JSON type
	ProtocolV9  ProtocolVersion = 9  // CAS health check
	ProtocolV10 ProtocolVersion = 10 // SSL/TLS
	ProtocolV11 ProtocolVersion = 11 // Out-of-band result set
	ProtocolV12 ProtocolVersion = 12 // Trailing zero removal

	ProtocolLatest = ProtocolV12
)

// TransactionOp represents a transaction operation for END_TRAN.
type TransactionOp byte

const (
	TranCommit   TransactionOp = 1
	TranRollback TransactionOp = 2
)

// Collection type flags encoded in the high bits of the type byte (PROTOCOL_V7+).
const (
	CollectionFlagMask byte = 0x60
	CollectionSet      byte = 0x20
	CollectionMultiSet byte = 0x40
	CollectionSequence byte = 0x60
	CollectionMarker   byte = 0x80 // High bit indicates collection type follows
)

// BrokerInfo holds the 8-byte broker metadata from the handshake response.
type BrokerInfo struct {
	DBMSType          byte
	KeepConnection    byte
	StatementPooling  byte
	CCIPermanent      byte
	ProtocolVersion   ProtocolVersion
	FunctionFlags     byte
	Reserved1         byte
	Reserved2         byte
}

// ParseBrokerInfo decodes 8 bytes into a BrokerInfo.
func ParseBrokerInfo(data [8]byte) BrokerInfo {
	pv := ProtocolVersion(data[4] & 0x3F) // strip 0x40 prefix
	return BrokerInfo{
		DBMSType:         data[0],
		KeepConnection:   data[1],
		StatementPooling: data[2],
		CCIPermanent:     data[3],
		ProtocolVersion:  pv,
		FunctionFlags:    data[5],
		Reserved1:        data[6],
		Reserved2:        data[7],
	}
}

// Broker function flags.
const (
	BrokerRenewedErrorCode      byte = 0x80
	BrokerSupportHoldableResult byte = 0x40
)

// Client type sent during broker negotiation.
const (
	ClientTypeJDBC byte = 3
)

// OID represents a CUBRID Object Identifier (8 bytes).
type OID struct {
	PageID int32
	SlotID int16
	VolID  int16
}
