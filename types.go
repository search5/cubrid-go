package cubrid

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

// encodeBindValue encodes a Go value into the CCI wire format for parameter binding.
// Returns the encoded bytes (excluding the length prefix), the CUBRID type, and any error.
// NULL values return empty data with CubridTypeNull.
func encodeBindValue(v interface{}) ([]byte, protocol.CubridType, error) {
	if v == nil {
		return nil, protocol.CubridTypeNull, nil
	}

	switch val := v.(type) {
	case int16:
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(val))
		return b, protocol.CubridTypeShort, nil

	case int32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(val))
		return b, protocol.CubridTypeInt, nil

	case int:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(int32(val)))
		return b, protocol.CubridTypeInt, nil

	case int64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(val))
		return b, protocol.CubridTypeBigInt, nil

	case float32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, math.Float32bits(val))
		return b, protocol.CubridTypeFloat, nil

	case float64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, math.Float64bits(val))
		return b, protocol.CubridTypeDouble, nil

	case string:
		// Null-terminated string (without the length prefix; caller adds it).
		b := append([]byte(val), 0x00)
		return b, protocol.CubridTypeString, nil

	case []byte:
		return val, protocol.CubridTypeVarBit, nil

	case bool:
		b := make([]byte, 2)
		if val {
			binary.BigEndian.PutUint16(b, 1)
		}
		return b, protocol.CubridTypeShort, nil

	case time.Time:
		b := make([]byte, 14)
		binary.BigEndian.PutUint16(b[0:2], uint16(val.Year()))
		binary.BigEndian.PutUint16(b[2:4], uint16(val.Month()))
		binary.BigEndian.PutUint16(b[4:6], uint16(val.Day()))
		binary.BigEndian.PutUint16(b[6:8], uint16(val.Hour()))
		binary.BigEndian.PutUint16(b[8:10], uint16(val.Minute()))
		binary.BigEndian.PutUint16(b[10:12], uint16(val.Second()))
		binary.BigEndian.PutUint16(b[12:14], uint16(val.Nanosecond()/1_000_000))
		return b, protocol.CubridTypeDatetime, nil

	case json.RawMessage:
		b := append([]byte(val), 0x00)
		return b, protocol.CubridTypeString, nil

	case *CubridNumeric:
		b := append([]byte(val.value), 0x00)
		return b, protocol.CubridTypeString, nil

	case CubridNumeric:
		b := append([]byte(val.value), 0x00)
		return b, protocol.CubridTypeString, nil

	case *CubridJson:
		b := append([]byte(val.value), 0x00)
		return b, protocol.CubridTypeString, nil

	case CubridJson:
		b := append([]byte(val.value), 0x00)
		return b, protocol.CubridTypeString, nil

	case *CubridEnum:
		b := append([]byte(val.Name), 0x00)
		return b, protocol.CubridTypeString, nil

	case CubridEnum:
		b := append([]byte(val.Name), 0x00)
		return b, protocol.CubridTypeString, nil

	case *CubridMonetary:
		b := make([]byte, 12)
		binary.BigEndian.PutUint32(b[0:4], uint32(val.Currency))
		binary.BigEndian.PutUint64(b[4:12], math.Float64bits(val.Amount))
		return b, protocol.CubridTypeMonetary, nil

	case CubridMonetary:
		b := make([]byte, 12)
		binary.BigEndian.PutUint32(b[0:4], uint32(val.Currency))
		binary.BigEndian.PutUint64(b[4:12], math.Float64bits(val.Amount))
		return b, protocol.CubridTypeMonetary, nil

	default:
		return nil, 0, fmt.Errorf("cubrid: unsupported bind type %T", v)
	}
}

// decodeValue decodes a wire-format value into a Go type.
// NULL values (CubridTypeNull or nil data) return nil.
func decodeValue(cubType protocol.CubridType, data []byte) (interface{}, error) {
	if cubType == protocol.CubridTypeNull || data == nil {
		return nil, nil
	}

	switch cubType {
	case protocol.CubridTypeShort:
		if len(data) < 2 {
			return nil, fmt.Errorf("cubrid: SHORT requires 2 bytes, got %d", len(data))
		}
		return int16(binary.BigEndian.Uint16(data[:2])), nil

	case protocol.CubridTypeInt:
		if len(data) < 4 {
			return nil, fmt.Errorf("cubrid: INT requires 4 bytes, got %d", len(data))
		}
		return int32(binary.BigEndian.Uint32(data[:4])), nil

	case protocol.CubridTypeBigInt:
		if len(data) < 8 {
			return nil, fmt.Errorf("cubrid: BIGINT requires 8 bytes, got %d", len(data))
		}
		return int64(binary.BigEndian.Uint64(data[:8])), nil

	case protocol.CubridTypeUShort:
		if len(data) < 2 {
			return nil, fmt.Errorf("cubrid: USHORT requires 2 bytes, got %d", len(data))
		}
		return binary.BigEndian.Uint16(data[:2]), nil

	case protocol.CubridTypeUInt:
		if len(data) < 4 {
			return nil, fmt.Errorf("cubrid: UINT requires 4 bytes, got %d", len(data))
		}
		return binary.BigEndian.Uint32(data[:4]), nil

	case protocol.CubridTypeUBigInt:
		if len(data) < 8 {
			return nil, fmt.Errorf("cubrid: UBIGINT requires 8 bytes, got %d", len(data))
		}
		return binary.BigEndian.Uint64(data[:8]), nil

	case protocol.CubridTypeFloat:
		if len(data) < 4 {
			return nil, fmt.Errorf("cubrid: FLOAT requires 4 bytes, got %d", len(data))
		}
		bits := binary.BigEndian.Uint32(data[:4])
		return math.Float32frombits(bits), nil

	case protocol.CubridTypeDouble:
		if len(data) < 8 {
			return nil, fmt.Errorf("cubrid: DOUBLE requires 8 bytes, got %d", len(data))
		}
		bits := binary.BigEndian.Uint64(data[:8])
		return math.Float64frombits(bits), nil

	case protocol.CubridTypeMonetary:
		if len(data) < 8 {
			return nil, fmt.Errorf("cubrid: MONETARY requires 8 bytes, got %d", len(data))
		}
		bits := binary.BigEndian.Uint64(data[:8])
		return &CubridMonetary{Amount: math.Float64frombits(bits), Currency: CurrencyUSD}, nil

	case protocol.CubridTypeString, protocol.CubridTypeChar, protocol.CubridTypeNChar, protocol.CubridTypeVarNChar:
		return strings.TrimRight(string(data), "\x00"), nil

	case protocol.CubridTypeNumeric:
		return &CubridNumeric{value: strings.TrimRight(string(data), "\x00")}, nil

	case protocol.CubridTypeDate:
		if len(data) < 6 {
			return nil, fmt.Errorf("cubrid: DATE requires 6 bytes, got %d", len(data))
		}
		year := int(binary.BigEndian.Uint16(data[0:2]))
		month := time.Month(binary.BigEndian.Uint16(data[2:4]))
		day := int(binary.BigEndian.Uint16(data[4:6]))
		return time.Date(year, month, day, 0, 0, 0, 0, time.UTC), nil

	case protocol.CubridTypeTime:
		if len(data) < 6 {
			return nil, fmt.Errorf("cubrid: TIME requires 6 bytes, got %d", len(data))
		}
		hour := int(binary.BigEndian.Uint16(data[0:2]))
		min := int(binary.BigEndian.Uint16(data[2:4]))
		sec := int(binary.BigEndian.Uint16(data[4:6]))
		return time.Date(0, 1, 1, hour, min, sec, 0, time.UTC), nil

	case protocol.CubridTypeTimestamp:
		if len(data) < 12 {
			return nil, fmt.Errorf("cubrid: TIMESTAMP requires 12 bytes, got %d", len(data))
		}
		year := int(binary.BigEndian.Uint16(data[0:2]))
		month := time.Month(binary.BigEndian.Uint16(data[2:4]))
		day := int(binary.BigEndian.Uint16(data[4:6]))
		hour := int(binary.BigEndian.Uint16(data[6:8]))
		min := int(binary.BigEndian.Uint16(data[8:10]))
		sec := int(binary.BigEndian.Uint16(data[10:12]))
		return time.Date(year, month, day, hour, min, sec, 0, time.UTC), nil

	case protocol.CubridTypeDatetime:
		if len(data) < 14 {
			return nil, fmt.Errorf("cubrid: DATETIME requires 14 bytes, got %d", len(data))
		}
		year := int(binary.BigEndian.Uint16(data[0:2]))
		month := time.Month(binary.BigEndian.Uint16(data[2:4]))
		day := int(binary.BigEndian.Uint16(data[4:6]))
		hour := int(binary.BigEndian.Uint16(data[6:8]))
		min := int(binary.BigEndian.Uint16(data[8:10]))
		sec := int(binary.BigEndian.Uint16(data[10:12]))
		msec := int(binary.BigEndian.Uint16(data[12:14]))
		return time.Date(year, month, day, hour, min, sec, msec*1_000_000, time.UTC), nil

	case protocol.CubridTypeTsTz:
		t, err := decodeTimestampTz(data)
		if err != nil {
			return nil, err
		}
		tzStr := strings.TrimRight(string(data[12:]), "\x00")
		return &CubridTimestampTz{Time: t, Timezone: tzStr}, nil

	case protocol.CubridTypeTsLtz:
		t, err := decodeTimestampTz(data)
		if err != nil {
			return nil, err
		}
		tzStr := strings.TrimRight(string(data[12:]), "\x00")
		return &CubridTimestampLtz{Time: t, Timezone: tzStr}, nil

	case protocol.CubridTypeDtTz:
		t, err := decodeDatetimeTz(data)
		if err != nil {
			return nil, err
		}
		tzStr := strings.TrimRight(string(data[14:]), "\x00")
		return &CubridDateTimeTz{Time: t, Timezone: tzStr}, nil

	case protocol.CubridTypeDtLtz:
		t, err := decodeDatetimeTz(data)
		if err != nil {
			return nil, err
		}
		tzStr := strings.TrimRight(string(data[14:]), "\x00")
		return &CubridDateTimeLtz{Time: t, Timezone: tzStr}, nil

	case protocol.CubridTypeBit, protocol.CubridTypeVarBit:
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil

	case protocol.CubridTypeBlob, protocol.CubridTypeClob:
		// LOB columns return a handle (locator), not the actual data.
		// If the data is large enough to be a LOB handle (>= 17 bytes), decode it.
		if len(data) >= 17 {
			handle, err := decodeLobHandle(data)
			if err == nil {
				return handle, nil
			}
		}
		// Fallback: return raw bytes for small inline data.
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil

	case protocol.CubridTypeEnum:
		return &CubridEnum{Name: strings.TrimRight(string(data), "\x00")}, nil

	case protocol.CubridTypeJSON:
		return &CubridJson{value: strings.TrimRight(string(data), "\x00")}, nil

	case protocol.CubridTypeObject:
		return decodeCubridOid(data)

	default:
		// Return raw bytes for unrecognized types.
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	}
}

// scanTypeForCubridType returns the Go reflect.Type appropriate for scanning a CUBRID column.
func scanTypeForCubridType(ct protocol.CubridType) reflect.Type {
	switch ct {
	case protocol.CubridTypeShort:
		return reflect.TypeOf(int16(0))
	case protocol.CubridTypeInt:
		return reflect.TypeOf(int32(0))
	case protocol.CubridTypeBigInt:
		return reflect.TypeOf(int64(0))
	case protocol.CubridTypeUShort:
		return reflect.TypeOf(uint16(0))
	case protocol.CubridTypeUInt:
		return reflect.TypeOf(uint32(0))
	case protocol.CubridTypeUBigInt:
		return reflect.TypeOf(uint64(0))
	case protocol.CubridTypeFloat:
		return reflect.TypeOf(float32(0))
	case protocol.CubridTypeDouble:
		return reflect.TypeOf(float64(0))
	case protocol.CubridTypeMonetary:
		return reflect.TypeOf(&CubridMonetary{})
	case protocol.CubridTypeString, protocol.CubridTypeChar, protocol.CubridTypeNChar,
		protocol.CubridTypeVarNChar:
		return reflect.TypeOf("")
	case protocol.CubridTypeNumeric:
		return reflect.TypeOf(&CubridNumeric{})
	case protocol.CubridTypeEnum:
		return reflect.TypeOf(&CubridEnum{})
	case protocol.CubridTypeBlob, protocol.CubridTypeClob:
		return reflect.TypeOf(&CubridLobHandle{})
	case protocol.CubridTypeDate, protocol.CubridTypeTime, protocol.CubridTypeTimestamp,
		protocol.CubridTypeDatetime:
		return reflect.TypeOf(time.Time{})
	case protocol.CubridTypeTsTz:
		return reflect.TypeOf(&CubridTimestampTz{})
	case protocol.CubridTypeTsLtz:
		return reflect.TypeOf(&CubridTimestampLtz{})
	case protocol.CubridTypeDtTz:
		return reflect.TypeOf(&CubridDateTimeTz{})
	case protocol.CubridTypeDtLtz:
		return reflect.TypeOf(&CubridDateTimeLtz{})
	case protocol.CubridTypeBit, protocol.CubridTypeVarBit:
		return reflect.TypeOf([]byte{})
	case protocol.CubridTypeJSON:
		return reflect.TypeOf(&CubridJson{})
	case protocol.CubridTypeObject:
		return reflect.TypeOf(&CubridOid{})
	case protocol.CubridTypeSet:
		return reflect.TypeOf(&CubridSet{})
	case protocol.CubridTypeMultiSet:
		return reflect.TypeOf(&CubridMultiSet{})
	case protocol.CubridTypeSequence:
		return reflect.TypeOf(&CubridSequence{})
	default:
		return reflect.TypeOf([]byte{})
	}
}
