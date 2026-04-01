package cubrid

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// CubridJson represents a CUBRID JSON document value.
//
// On the wire, JSON is serialized as a null-terminated UTF-8 string.
// This type wraps the raw JSON text without parsing or validation.
// Available on CUBRID 11.2+ (PROTOCOL_V8).
type CubridJson struct {
	value string
}

// NewCubridJson creates a new CubridJson from a JSON string.
func NewCubridJson(jsonStr string) *CubridJson {
	return &CubridJson{value: jsonStr}
}

// String returns the raw JSON string.
func (j *CubridJson) String() string {
	return j.value
}

// Unmarshal parses the JSON into the given value (like json.Unmarshal).
func (j *CubridJson) Unmarshal(v interface{}) error {
	return json.Unmarshal([]byte(j.value), v)
}

// MarshalFrom serializes the given value into this CubridJson (like json.Marshal).
func MarshalCubridJson(v interface{}) (*CubridJson, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &CubridJson{value: string(data)}, nil
}

// DriverValue implements driver.Valuer. Sent as string.
func (j *CubridJson) DriverValue() (driver.Value, error) {
	return j.value, nil
}

// Scan implements sql.Scanner.
func (j *CubridJson) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		j.value = v
		return nil
	case []byte:
		j.value = string(v)
		return nil
	case json.RawMessage:
		j.value = string(v)
		return nil
	case *CubridJson:
		*j = *v
		return nil
	case nil:
		j.value = ""
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridJson", src)
	}
}
