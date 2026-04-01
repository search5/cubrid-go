package cubrid

import (
	"database/sql/driver"
	"fmt"
)

// CubridEnum represents a CUBRID ENUM value with both a member name
// and its 1-based ordinal index.
//
// The wire format transmits only the member name as a null-terminated string.
// The Value field defaults to 0 when decoded from the wire since the ordinal
// is not part of the wire encoding.
type CubridEnum struct {
	// Name is the enum member label.
	Name string
	// Value is the 1-based ordinal index of the enum member.
	// Defaults to 0 when decoded from wire (ordinal not transmitted).
	Value int16
}

// NewCubridEnum creates a new CubridEnum with the given name and ordinal value.
func NewCubridEnum(name string, value int16) *CubridEnum {
	return &CubridEnum{Name: name, Value: value}
}

// String returns a string representation of the enum value.
func (e *CubridEnum) String() string {
	return fmt.Sprintf("%s(%d)", e.Name, e.Value)
}

// Value implements driver.Valuer. The enum is sent as its string name.
func (e *CubridEnum) DriverValue() (driver.Value, error) {
	return e.Name, nil
}

// Scan implements sql.Scanner. Accepts string or []byte values.
func (e *CubridEnum) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		e.Name = v
		e.Value = 0
		return nil
	case []byte:
		e.Name = string(v)
		e.Value = 0
		return nil
	case *CubridEnum:
		*e = *v
		return nil
	case nil:
		e.Name = ""
		e.Value = 0
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridEnum", src)
	}
}
