package cubrid

import (
	"database/sql/driver"
	"fmt"
	"strings"
)

// CubridNumeric represents a CUBRID NUMERIC/DECIMAL value as a string,
// preserving exact decimal precision without floating-point rounding.
//
// Wire format: null-terminated ASCII string (e.g., "3.14\0", "-100.5\0").
type CubridNumeric struct {
	value string
}

// NewCubridNumeric creates a CubridNumeric without validation.
// Use this for server-originated values where the string is known to be well-formed.
func NewCubridNumeric(s string) *CubridNumeric {
	return &CubridNumeric{value: s}
}

// TryNewCubridNumeric creates a CubridNumeric with validation.
// The string must match the pattern: optional minus, digits, optional dot + digits.
// Leading/trailing whitespace is stripped.
func TryNewCubridNumeric(s string) (*CubridNumeric, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil, fmt.Errorf("cubrid: NUMERIC string must not be empty")
	}

	b := []byte(trimmed)
	i := 0

	// Optional leading minus.
	if b[i] == '-' {
		i++
		if i >= len(b) {
			return nil, fmt.Errorf("cubrid: NUMERIC string contains only a minus sign")
		}
	}

	// At least one digit before optional decimal point.
	start := i
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		i++
	}
	if i == start {
		return nil, fmt.Errorf("cubrid: NUMERIC string has no digits before decimal point: %q", trimmed)
	}

	// Optional decimal part.
	if i < len(b) {
		if b[i] == '.' {
			i++
			fracStart := i
			for i < len(b) && b[i] >= '0' && b[i] <= '9' {
				i++
			}
			if i == fracStart {
				return nil, fmt.Errorf("cubrid: NUMERIC string has no digits after decimal point: %q", trimmed)
			}
		}
	}

	if i != len(b) {
		return nil, fmt.Errorf("cubrid: NUMERIC string contains invalid character at position %d: %q", i, trimmed)
	}

	return &CubridNumeric{value: trimmed}, nil
}

// String returns the decimal string representation.
func (n *CubridNumeric) String() string {
	return n.value
}

// IsValid returns true if the value is a valid NUMERIC representation.
func (n *CubridNumeric) IsValid() bool {
	_, err := TryNewCubridNumeric(n.value)
	return err == nil
}

// DriverValue implements driver.Valuer. Sent as string.
func (n *CubridNumeric) DriverValue() (driver.Value, error) {
	return n.value, nil
}

// Scan implements sql.Scanner.
func (n *CubridNumeric) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		n.value = v
		return nil
	case []byte:
		n.value = string(v)
		return nil
	case *CubridNumeric:
		*n = *v
		return nil
	case nil:
		n.value = ""
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridNumeric", src)
	}
}
