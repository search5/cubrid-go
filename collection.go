package cubrid

import (
	"bytes"
	"database/sql/driver"
	"fmt"

	"github.com/search5/cubrid-go/protocol"
)

// CubridSet represents a CUBRID SET collection (unordered, unique elements).
type CubridSet struct {
	Type     protocol.CubridType // Element type.
	Elements []interface{}
}

// Value implements driver.Valuer.
func (s *CubridSet) Value() (driver.Value, error) {
	return s.Elements, nil
}

// Scan implements sql.Scanner.
func (s *CubridSet) Scan(src interface{}) error {
	switch v := src.(type) {
	case *CubridSet:
		*s = *v
		return nil
	case []interface{}:
		s.Elements = v
		return nil
	case nil:
		s.Elements = nil
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridSet", src)
	}
}

// CubridMultiSet represents a CUBRID MULTISET collection (unordered, allows duplicates).
type CubridMultiSet struct {
	Type     protocol.CubridType
	Elements []interface{}
}

// Value implements driver.Valuer.
func (s *CubridMultiSet) Value() (driver.Value, error) {
	return s.Elements, nil
}

// Scan implements sql.Scanner.
func (s *CubridMultiSet) Scan(src interface{}) error {
	switch v := src.(type) {
	case *CubridMultiSet:
		*s = *v
		return nil
	case []interface{}:
		s.Elements = v
		return nil
	case nil:
		s.Elements = nil
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridMultiSet", src)
	}
}

// CubridSequence represents a CUBRID SEQUENCE (LIST) collection (ordered).
type CubridSequence struct {
	Type     protocol.CubridType
	Elements []interface{}
}

// Value implements driver.Valuer.
func (s *CubridSequence) Value() (driver.Value, error) {
	return s.Elements, nil
}

// Scan implements sql.Scanner.
func (s *CubridSequence) Scan(src interface{}) error {
	switch v := src.(type) {
	case *CubridSequence:
		*s = *v
		return nil
	case []interface{}:
		s.Elements = v
		return nil
	case nil:
		s.Elements = nil
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridSequence", src)
	}
}

// DecodeCollectionValue decodes a wire-format collection into a Go collection type.
// The wire format for collection data is a sequence of length-prefixed elements:
//
//	[4-byte element_size][element_data] repeated for each element
//
// Each element is decoded according to the element type using DecodeValue.
// A zero-length element (size <= 0) is decoded as nil (NULL).
func decodeCollectionValue(collType, elemType protocol.CubridType, data []byte) (interface{}, error) {
	elements, err := decodeCollectionElements(elemType, data)
	if err != nil {
		return nil, err
	}

	switch collType {
	case protocol.CubridTypeSet:
		return &CubridSet{Type: elemType, Elements: elements}, nil
	case protocol.CubridTypeMultiSet:
		return &CubridMultiSet{Type: elemType, Elements: elements}, nil
	case protocol.CubridTypeSequence:
		return &CubridSequence{Type: elemType, Elements: elements}, nil
	default:
		return &CubridSequence{Type: elemType, Elements: elements}, nil
	}
}

// decodeCollectionElements parses the element list from collection wire data.
//
// Wire format:
//
//	[1-byte element_type_code]
//	[4-byte element_count]
//	[repeated: [4-byte size][data]]
//
// The element_type_code from the wire overrides the metadata element type
// if they differ (the wire value is authoritative).
func decodeCollectionElements(elemType protocol.CubridType, data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return []interface{}{}, nil
	}

	r := bytes.NewReader(data)

	// Read the wire element type code.
	wireType, err := protocol.ReadByte(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read collection element type: %w", err)
	}
	actualType := protocol.CubridType(wireType)
	if actualType != protocol.CubridTypeNull && elemType != protocol.CubridTypeNull {
		actualType = elemType // Prefer metadata type if available.
	}

	// Read element count.
	count, err := protocol.ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("cubrid: read collection element count: %w", err)
	}
	if count <= 0 {
		return []interface{}{}, nil
	}

	elements := make([]interface{}, 0, count)
	for i := int32(0); i < count; i++ {
		size, err := protocol.ReadInt(r)
		if err != nil {
			return elements, nil // Partial read at end is OK.
		}

		if size <= 0 {
			elements = append(elements, nil) // NULL element.
			continue
		}

		elemData, err := protocol.ReadBytes(r, int(size))
		if err != nil {
			return elements, fmt.Errorf("cubrid: read collection element data: %w", err)
		}

		val, err := decodeValue(actualType, elemData)
		if err != nil {
			return elements, fmt.Errorf("cubrid: decode collection element: %w", err)
		}
		elements = append(elements, val)
	}

	return elements, nil
}
