package cubrid

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// Well-known CAS error codes.
const (
	ErrCodeDBMSError       int32 = -1000
	ErrCodeCASInternal     int32 = -1001
	ErrCodeOutOfMemory     int32 = -1002
	ErrCodeCommFail        int32 = -1003
	ErrCodeInvalidHandle   int32 = -1006
	ErrCodeBindMismatch    int32 = -1007
	ErrCodeUnknownType     int32 = -1008
	ErrCodeNoMoreData      int32 = -1012
	ErrCodeVersionMismatch int32 = -1016
	ErrCodeNotImplemented  int32 = -1100
)

// Sentinel errors for common CAS error codes.
// Use errors.Is to match.
var (
	ErrDBMSError       = &CubridError{Code: ErrCodeDBMSError}
	ErrCASInternal     = &CubridError{Code: ErrCodeCASInternal}
	ErrOutOfMemory     = &CubridError{Code: ErrCodeOutOfMemory}
	ErrCommFail        = &CubridError{Code: ErrCodeCommFail}
	ErrBindMismatch    = &CubridError{Code: ErrCodeBindMismatch}
	ErrUnknownType     = &CubridError{Code: ErrCodeUnknownType}
	ErrNoMoreData      = &CubridError{Code: ErrCodeNoMoreData}
	ErrVersionMismatch = &CubridError{Code: ErrCodeVersionMismatch}
	ErrNotImplemented  = &CubridError{Code: ErrCodeNotImplemented}
)

// CubridError represents an error returned by the CUBRID server.
type CubridError struct {
	Code    int32
	Message string
}

func (e *CubridError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("cubrid: error %d: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("cubrid: error %d", e.Code)
}

// Is reports whether the target matches this error by code.
func (e *CubridError) Is(target error) bool {
	t, ok := target.(*CubridError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// parseErrorResponse parses the body of a CAS error response.
// Format: error_code(4) + error_message(null-terminated)
func parseErrorResponse(body []byte) error {
	if len(body) < 4 {
		return &CubridError{Code: -1, Message: "malformed error response"}
	}

	code := int32(binary.BigEndian.Uint32(body[0:4]))
	msg := ""
	if len(body) > 4 {
		msg = strings.TrimRight(string(body[4:]), "\x00")
	}

	return &CubridError{Code: code, Message: msg}
}
