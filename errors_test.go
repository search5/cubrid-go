package cubrid

import (
	"errors"
	"testing"
)

func TestCubridError(t *testing.T) {
	err := &CubridError{
		Code:    -1008,
		Message: "Unknown type code",
	}

	if err.Error() != "cubrid: error -1008: Unknown type code" {
		t.Errorf("Error() = %q", err.Error())
	}
}

func TestCubridErrorIs(t *testing.T) {
	err := &CubridError{Code: ErrCodeNoMoreData}
	if !errors.Is(err, ErrNoMoreData) {
		t.Error("expected ErrNoMoreData match")
	}
}

func TestParseErrorResponse(t *testing.T) {
	// Error indicator (-1 = CAS), error code, error message
	body := []byte{
		0xFF, 0xFF, 0xFC, 0x18, // error code -1000
		't', 'a', 'b', 'l', 'e', ' ', 'n', 'o', 't', ' ', 'f', 'o', 'u', 'n', 'd', 0x00,
	}

	err := ParseErrorResponse(body)
	if err == nil {
		t.Fatal("expected error")
	}

	var cubErr *CubridError
	if !errors.As(err, &cubErr) {
		t.Fatal("expected *CubridError")
	}
	if cubErr.Code != -1000 {
		t.Errorf("Code = %d, want -1000", cubErr.Code)
	}
	if cubErr.Message != "table not found" {
		t.Errorf("Message = %q, want %q", cubErr.Message, "table not found")
	}
}

func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		code int32
		want error
	}{
		{ErrCodeDBMSError, ErrDBMSError},
		{ErrCodeCASInternal, ErrCASInternal},
		{ErrCodeOutOfMemory, ErrOutOfMemory},
		{ErrCodeCommFail, ErrCommFail},
		{ErrCodeNoMoreData, ErrNoMoreData},
		{ErrCodeBindMismatch, ErrBindMismatch},
		{ErrCodeUnknownType, ErrUnknownType},
		{ErrCodeVersionMismatch, ErrVersionMismatch},
		{ErrCodeNotImplemented, ErrNotImplemented},
	}
	for _, tt := range tests {
		err := &CubridError{Code: tt.code}
		if !errors.Is(err, tt.want) {
			t.Errorf("code %d: expected Is(%v) to be true", tt.code, tt.want)
		}
	}
}
