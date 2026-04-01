package cubrid

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

func TestEncodeBindValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		wantType protocol.CubridType
	}{
		{"nil", nil, protocol.CubridTypeNull},
		{"int32", int32(42), protocol.CubridTypeInt},
		{"int64", int64(123456), protocol.CubridTypeBigInt},
		{"int", int(99), protocol.CubridTypeInt},
		{"int16", int16(7), protocol.CubridTypeShort},
		{"float32", float32(3.14), protocol.CubridTypeFloat},
		{"float64", float64(2.718), protocol.CubridTypeDouble},
		{"string", "hello", protocol.CubridTypeString},
		{"bytes", []byte{1, 2, 3}, protocol.CubridTypeVarBit},
		{"bool_true", true, protocol.CubridTypeShort},
		{"bool_false", false, protocol.CubridTypeShort},
		{"time", time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC), protocol.CubridTypeDatetime},
		{"json", json.RawMessage(`{"key":"val"}`), protocol.CubridTypeString},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, cubType, err := EncodeBindValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cubType != tt.wantType {
				t.Errorf("type = %v, want %v", cubType, tt.wantType)
			}
			if tt.value == nil {
				if len(data) != 0 {
					t.Errorf("nil should produce empty data, got %x", data)
				}
			} else if len(data) == 0 {
				t.Error("non-nil value produced empty data")
			}
		})
	}
}

func TestEncodeBindValueUnsupported(t *testing.T) {
	_, _, err := EncodeBindValue(struct{}{})
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestDecodeValue(t *testing.T) {
	tests := []struct {
		name     string
		cubType  protocol.CubridType
		data     []byte
		want     interface{}
	}{
		{
			name:    "int",
			cubType: protocol.CubridTypeInt,
			data:    []byte{0x00, 0x00, 0x00, 0x2A},
			want:    int32(42),
		},
		{
			name:    "short",
			cubType: protocol.CubridTypeShort,
			data:    []byte{0x00, 0x07},
			want:    int16(7),
		},
		{
			name:    "bigint",
			cubType: protocol.CubridTypeBigInt,
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xE2, 0x40},
			want:    int64(123456),
		},
		{
			name:    "float",
			cubType: protocol.CubridTypeFloat,
			data:    []byte{0x40, 0x48, 0xF5, 0xC3},
			want:    float32(3.14),
		},
		{
			name:    "double",
			cubType: protocol.CubridTypeDouble,
			data:    []byte{0x40, 0x09, 0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F},
			want:    float64(3.14),
		},
		{
			name:    "string",
			cubType: protocol.CubridTypeString,
			data:    append([]byte("hello"), 0x00),
			want:    "hello",
		},
		{
			name:    "char",
			cubType: protocol.CubridTypeChar,
			data:    append([]byte("abc"), 0x00),
			want:    "abc",
		},
		{
			name:    "date",
			cubType: protocol.CubridTypeDate,
			// year=2024(0x07E8), month=3(0x0003), day=15(0x000F)
			data: []byte{0x07, 0xE8, 0x00, 0x03, 0x00, 0x0F},
			want: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "time",
			cubType: protocol.CubridTypeTime,
			// hour=10, min=30, sec=45
			data: []byte{0x00, 0x0A, 0x00, 0x1E, 0x00, 0x2D},
			want: time.Date(0, 1, 1, 10, 30, 45, 0, time.UTC),
		},
		{
			name:    "timestamp",
			cubType: protocol.CubridTypeTimestamp,
			// date(6) + time(6)
			data: []byte{0x07, 0xE8, 0x00, 0x03, 0x00, 0x0F, 0x00, 0x0A, 0x00, 0x1E, 0x00, 0x2D},
			want: time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			name:    "datetime",
			cubType: protocol.CubridTypeDatetime,
			// date(6) + time(6) + msec(2)
			data: []byte{0x07, 0xE8, 0x00, 0x03, 0x00, 0x0F, 0x00, 0x0A, 0x00, 0x1E, 0x00, 0x2D, 0x01, 0xF4},
			want: time.Date(2024, 3, 15, 10, 30, 45, 500000000, time.UTC),
		},
		{
			name:    "varbit",
			cubType: protocol.CubridTypeVarBit,
			data:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
			want:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
		},
		{
			name:    "numeric as string",
			cubType: protocol.CubridTypeNumeric,
			data:    append([]byte("123.45"), 0x00),
			want:    &CubridNumeric{value: "123.45"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeValue(tt.cubType, tt.data)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestDecodeValueNull(t *testing.T) {
	got, err := DecodeValue(protocol.CubridTypeNull, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestScanTypeForCubridType(t *testing.T) {
	tests := []struct {
		cubType protocol.CubridType
		want    reflect.Type
	}{
		{protocol.CubridTypeInt, reflect.TypeOf(int32(0))},
		{protocol.CubridTypeShort, reflect.TypeOf(int16(0))},
		{protocol.CubridTypeBigInt, reflect.TypeOf(int64(0))},
		{protocol.CubridTypeFloat, reflect.TypeOf(float32(0))},
		{protocol.CubridTypeDouble, reflect.TypeOf(float64(0))},
		{protocol.CubridTypeString, reflect.TypeOf("")},
		{protocol.CubridTypeDate, reflect.TypeOf(time.Time{})},
		{protocol.CubridTypeVarBit, reflect.TypeOf([]byte{})},
		{protocol.CubridTypeJSON, reflect.TypeOf(&CubridJson{})},
	}
	for _, tt := range tests {
		got := ScanTypeForCubridType(tt.cubType)
		if got != tt.want {
			t.Errorf("ScanType(%v) = %v, want %v", tt.cubType, got, tt.want)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"int32", int32(-42)},
		{"int64", int64(9999999999)},
		{"float32", float32(1.5)},
		{"float64", float64(1.5)},
		{"string", "round trip test"},
		{"bytes", []byte{0x01, 0x02, 0x03}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, cubType, err := EncodeBindValue(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeValue(cubType, data)
			if err != nil {
				t.Fatal(err)
			}

			// For float32/float64, the round-trip should be exact since we use IEEE 754.
			switch v := tt.value.(type) {
			case float32:
				if got.(float32) != v {
					t.Errorf("round-trip: got %v, want %v", got, v)
				}
			case float64:
				if got.(float64) != v {
					t.Errorf("round-trip: got %v, want %v", got, v)
				}
			case []byte:
				if !bytes.Equal(got.([]byte), v) {
					t.Errorf("round-trip: got %x, want %x", got, v)
				}
			default:
				if !reflect.DeepEqual(got, tt.value) {
					t.Errorf("round-trip: got %v (%T), want %v (%T)", got, got, tt.value, tt.value)
				}
			}
		})
	}
}
