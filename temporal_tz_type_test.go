package cubrid

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/search5/cubrid-go/protocol"
)

func buildTimestampTzData(year, month, day, hour, min, sec int, tz string) []byte {
	data := make([]byte, 12+len(tz)+1)
	binary.BigEndian.PutUint16(data[0:2], uint16(year))
	binary.BigEndian.PutUint16(data[2:4], uint16(month))
	binary.BigEndian.PutUint16(data[4:6], uint16(day))
	binary.BigEndian.PutUint16(data[6:8], uint16(hour))
	binary.BigEndian.PutUint16(data[8:10], uint16(min))
	binary.BigEndian.PutUint16(data[10:12], uint16(sec))
	copy(data[12:], tz)
	data[12+len(tz)] = 0x00
	return data
}

func buildDateTimeTzData(year, month, day, hour, min, sec, msec int, tz string) []byte {
	data := make([]byte, 14+len(tz)+1)
	binary.BigEndian.PutUint16(data[0:2], uint16(year))
	binary.BigEndian.PutUint16(data[2:4], uint16(month))
	binary.BigEndian.PutUint16(data[4:6], uint16(day))
	binary.BigEndian.PutUint16(data[6:8], uint16(hour))
	binary.BigEndian.PutUint16(data[8:10], uint16(min))
	binary.BigEndian.PutUint16(data[10:12], uint16(sec))
	binary.BigEndian.PutUint16(data[12:14], uint16(msec))
	copy(data[14:], tz)
	data[14+len(tz)] = 0x00
	return data
}

func TestDecodeValueTimestampTz(t *testing.T) {
	data := buildTimestampTzData(2026, 3, 30, 10, 15, 30, "Asia/Seoul")
	val, err := DecodeValue(protocol.CubridTypeTsTz, data)
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := val.(*CubridTimestampTz)
	if !ok {
		t.Fatalf("expected *CubridTimestampTz, got %T", val)
	}
	if ts.Timezone != "Asia/Seoul" {
		t.Fatalf("timezone: got %q, want %q", ts.Timezone, "Asia/Seoul")
	}
	if ts.Time.Year() != 2026 || ts.Time.Month() != 3 || ts.Time.Day() != 30 {
		t.Fatalf("date: got %v", ts.Time)
	}
}

func TestDecodeValueTimestampLtz(t *testing.T) {
	data := buildTimestampTzData(2026, 7, 4, 18, 30, 0, "UTC")
	val, err := DecodeValue(protocol.CubridTypeTsLtz, data)
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := val.(*CubridTimestampLtz)
	if !ok {
		t.Fatalf("expected *CubridTimestampLtz, got %T", val)
	}
	if ts.Timezone != "UTC" {
		t.Fatalf("timezone: got %q, want %q", ts.Timezone, "UTC")
	}
}

func TestDecodeValueDateTimeTz(t *testing.T) {
	data := buildDateTimeTzData(2026, 12, 25, 23, 59, 59, 500, "America/New_York")
	val, err := DecodeValue(protocol.CubridTypeDtTz, data)
	if err != nil {
		t.Fatal(err)
	}
	dt, ok := val.(*CubridDateTimeTz)
	if !ok {
		t.Fatalf("expected *CubridDateTimeTz, got %T", val)
	}
	if dt.Timezone != "America/New_York" {
		t.Fatalf("timezone: got %q, want %q", dt.Timezone, "America/New_York")
	}
	if dt.Time.Nanosecond()/1_000_000 != 500 {
		t.Fatalf("milliseconds: got %d, want 500", dt.Time.Nanosecond()/1_000_000)
	}
}

func TestDecodeValueDateTimeLtz(t *testing.T) {
	data := buildDateTimeTzData(2026, 6, 15, 12, 0, 0, 0, "Europe/London")
	val, err := DecodeValue(protocol.CubridTypeDtLtz, data)
	if err != nil {
		t.Fatal(err)
	}
	dt, ok := val.(*CubridDateTimeLtz)
	if !ok {
		t.Fatalf("expected *CubridDateTimeLtz, got %T", val)
	}
	if dt.Timezone != "Europe/London" {
		t.Fatalf("timezone: got %q, want %q", dt.Timezone, "Europe/London")
	}
}

func TestTimestampTzString(t *testing.T) {
	ts := NewCubridTimestampTz(
		time.Date(2026, 3, 30, 10, 15, 30, 0, time.UTC),
		"Asia/Seoul",
	)
	got := ts.String()
	want := "2026-03-30 10:15:30 Asia/Seoul"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestDateTimeTzString(t *testing.T) {
	dt := NewCubridDateTimeTz(
		time.Date(2026, 12, 25, 23, 59, 59, 500_000_000, time.UTC),
		"UTC",
	)
	got := dt.String()
	want := "2026-12-25 23:59:59.500 UTC"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTimestampTzScan(t *testing.T) {
	var ts CubridTimestampTz
	now := time.Now()
	if err := ts.Scan(now); err != nil {
		t.Fatal(err)
	}
	if ts.Time != now {
		t.Fatal("time mismatch")
	}
}

func TestScanTypeForTzTypes(t *testing.T) {
	tests := []struct {
		ct   protocol.CubridType
		want reflect.Type
	}{
		{protocol.CubridTypeTsTz, reflect.TypeOf(&CubridTimestampTz{})},
		{protocol.CubridTypeTsLtz, reflect.TypeOf(&CubridTimestampLtz{})},
		{protocol.CubridTypeDtTz, reflect.TypeOf(&CubridDateTimeTz{})},
		{protocol.CubridTypeDtLtz, reflect.TypeOf(&CubridDateTimeLtz{})},
	}
	for _, tt := range tests {
		got := ScanTypeForCubridType(tt.ct)
		if got != tt.want {
			t.Errorf("ScanType(%v): got %v, want %v", tt.ct, got, tt.want)
		}
	}
}

func TestTimezonePreserved(t *testing.T) {
	// Verify the original timezone string is preserved, not converted to Go Location name.
	data := buildTimestampTzData(2026, 1, 1, 0, 0, 0, "+09:00")
	val, _ := DecodeValue(protocol.CubridTypeTsTz, data)
	ts := val.(*CubridTimestampTz)
	if ts.Timezone != "+09:00" {
		t.Fatalf("timezone string not preserved: got %q, want %q", ts.Timezone, "+09:00")
	}
}
