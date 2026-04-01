package cubrid

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestDecodeTimestampTz(t *testing.T) {
	// Wire: year(2) month(2) day(2) hour(2) min(2) sec(2) + "Asia/Seoul\0"
	data := make([]byte, 12+11)
	binary.BigEndian.PutUint16(data[0:2], 2026)
	binary.BigEndian.PutUint16(data[2:4], 3)
	binary.BigEndian.PutUint16(data[4:6], 30)
	binary.BigEndian.PutUint16(data[6:8], 10)
	binary.BigEndian.PutUint16(data[8:10], 15)
	binary.BigEndian.PutUint16(data[10:12], 30)
	copy(data[12:], "Asia/Seoul\x00")

	val, err := decodeTimestampTz(data)
	if err != nil {
		t.Fatal(err)
	}

	loc, _ := time.LoadLocation("Asia/Seoul")
	want := time.Date(2026, 3, 30, 10, 15, 30, 0, loc)
	if !val.Equal(want) {
		t.Errorf("got %v, want %v", val, want)
	}
}

func TestDecodeTimestampTzUTC(t *testing.T) {
	data := make([]byte, 12+4)
	binary.BigEndian.PutUint16(data[0:2], 2026)
	binary.BigEndian.PutUint16(data[2:4], 1)
	binary.BigEndian.PutUint16(data[4:6], 1)
	binary.BigEndian.PutUint16(data[6:8], 0)
	binary.BigEndian.PutUint16(data[8:10], 0)
	binary.BigEndian.PutUint16(data[10:12], 0)
	copy(data[12:], "UTC\x00")

	val, err := decodeTimestampTz(data)
	if err != nil {
		t.Fatal(err)
	}
	if val.Location().String() != "UTC" {
		t.Errorf("location = %q", val.Location())
	}
}

func TestDecodeTimestampTzOffset(t *testing.T) {
	data := make([]byte, 12+7)
	binary.BigEndian.PutUint16(data[0:2], 2026)
	binary.BigEndian.PutUint16(data[2:4], 6)
	binary.BigEndian.PutUint16(data[4:6], 15)
	binary.BigEndian.PutUint16(data[6:8], 12)
	binary.BigEndian.PutUint16(data[8:10], 0)
	binary.BigEndian.PutUint16(data[10:12], 0)
	copy(data[12:], "+09:00\x00")

	val, err := decodeTimestampTz(data)
	if err != nil {
		t.Fatal(err)
	}
	_, offset := val.Zone()
	if offset != 9*3600 {
		t.Errorf("offset = %d, want %d", offset, 9*3600)
	}
}

func TestDecodeDatetimeTz(t *testing.T) {
	// Wire: year(2) month(2) day(2) hour(2) min(2) sec(2) msec(2) + "UTC\0"
	data := make([]byte, 14+4)
	binary.BigEndian.PutUint16(data[0:2], 2026)
	binary.BigEndian.PutUint16(data[2:4], 12)
	binary.BigEndian.PutUint16(data[4:6], 31)
	binary.BigEndian.PutUint16(data[6:8], 23)
	binary.BigEndian.PutUint16(data[8:10], 59)
	binary.BigEndian.PutUint16(data[10:12], 59)
	binary.BigEndian.PutUint16(data[12:14], 500)
	copy(data[14:], "UTC\x00")

	val, err := decodeDatetimeTz(data)
	if err != nil {
		t.Fatal(err)
	}

	want := time.Date(2026, 12, 31, 23, 59, 59, 500_000_000, time.UTC)
	if !val.Equal(want) {
		t.Errorf("got %v, want %v", val, want)
	}
}

func TestDecodeTzTooShort(t *testing.T) {
	_, err := decodeTimestampTz(make([]byte, 10))
	if err == nil {
		t.Error("expected error for short TIMESTAMPTZ")
	}
	_, err = decodeDatetimeTz(make([]byte, 13))
	if err == nil {
		t.Error("expected error for short DATETIMETZ")
	}
}

func TestParseTimezoneLocation(t *testing.T) {
	tests := []struct {
		tz     string
		offset int // expected offset in seconds, -1 to skip check
	}{
		{"UTC", 0},
		{"Asia/Seoul", 9 * 3600},
		{"+09:00", 9 * 3600},
		{"-05:00", -5 * 3600},
		{"+00:00", 0},
		{"America/New_York", -1}, // DST-dependent
	}
	for _, tt := range tests {
		loc, err := parseTimezoneLocation(tt.tz)
		if err != nil {
			t.Errorf("parseTimezoneLocation(%q): %v", tt.tz, err)
			continue
		}
		if tt.offset >= 0 {
			tm := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
			_, off := tm.Zone()
			if off != tt.offset {
				t.Errorf("%q: offset = %d, want %d", tt.tz, off, tt.offset)
			}
		}
	}
}
