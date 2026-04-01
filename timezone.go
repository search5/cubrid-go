package cubrid

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// decodeTimestampTz decodes a TIMESTAMPTZ or TIMESTAMPLTZ wire value into time.Time.
// Wire format: year(2) + month(2) + day(2) + hour(2) + min(2) + sec(2) + tz_string(null-terminated)
func decodeTimestampTz(data []byte) (time.Time, error) {
	if len(data) < 13 {
		return time.Time{}, fmt.Errorf("cubrid: TIMESTAMPTZ requires at least 13 bytes, got %d", len(data))
	}

	year := int(binary.BigEndian.Uint16(data[0:2]))
	month := time.Month(binary.BigEndian.Uint16(data[2:4]))
	day := int(binary.BigEndian.Uint16(data[4:6]))
	hour := int(binary.BigEndian.Uint16(data[6:8]))
	min := int(binary.BigEndian.Uint16(data[8:10]))
	sec := int(binary.BigEndian.Uint16(data[10:12]))

	tzStr := strings.TrimRight(string(data[12:]), "\x00")
	loc, err := parseTimezoneLocation(tzStr)
	if err != nil {
		// Fallback to UTC if timezone cannot be resolved.
		loc = time.UTC
	}

	return time.Date(year, month, day, hour, min, sec, 0, loc), nil
}

// decodeDatetimeTz decodes a DATETIMETZ or DATETIMELTZ wire value into time.Time.
// Wire format: year(2) + month(2) + day(2) + hour(2) + min(2) + sec(2) + msec(2) + tz_string(null-terminated)
func decodeDatetimeTz(data []byte) (time.Time, error) {
	if len(data) < 15 {
		return time.Time{}, fmt.Errorf("cubrid: DATETIMETZ requires at least 15 bytes, got %d", len(data))
	}

	year := int(binary.BigEndian.Uint16(data[0:2]))
	month := time.Month(binary.BigEndian.Uint16(data[2:4]))
	day := int(binary.BigEndian.Uint16(data[4:6]))
	hour := int(binary.BigEndian.Uint16(data[6:8]))
	min := int(binary.BigEndian.Uint16(data[8:10]))
	sec := int(binary.BigEndian.Uint16(data[10:12]))
	msec := int(binary.BigEndian.Uint16(data[12:14]))

	tzStr := strings.TrimRight(string(data[14:]), "\x00")
	loc, err := parseTimezoneLocation(tzStr)
	if err != nil {
		loc = time.UTC
	}

	return time.Date(year, month, day, hour, min, sec, msec*1_000_000, loc), nil
}

// parseTimezoneLocation resolves a timezone string to a *time.Location.
// Accepts IANA identifiers ("Asia/Seoul", "UTC") and offset formats ("+09:00", "-05:00").
func parseTimezoneLocation(tz string) (*time.Location, error) {
	if tz == "" {
		return time.UTC, nil
	}

	// Try IANA timezone name first.
	loc, err := time.LoadLocation(tz)
	if err == nil {
		return loc, nil
	}

	// Try offset format: +HH:MM or -HH:MM.
	if len(tz) >= 6 && (tz[0] == '+' || tz[0] == '-') {
		parts := strings.SplitN(tz[1:], ":", 2)
		if len(parts) == 2 {
			hours, err1 := strconv.Atoi(parts[0])
			mins, err2 := strconv.Atoi(parts[1])
			if err1 == nil && err2 == nil {
				offset := hours*3600 + mins*60
				if tz[0] == '-' {
					offset = -offset
				}
				return time.FixedZone(tz, offset), nil
			}
		}
	}

	return nil, fmt.Errorf("cubrid: unknown timezone %q", tz)
}
