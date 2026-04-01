package cubrid

import (
	"fmt"
	"time"
)

// CubridTimestampTz represents a CUBRID TIMESTAMPTZ value: timestamp with explicit timezone.
// Unlike plain time.Time decoding, this preserves the original timezone string.
type CubridTimestampTz struct {
	// Time is the timestamp as a Go time.Time (with timezone applied).
	Time time.Time
	// Timezone is the original timezone identifier (e.g., "Asia/Seoul", "+09:00").
	Timezone string
}

// NewCubridTimestampTz creates a new CubridTimestampTz.
func NewCubridTimestampTz(t time.Time, timezone string) *CubridTimestampTz {
	return &CubridTimestampTz{Time: t, Timezone: timezone}
}

// String returns a human-readable representation.
func (ts *CubridTimestampTz) String() string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d %s",
		ts.Time.Year(), ts.Time.Month(), ts.Time.Day(),
		ts.Time.Hour(), ts.Time.Minute(), ts.Time.Second(),
		ts.Timezone)
}

// Scan implements sql.Scanner.
func (ts *CubridTimestampTz) Scan(src interface{}) error {
	switch v := src.(type) {
	case time.Time:
		ts.Time = v
		ts.Timezone = v.Location().String()
		return nil
	case *CubridTimestampTz:
		*ts = *v
		return nil
	case nil:
		ts.Time = time.Time{}
		ts.Timezone = ""
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridTimestampTz", src)
	}
}

// CubridTimestampLtz represents a CUBRID TIMESTAMPLTZ value: timestamp with local timezone.
// Wire format is identical to CubridTimestampTz; the distinction is semantic.
type CubridTimestampLtz struct {
	Time     time.Time
	Timezone string
}

// NewCubridTimestampLtz creates a new CubridTimestampLtz.
func NewCubridTimestampLtz(t time.Time, timezone string) *CubridTimestampLtz {
	return &CubridTimestampLtz{Time: t, Timezone: timezone}
}

// String returns a human-readable representation.
func (ts *CubridTimestampLtz) String() string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d %s",
		ts.Time.Year(), ts.Time.Month(), ts.Time.Day(),
		ts.Time.Hour(), ts.Time.Minute(), ts.Time.Second(),
		ts.Timezone)
}

// Scan implements sql.Scanner.
func (ts *CubridTimestampLtz) Scan(src interface{}) error {
	switch v := src.(type) {
	case time.Time:
		ts.Time = v
		ts.Timezone = v.Location().String()
		return nil
	case *CubridTimestampLtz:
		*ts = *v
		return nil
	case nil:
		ts.Time = time.Time{}
		ts.Timezone = ""
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridTimestampLtz", src)
	}
}

// CubridDateTimeTz represents a CUBRID DATETIMETZ value: datetime with explicit timezone.
// Includes millisecond precision.
type CubridDateTimeTz struct {
	Time     time.Time
	Timezone string
}

// NewCubridDateTimeTz creates a new CubridDateTimeTz.
func NewCubridDateTimeTz(t time.Time, timezone string) *CubridDateTimeTz {
	return &CubridDateTimeTz{Time: t, Timezone: timezone}
}

// String returns a human-readable representation with milliseconds.
func (dt *CubridDateTimeTz) String() string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d %s",
		dt.Time.Year(), dt.Time.Month(), dt.Time.Day(),
		dt.Time.Hour(), dt.Time.Minute(), dt.Time.Second(),
		dt.Time.Nanosecond()/1_000_000,
		dt.Timezone)
}

// Scan implements sql.Scanner.
func (dt *CubridDateTimeTz) Scan(src interface{}) error {
	switch v := src.(type) {
	case time.Time:
		dt.Time = v
		dt.Timezone = v.Location().String()
		return nil
	case *CubridDateTimeTz:
		*dt = *v
		return nil
	case nil:
		dt.Time = time.Time{}
		dt.Timezone = ""
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridDateTimeTz", src)
	}
}

// CubridDateTimeLtz represents a CUBRID DATETIMELTZ value: datetime with local timezone.
// Wire format is identical to CubridDateTimeTz; the distinction is semantic.
type CubridDateTimeLtz struct {
	Time     time.Time
	Timezone string
}

// NewCubridDateTimeLtz creates a new CubridDateTimeLtz.
func NewCubridDateTimeLtz(t time.Time, timezone string) *CubridDateTimeLtz {
	return &CubridDateTimeLtz{Time: t, Timezone: timezone}
}

// String returns a human-readable representation with milliseconds.
func (dt *CubridDateTimeLtz) String() string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%03d %s",
		dt.Time.Year(), dt.Time.Month(), dt.Time.Day(),
		dt.Time.Hour(), dt.Time.Minute(), dt.Time.Second(),
		dt.Time.Nanosecond()/1_000_000,
		dt.Timezone)
}

// Scan implements sql.Scanner.
func (dt *CubridDateTimeLtz) Scan(src interface{}) error {
	switch v := src.(type) {
	case time.Time:
		dt.Time = v
		dt.Timezone = v.Location().String()
		return nil
	case *CubridDateTimeLtz:
		*dt = *v
		return nil
	case nil:
		dt.Time = time.Time{}
		dt.Timezone = ""
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridDateTimeLtz", src)
	}
}
