package cubrid

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestCurrencyString(t *testing.T) {
	tests := []struct {
		c    Currency
		want string
	}{
		{CurrencyUSD, "USD"},
		{CurrencyJPY, "JPY"},
		{CurrencyGBP, "GBP"},
		{CurrencyKRW, "KRW"},
		{CurrencyEUR, "EUR"},
		{CurrencyCNY, "CNY"},
		{CurrencyRSD, "RSD"},
		{Currency(99), "Currency(99)"},
		{Currency(-1), "Currency(-1)"},
	}
	for _, tt := range tests {
		got := tt.c.String()
		if got != tt.want {
			t.Errorf("Currency(%d).String() = %q, want %q", tt.c, got, tt.want)
		}
	}
}

func TestAllCurrencyCodes(t *testing.T) {
	expected := []string{
		"USD", "JPY", "GBP", "KRW", "TRY", "KHR", "CNY", "INR",
		"RUB", "AUD", "CAD", "BRL", "RON", "EUR", "CHF", "DKK",
		"NOK", "BGN", "VND", "CZK", "PLN", "SEK", "HRK", "RSD",
	}
	for i, want := range expected {
		got := Currency(i).String()
		if got != want {
			t.Errorf("Currency(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestCubridMonetaryNew(t *testing.T) {
	m := NewCubridMonetary(42.5, CurrencyEUR)
	if m.Amount != 42.5 {
		t.Fatalf("Amount: got %f, want 42.5", m.Amount)
	}
	if m.Currency != CurrencyEUR {
		t.Fatalf("Currency: got %v, want EUR", m.Currency)
	}
}

func TestCubridMonetaryString(t *testing.T) {
	m := NewCubridMonetary(100.5, CurrencyKRW)
	got := m.String()
	if got != "100.5 KRW" {
		t.Fatalf("String(): got %q, want %q", got, "100.5 KRW")
	}
}

func TestCubridMonetaryDriverValue(t *testing.T) {
	m := NewCubridMonetary(99.99, CurrencyUSD)
	v, err := m.DriverValue()
	if err != nil {
		t.Fatal(err)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", v)
	}
	if f != 99.99 {
		t.Fatalf("DriverValue: got %f, want 99.99", f)
	}
}

func TestCubridMonetaryScan(t *testing.T) {
	tests := []struct {
		name   string
		src    interface{}
		amount float64
	}{
		{"float64", float64(42.5), 42.5},
		{"int64", int64(100), 100.0},
		{"monetary_ptr", NewCubridMonetary(99.0, CurrencyEUR), 99.0},
		{"nil", nil, 0.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m CubridMonetary
			if err := m.Scan(tt.src); err != nil {
				t.Fatal(err)
			}
			if m.Amount != tt.amount {
				t.Fatalf("Amount: got %f, want %f", m.Amount, tt.amount)
			}
		})
	}
}

func TestCubridMonetaryScanCopiesCurrency(t *testing.T) {
	src := NewCubridMonetary(50.0, CurrencyJPY)
	var m CubridMonetary
	if err := m.Scan(src); err != nil {
		t.Fatal(err)
	}
	if m.Currency != CurrencyJPY {
		t.Fatalf("Currency: got %v, want JPY", m.Currency)
	}
}

func TestCubridMonetaryScanError(t *testing.T) {
	var m CubridMonetary
	if err := m.Scan("not a number"); err == nil {
		t.Fatal("expected error scanning string into CubridMonetary")
	}
}

func TestDecodeValueMonetary(t *testing.T) {
	amount := 123.456
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, math.Float64bits(amount))

	val, err := DecodeValue(protocol.CubridTypeMonetary, data)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := val.(*CubridMonetary)
	if !ok {
		t.Fatalf("expected *CubridMonetary, got %T", val)
	}
	if math.Abs(m.Amount-amount) > 1e-10 {
		t.Fatalf("Amount: got %f, want %f", m.Amount, amount)
	}
	if m.Currency != CurrencyUSD {
		t.Fatalf("Currency: got %v, want USD (default)", m.Currency)
	}
}

func TestDecodeValueMonetaryTooShort(t *testing.T) {
	_, err := DecodeValue(protocol.CubridTypeMonetary, []byte{0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error for short monetary data")
	}
}

func TestEncodeBindValueMonetary(t *testing.T) {
	m := NewCubridMonetary(99.99, CurrencyEUR)
	data, cubType, err := EncodeBindValue(m)
	if err != nil {
		t.Fatal(err)
	}
	if cubType != protocol.CubridTypeDouble {
		t.Fatalf("type: got %d, want %d", cubType, protocol.CubridTypeDouble)
	}
	if len(data) != 8 {
		t.Fatalf("data len: got %d, want 8", len(data))
	}
	decoded := math.Float64frombits(binary.BigEndian.Uint64(data))
	if math.Abs(decoded-99.99) > 1e-10 {
		t.Fatalf("decoded amount: got %f, want 99.99", decoded)
	}
}

func TestEncodeBindValueMonetaryByValue(t *testing.T) {
	m := CubridMonetary{Amount: -42.5, Currency: CurrencyGBP}
	data, cubType, err := EncodeBindValue(m)
	if err != nil {
		t.Fatal(err)
	}
	if cubType != protocol.CubridTypeDouble {
		t.Fatalf("type: got %d, want %d", cubType, protocol.CubridTypeDouble)
	}
	decoded := math.Float64frombits(binary.BigEndian.Uint64(data))
	if decoded != -42.5 {
		t.Fatalf("decoded amount: got %f, want -42.5", decoded)
	}
}

func TestScanTypeForMonetary(t *testing.T) {
	got := ScanTypeForCubridType(protocol.CubridTypeMonetary)
	want := reflect.TypeOf(&CubridMonetary{})
	if got != want {
		t.Fatalf("ScanType: got %v, want %v", got, want)
	}
}

func TestCubridMonetaryEqual(t *testing.T) {
	a := NewCubridMonetary(100.0, CurrencyUSD)
	b := NewCubridMonetary(100.0, CurrencyUSD)
	c := NewCubridMonetary(100.0, CurrencyEUR)
	d := NewCubridMonetary(200.0, CurrencyUSD)

	if !a.Equal(b) {
		t.Fatal("expected a == b")
	}
	if a.Equal(c) {
		t.Fatal("expected a != c (different currency)")
	}
	if a.Equal(d) {
		t.Fatal("expected a != d (different amount)")
	}
	if a.Equal(nil) {
		t.Fatal("expected a != nil")
	}

	nan := NewCubridMonetary(math.NaN(), CurrencyUSD)
	if nan.Equal(nan) {
		t.Fatal("NaN should not equal NaN")
	}
}

func TestDecodeValueDoubleUnchanged(t *testing.T) {
	// Verify DOUBLE still returns float64 (not *CubridMonetary).
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, math.Float64bits(3.14))
	val, err := DecodeValue(protocol.CubridTypeDouble, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := val.(float64); !ok {
		t.Fatalf("expected float64 for DOUBLE, got %T", val)
	}
}
