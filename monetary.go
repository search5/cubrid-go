package cubrid

import (
	"database/sql/driver"
	"fmt"
	"math"
)

// Currency represents one of 24 CUBRID currency types defined in cas_protocol.h.
type Currency int32

const (
	CurrencyUSD Currency = 0  // US Dollar
	CurrencyJPY Currency = 1  // Japanese Yen
	CurrencyGBP Currency = 2  // British Pound Sterling
	CurrencyKRW Currency = 3  // South Korean Won
	CurrencyTRY Currency = 4  // Turkish Lira
	CurrencyKHR Currency = 5  // Cambodian Riel
	CurrencyCNY Currency = 6  // Chinese Renminbi
	CurrencyINR Currency = 7  // Indian Rupee
	CurrencyRUB Currency = 8  // Russian Ruble
	CurrencyAUD Currency = 9  // Australian Dollar
	CurrencyCAD Currency = 10 // Canadian Dollar
	CurrencyBRL Currency = 11 // Brazilian Real
	CurrencyRON Currency = 12 // Romanian Leu
	CurrencyEUR Currency = 13 // Euro
	CurrencyCHF Currency = 14 // Swiss Franc
	CurrencyDKK Currency = 15 // Danish Krone
	CurrencyNOK Currency = 16 // Norwegian Krone
	CurrencyBGN Currency = 17 // Bulgarian Lev
	CurrencyVND Currency = 18 // Vietnamese Dong
	CurrencyCZK Currency = 19 // Czech Koruna
	CurrencyPLN Currency = 20 // Polish Zloty
	CurrencySEK Currency = 21 // Swedish Krona
	CurrencyHRK Currency = 22 // Croatian Kuna
	CurrencyRSD Currency = 23 // Serbian Dinar
)

var currencyNames = [24]string{
	"USD", "JPY", "GBP", "KRW", "TRY", "KHR", "CNY", "INR",
	"RUB", "AUD", "CAD", "BRL", "RON", "EUR", "CHF", "DKK",
	"NOK", "BGN", "VND", "CZK", "PLN", "SEK", "HRK", "RSD",
}

// String returns the ISO currency code.
func (c Currency) String() string {
	if c >= 0 && c <= 23 {
		return currencyNames[c]
	}
	return fmt.Sprintf("Currency(%d)", c)
}

// CubridMonetary represents a CUBRID MONETARY value with an amount and currency.
//
// Wire format (CCI): currency_code(int32, 4 bytes) + amount(float64, 8 bytes) = 12 bytes.
type CubridMonetary struct {
	// Amount is the monetary value.
	Amount float64
	// Currency is the currency type.
	Currency Currency
}

// NewCubridMonetary creates a new CubridMonetary value.
func NewCubridMonetary(amount float64, currency Currency) *CubridMonetary {
	return &CubridMonetary{Amount: amount, Currency: currency}
}

// String returns a human-readable representation.
func (m *CubridMonetary) String() string {
	return fmt.Sprintf("%g %s", m.Amount, m.Currency)
}

// DriverValue implements driver.Valuer. The monetary value is sent as its float64 amount.
func (m *CubridMonetary) DriverValue() (driver.Value, error) {
	return m.Amount, nil
}

// Scan implements sql.Scanner. Accepts float64, int64, *CubridMonetary, or nil.
func (m *CubridMonetary) Scan(src interface{}) error {
	switch v := src.(type) {
	case float64:
		m.Amount = v
		m.Currency = CurrencyUSD
		return nil
	case int64:
		m.Amount = float64(v)
		m.Currency = CurrencyUSD
		return nil
	case *CubridMonetary:
		*m = *v
		return nil
	case nil:
		m.Amount = 0
		m.Currency = CurrencyUSD
		return nil
	default:
		return fmt.Errorf("cubrid: cannot scan %T into CubridMonetary", src)
	}
}

// Equal reports whether two CubridMonetary values are equal.
// NaN amounts are not considered equal (IEEE 754 behavior).
func (m *CubridMonetary) Equal(other *CubridMonetary) bool {
	if m == nil || other == nil {
		return m == other
	}
	return m.Currency == other.Currency &&
		!math.IsNaN(m.Amount) && !math.IsNaN(other.Amount) &&
		m.Amount == other.Amount
}
