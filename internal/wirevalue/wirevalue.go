// Package wirevalue validates schema-typed exact numeric strings used by the
// Oversync wire contract.
package wirevalue

import (
	"fmt"
	"regexp"
	"strconv"
)

// NumericKind identifies the representation of one configured numeric column.
type NumericKind string

const (
	NumericKindExactInt64   NumericKind = "exact_int64"
	NumericKindExactDecimal NumericKind = "exact_decimal"
	NumericKindApproximate  NumericKind = "approximate"
)

var decimalPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?(0|[1-9][0-9]*))?$`)

// ParseInt64 validates the canonical exact-int64 string grammar and range.
func ParseInt64(raw string) (int64, error) {
	if raw == "" || raw == "-0" || !canonicalIntegerSpelling(raw) {
		return 0, fmt.Errorf("must use canonical signed 64-bit integer text")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("must be within signed 64-bit range")
	}
	return value, nil
}

// ValidateDecimal validates the accepted exact-decimal lexical grammar. It
// does not normalize precision: PostgreSQL's committed text is authoritative.
func ValidateDecimal(raw string) error {
	if raw == "" || !decimalPattern.MatchString(raw) || isNegativeDecimalZero(raw) {
		return fmt.Errorf("must use canonical finite decimal text")
	}
	return nil
}

func isNegativeDecimalZero(raw string) bool {
	if len(raw) == 0 || raw[0] != '-' {
		return false
	}
	for i := 1; i < len(raw); i++ {
		if raw[i] == 'e' || raw[i] == 'E' {
			break
		}
		if raw[i] >= '1' && raw[i] <= '9' {
			return false
		}
	}
	return true
}

func canonicalIntegerSpelling(raw string) bool {
	start := 0
	if raw[0] == '-' {
		start = 1
		if len(raw) == 1 {
			return false
		}
	}
	if raw[start] == '0' {
		return len(raw) == start+1
	}
	if raw[start] < '1' || raw[start] > '9' {
		return false
	}
	for i := start + 1; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false
		}
	}
	return true
}
