package wirevalue

import "testing"

func TestValidateDecimal(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "0.0", "1234567890.123456789", "1e700", "-1.25E-20"} {
		if err := ValidateDecimal(value); err != nil {
			t.Errorf("ValidateDecimal(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "+1", "01", ".1", "1.", "1e01", "-0", "-0.0", "-0e10", "-0.000E-9"} {
		if err := ValidateDecimal(value); err == nil {
			t.Errorf("ValidateDecimal(%q) succeeded, want rejection", value)
		}
	}
}

func TestParseInt64(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "-9223372036854775808", "9223372036854775807"} {
		if _, err := ParseInt64(value); err != nil {
			t.Errorf("ParseInt64(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "-0", "+1", "01", "9223372036854775808", "-9223372036854775809"} {
		if _, err := ParseInt64(value); err == nil {
			t.Errorf("ParseInt64(%q) succeeded, want rejection", value)
		}
	}
}
