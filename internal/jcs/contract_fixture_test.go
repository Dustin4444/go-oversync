package jcs

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// The language-neutral source is maintained by sqlitenow-kmp because KMP and
// Dart package it directly. Go keeps equivalent executable vectors here and a
// checksum that makes fixture drift explicit when the sibling checkout exists.
const authoritativeContractSHA256 = "152c66cec10a5355808c809c30d684aa68ed747c77d6be6491cc6d9587722552"

func TestAuthoritativeTypedNumericJCSVectors(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`333333333.33333329`, `333333333.3333333`},
		{`-0`, `0`},
		{`[1e30,4.50e-6,1e-7]`, `[1e+30,0.0000045,1e-7]`},
		{`[5e-324,-5e-324,1.7976931348623157e308,-1.7976931348623157e308,9007199254740992,-9007199254740992,295147905179352830000,9.999999999999997e22,1e23,1.0000000000000001e23,999999999999999700000,999999999999999900000,1e21,9.999999999999997e-7,0.000001,333333333.3333332,333333333.33333325,333333333.3333333,333333333.3333334,333333333.33333343,-0.0000033333333333333333,1424953923781206.2]`, `[5e-324,-5e-324,1.7976931348623157e+308,-1.7976931348623157e+308,9007199254740992,-9007199254740992,295147905179352830000,9.999999999999997e+22,1e+23,1.0000000000000001e+23,999999999999999700000,999999999999999900000,1e+21,9.999999999999997e-7,0.000001,333333333.3333332,333333333.33333325,333333333.3333333,333333333.3333334,333333333.33333343,-0.0000033333333333333333,1424953923781206.2]`},
		{`{"max":"9223372036854775807","above53":"9007199254740993","amount":"1234567890.123456789","large":"1e700"}`, `{"above53":"9007199254740993","amount":"1234567890.123456789","large":"1e700","max":"9223372036854775807"}`},
		{`{"\ue000":"bmp","\ud800\udc00":"supplementary"}`, "{\"𐀀\":\"supplementary\",\"\":\"bmp\"}"},
		{`{"nfd":"e\u0301","nfc":"\u00e9"}`, "{\"nfc\":\"é\",\"nfd\":\"é\"}"},
	}
	for _, test := range tests {
		got, err := Canonicalize([]byte(test.input))
		if err != nil {
			t.Fatalf("Canonicalize(%s): %v", test.input, err)
		}
		if string(got) != test.want {
			t.Fatalf("Canonicalize(%s) = %s, want %s", test.input, got, test.want)
		}
	}

	request := `[{"base_row_version":"0","key":{"id":"n-1"},"op":"INSERT","payload":{"amount":"1234567890.123456789","id":"n-1","max":"9223372036854775807"},"row_ordinal":"0","schema":"public","table":"exact_numbers"}]`
	committed := `[{"key":{"id":"n-1"},"op":"INSERT","payload":{"amount":"1234567890.123456789","id":"n-1","max":"9223372036854775807"},"row_ordinal":"0","row_version":"1","schema":"public","table":"exact_numbers"}]`
	requireSHA256(t, request, "f9952b89b7b7ce1f4a2b9a3ea31ee7e8ca05d19bb6cb2cd400c8d18a1043fc83")
	requireSHA256(t, committed, "e012c6db4263db7fe674f7d0ebd95192eb3551e995d5e971a7e9cd3f65bf70f0")
}

func TestAuthoritativeFixtureHasNotDrifted(t *testing.T) {
	path := filepath.Join("..", "..", "..", "sqlitenow-kmp", "oversqlite-contracts", "canonical-json", "jcs-typed-numerics.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("sqlitenow-kmp sibling checkout is not present")
	}
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != authoritativeContractSHA256 {
		t.Fatalf("authoritative fixture checksum = %s, update Go vectors and checksum with the shared fixture", got)
	}
}

func requireSHA256(t *testing.T, value, want string) {
	t.Helper()
	sum := sha256.Sum256([]byte(value))
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("SHA-256(%s) = %s, want %s", value, got, want)
	}
}
