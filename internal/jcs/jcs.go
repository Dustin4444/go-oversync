// Package jcs exposes Oversync's RFC 8785 canonical JSON boundary.
//
// Exact database integers and decimals are strings before they reach this
// package. JSON number tokens are therefore limited to the ordinary JCS
// binary64 domain; this package intentionally has no arbitrary-precision
// number extension.
package jcs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

// Canonicalize validates raw JSON and returns its RFC 8785 representation.
// Oversync hash inputs are always objects or arrays, as required by the
// reference implementation.
func Canonicalize(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("JCS input is not valid UTF-8")
	}
	value, err := validateSingleJSONValue(raw)
	if err != nil {
		return nil, err
	}
	input := raw
	scalar := false
	if _, ok := value.(map[string]any); !ok {
		if _, ok := value.([]any); !ok {
			scalar = true
			input = append(append([]byte{'['}, raw...), ']')
		}
	}
	canonical, err := jsoncanonicalizer.Transform(input)
	if err != nil {
		return nil, fmt.Errorf("canonicalize RFC 8785 JSON: %w", err)
	}
	if scalar {
		canonical = canonical[1 : len(canonical)-1]
	}
	return canonical, nil
}

// Decode validates one JSON value and decodes it using json.Number so callers
// can replace schema-typed database number tokens with strings before JCS
// canonicalization. It never routes number tokens through binary floating
// point.
func Decode(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("JSON input is not valid UTF-8")
	}
	return validateSingleJSONValue(raw)
}

// DecodeObject is Decode restricted to a JSON object.
func DecodeObject(raw []byte) (map[string]any, error) {
	value, err := Decode(raw)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JCS value must be an object")
	}
	return object, nil
}

// Marshal serializes value and returns RFC 8785 canonical bytes.
func Marshal(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON for JCS: %w", err)
	}
	return Canonicalize(raw)
}

// Equal reports whether two JSON objects or arrays have identical RFC 8785
// canonical representations.
func Equal(left, right []byte) (bool, error) {
	leftCanonical, err := Canonicalize(left)
	if err != nil {
		return false, err
	}
	rightCanonical, err := Canonicalize(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftCanonical, rightCanonical), nil
}

func validateSingleJSONValue(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("invalid JSON for JCS: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("JCS input contains more than one JSON value")
		}
		return nil, fmt.Errorf("invalid trailing JSON data for JCS: %w", err)
	}
	return value, nil
}
