// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

// Package sourceid validates exact Oversync source identity tokens.
package sourceid

import "errors"

// ErrInvalid is returned when a source identity is not a non-empty visible
// ASCII token. The error intentionally excludes the rejected value.
var ErrInvalid = errors.New("source id must be a non-empty visible ASCII token")

// Validate requires value to match [!-~]+ exactly.
func Validate(value string) error {
	if value == "" || len(value) > 256 {
		return ErrInvalid
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '!' || value[index] > '~' {
			return ErrInvalid
		}
	}
	return nil
}

// ValidateOptional accepts an absent value or the exact required grammar.
func ValidateOptional(value string) error {
	if value == "" {
		return nil
	}
	return Validate(value)
}
