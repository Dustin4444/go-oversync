// Package sessionid validates canonical push-session and initialization tokens.
package sessionid

import (
	"errors"

	"github.com/google/uuid"
)

// ErrInvalid is returned when a session token is not an exact lowercase dashed UUID.
var ErrInvalid = errors.New("invalid canonical session token")

// Validate requires an exact lowercase dashed UUID spelling.
func Validate(raw string) error {
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed.String() != raw {
		return ErrInvalid
	}
	return nil
}

// ValidateOptional accepts an empty absent token or an exact canonical token.
func ValidateOptional(raw string) error {
	if raw == "" {
		return nil
	}
	return Validate(raw)
}
