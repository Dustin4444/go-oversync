package oversqlite

import (
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

func validateSourceID(value string) error {
	if err := sourceid.Validate(value); err != nil {
		return fmt.Errorf("invalid source id: %w", err)
	}
	return nil
}

func validateOptionalSourceID(value string) error {
	if err := sourceid.ValidateOptional(value); err != nil {
		return fmt.Errorf("invalid source id: %w", err)
	}
	return nil
}
