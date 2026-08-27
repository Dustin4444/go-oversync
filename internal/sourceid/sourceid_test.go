package sourceid

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate_ExactVisibleASCIIToken(t *testing.T) {
	for _, value := range []string{
		"device-a",
		"019f6719-06f8-7d01-baa8-8e130b36f57e",
		"!",
		"~",
		"a.b/c?d#e%f",
	} {
		t.Run(value, func(t *testing.T) {
			require.NoError(t, Validate(value))
		})
	}
}

func TestValidate_Enforces256ByteMaximum(t *testing.T) {
	require.NoError(t, Validate(strings.Repeat("x", 256)))
	require.ErrorIs(t, Validate(strings.Repeat("x", 257)), ErrInvalid)
}

func TestValidate_RejectsAbsentWhitespaceUnicodeAndControl(t *testing.T) {
	for _, value := range []string{"", " ", " device-a", "device-a ", "device\ta", "device\na", "café", "\x7f"} {
		require.ErrorIs(t, Validate(value), ErrInvalid)
	}
	require.NoError(t, ValidateOptional(""))
	require.ErrorIs(t, ValidateOptional(" source"), ErrInvalid)
}
