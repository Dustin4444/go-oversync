package sessionid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const canonicalToken = "11111111-1111-4111-8111-111111111111"

func TestValidate_AcceptsOnlyCanonicalLowercaseDashedUUID(t *testing.T) {
	require.NoError(t, Validate(canonicalToken))

	invalid := []string{
		"",
		" " + canonicalToken,
		canonicalToken + " ",
		"11111111-1111-4111-8111-11111111111A",
		"11111111111141118111111111111111",
		"{" + canonicalToken + "}",
		canonicalToken + "\n",
		"１１１１１１１１-１１１１-４１１１-８１１１-１１１１１１１１１１１１",
		"not-a-uuid",
	}
	for _, raw := range invalid {
		t.Run(raw, func(t *testing.T) {
			err := Validate(raw)
			require.Error(t, err)
			if raw != "" {
				require.NotContains(t, err.Error(), raw)
			}
		})
	}
}

func TestValidateOptional_AllowsOnlyEmptyOrCanonical(t *testing.T) {
	require.NoError(t, ValidateOptional(""))
	require.NoError(t, ValidateOptional(canonicalToken))
	require.Error(t, ValidateOptional(" "))
	require.Error(t, ValidateOptional("{"+canonicalToken+"}"))
}
