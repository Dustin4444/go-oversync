package oversqlite

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSharedSnapshotCapacityRetryFixture(t *testing.T) {
	path := filepath.Join("..", "..", "sqlitenow-kmp", "oversqlite-contracts", "snapshot-capacity", "retry.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skip("sqlitenow-kmp shared contract checkout is not available")
	}
	require.NoError(t, err)
	var fixture struct {
		Contract string `json:"contract"`
		Defaults struct {
			Enabled       bool  `json:"enabled"`
			MaxWait       int64 `json:"max_wait_millis"`
			FallbackDelay int64 `json:"fallback_delay_millis"`
		} `json:"defaults"`
		RetryAfterCases []struct {
			Wire     string `json:"wire"`
			Expected *int64 `json:"expected_millis"`
		} `json:"retry_after_cases"`
		CapacityErrorCodes []string `json:"capacity_error_codes"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Equal(t, "snapshot-capacity-retry-v1", fixture.Contract)
	require.True(t, fixture.Defaults.Enabled)
	require.Equal(t, int64((30*time.Second)/time.Millisecond), fixture.Defaults.MaxWait)
	require.Equal(t, int64(time.Second/time.Millisecond), fixture.Defaults.FallbackDelay)
	for _, testCase := range fixture.RetryAfterCases {
		actual := parseRetryAfterDeltaSeconds(testCase.Wire)
		if testCase.Expected == nil {
			require.Zero(t, actual, testCase.Wire)
			continue
		}
		require.Equal(t, time.Duration(*testCase.Expected)*time.Millisecond, actual, testCase.Wire)
	}
	require.ElementsMatch(t, []string{"snapshot_build_capacity", "snapshot_chunk_capacity"}, fixture.CapacityErrorCodes)
}
