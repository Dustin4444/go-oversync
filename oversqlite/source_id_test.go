package oversqlite

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestSourceIDValidation_PreservesExactVisibleASCIIAndRejectsNormalization(t *testing.T) {
	for _, value := range []string{"device-a", "019f6719-06f8-7d01-baa8-8e130b36f57e", "example/source?token#part%25"} {
		require.NoError(t, validateSourceID(value))
	}
	for _, value := range []string{"", " device-a", "device-a ", "device\ta", "device\x00a", "dévice"} {
		require.Error(t, validateSourceID(value))
	}
	require.NoError(t, validateOptionalSourceID(""))
}

func TestApplyAuthenticatedSyncHeadersWithSourceID_PreservesExactTokenAndRejectsInvalidValues(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
	require.NoError(t, applyAuthenticatedSyncHeadersWithSourceID(req, "token", "device!~./?#%"))
	require.Equal(t, "device!~./?#%", req.Header.Get(oversync.SourceIDHeader))

	for _, invalid := range []string{" source", "source ", "source\tvalue", "söurce"} {
		req := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
		require.Error(t, applyAuthenticatedSyncHeadersWithSourceID(req, "token", invalid))
		require.Empty(t, req.Header.Get(oversync.SourceIDHeader))
	}
}

func TestSourceIDComparisonBoundariesRejectAndRedactInvalidRemoteValues(t *testing.T) {
	hostile := "remote-secret\nretry-me"
	valid := "client-source"
	canonicalHash := strings.Repeat("a", 64)

	create := &oversync.PushSessionCreateResponse{
		Status:               "already_committed",
		SourceID:             hostile,
		SourceBundleID:       1,
		BundleSeq:            1,
		RowCount:             1,
		BundleHash:           "bundle-hash",
		CanonicalRequestHash: canonicalHash,
	}
	err := validatePushSessionCreateResponse(create, 1, 1, valid, canonicalHash)
	require.Error(t, err)
	require.NotContains(t, err.Error(), hostile)

	commit := &oversync.PushSessionCommitResponse{
		SourceID:             hostile,
		SourceBundleID:       1,
		BundleSeq:            1,
		RowCount:             1,
		BundleHash:           "bundle-hash",
		CanonicalRequestHash: canonicalHash,
	}
	err = validatePushSessionCommitResponse(commit, valid)
	require.Error(t, err)
	require.NotContains(t, err.Error(), hostile)

	rows := &oversync.CommittedBundleRowsResponse{SourceID: hostile}
	err = validateCommittedBundleRowsResponse(rows, &committedPushBundle{SourceID: valid}, nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), hostile)
}
