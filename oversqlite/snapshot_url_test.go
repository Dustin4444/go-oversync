package oversqlite

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotSessionURLs_EncodeOpaqueIDAsOnePathSegment(t *testing.T) {
	snapshotID := "a/b?c#d%e.f..g/雪"
	encoded := "a%2Fb%3Fc%23d%25e%2Ef%2E%2Eg%2F%E9%9B%AA"

	fetch := buildSnapshotChunkURL("https://example.test", snapshotID, 4, 5, 6)
	require.Equal(t, "https://example.test/sync/snapshot-sessions/"+encoded+"?after_row_ordinal=4&max_bytes=6&max_rows=5", fetch)
	deleteURL := buildSnapshotSessionDeleteURL("https://example.test", snapshotID)
	require.Equal(t, "https://example.test/sync/snapshot-sessions/"+encoded, deleteURL)

	for _, raw := range []string{fetch, deleteURL} {
		parsed, err := url.Parse(raw)
		require.NoError(t, err)
		require.Equal(t, "/sync/snapshot-sessions/"+snapshotID, parsed.Path)
		require.Equal(t, "/sync/snapshot-sessions/"+encoded, parsed.EscapedPath())
	}
}
