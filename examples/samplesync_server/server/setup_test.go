package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/examples/internal/exampleauth"
	"github.com/stretchr/testify/require"
)

func TestConfiguredSnapshotConcurrencyFromEnv(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "12")
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", "6")

	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(8, 4)
	require.NoError(t, err)
	require.Equal(t, 12, builds)
	require.Equal(t, 6, chunks)
}

func TestConfiguredSnapshotConcurrencyFromEnv_PreservesConfigWithoutOverrides(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "")
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", "")

	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(11, 7)
	require.NoError(t, err)
	require.Equal(t, 11, builds)
	require.Equal(t, 7, chunks)
}

func TestConfiguredSnapshotConcurrencyFromEnv_RejectsInvalidValues(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "0")

	_, _, err := configuredSnapshotConcurrencyFromEnv(8, 4)
	require.ErrorContains(t, err, "OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS must be a positive integer")
}

func TestResponseCapture_BoundsBufferedBody(t *testing.T) {
	recorder := httptest.NewRecorder()
	capture := &respCapture{
		ResponseWriter: recorder,
		status:         200,
		limit:          maxLoggedBodyBytes,
	}
	body := bytes.Repeat([]byte("x"), maxLoggedBodyBytes*3)

	n, err := capture.Write(body)
	require.NoError(t, err)
	require.Equal(t, len(body), n)
	require.Equal(t, body, recorder.Body.Bytes())
	require.Len(t, capture.buf, maxLoggedBodyBytes)
	require.True(t, capture.truncated)
}

func TestDummySigninHandlerPreservesExactUserIdentity(t *testing.T) {
	auth := exampleauth.New("exact-identity-test-secret")
	handler := sampleSyncDummySigninHandler(auth, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, userID := range []string{"test-user", " test-user "} {
		body, err := json.Marshal(map[string]string{"user": userID, "password": "any"})
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, "/dummy-signin", bytes.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code)

		var result struct {
			Token string `json:"token"`
			User  string `json:"user"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.Equal(t, userID, result.User)
		claims, err := auth.ValidateToken(result.Token)
		require.NoError(t, err)
		require.Equal(t, userID, claims.Subject)
		require.NotNil(t, claims.ExpiresAt)
		require.Greater(t, time.Until(claims.ExpiresAt.Time), time.Duration(0))
	}
}
