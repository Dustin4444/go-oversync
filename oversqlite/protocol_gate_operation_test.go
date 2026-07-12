package oversqlite

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const hostileProtocolVersion = "HOSTILE_PROTOCOL_VERSION_SECRET"

func phase2ProtocolGateAcceptedAndRejectedVersions() (string, string) {
	return requiredProtocolVersion, hostileProtocolVersion
}

func newProtocolMismatchAfterAttachClient(t *testing.T) (*Client, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	attachedVersion, rejectedVersion := phase2ProtocolGateAcceptedAndRejectedVersions()
	require.Equal(t, requiredProtocolVersion, attachedVersion)

	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "local-1", "Local", "local@example.com")
	require.NoError(t, err)

	capabilityRequests := &atomic.Int64{}
	remoteWorkRequests := &atomic.Int64{}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/capabilities" {
			capabilityRequests.Add(1)
			return jsonResponse(oversync.CapabilitiesResponse{
				ProtocolVersion: rejectedVersion,
				Features:        map[string]bool{"connect_lifecycle": true, "bundle_change_watch": true},
			}), nil
		}
		remoteWorkRequests.Add(1)
		return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_remote_work"}), nil
	})}
	return client, capabilityRequests, remoteWorkRequests
}

func requireProtocolMismatchPreservedPreOperationState(t *testing.T, client *Client) {
	t.Helper()
	var outboxState string
	var outboxRows, dirtyRows int
	var bindingState string
	require.NoError(t, client.DB.QueryRow(`SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1`).Scan(&outboxState))
	require.NoError(t, client.DB.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows))
	require.NoError(t, client.DB.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyRows))
	require.NoError(t, client.DB.QueryRow(`SELECT binding_state FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&bindingState))
	require.Equal(t, "none", outboxState)
	require.Zero(t, outboxRows)
	require.Equal(t, 1, dirtyRows)
	require.Equal(t, attachmentBindingAttached, bindingState)
}

func TestSyncOperations_RevalidateProtocolGateAfterAttach(t *testing.T) {
	operations := []struct {
		name string
		run  func(context.Context, *Client) error
	}{
		{name: "PushPending", run: func(ctx context.Context, client *Client) error { _, err := client.PushPending(ctx); return err }},
		{name: "PullToStable", run: func(ctx context.Context, client *Client) error { _, err := client.PullToStable(ctx); return err }},
		{name: "Rebuild", run: func(ctx context.Context, client *Client) error { _, err := client.Rebuild(ctx); return err }},
		{name: "Sync", run: func(ctx context.Context, client *Client) error { _, err := client.Sync(ctx); return err }},
		{name: "SyncThenDetach", run: func(ctx context.Context, client *Client) error { _, err := client.SyncThenDetach(ctx); return err }},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			client, capabilityRequests, remoteWorkRequests := newProtocolMismatchAfterAttachClient(t)
			err := operation.run(context.Background(), client)
			var mismatch *ProtocolVersionMismatchError
			require.ErrorAs(t, err, &mismatch)
			require.Equal(t, "v1", mismatch.Expected)
			require.Empty(t, mismatch.Actual)
			require.Equal(t, "oversqlite protocol version mismatch", mismatch.Error())
			require.NotContains(t, mismatch.Error(), hostileProtocolVersion)
			require.EqualValues(t, 1, capabilityRequests.Load())
			require.Zero(t, remoteWorkRequests.Load())
			requireProtocolMismatchPreservedPreOperationState(t, client)
		})
	}
}

func runLoopAndRequireProtocolMismatchStop(
	t *testing.T,
	client *Client,
	run func(context.Context, *Client),
	capabilityRequests *atomic.Int64,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx, client)
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	select {
	case <-done:
	case <-deadline.C:
		t.Fatal("automatic loop did not stop after protocol mismatch")
	}
	require.EqualValues(t, 1, capabilityRequests.Load())
}

func TestAutomaticLoops_StopOnProtocolMismatchAfterAttach(t *testing.T) {
	loopCases := []struct {
		name string
		run  func(context.Context, *Client)
	}{
		{name: "uploader", run: func(ctx context.Context, client *Client) { client.uploaderLoop(ctx) }},
		{name: "polling downloader", run: func(ctx context.Context, client *Client) { client.downloaderLoop(ctx) }},
		{name: "watch capability setup", run: func(ctx context.Context, client *Client) {
			client.config.BundleChangeWatchMode = BundleChangeWatchAuto
			client.startDownloaderLoop(ctx)
		}},
	}

	for _, loopCase := range loopCases {
		t.Run(loopCase.name, func(t *testing.T) {
			client, capabilityRequests, remoteWorkRequests := newProtocolMismatchAfterAttachClient(t)
			var logs bytes.Buffer
			client.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))
			runLoopAndRequireProtocolMismatchStop(t, client, loopCase.run, capabilityRequests)
			require.Zero(t, remoteWorkRequests.Load())
			requireProtocolMismatchPreservedPreOperationState(t, client)
			require.Contains(t, logs.String(), "error_type=*oversqlite.ProtocolVersionMismatchError")
			require.NotContains(t, logs.String(), hostileProtocolVersion)
		})
	}
}

func TestWatchAwareLoop_StopsOnProtocolMismatchAfterAttach(t *testing.T) {
	testCases := []struct {
		name        string
		watchStream string
		errorType   string
	}{
		{
			name:        "bundle event pull",
			watchStream: "event: bundle\ndata: {\"bundle_seq\":1}\n\n",
			errorType:   "*oversqlite.bundleChangeWatchPullError",
		},
		{
			name:        "malformed stream fallback pull",
			watchStream: "malformed\n\n",
			errorType:   "*oversqlite.ProtocolVersionMismatchError",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			client, capabilityRequests, _ := newProtocolMismatchAfterAttachClient(t)
			var logs bytes.Buffer
			client.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))
			_, rejectedVersion := phase2ProtocolGateAcceptedAndRejectedVersions()
			var watchRequests, pullRequests atomic.Int64
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/sync/watch":
					watchRequests.Add(1)
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(testCase.watchStream))}, nil
				case "/sync/capabilities":
					capabilityRequests.Add(1)
					return jsonResponse(oversync.CapabilitiesResponse{ProtocolVersion: rejectedVersion}), nil
				case "/sync/pull":
					pullRequests.Add(1)
					return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_pull"}), nil
				default:
					return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
				}
			})}

			runLoopAndRequireProtocolMismatchStop(t, client, func(ctx context.Context, client *Client) {
				client.watchAwareDownloaderLoop(ctx)
			}, capabilityRequests)
			require.EqualValues(t, 1, watchRequests.Load())
			require.Zero(t, pullRequests.Load())
			requireProtocolMismatchPreservedPreOperationState(t, client)
			require.Contains(t, logs.String(), "error_type="+testCase.errorType)
			require.NotContains(t, logs.String(), hostileProtocolVersion)
		})
	}
}
