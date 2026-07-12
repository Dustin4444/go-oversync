package oversqlite

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestSnapshotTransfer_ExactTotalsAndAccumulatedOverflow(t *testing.T) {
	t.Run("exact totals", func(t *testing.T) {
		client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		responses := []oversync.SnapshotChunkResponse{
			{SnapshotID: "snapshot-totals", SnapshotBundleSeq: 7, Rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "1"}, RowVersion: 1, Payload: mustJSONPayload(t, map[string]any{"id": "1", "name": "One", "email": "one@example.com"})}}, NextRowOrdinal: 1, HasMore: true, ByteCount: 5},
			{SnapshotID: "snapshot-totals", SnapshotBundleSeq: 7, Rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "2"}, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": "2", "name": "Two", "email": "two@example.com"})}}, NextRowOrdinal: 2, ByteCount: 7},
		}
		var request atomic.Int64
		client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "1", r.URL.Query().Get("max_rows"))
			require.Equal(t, "100", r.URL.Query().Get("max_bytes"))
			idx := int(request.Add(1) - 1)
			return jsonResponse(responses[idx]), nil
		})}
		totals, err := client.downloadSnapshotSession(context.Background(), &oversync.SnapshotSession{
			SnapshotID: "snapshot-totals", SnapshotBundleSeq: 7, RowCount: 2, ByteCount: 12, ExpiresAt: "2030-01-01T00:00:00Z",
		}, snapshotNegotiation{maxRows: 1, maxBytes: 100})
		require.NoError(t, err)
		require.Equal(t, snapshotTransferTotals{rows: 2, bytes: 12}, totals)
		require.Equal(t, 2, snapshotStageCount(t, db))
	})

	t.Run("accumulated declared bytes overflow", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		maxBytes := int64(math.MaxInt64 / 2)
		var request atomic.Int64
		client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			ordinal := request.Add(1)
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: "snapshot-overflow", SnapshotBundleSeq: 8,
				Rows:           []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": string(rune('0' + ordinal))}, RowVersion: ordinal, Payload: mustJSONPayload(t, map[string]any{"id": string(rune('0' + ordinal)), "name": "N", "email": "n@example.com"})}},
				NextRowOrdinal: ordinal, HasMore: true, ByteCount: maxBytes,
			}), nil
		})}
		_, err := client.downloadSnapshotSession(context.Background(), &oversync.SnapshotSession{
			SnapshotID: "snapshot-overflow", SnapshotBundleSeq: 8, RowCount: 3, ByteCount: math.MaxInt64, ExpiresAt: "2030-01-01T00:00:00Z",
		}, snapshotNegotiation{maxRows: 1, maxBytes: maxBytes})
		require.ErrorContains(t, err, "accumulated byte count overflow")
	})
}

func TestSnapshotTransfer_ExactEmptySnapshot(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return rawJSONResponse(http.StatusOK, `{"snapshot_id":"snapshot-empty","snapshot_bundle_seq":0,"rows":[],"next_row_ordinal":0,"has_more":false,"byte_count":0}`), nil
	})}
	totals, err := client.downloadSnapshotSession(context.Background(), &oversync.SnapshotSession{
		SnapshotID: "snapshot-empty", SnapshotBundleSeq: 0, RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z",
	}, snapshotNegotiation{maxRows: 10, maxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, snapshotTransferTotals{}, totals)
	require.Equal(t, 0, snapshotStageCount(t, db))
}

func TestSnapshotExpiryAfterCompleteLocalStagingDoesNotPreventApply(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	var expired atomic.Bool
	client.beforeSnapshotApplyHook = func(context.Context) error {
		expired.Store(true)
		return nil
	}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.URL.Path == "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{ProtocolVersion: requiredProtocolVersion, Features: map[string]bool{"connect_lifecycle": true}}), nil
		case r.Method == http.MethodPost:
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "snapshot-expiry", SnapshotBundleSeq: 5, RowCount: 1, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case r.Method == http.MethodGet:
			require.False(t, expired.Load(), "all remote reads must finish before simulated expiry")
			return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "snapshot-expiry", SnapshotBundleSeq: 5, Rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote"}, RowVersion: 5, Payload: mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"})}}, NextRowOrdinal: 1}), nil
		case r.Method == http.MethodDelete:
			require.True(t, expired.Load())
			return errorJSONResponse(http.StatusGone, oversync.ErrorResponse{Error: "snapshot_session_expired"}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}
	_, err = client.Rebuild(context.Background())
	require.NoError(t, err)
	requireUserCount(t, db, "remote", 1)
	require.Equal(t, 0, snapshotStageCount(t, db))
}

func TestSnapshotRetirement_EveryTerminalPathUsesIndependentBoundedContext(t *testing.T) {
	tests := []struct {
		name      string
		chunk     func(*testing.T) *http.Response
		cancelGet bool
		wantError bool
	}{
		{name: "success", chunk: func(t *testing.T) *http.Response {
			return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "snapshot-terminal", SnapshotBundleSeq: 7, Rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote"}, RowVersion: 7, Payload: mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"})}}, NextRowOrdinal: 1})
		}},
		{name: "validation failure", chunk: func(t *testing.T) *http.Response {
			return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "wrong", SnapshotBundleSeq: 7, Rows: nil})
		}, wantError: true},
		{name: "stage failure", chunk: func(t *testing.T) *http.Response {
			return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "snapshot-terminal", SnapshotBundleSeq: 7, Rows: []oversync.SnapshotRow{{Schema: "other", Table: "users", Key: oversync.SyncKey{"id": "remote"}, RowVersion: 7, Payload: mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"})}}, NextRowOrdinal: 1})
		}, wantError: true},
		{name: "apply failure", chunk: func(t *testing.T) *http.Response {
			return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "snapshot-terminal", SnapshotBundleSeq: 7, Rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote"}, RowVersion: 7, Payload: []byte(`{"id":"remote","name":"missing-email"}`)}}, NextRowOrdinal: 1})
		}, wantError: true},
		{name: "cancellation", chunk: func(t *testing.T) *http.Response {
			return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "snapshot-terminal", SnapshotBundleSeq: 7, Rows: nil})
		}, cancelGet: true, wantError: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
			require.NoError(t, err)
			_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
			require.NoError(t, err)
			var retired atomic.Int64
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch {
				case r.URL.Path == "/sync/capabilities":
					return jsonResponse(oversync.CapabilitiesResponse{ProtocolVersion: requiredProtocolVersion, Features: map[string]bool{"connect_lifecycle": true}}), nil
				case r.Method == http.MethodPost:
					return jsonResponse(oversync.SnapshotSession{SnapshotID: "snapshot-terminal", SnapshotBundleSeq: 7, RowCount: 1, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
				case r.Method == http.MethodGet:
					if testCase.cancelGet {
						cancel()
					}
					return testCase.chunk(t), nil
				case r.Method == http.MethodDelete:
					retired.Add(1)
					deadline, ok := r.Context().Deadline()
					require.True(t, ok)
					require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
					require.NoError(t, r.Context().Err())
					return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
				default:
					return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
				}
			})}
			_, err = client.Rebuild(ctx)
			if testCase.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int64(1), retired.Load())
		})
	}
}

type panicOnReadBody struct{ closed atomic.Int64 }

func (b *panicOnReadBody) Read([]byte) (int, error) {
	panic("retirement response body must not be read")
}
func (b *panicOnReadBody) Close() error {
	b.closed.Add(1)
	return nil
}

func TestSnapshotRetirement_DeleteBodyIsNeverBuffered(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	deleteBody := &panicOnReadBody{}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.URL.Path == "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{ProtocolVersion: requiredProtocolVersion, Features: map[string]bool{"connect_lifecycle": true}}), nil
		case r.Method == http.MethodPost:
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "snapshot-delete-body", SnapshotBundleSeq: 0, RowCount: 0, ByteCount: 0, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case r.Method == http.MethodGet:
			return rawJSONResponse(http.StatusOK, `{"snapshot_id":"snapshot-delete-body","snapshot_bundle_seq":0,"rows":[],"next_row_ordinal":0,"has_more":false,"byte_count":0}`), nil
		case r.Method == http.MethodDelete:
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: deleteBody}, nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}
	_, err = client.Rebuild(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteBody.closed.Load())
}

func TestSnapshotFailedAttemptsKeepOneLocalStageIdentity(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	var sessionNumber atomic.Int64
	var mu sync.Mutex
	chunkCalls := map[string]int{}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.URL.Path == "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{ProtocolVersion: requiredProtocolVersion, Features: map[string]bool{"connect_lifecycle": true}}), nil
		case r.Method == http.MethodPost:
			n := sessionNumber.Add(1)
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "snapshot-" + strconv.FormatInt(n, 10), SnapshotBundleSeq: 8, RowCount: 2, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case r.Method == http.MethodDelete:
			return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
		case r.Method == http.MethodGet:
			id := strings.TrimPrefix(r.URL.Path, "/sync/snapshot-sessions/")
			mu.Lock()
			chunkCalls[id]++
			call := chunkCalls[id]
			mu.Unlock()
			if call == 1 {
				return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: id, SnapshotBundleSeq: 8, Rows: []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, RowVersion: 8, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": "Partial", "email": "partial@example.com"})}}, NextRowOrdinal: 1, HasMore: true}), nil
			}
			return errorJSONResponse(http.StatusGone, oversync.ErrorResponse{Error: "snapshot_session_expired"}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := client.Rebuild(context.Background())
		require.ErrorContains(t, err, "snapshot_session_expired")
		var identities, rows int
		require.NoError(t, db.QueryRow(`SELECT COUNT(DISTINCT snapshot_id), COUNT(*) FROM _sync_snapshot_stage`).Scan(&identities, &rows))
		require.Equal(t, 1, identities)
		require.Equal(t, 1, rows)
		var id string
		require.NoError(t, db.QueryRow(`SELECT snapshot_id FROM _sync_snapshot_stage LIMIT 1`).Scan(&id))
		require.Equal(t, "snapshot-"+strconv.Itoa(attempt), id)
	}
}

func TestSnapshotDiagnostics_PerRestoreAndStreamingApplyHighWater(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	var observed snapshotRestoreObservation
	client.snapshotObserver = func(value snapshotRestoreObservation) { observed = value }
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.URL.Path == "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{ProtocolVersion: requiredProtocolVersion, Features: map[string]bool{"connect_lifecycle": true}}), nil
		case r.Method == http.MethodPost:
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "snapshot-diagnostics", SnapshotBundleSeq: 9, RowCount: 2, ByteCount: 12, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case r.Method == http.MethodGet:
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: "snapshot-diagnostics", SnapshotBundleSeq: 9,
				Rows: []oversync.SnapshotRow{
					{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "1"}, RowVersion: 1, Payload: mustJSONPayload(t, map[string]any{"id": "1", "name": "One", "email": "one@example.com"})},
					{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "2"}, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": "2", "name": "Two", "email": "two@example.com"})},
				}, NextRowOrdinal: 2, ByteCount: 12,
			}), nil
		case r.Method == http.MethodDelete:
			return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}
	_, err = client.Rebuild(context.Background())
	require.NoError(t, err)
	stats := client.SnapshotTransferDiagnostics()
	require.Equal(t, int64(1), stats.SessionsCreated)
	require.Equal(t, int64(1), stats.ChunksFetched)
	require.Equal(t, int64(2), stats.MaxChunkRows)
	require.Equal(t, int64(12), stats.MaxChunkWireBytes)
	require.Greater(t, stats.MaxChunkDecodedBodyBytes, int64(0))
	require.Equal(t, int64(1), stats.MaxLiveStagedApplyRows)
	require.Greater(t, stats.MaxLiveStagedApplyTextBytes, int64(0))
	require.Equal(t, int64(1), stats.MaxAppliedInMemoryRows)
	require.Equal(t, int64(2), observed.StagedRows)
	require.Equal(t, int64(12), observed.DeclaredWireBytes)
	require.Equal(t, int64(2), observed.AppliedRows)
	require.Greater(t, observed.Duration, time.Duration(0))

	client.ResetSnapshotTransferDiagnostics()
	require.Equal(t, SnapshotTransferStats{}, client.SnapshotTransferDiagnostics())
}

func TestSnapshotLimitsDoNotCapOrdinaryIncrementalPullSuccessBody(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	largeName := strings.Repeat("n", 70<<10)
	response := oversync.PullResponse{
		StableBundleSeq: 1,
		Bundles: []oversync.Bundle{{
			BundleSeq: 1, SourceID: "remote-source", SourceBundleID: 1,
			Rows: []oversync.BundleRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "large"}, Op: oversync.OpInsert, RowVersion: 1, Payload: mustJSONPayload(t, map[string]any{"id": "large", "name": largeName, "email": "large@example.com"})}},
		}},
	}
	body, err := json.Marshal(response)
	require.NoError(t, err)
	require.Greater(t, len(body), 64<<10)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	_, err = client.PullToStable(context.Background())
	require.NoError(t, err)
}

func TestSnapshotCommitSurvivesRetirementFailureAndNextPullIsIdempotent(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users VALUES('old','Old','old@example.com')`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
	require.NoError(t, err)
	setCurrentSourceBundleState(t, db, 1, 4)

	var retireAttempts atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.URL.Path == "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{ProtocolVersion: requiredProtocolVersion, Features: map[string]bool{"connect_lifecycle": true}}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "snapshot-committed-before-retire", SnapshotBundleSeq: 9, RowCount: 1, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case r.Method == http.MethodGet && r.URL.Path == "/sync/snapshot-sessions/snapshot-committed-before-retire":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: "snapshot-committed-before-retire", SnapshotBundleSeq: 9,
				Rows: []oversync.SnapshotRow{{
					Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote"}, RowVersion: 9,
					Payload: mustJSONPayload(t, map[string]any{"id": "remote", "name": "Remote", "email": "remote@example.com"}),
				}},
				NextRowOrdinal: 1,
			}), nil
		case r.Method == http.MethodDelete && r.URL.Path == "/sync/snapshot-sessions/snapshot-committed-before-retire":
			retireAttempts.Add(1)
			return nil, errors.New("simulated process loss before retirement completed")
		case r.Method == http.MethodGet && r.URL.Path == "/sync/pull":
			require.Equal(t, "9", r.URL.Query().Get("after_bundle_seq"))
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 9}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	report, err := client.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
	require.Equal(t, int64(1), retireAttempts.Load())
	requireUserCount(t, db, "old", 0)
	requireUserCount(t, db, "remote", 1)
	requireLastBundleSeqSeen(t, client, ctx, 9)
	require.Zero(t, snapshotStageCount(t, db))

	pullReport, err := client.PullToStable(ctx)
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAlreadyAtTarget, pullReport.Outcome)
	requireUserCount(t, db, "remote", 1)
	requireLastBundleSeqSeen(t, client, ctx, 9)
}
