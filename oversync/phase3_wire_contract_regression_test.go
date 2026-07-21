package oversync

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type phase3RowFunc func(...any) error

func (fn phase3RowFunc) Scan(dest ...any) error { return fn(dest...) }

type phase3Tx struct {
	pgx.Tx
	row pgx.Row
}

func (tx *phase3Tx) QueryRow(context.Context, string, ...any) pgx.Row { return tx.row }

func TestPhase3CapabilitiesRequiredPresenceAtDecodeBoundary(t *testing.T) {
	valid := `{"protocol_version":"v1","schema_version":1,"registered_table_specs":[{"schema":"business","table":"users","sync_key_columns":["id"]}],"features":{},"bundle_limits":{"default_rows_per_snapshot_chunk":1000,"max_rows_per_snapshot_chunk":2000,"default_bytes_per_snapshot_chunk":4194304,"max_bytes_per_snapshot_chunk":8388608,"max_bytes_per_snapshot_row":1048576,"max_concurrent_snapshot_builds":2,"max_concurrent_snapshot_chunk_requests":3}}`
	require.NoError(t, json.Unmarshal([]byte(valid), &CapabilitiesResponse{}))

	for _, field := range []string{
		"protocol_version",
		"schema_version",
		"registered_table_specs",
		"features",
		"bundle_limits",
		"default_rows_per_snapshot_chunk",
		"max_rows_per_snapshot_chunk",
		"default_bytes_per_snapshot_chunk",
		"max_bytes_per_snapshot_chunk",
		"max_bytes_per_snapshot_row",
		"max_concurrent_snapshot_builds",
		"max_concurrent_snapshot_chunk_requests",
	} {
		t.Run(field, func(t *testing.T) {
			var document map[string]any
			require.NoError(t, json.Unmarshal([]byte(valid), &document))
			if field == "protocol_version" || field == "schema_version" || field == "registered_table_specs" || field == "features" || field == "bundle_limits" {
				delete(document, field)
			} else {
				delete(document["bundle_limits"].(map[string]any), field)
			}
			encoded, err := json.Marshal(document)
			require.NoError(t, err)
			require.Error(t, json.Unmarshal(encoded, &CapabilitiesResponse{}))
		})
	}

	for _, testCase := range []struct {
		name  string
		value any
	}{
		{name: "null specs", value: nil},
		{name: "non-array specs", value: map[string]any{}},
		{name: "missing schema", value: []any{map[string]any{"table": "users", "sync_key_columns": []any{"id"}}}},
		{name: "blank schema", value: []any{map[string]any{"schema": " ", "table": "users", "sync_key_columns": []any{"id"}}}},
		{name: "missing table", value: []any{map[string]any{"schema": "business", "sync_key_columns": []any{"id"}}}},
		{name: "blank table", value: []any{map[string]any{"schema": "business", "table": " ", "sync_key_columns": []any{"id"}}}},
		{name: "missing keys", value: []any{map[string]any{"schema": "business", "table": "users"}}},
		{name: "null keys", value: []any{map[string]any{"schema": "business", "table": "users", "sync_key_columns": nil}}},
		{name: "empty keys", value: []any{map[string]any{"schema": "business", "table": "users", "sync_key_columns": []any{}}}},
		{name: "multiple keys", value: []any{map[string]any{"schema": "business", "table": "users", "sync_key_columns": []any{"id", "tenant_id"}}}},
		{name: "blank key", value: []any{map[string]any{"schema": "business", "table": "users", "sync_key_columns": []any{" "}}}},
		{name: "duplicate spec", value: []any{
			map[string]any{"schema": "business", "table": "users", "sync_key_columns": []any{"id"}},
			map[string]any{"schema": "business", "table": "users", "sync_key_columns": []any{"id"}},
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var document map[string]any
			require.NoError(t, json.Unmarshal([]byte(valid), &document))
			document["registered_table_specs"] = testCase.value
			encoded, err := json.Marshal(document)
			require.NoError(t, err)
			require.Error(t, json.Unmarshal(encoded, &CapabilitiesResponse{}))
		})
	}

	for _, field := range []string{"snapshot_materialization_batch_rows", "snapshot_materialization_batch_bytes"} {
		t.Run(field+" null", func(t *testing.T) {
			var document map[string]any
			require.NoError(t, json.Unmarshal([]byte(valid), &document))
			document["bundle_limits"].(map[string]any)[field] = nil
			encoded, err := json.Marshal(document)
			require.NoError(t, err)
			require.Error(t, json.Unmarshal(encoded, &CapabilitiesResponse{}))
		})
	}
}

func TestPhase3SourceRetiredJSONPresenceDistinctions(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "absent", body: `{"error":"source_retired","message":"retired","source_id":"source-a"}`},
		{name: "present null", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":null}`, wantErr: true},
		{name: "present empty", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":""}`, wantErr: true},
		{name: "valid same", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-a"}`},
		{name: "valid different", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-b"}`},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var response SourceRetiredResponse
			err := json.Unmarshal([]byte(testCase.body), &response)
			if testCase.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestPhase3SourceRetiredWireMessageIsFixedAndRedacted(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recorder := httptest.NewRecorder()
	handler.writeSourceRetired(recorder, "request-source", &SourceRetiredError{
		UserID:             "private-user",
		SourceID:           "request-source",
		ReplacedBySourceID: "replacement-source",
	})

	require.Equal(t, http.StatusConflict, recorder.Code)
	var response SourceRetiredResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, "source is retired", response.Message)
	require.Equal(t, "request-source", response.SourceID)
	require.False(t, strings.Contains(response.Message, "private-user"))
	require.False(t, strings.Contains(response.Message, "request-source"))
	require.False(t, strings.Contains(response.Message, "replacement-source"))
}

func TestPhase3SourceRetiredWriterRejectsMismatchedRequestSource(t *testing.T) {
	handler := &HTTPSyncHandlers{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recorder := httptest.NewRecorder()
	handler.writeSourceRetired(recorder, "request-source", &SourceRetiredError{
		UserID:   "private-user",
		SourceID: "different-source",
	})

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.NotContains(t, recorder.Body.String(), "request-source")
	require.NotContains(t, recorder.Body.String(), "different-source")
	require.NotContains(t, recorder.Body.String(), "private-user")
}

func TestPhase3TerminalSourceErrorsAreRedacted(t *testing.T) {
	const (
		userSentinel   = "private-user-sentinel"
		sourceSentinel = "private-source-sentinel"
	)
	errorsToCheck := []error{
		&SourceTupleHistoryPrunedError{UserID: userSentinel, SourceID: sourceSentinel, SourceBundleID: 1, MaxCommittedSourceBundleIDHint: 2},
		&SourceSequenceOutOfOrderError{UserID: userSentinel, SourceID: sourceSentinel, Expected: 2, Actual: 3},
		&SourceSequenceChangedError{UserID: userSentinel, SourceID: sourceSentinel, Expected: 2, Actual: 1},
	}
	for _, err := range errorsToCheck {
		require.NotContains(t, err.Error(), userSentinel)
		require.NotContains(t, err.Error(), sourceSentinel)
	}
}

func TestPhase3PersistedAndNotificationSourceIDsFailClosed(t *testing.T) {
	invalidSourceID := "private source"

	t.Run("scope_state initializer", func(t *testing.T) {
		tx := &phase3Tx{row: phase3RowFunc(func(dest ...any) error {
			*dest[0].(*int64) = 1
			*dest[1].(*string) = "user"
			*dest[2].(*int16) = scopeStateCodeInitializing
			*dest[3].(*sql.NullString) = sql.NullString{String: invalidSourceID, Valid: true}
			*dest[4].(*sql.NullString) = sql.NullString{String: "00000000-0000-0000-0000-000000000001", Valid: true}
			*dest[5].(*sql.NullTime) = sql.NullTime{Time: time.Now().Add(time.Minute), Valid: true}
			*dest[6].(*sql.NullTime) = sql.NullTime{}
			*dest[7].(*sql.NullString) = sql.NullString{}
			return nil
		})}
		_, err := loadScopeStateForUpdate(context.Background(), tx, "user")
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalidSourceID)
	})

	t.Run("push_sessions", func(t *testing.T) {
		tx := &phase3Tx{row: phase3RowFunc(func(dest ...any) error {
			*dest[0].(*string) = "00000000-0000-0000-0000-000000000001"
			*dest[1].(*int64) = 1
			*dest[2].(*string) = "user"
			*dest[3].(*string) = invalidSourceID
			*dest[4].(*int64) = 1
			*dest[5].(*int64) = 0
			*dest[6].(*string) = strings.Repeat("a", 64)
			*dest[7].(*int64) = 0
			*dest[8].(*string) = ""
			*dest[9].(*time.Time) = time.Now().Add(time.Minute)
			return nil
		})}
		_, err := loadPushSessionForUpdate(context.Background(), tx, "00000000-0000-0000-0000-000000000001")
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalidSourceID)
	})

	t.Run("bundle_log", func(t *testing.T) {
		tx := &phase3Tx{row: phase3RowFunc(func(dest ...any) error {
			*dest[0].(*int64) = 1
			*dest[1].(*string) = invalidSourceID
			*dest[2].(*int64) = 1
			*dest[3].(*int64) = 0
			*dest[4].(*[]byte) = []byte{1}
			*dest[5].(*string) = strings.Repeat("a", 64)
			return nil
		})}
		_, err := loadCommittedPushMetadataBySourceTuple(context.Background(), tx, 1, "request-source", 1)
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalidSourceID)
	})

	t.Run("notification", func(t *testing.T) {
		service := &SyncService{}
		payload := `{"user_pk":1,"bundle_seq":1,"source_id":"private source","source_bundle_id":1}`
		err := service.publishBundleChangeNotification(payload)
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalidSourceID)
	})
}
