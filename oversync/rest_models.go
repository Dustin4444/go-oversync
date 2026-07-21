// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

// REST/JSON models for HTTP API requests and responses
// These models are used for serialization/deserialization of HTTP requests and responses

// SyncKey represents the canonical structured identity for a synced row.
type SyncKey map[string]any

// BundleRow describes one normalized row effect inside a committed sync bundle.
type BundleRow struct {
	Schema     string          `json:"schema"`
	Table      string          `json:"table"`
	Key        SyncKey         `json:"key"`
	Op         string          `json:"op"`
	RowVersion int64           `json:"row_version"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// Bundle represents one committed durable sync unit in the target bundle-based contract.
type Bundle struct {
	BundleSeq            int64       `json:"bundle_seq"`
	SourceID             string      `json:"source_id"`
	SourceBundleID       int64       `json:"source_bundle_id"`
	RowCount             int64       `json:"row_count,omitempty"`
	BundleHash           string      `json:"bundle_hash,omitempty"`
	CanonicalRequestHash string      `json:"canonical_request_hash,omitempty"`
	Rows                 []BundleRow `json:"rows"`
}

// PushRequestRow is one locally dirty row intent sent by the client.
type PushRequestRow struct {
	Schema         string          `json:"schema"`
	Table          string          `json:"table"`
	Key            SyncKey         `json:"key"`
	Op             string          `json:"op"`
	BaseRowVersion int64           `json:"base_row_version"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

type PushSessionCreateRequest struct {
	SourceBundleID       int64  `json:"source_bundle_id"`
	PlannedRowCount      int64  `json:"planned_row_count"`
	CanonicalRequestHash string `json:"canonical_request_hash"`
	InitializationID     string `json:"initialization_id,omitempty"`
}

type PushSessionCreateResponse struct {
	PushID                 string `json:"push_id,omitempty"`
	Status                 string `json:"status"`
	PlannedRowCount        int64  `json:"planned_row_count,omitempty"`
	NextExpectedRowOrdinal int64  `json:"next_expected_row_ordinal,omitempty"`
	BundleSeq              int64  `json:"bundle_seq,omitempty"`
	SourceID               string `json:"source_id,omitempty"`
	SourceBundleID         int64  `json:"source_bundle_id,omitempty"`
	RowCount               int64  `json:"row_count,omitempty"`
	BundleHash             string `json:"bundle_hash,omitempty"`
	CanonicalRequestHash   string `json:"canonical_request_hash,omitempty"`
}

type ConnectRequest struct {
	HasLocalPendingRows bool `json:"has_local_pending_rows"`
}

type ConnectResponse struct {
	Resolution       string `json:"resolution"`
	InitializationID string `json:"initialization_id,omitempty"`
	LeaseExpiresAt   string `json:"lease_expires_at,omitempty"`
	RetryAfterSec    int    `json:"retry_after_seconds,omitempty"`
}

type PushSessionChunkRequest struct {
	StartRowOrdinal int64            `json:"start_row_ordinal"`
	Rows            []PushRequestRow `json:"rows"`
}

type PushSessionChunkResponse struct {
	PushID                 string `json:"push_id"`
	NextExpectedRowOrdinal int64  `json:"next_expected_row_ordinal"`
}

type PushSessionCommitResponse struct {
	BundleSeq            int64  `json:"bundle_seq"`
	SourceID             string `json:"source_id"`
	SourceBundleID       int64  `json:"source_bundle_id"`
	RowCount             int64  `json:"row_count"`
	BundleHash           string `json:"bundle_hash"`
	CanonicalRequestHash string `json:"canonical_request_hash"`
}

type CommittedBundleRowsResponse struct {
	BundleSeq            int64       `json:"bundle_seq"`
	SourceID             string      `json:"source_id"`
	SourceBundleID       int64       `json:"source_bundle_id"`
	RowCount             int64       `json:"row_count"`
	BundleHash           string      `json:"bundle_hash"`
	CanonicalRequestHash string      `json:"canonical_request_hash"`
	Rows                 []BundleRow `json:"rows"`
	NextRowOrdinal       int64       `json:"next_row_ordinal"`
	HasMore              bool        `json:"has_more"`
}

// PullResponse returns one or more complete committed bundles.
type PullResponse struct {
	StableBundleSeq int64    `json:"stable_bundle_seq"`
	Bundles         []Bundle `json:"bundles"`
	HasMore         bool     `json:"has_more"`
}

// SnapshotRow is the current after-image for one non-deleted row in the bundle-based contract.
type SnapshotRow struct {
	Schema     string          `json:"schema"`
	Table      string          `json:"table"`
	Key        SyncKey         `json:"key"`
	RowVersion int64           `json:"row_version"`
	Payload    json.RawMessage `json:"payload"`
}

type SnapshotSessionCreateRequest struct {
	SourceReplacement *SnapshotSourceReplacement `json:"source_replacement,omitempty"`
}

type SnapshotSourceReplacement struct {
	PreviousSourceID string `json:"previous_source_id"`
	NewSourceID      string `json:"new_source_id"`
	Reason           string `json:"reason"`
}

// SnapshotSession describes one frozen server-side snapshot session.
type SnapshotSession struct {
	SnapshotID        string `json:"snapshot_id"`
	SnapshotBundleSeq int64  `json:"snapshot_bundle_seq"`
	RowCount          int64  `json:"row_count"`
	ByteCount         int64  `json:"byte_count"`
	ExpiresAt         string `json:"expires_at"`
}

// SnapshotChunkResponse returns one chunk of snapshot rows from a frozen session.
type SnapshotChunkResponse struct {
	SnapshotID        string        `json:"snapshot_id"`
	SnapshotBundleSeq int64         `json:"snapshot_bundle_seq"`
	Rows              []SnapshotRow `json:"rows"`
	NextRowOrdinal    int64         `json:"next_row_ordinal"`
	HasMore           bool          `json:"has_more"`
	ByteCount         int64         `json:"byte_count"`
}

// Common response models

const SyncProtocolVersion = "v1"

// CapabilitiesResponse describes the currently supported sync protocol surface.
type CapabilitiesResponse struct {
	ProtocolVersion      string                   `json:"protocol_version"`
	SchemaVersion        int                      `json:"schema_version"`
	AppName              string                   `json:"app_name,omitempty"`
	RegisteredTables     []string                 `json:"registered_tables,omitempty"`
	RegisteredTableSpecs []RegisteredTableSpec    `json:"registered_table_specs"`
	Features             map[string]bool          `json:"features"`
	BundleLimits         BundleCapabilitiesLimits `json:"bundle_limits"`
}

// BundleCapabilitiesLimits reports bundle-oriented guardrails in the target contract.
type BundleCapabilitiesLimits struct {
	MaxRowsPerBundle                   int   `json:"max_rows_per_bundle,omitempty"`
	MaxBytesPerBundle                  int   `json:"max_bytes_per_bundle,omitempty"`
	MaxBundlesPerPull                  int   `json:"max_bundles_per_pull,omitempty"`
	DefaultRowsPerPushChunk            int   `json:"default_rows_per_push_chunk,omitempty"`
	MaxRowsPerPushChunk                int   `json:"max_rows_per_push_chunk,omitempty"`
	PushSessionTTLSeconds              int   `json:"push_session_ttl_seconds,omitempty"`
	DefaultRowsPerCommittedBundleChunk int   `json:"default_rows_per_committed_bundle_chunk,omitempty"`
	MaxRowsPerCommittedBundleChunk     int   `json:"max_rows_per_committed_bundle_chunk,omitempty"`
	DefaultRowsPerSnapshotChunk        int   `json:"default_rows_per_snapshot_chunk,omitempty"`
	MaxRowsPerSnapshotChunk            int   `json:"max_rows_per_snapshot_chunk,omitempty"`
	SnapshotSessionTTLSeconds          int   `json:"snapshot_session_ttl_seconds,omitempty"`
	MaxRowsPerSnapshotSession          int64 `json:"max_rows_per_snapshot_session,omitempty"`
	MaxBytesPerSnapshotSession         int64 `json:"max_bytes_per_snapshot_session,omitempty"`
	DefaultBytesPerSnapshotChunk       int64 `json:"default_bytes_per_snapshot_chunk"`
	MaxBytesPerSnapshotChunk           int64 `json:"max_bytes_per_snapshot_chunk"`
	MaxBytesPerSnapshotRow             int64 `json:"max_bytes_per_snapshot_row"`
	SnapshotMaterializationBatchRows   int   `json:"snapshot_materialization_batch_rows"`
	SnapshotMaterializationBatchBytes  int64 `json:"snapshot_materialization_batch_bytes"`
	MaxConcurrentSnapshotBuilds        int   `json:"max_concurrent_snapshot_builds"`
	MaxConcurrentSnapshotChunkRequests int   `json:"max_concurrent_snapshot_chunk_requests"`
	InitializationLeaseTTLSeconds      int   `json:"initialization_lease_ttl_seconds,omitempty"`
}

func (limits *BundleCapabilitiesLimits) UnmarshalJSON(data []byte) error {
	type plainLimits BundleCapabilitiesLimits
	var decoded plainLimits
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	required := []string{
		"default_rows_per_snapshot_chunk",
		"max_rows_per_snapshot_chunk",
		"default_bytes_per_snapshot_chunk",
		"max_bytes_per_snapshot_chunk",
		"max_bytes_per_snapshot_row",
		"max_concurrent_snapshot_builds",
		"max_concurrent_snapshot_chunk_requests",
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("capabilities bundle_limits missing required %s", name)
		}
	}
	for _, name := range []string{
		"snapshot_materialization_batch_rows",
		"snapshot_materialization_batch_bytes",
	} {
		if value, ok := fields[name]; ok && string(value) == "null" {
			return fmt.Errorf("capabilities bundle_limits %s must be an integer when present", name)
		}
	}
	*limits = BundleCapabilitiesLimits(decoded)
	if limits.DefaultRowsPerSnapshotChunk <= 0 || limits.MaxRowsPerSnapshotChunk <= 0 ||
		limits.DefaultBytesPerSnapshotChunk <= 0 || limits.MaxBytesPerSnapshotChunk <= 0 ||
		limits.MaxBytesPerSnapshotRow <= 0 || limits.MaxConcurrentSnapshotBuilds <= 0 ||
		limits.MaxConcurrentSnapshotChunkRequests <= 0 {
		return fmt.Errorf("capabilities bundle_limits required snapshot limits must be positive")
	}
	if limits.DefaultRowsPerSnapshotChunk > limits.MaxRowsPerSnapshotChunk {
		return fmt.Errorf("capabilities default_rows_per_snapshot_chunk exceeds maximum")
	}
	if limits.DefaultBytesPerSnapshotChunk > limits.MaxBytesPerSnapshotChunk {
		return fmt.Errorf("capabilities default_bytes_per_snapshot_chunk exceeds maximum")
	}
	if limits.MaxBytesPerSnapshotRow > limits.MaxBytesPerSnapshotChunk {
		return fmt.Errorf("capabilities max_bytes_per_snapshot_row exceeds chunk maximum")
	}
	return nil
}

func (response *CapabilitiesResponse) UnmarshalJSON(data []byte) error {
	type plainResponse CapabilitiesResponse
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"protocol_version", "schema_version", "registered_table_specs", "features", "bundle_limits"} {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("capabilities response missing required %s", name)
		}
	}
	var decoded plainResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Features == nil {
		return fmt.Errorf("capabilities response features must be an object")
	}
	if decoded.RegisteredTableSpecs == nil {
		return fmt.Errorf("capabilities response registered_table_specs must be an array")
	}
	seenTables := make(map[[2]string]struct{}, len(decoded.RegisteredTableSpecs))
	for _, spec := range decoded.RegisteredTableSpecs {
		key := [2]string{spec.Schema, spec.Table}
		if _, exists := seenTables[key]; exists {
			return fmt.Errorf("capabilities response registered_table_specs contains a duplicate table")
		}
		seenTables[key] = struct{}{}
	}
	*response = CapabilitiesResponse(decoded)
	return nil
}

// RegisteredTableSpec describes one registered sync table in the newer contract surface.
type RegisteredTableSpec struct {
	Schema         string   `json:"schema"`
	Table          string   `json:"table"`
	SyncKeyColumns []string `json:"sync_key_columns"`
}

func (spec *RegisteredTableSpec) UnmarshalJSON(data []byte) error {
	type plainSpec RegisteredTableSpec
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"schema", "table", "sync_key_columns"} {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("registered table spec missing required %s", name)
		}
	}
	var decoded plainSpec
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if strings.TrimSpace(decoded.Schema) == "" {
		return fmt.Errorf("registered table spec schema must be a non-blank identifier")
	}
	if strings.TrimSpace(decoded.Table) == "" {
		return fmt.Errorf("registered table spec table must be a non-blank identifier")
	}
	if len(decoded.SyncKeyColumns) != 1 {
		return fmt.Errorf("registered table spec sync_key_columns must contain exactly one entry")
	}
	if strings.TrimSpace(decoded.SyncKeyColumns[0]) == "" {
		return fmt.Errorf("registered table spec sync_key_columns must contain a non-blank identifier")
	}
	*spec = RegisteredTableSpec(decoded)
	return nil
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error             string `json:"error"`
	Message           string `json:"message"`
	RequiredByteCount int64  `json:"required_byte_count,omitempty"`
}

// SnapshotSessionLimitResponse reports a stable snapshot materialization limit failure.
type SnapshotSessionLimitResponse struct {
	Error     string `json:"error"`
	Message   string `json:"message"`
	Dimension string `json:"dimension"`
	Actual    int64  `json:"actual"`
	Limit     int64  `json:"limit"`
}

type SourceRetiredResponse struct {
	Error              string `json:"error"`
	Message            string `json:"message"`
	SourceID           string `json:"source_id"`
	ReplacedBySourceID string `json:"replaced_by_source_id,omitempty"`
}

func (response *SourceRetiredResponse) UnmarshalJSON(data []byte) error {
	type plainResponse SourceRetiredResponse
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"error", "message", "source_id"} {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("source_retired response missing required %s", name)
		}
	}
	if replacement, ok := fields["replaced_by_source_id"]; ok && string(replacement) == "null" {
		return fmt.Errorf("source_retired replaced_by_source_id must be omitted or non-null")
	}
	var decoded plainResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Error != "source_retired" {
		return fmt.Errorf("source_retired response has invalid error code")
	}
	if err := sourceid.Validate(decoded.SourceID); err != nil {
		return fmt.Errorf("source_retired response source_id is invalid: %w", err)
	}
	if _, present := fields["replaced_by_source_id"]; present {
		if err := sourceid.Validate(decoded.ReplacedBySourceID); err != nil {
			return fmt.Errorf("source_retired response replaced_by_source_id is invalid: %w", err)
		}
	}
	*response = SourceRetiredResponse(decoded)
	return nil
}

// PushConflictDetails describes one authoritative row state that rejected a push commit.
// The payload is deterministic:
//   - schema/table are normalized lowercase identifiers
//   - key uses the same canonical sync-key JSON shape as the rest of the API
//   - server_row_version is 0 when no authoritative row_state exists yet
//   - server_row_deleted distinguishes tombstones from an absent row when server_row is null
//   - server_row is the canonical wire-format full row after-image, or null if deleted/missing
type PushConflictDetails struct {
	Schema           string          `json:"schema"`
	Table            string          `json:"table"`
	Key              SyncKey         `json:"key"`
	Op               string          `json:"op"`
	BaseRowVersion   int64           `json:"base_row_version"`
	ServerRowVersion int64           `json:"server_row_version"`
	ServerRowDeleted bool            `json:"server_row_deleted"`
	ServerRow        json.RawMessage `json:"server_row"`
}

// PushConflictResponse preserves the legacy error/message envelope while adding machine-readable conflict details.
type PushConflictResponse struct {
	Error    string               `json:"error"`
	Message  string               `json:"message"`
	Conflict *PushConflictDetails `json:"conflict,omitempty"`
}

// StatusResponse represents service status response
type StatusResponse struct {
	Status                            string          `json:"status"`                                 // healthy or unhealthy
	Version                           string          `json:"version"`                                // API version
	AppName                           string          `json:"app_name"`                               // Application name
	Lifecycle                         string          `json:"lifecycle"`                              // running, shutting_down, closed
	AcceptingOperations               bool            `json:"accepting_operations"`                   // Whether new sync operations are accepted
	InFlightOperations                int             `json:"inflight_operations"`                    // Current in-flight sync operations
	RegisteredTables                  []string        `json:"registered_tables"`                      // Tables registered for sync
	Features                          map[string]bool `json:"features"`                               // Enabled features
	UserStateRetentionFloorAheadCount int64           `json:"user_state_retention_floor_ahead_count"` // user_state rows with retained_bundle_floor > next_bundle_seq - 1
	LatestBundleSeqMax                int64           `json:"latest_bundle_seq_max"`                  // highest committed bundle seq visible across sync.user_state
	RetainedBundleFloorMin            int64           `json:"retained_bundle_floor_min"`              // minimum retained_bundle_floor across sync.user_state
	RetainedBundleFloorMax            int64           `json:"retained_bundle_floor_max"`              // maximum retained_bundle_floor across sync.user_state
	RetainedBundleWindowMin           int64           `json:"retained_bundle_window_min"`             // minimum retained history window (latest bundle seq - retained floor)
	RetainedBundleWindowMax           int64           `json:"retained_bundle_window_max"`             // maximum retained history window (latest bundle seq - retained floor)
	HistoryPrunedErrorCount           int64           `json:"history_pruned_error_count"`             // total history_pruned responses observed by the server
	AcceptedPushReplayCount           int64           `json:"accepted_push_replay_count"`             // total accepted-push replay hits observed by the server
	RejectedRegisteredWriteCount      int64           `json:"rejected_registered_write_count"`        // total writes rejected for missing sync bundle context
	CommittedBundleCount              int64           `json:"committed_bundle_count"`                 // total committed bundle count visible in sync.bundle_log
	CommittedBundleBytes              int64           `json:"committed_bundle_bytes"`                 // total committed bundle bytes visible in sync.bundle_log
}
