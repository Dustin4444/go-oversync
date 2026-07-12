package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

const (
	lifecycleBindingAnonymous = attachmentBindingAnonymous
	lifecycleBindingAttached  = attachmentBindingAttached

	lifecycleTransitionNone   = operationKindNone
	lifecycleTransitionRemote = operationKindRemoteReplace
)

type lifecycleState struct {
	SourceID                 string
	BindingState             string
	BindingScope             string
	PendingTransitionKind    string
	PendingTargetScope       string
	PendingStagedSnapshotID  string
	PendingSnapshotBundleSeq int64
	PendingSnapshotRowCount  int64
	PendingInitializationID  string
}

// AttachLifecycleUnsupportedError reports that the server does not support the attach lifecycle contract.
type AttachLifecycleUnsupportedError struct {
	Reason string
}

// Error implements error.
func (e *AttachLifecycleUnsupportedError) Error() string {
	if e == nil || strings.TrimSpace(e.Reason) == "" {
		return "server does not support the oversqlite attach lifecycle"
	}
	return fmt.Sprintf("server does not support the oversqlite attach lifecycle: %s", e.Reason)
}

// AttachBindingConflictError reports that the local database is already attached to a different user.
type AttachBindingConflictError struct {
	AttachedUserID  string
	RequestedUserID string
}

// Error implements error.
func (e *AttachBindingConflictError) Error() string {
	return fmt.Sprintf("local database is already attached to user %q; detach before attaching user %q", e.AttachedUserID, e.RequestedUserID)
}

// AttachLocalStateConflictError reports that local durable sync state is incompatible with the requested attach flow.
type AttachLocalStateConflictError struct {
	Reason string
}

// Error implements error.
func (e *AttachLocalStateConflictError) Error() string {
	if e == nil || strings.TrimSpace(e.Reason) == "" {
		return "local sync state is incompatible with the requested attach lifecycle"
	}
	return fmt.Sprintf("local sync state is incompatible with the requested attach lifecycle: %s", e.Reason)
}

// RemoteReplacePendingError reports that a pending remote-authoritative replace must be finalized by Attach.
type RemoteReplacePendingError struct {
	TargetUserID string
}

// Error implements error.
func (e *RemoteReplacePendingError) Error() string {
	if e == nil || strings.TrimSpace(e.TargetUserID) == "" {
		return "remote-authoritative replacement is pending and must be finalized by Attach"
	}
	return fmt.Sprintf("remote-authoritative replacement for user %q is pending and must be finalized by Attach", e.TargetUserID)
}

// DestructiveTransitionInProgressError reports that a local lifecycle transition blocks safe sync execution.
type DestructiveTransitionInProgressError struct {
	TransitionKind string
}

// Error implements error.
func (e *DestructiveTransitionInProgressError) Error() string {
	if e == nil || strings.TrimSpace(e.TransitionKind) == "" {
		return "a destructive local lifecycle transition is in progress"
	}
	return fmt.Sprintf("destructive local lifecycle transition %q is in progress", e.TransitionKind)
}

// OpenRequiredError reports that an operation requires Open() first.
type OpenRequiredError struct {
	Operation string
}

// Error implements error.
func (e *OpenRequiredError) Error() string {
	if e == nil || strings.TrimSpace(e.Operation) == "" {
		return "Open() must be called before this oversqlite operation"
	}
	return fmt.Sprintf("Open() must be called before %s", e.Operation)
}

// AttachRequiredError reports that an operation requires a successful Attach(userID).
type AttachRequiredError struct {
	Operation string
}

// Error implements error.
func (e *AttachRequiredError) Error() string {
	if e == nil || strings.TrimSpace(e.Operation) == "" {
		return "Attach(userID) must complete successfully before this oversqlite operation"
	}
	return fmt.Sprintf("Attach(userID) must complete successfully before %s", e.Operation)
}

// IsLifecyclePreconditionError reports whether err is one of the typed lifecycle-precondition errors.
func IsLifecyclePreconditionError(err error) bool {
	if err == nil {
		return false
	}
	var openErr *OpenRequiredError
	if errors.As(err, &openErr) {
		return true
	}
	var connectErr *AttachRequiredError
	if errors.As(err, &connectErr) {
		return true
	}
	var transitionErr *DestructiveTransitionInProgressError
	return errors.As(err, &transitionErr)
}

func toLifecycleState(attachment *attachmentStateRecord, operation *operationStateRecord) *lifecycleState {
	if attachment == nil {
		attachment = &attachmentStateRecord{BindingState: attachmentBindingAnonymous}
	}
	if operation == nil {
		operation = &operationStateRecord{Kind: operationKindNone}
	}
	pendingTransitionKind := operation.Kind
	pendingTargetScope := operation.TargetUserID
	if operation.Kind == operationKindSourceRecovery {
		pendingTransitionKind = lifecycleTransitionNone
		pendingTargetScope = ""
	}
	return &lifecycleState{
		SourceID:                 attachment.CurrentSourceID,
		BindingState:             attachment.BindingState,
		BindingScope:             attachment.AttachedUserID,
		PendingTransitionKind:    pendingTransitionKind,
		PendingTargetScope:       pendingTargetScope,
		PendingStagedSnapshotID:  operation.StagedSnapshotID,
		PendingSnapshotBundleSeq: operation.SnapshotBundleSeq,
		PendingSnapshotRowCount:  operation.SnapshotRowCount,
		PendingInitializationID:  attachment.PendingInitializationID,
	}
}

func (c *Client) loadLifecycleState(ctx context.Context) (*lifecycleState, error) {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return nil, err
	}
	return toLifecycleState(attachment, operation), nil
}

func (c *Client) persistLifecycleStateInTx(ctx context.Context, tx sqliteTransaction, state *lifecycleState) error {
	if state == nil {
		return fmt.Errorf("lifecycle state is required")
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.CurrentSourceID = state.SourceID
	attachment.BindingState = state.BindingState
	attachment.AttachedUserID = state.BindingScope
	attachment.PendingInitializationID = state.PendingInitializationID
	if strings.TrimSpace(attachment.SchemaName) == "" {
		attachment.SchemaName = c.config.Schema
	}
	if err := ensureSourceState(ctx, tx, state.SourceID); err != nil {
		return err
	}
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return err
	}
	return persistOperationState(ctx, tx, &operationStateRecord{
		Kind:              state.PendingTransitionKind,
		TargetUserID:      state.PendingTargetScope,
		StagedSnapshotID:  state.PendingStagedSnapshotID,
		SnapshotBundleSeq: state.PendingSnapshotBundleSeq,
		SnapshotRowCount:  state.PendingSnapshotRowCount,
	})
}

func (c *Client) persistLifecycleStateInTxless(ctx context.Context, state *lifecycleState) error {
	if state == nil {
		return fmt.Errorf("lifecycle state is required")
	}
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin lifecycle persistence transaction: %w", err)
	}
	defer tx.Rollback()
	if err := c.persistLifecycleStateInTx(ctx, tx, state); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit lifecycle persistence transaction: %w", err)
	}
	return nil
}

func (c *Client) clearPersistedPendingInitializationID(ctx context.Context) error {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	attachment.PendingInitializationID = ""
	return persistAttachmentState(ctx, c.DB, attachment)
}

func (c *Client) generateFreshSourceID(ctx context.Context, q queryRower, currentSourceID string) (string, error) {
	if c == nil || c.sourceIDGenerator == nil {
		return "", fmt.Errorf("source-id generator is not configured")
	}
	if err := validateOptionalSourceID(currentSourceID); err != nil {
		return "", err
	}
	for attempt := 0; attempt < 100; attempt++ {
		candidate := c.sourceIDGenerator()
		if err := validateSourceID(candidate); err != nil {
			return "", fmt.Errorf("source-id generator returned an invalid value: %w", err)
		}
		if candidate == currentSourceID {
			continue
		}
		state, err := loadSourceState(ctx, q, candidate)
		if err != nil {
			return "", err
		}
		if state == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("failed to generate a fresh internal source id")
}

func (c *Client) openLocked(ctx context.Context) (*lifecycleState, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin lifecycle open transaction: %w", err)
	}
	defer tx.Rollback()

	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, tx)
	if err != nil {
		return nil, err
	}

	persistedSourceID := attachment.CurrentSourceID
	if err := validateOptionalSourceID(persistedSourceID); err != nil {
		return nil, fmt.Errorf("persisted current source id is invalid: %w", err)
	}
	needsBootstrapAnonymousCapture := persistedSourceID == "" && attachment.BindingState == attachmentBindingAnonymous
	if persistedSourceID == "" {
		generatedSourceID, err := c.generateFreshSourceID(ctx, tx, "")
		if err != nil {
			return nil, err
		}
		attachment.CurrentSourceID = generatedSourceID
	}
	if strings.TrimSpace(attachment.SchemaName) == "" {
		attachment.SchemaName = c.config.Schema
	}
	if needsBootstrapAnonymousCapture {
		if err := c.capturePreexistingAnonymousRowsInTx(ctx, tx); err != nil {
			return nil, err
		}
	}
	if err := ensureSourceState(ctx, tx, attachment.CurrentSourceID); err != nil {
		return nil, err
	}
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return nil, err
	}
	if err := setApplyMode(ctx, tx, false); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit lifecycle open transaction: %w", err)
	}
	state := toLifecycleState(attachment, operation)
	if state.PendingTransitionKind == lifecycleTransitionRemote {
		return state, &RemoteReplacePendingError{TargetUserID: state.PendingTargetScope}
	}
	return state, nil
}

func (c *Client) capturePreexistingAnonymousRowsInTx(ctx context.Context, tx *sql.Tx) error {
	for _, syncTable := range c.config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if tableName == "" {
			continue
		}

		pkColumn, err := c.primaryKeyColumnForTable(tableName)
		if err != nil {
			return err
		}
		isBlobPK, err := c.isPrimaryKeyBlobInTx(tx, tableName)
		if err != nil {
			return err
		}

		var pkExpr string
		if isBlobPK {
			pkExpr = fmt.Sprintf("lower(hex(%s))", quoteIdent(pkColumn))
		} else {
			pkExpr = fmt.Sprintf("CAST(%s AS TEXT)", quoteIdent(pkColumn))
		}
		query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", pkExpr, quoteIdent(tableName), quoteIdent(pkColumn))
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query preexisting keys for %s: %w", tableName, err)
		}

		var localPKs []string
		for rows.Next() {
			var localPK string
			if err := rows.Scan(&localPK); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan preexisting key for %s: %w", tableName, err)
			}
			localPKs = append(localPKs, localPK)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("failed to iterate preexisting keys for %s: %w", tableName, err)
		}
		rows.Close()

		keyColumns, err := c.syncKeyColumnsForTable(tableName)
		if err != nil {
			return err
		}
		if len(keyColumns) != 1 {
			return fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
		}
		keyName := strings.ToLower(strings.TrimSpace(keyColumns[0]))

		for _, localPK := range localPKs {
			keyJSONBytes, err := json.Marshal(map[string]any{keyName: localPK})
			if err != nil {
				return fmt.Errorf("failed to encode preexisting key for %s.%s: %w", tableName, localPK, err)
			}
			payload, err := c.serializeRowInTx(ctx, tx, tableName, localPK)
			if err != nil {
				return fmt.Errorf("failed to serialize preexisting row for %s.%s: %w", tableName, localPK, err)
			}
			if err := c.requeueDirtyIntentInTx(
				ctx,
				tx,
				c.config.Schema,
				tableName,
				string(keyJSONBytes),
				oversync.OpInsert,
				0,
				sql.NullString{String: string(payload), Valid: true},
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) ensureAttachedClientStateLocked(ctx context.Context, userID, sourceID string) error {
	if err := ensureSourceState(ctx, c.DB, sourceID); err != nil {
		return err
	}
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	if attachment.BindingState == attachmentBindingAttached && attachment.AttachedUserID != "" && attachment.AttachedUserID != userID {
		return &AttachLocalStateConflictError{Reason: "existing attached state belongs to another user"}
	}
	if attachment.CurrentSourceID != "" && attachment.CurrentSourceID != sourceID {
		return &AttachLocalStateConflictError{Reason: "existing attached state is bound to another source"}
	}
	return nil
}

func (c *Client) hasAttachedClientStateLocked(ctx context.Context, userID, sourceID string) (bool, error) {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return false, err
	}
	return attachment.BindingState == attachmentBindingAttached &&
		attachment.AttachedUserID == userID &&
		attachment.CurrentSourceID == sourceID, nil
}

func (c *Client) verifyConnectLifecycleSupported(ctx context.Context) error {
	caps, err := c.fetchValidatedCapabilities(ctx, "connect_capabilities")
	if err != nil {
		return err
	}
	if caps.Features == nil || !caps.Features["connect_lifecycle"] {
		return &AttachLifecycleUnsupportedError{Reason: "connect_lifecycle capability is absent"}
	}
	return nil
}

const requiredProtocolVersion = "v1"

// ProtocolVersionMismatchError is a non-retryable decoded protocol incompatibility.
type ProtocolVersionMismatchError struct {
	Expected string
	Actual   string
}

func (e *ProtocolVersionMismatchError) Error() string {
	return "oversqlite protocol version mismatch"
}

func requireSupportedProtocolVersion(actual string) error {
	if actual != requiredProtocolVersion {
		return &ProtocolVersionMismatchError{Expected: requiredProtocolVersion}
	}
	return nil
}

func isProtocolVersionMismatch(err error) bool {
	var mismatch *ProtocolVersionMismatchError
	return errors.As(err, &mismatch)
}

func (c *Client) validateProtocolGateLocked(ctx context.Context, operation string) error {
	_, err := c.fetchValidatedCapabilities(ctx, operation)
	return err
}

func (c *Client) fetchValidatedCapabilities(ctx context.Context, operation string) (*oversync.CapabilitiesResponse, error) {
	result, err := c.doAuthenticatedBoundedRequestWithRetry(
		ctx,
		operation,
		http.MethodGet,
		strings.TrimRight(c.BaseURL, "/")+"/sync/capabilities",
		nil,
		"",
		snapshotCapabilitiesBodyLimit,
		snapshotControlBodyLimit,
		nil,
		nil,
	)
	if err != nil {
		return nil, err
	}
	if result.statusCode == http.StatusNotFound || result.statusCode == http.StatusMethodNotAllowed || result.statusCode == http.StatusNotImplemented {
		return nil, &AttachLifecycleUnsupportedError{Reason: "missing /sync/capabilities endpoint"}
	}
	if result.statusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d while fetching capabilities", result.statusCode)
	}

	var caps oversync.CapabilitiesResponse
	if err := json.Unmarshal(result.body, &caps); err != nil {
		return nil, fmt.Errorf("invalid capabilities response: %w", err)
	}
	if err := requireSupportedProtocolVersion(caps.ProtocolVersion); err != nil {
		return nil, err
	}
	return &caps, nil
}

type snapshotNegotiation struct {
	maxRows  int
	maxBytes int64
}

func (c *Client) negotiateSnapshotLimits(ctx context.Context) (snapshotNegotiation, error) {
	caps, err := c.fetchValidatedCapabilities(ctx, "snapshot_capabilities")
	if err != nil {
		return snapshotNegotiation{}, err
	}
	limits := caps.BundleLimits
	if limits.DefaultRowsPerSnapshotChunk <= 0 || limits.MaxRowsPerSnapshotChunk <= 0 {
		return snapshotNegotiation{}, fmt.Errorf("snapshot capabilities require positive default_rows_per_snapshot_chunk and max_rows_per_snapshot_chunk")
	}
	if limits.DefaultRowsPerSnapshotChunk > limits.MaxRowsPerSnapshotChunk {
		return snapshotNegotiation{}, fmt.Errorf("snapshot capability default_rows_per_snapshot_chunk exceeds max_rows_per_snapshot_chunk")
	}
	if limits.DefaultBytesPerSnapshotChunk <= 0 || limits.MaxBytesPerSnapshotChunk <= 0 || limits.MaxBytesPerSnapshotRow <= 0 {
		return snapshotNegotiation{}, fmt.Errorf("snapshot capabilities require positive default/max chunk byte and max row byte limits")
	}
	if limits.DefaultBytesPerSnapshotChunk > limits.MaxBytesPerSnapshotChunk {
		return snapshotNegotiation{}, fmt.Errorf("snapshot capability default_bytes_per_snapshot_chunk exceeds max_bytes_per_snapshot_chunk")
	}
	if limits.MaxBytesPerSnapshotRow > limits.MaxBytesPerSnapshotChunk {
		return snapshotNegotiation{}, fmt.Errorf("snapshot capability max_bytes_per_snapshot_row exceeds max_bytes_per_snapshot_chunk")
	}
	if limits.MaxConcurrentSnapshotBuilds <= 0 || limits.MaxConcurrentSnapshotChunkRequests <= 0 {
		return snapshotNegotiation{}, fmt.Errorf("snapshot capabilities require positive max_concurrent_snapshot_builds and max_concurrent_snapshot_chunk_requests")
	}
	if c == nil || c.config == nil || c.config.SnapshotChunkRows <= 0 || c.config.SnapshotChunkBytes <= 0 {
		return snapshotNegotiation{}, fmt.Errorf("snapshot client row and byte budgets must be positive")
	}
	effectiveRows := min(c.config.SnapshotChunkRows, limits.MaxRowsPerSnapshotChunk)
	effectiveBytes := min(c.config.SnapshotChunkBytes, limits.MaxBytesPerSnapshotChunk)
	if effectiveBytes < limits.MaxBytesPerSnapshotRow {
		return snapshotNegotiation{}, fmt.Errorf(
			"effective snapshot chunk byte budget %d is below server max_bytes_per_snapshot_row %d; increase Config.SnapshotChunkBytes",
			effectiveBytes,
			limits.MaxBytesPerSnapshotRow,
		)
	}
	return snapshotNegotiation{maxRows: effectiveRows, maxBytes: effectiveBytes}, nil
}

func (c *Client) ensureNoDestructiveTransitionLocked(ctx context.Context) error {
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return err
	}
	switch operation.Kind {
	case operationKindNone, operationKindRemoteReplace:
		return nil
	default:
		return &DestructiveTransitionInProgressError{TransitionKind: operation.Kind}
	}
}

func (c *Client) ensureConnectedSessionLocked(ctx context.Context, operation string) error {
	if err := validateOptionalSourceID(c.sourceID); err != nil {
		return err
	}
	if c.sourceID == "" {
		return &OpenRequiredError{Operation: operation}
	}
	userID := strings.TrimSpace(c.UserID)
	if userID == "" {
		return &AttachRequiredError{Operation: operation}
	}
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	if attachment.BindingState != attachmentBindingAttached ||
		attachment.AttachedUserID != userID ||
		attachment.CurrentSourceID != c.sourceID ||
		!c.sessionConnected {
		return &AttachRequiredError{Operation: operation}
	}
	return nil
}
