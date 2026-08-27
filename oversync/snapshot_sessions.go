package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

type SnapshotSessionNotFoundError struct {
	SnapshotID string
}

func (e *SnapshotSessionNotFoundError) Error() string {
	return fmt.Sprintf("snapshot session %s was not found", e.SnapshotID)
}

type SnapshotSessionExpiredError struct {
	SnapshotID string
}

func (e *SnapshotSessionExpiredError) Error() string {
	return fmt.Sprintf("snapshot session %s has expired; start a new snapshot session", e.SnapshotID)
}

type SnapshotChunkInvalidError struct {
	Message string
}

func (e *SnapshotChunkInvalidError) Error() string {
	return e.Message
}

type SnapshotSessionForbiddenError struct {
	SnapshotID string
}

func (e *SnapshotSessionForbiddenError) Error() string {
	return fmt.Sprintf("snapshot session %s does not belong to the authenticated user", e.SnapshotID)
}

type SnapshotSessionLimitExceededError struct {
	Dimension string
	Actual    int64
	Limit     int64
}

type SnapshotCapacityError struct{ Operation string }

func (e *SnapshotCapacityError) Error() string {
	return "snapshot " + e.Operation + " capacity is exhausted"
}

type SnapshotChunkTooSmallError struct{ RequiredByteCount int64 }

func (e *SnapshotChunkTooSmallError) Error() string {
	return fmt.Sprintf("max_bytes is too small for the next snapshot row; required_byte_count=%d", e.RequiredByteCount)
}

func (e *SnapshotSessionLimitExceededError) Error() string {
	return fmt.Sprintf("snapshot session %s %d exceeds limit %d", e.Dimension, e.Actual, e.Limit)
}

type snapshotMaterializedRow struct {
	rowOrdinal    int64
	tableID       int32
	keyBytes      []byte
	bundleSeq     int64
	payloadWire   []byte
	wireByteCount int64
}

func (s *SyncService) CreateSnapshotSession(ctx context.Context, actor Actor) (_ *SnapshotSession, err error) {
	return s.CreateSnapshotSessionWithRequest(ctx, actor, nil)
}

func (s *SyncService) CreateSnapshotSessionWithRequest(ctx context.Context, actor Actor, req *SnapshotSessionCreateRequest) (_ *SnapshotSession, err error) {
	if err := rejectRetryableCallbackContext(ctx, "CreateSnapshotSession"); err != nil {
		return nil, err
	}
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := s.validateClientActor(actor, false); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	if !s.tryAcquireSnapshotBuild() {
		s.logger.Info("Snapshot build rejected", "user_id", actor.UserID, "outcome", "rejected", "duration", time.Since(startedAt), "admission_outcome", "rejected", "active_build_count_high_water", s.snapshotMetrics.buildHighWater.Load())
		return nil, &SnapshotCapacityError{Operation: "build"}
	}
	defer s.releaseSnapshotBuild()
	var snapshotID string
	var batchHighWater snapshotBatchHighWater
	defer func() {
		if err != nil {
			s.logger.Info("Snapshot build failed", "user_id", actor.UserID, "snapshot_id", snapshotID, "outcome", "failed", "materialization_batch_count", batchHighWater.batches, "batch_rows_high_water", batchHighWater.rows, "batch_bytes_high_water", batchHighWater.bytes, "duration", time.Since(startedAt), "admission_outcome", "admitted", "active_build_count_high_water", s.snapshotMetrics.buildHighWater.Load(), "error", err)
		}
	}()
	if s.snapshotHooks != nil && s.snapshotHooks.afterBuildPermit != nil {
		if err := s.snapshotHooks.afterBuildPermit(ctx); err != nil {
			return nil, err
		}
	}

	var resp *SnapshotSession
	err = pgx.BeginTxFunc(ctx, s.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		if err := requireScopeInitializedQuerier(ctx, tx, actor.UserID); err != nil {
			return err
		}

		retainedState, err := lockSnapshotRetainedHistoryState(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}
		if retainedState == nil {
			return fmt.Errorf("missing retained history state for %q", actor.UserID)
		}
		if err := s.applySnapshotSourceReplacementInTx(ctx, tx, actor, retainedState.UserPK, req); err != nil {
			return err
		}

		snapshotBundleSeq := retainedState.highestBundleSeq()
		snapshotID = uuid.NewString()
		var expiresAt time.Time
		if err := tx.QueryRow(ctx, `
			INSERT INTO sync.snapshot_sessions (
				snapshot_id, user_pk, snapshot_bundle_seq, row_count, byte_count, expires_at
			) VALUES ($1::uuid, $2, $3, 0, 0,
				transaction_timestamp() + $4::bigint * interval '1 microsecond')
			RETURNING expires_at
		`, snapshotID, retainedState.UserPK, snapshotBundleSeq, s.snapshotSessionTTL().Microseconds()).Scan(&expiresAt); err != nil {
			return fmt.Errorf("insert snapshot session: %w", err)
		}
		if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotFence != nil {
			if err := s.snapshotHooks.afterSnapshotFence(ctx); err != nil {
				return err
			}
		}
		rowCount, byteCount, materializationHighWater, err := s.materializeSnapshotRows(ctx, tx, snapshotID, actor.UserID, retainedState.UserPK)
		batchHighWater = materializationHighWater
		observeAtomicHighWater(&s.snapshotMetrics.materializationBatchRowsHighWater, int64(batchHighWater.rows))
		observeAtomicHighWater(&s.snapshotMetrics.materializationBatchBytesHighWater, batchHighWater.bytes)
		if err != nil {
			return err
		}
		if s.snapshotHooks != nil && s.snapshotHooks.beforeSnapshotFinalize != nil {
			if err := s.snapshotHooks.beforeSnapshotFinalize(ctx); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE sync.snapshot_sessions SET row_count=$2, byte_count=$3 WHERE snapshot_id=$1::uuid`, snapshotID, rowCount, byteCount); err != nil {
			return fmt.Errorf("finalize snapshot session counts: %w", err)
		}

		resp = &SnapshotSession{
			SnapshotID:        snapshotID,
			SnapshotBundleSeq: snapshotBundleSeq,
			RowCount:          rowCount,
			ByteCount:         byteCount,
			ExpiresAt:         expiresAt.Format(time.RFC3339Nano),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotCommit != nil {
		if err := s.snapshotHooks.afterSnapshotCommit(ctx); err != nil {
			return nil, err
		}
	}
	s.logger.Info("Snapshot build completed", "user_id", actor.UserID, "snapshot_id", resp.SnapshotID, "outcome", "completed", "row_count", resp.RowCount, "byte_count", resp.ByteCount, "materialization_batch_count", batchHighWater.batches, "batch_rows_high_water", batchHighWater.rows, "batch_bytes_high_water", batchHighWater.bytes, "duration", time.Since(startedAt), "admission_outcome", "admitted", "active_build_count_high_water", s.snapshotMetrics.buildHighWater.Load())
	s.requestSnapshotCleanup("post_create")
	return resp, nil
}

func lockSnapshotRetainedHistoryState(ctx context.Context, tx pgx.Tx, userID string) (*retainedHistoryState, error) {
	state, err := scanRetainedHistoryState(tx.QueryRow(ctx, `
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = $1
		FOR UPDATE
	`, userID))
	if err != nil {
		return nil, fmt.Errorf("lock retained history state for snapshot %q: %w", userID, err)
	}
	return state, nil
}

func tryAcquireSnapshotPermit(permits chan struct{}) bool {
	select {
	case permits <- struct{}{}:
		return true
	default:
		return false
	}
}
func releaseSnapshotPermit(permits chan struct{}) { <-permits }

func (s *SyncService) validateSnapshotSourceReplacement(actor Actor, replacement *SnapshotSourceReplacement) (*SnapshotSourceReplacement, error) {
	if replacement == nil {
		return nil, nil
	}
	clean := &SnapshotSourceReplacement{
		PreviousSourceID: replacement.PreviousSourceID,
		NewSourceID:      replacement.NewSourceID,
		Reason:           replacement.Reason,
	}
	if err := sourceid.Validate(clean.PreviousSourceID); err != nil {
		return nil, &SnapshotSessionInvalidError{Message: "previous_source_id is invalid"}
	}
	if err := sourceid.Validate(clean.NewSourceID); err != nil {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id is invalid"}
	}
	if s.isReservedServerSourceID(clean.PreviousSourceID) || s.isReservedServerSourceID(clean.NewSourceID) {
		return nil, &SnapshotSessionInvalidError{Message: "source replacement is invalid"}
	}
	if clean.PreviousSourceID != actor.SourceID {
		return nil, &SnapshotSessionInvalidError{Message: "previous_source_id must match authenticated source_id"}
	}
	if clean.NewSourceID == clean.PreviousSourceID {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id must differ from previous_source_id"}
	}
	switch clean.Reason {
	case "history_pruned", "source_sequence_out_of_order", "source_sequence_changed", "source_retired":
	default:
		return nil, &SnapshotSessionInvalidError{Message: "source replacement reason is unsupported"}
	}
	return clean, nil
}

func validateSnapshotSourceReplacement(actor Actor, replacement *SnapshotSourceReplacement) (*SnapshotSourceReplacement, error) {
	return (*SyncService)(nil).validateSnapshotSourceReplacement(actor, replacement)
}

func (s *SyncService) applySnapshotSourceReplacementInTx(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, req *SnapshotSessionCreateRequest) error {
	if req == nil || req.SourceReplacement == nil {
		return nil
	}
	replacement, err := s.validateSnapshotSourceReplacement(actor, req.SourceReplacement)
	if err != nil {
		return err
	}

	previousState, err := loadSourceStateRow(ctx, tx, userPK, replacement.PreviousSourceID, true)
	if err != nil {
		return err
	}
	newState, err := loadSourceStateRow(ctx, tx, userPK, replacement.NewSourceID, true)
	if err != nil {
		return err
	}

	if previousState != nil && previousState.State == sourceStateRetired {
		if previousState.ReplacedBySourceID != replacement.NewSourceID {
			return &SourceRetiredError{
				UserID:             actor.UserID,
				SourceID:           replacement.PreviousSourceID,
				ReplacedBySourceID: previousState.ReplacedBySourceID,
			}
		}
		if newState == nil {
			if err := reserveSourceState(ctx, tx, userPK, replacement.NewSourceID); err != nil {
				return err
			}
		} else if newState.State != sourceStateReserved && newState.State != sourceStateActive {
			return &SourceReplacementInvalidError{Message: "source replacement is invalid"}
		}
	} else {
		if newState != nil {
			return &SourceReplacementInvalidError{Message: "source replacement is invalid"}
		}
		if err := reserveSourceState(ctx, tx, userPK, replacement.NewSourceID); err != nil {
			return err
		}
		if err := retireSourceState(ctx, tx, userPK, replacement.PreviousSourceID, replacement.NewSourceID, replacement.Reason); err != nil {
			return err
		}
	}

	return nil
}

type snapshotBatchHighWater struct {
	rows    int
	bytes   int64
	batches int
}

func (s *SyncService) materializeSnapshotRows(ctx context.Context, tx pgx.Tx, snapshotID, userID string, userPK int64) (int64, int64, snapshotBatchHighWater, error) {
	tableInfos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		tableInfos = append(tableInfos, info)
	}
	sort.Slice(tableInfos, func(i, j int) bool { return tableInfos[i].tableID < tableInfos[j].tableID })

	var rowCount, byteCount int64
	highWater := snapshotBatchHighWater{}
	for _, info := range tableInfos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		keyIdent := pgx.Identifier{info.syncKeyColumn}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		businessKeyFromBytesExpr := "convert_from(page.key_bytes, 'UTF8')"
		if info.syncKeyType == syncKeyTypeUUID {
			businessKeyFromBytesExpr = "encode(page.key_bytes, 'hex')::uuid"
		}
		var lastKey []byte
		for {
			cursorPredicate := ""
			limitPlaceholder := "$4"
			queryArgs := []any{userID, userPK, info.tableID, s.config.SnapshotMaterializationBatchRows}
			if lastKey != nil {
				cursorPredicate = " AND rs.key_bytes > $4"
				limitPlaceholder = "$5"
				queryArgs = []any{userID, userPK, info.tableID, lastKey, s.config.SnapshotMaterializationBatchRows}
			}
			query := fmt.Sprintf(`
				WITH page AS MATERIALIZED (
					SELECT rs.key_bytes, rs.bundle_seq
					FROM sync.row_state AS rs
					WHERE rs.user_pk=$2 AND rs.table_id=$3 AND rs.deleted=FALSE%s
					ORDER BY rs.key_bytes
					LIMIT %s
				)
				SELECT page.key_bytes, page.bundle_seq, to_jsonb(src) - '_sync_scope_id'
				FROM page
				JOIN %s AS src
				  ON src.%s=$1 AND src.%s=%s
				ORDER BY page.key_bytes
			`, cursorPredicate, limitPlaceholder, tableIdent, ownerIdent, keyIdent, businessKeyFromBytesExpr)
			liveRows, err := tx.Query(ctx, query, queryArgs...)
			if err != nil {
				return 0, 0, highWater, fmt.Errorf("query snapshot page for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			s.snapshotMetrics.materializationPageQueries.Add(1)

			batch := make([]snapshotMaterializedRow, 0, s.config.SnapshotMaterializationBatchRows)
			var batchBytes int64
			for liveRows.Next() {
				var keyBytes, payloadDB []byte
				var bundleSeq int64
				if err := liveRows.Scan(&keyBytes, &bundleSeq, &payloadDB); err != nil {
					liveRows.Close()
					return 0, 0, highWater, fmt.Errorf("scan snapshot page: %w", err)
				}
				if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotRowRead != nil {
					if err := s.snapshotHooks.afterSnapshotRowRead(ctx); err != nil {
						liveRows.Close()
						return 0, 0, highWater, err
					}
				}
				if s.snapshotHooks != nil && s.snapshotHooks.beforeSnapshotCanonicalize != nil {
					if err := s.snapshotHooks.beforeSnapshotCanonicalize(ctx); err != nil {
						liveRows.Close()
						return 0, 0, highWater, err
					}
				}
				payloadWire, err := s.canonicalizeWirePayload(info.schemaName, info.tableName, payloadDB)
				if err != nil {
					liveRows.Close()
					return 0, 0, highWater, err
				}
				key, err := wireSyncKeyFromBytes(info, keyBytes)
				if err != nil {
					liveRows.Close()
					return 0, 0, highWater, err
				}
				wire, err := json.Marshal(SnapshotRow{Schema: info.schemaName, Table: info.tableName, Key: key, RowVersion: bundleSeq, Payload: payloadWire})
				if err != nil {
					liveRows.Close()
					return 0, 0, highWater, fmt.Errorf("encode snapshot row wire form: %w", err)
				}
				wireBytes := int64(len(wire))
				if wireBytes > s.maxBytesPerSnapshotRow() {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "row_byte_count", Actual: wireBytes, Limit: s.maxBytesPerSnapshotRow()}
				}
				if len(batch) > 0 && batchBytes+wireBytes > s.config.SnapshotMaterializationBatchBytes {
					break
				}
				nextRows, nextBytes := rowCount+int64(len(batch))+1, byteCount+batchBytes+wireBytes
				if limit := s.maxRowsPerSnapshotSession(); limit > 0 && nextRows > limit {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "row_count", Actual: nextRows, Limit: limit}
				}
				if limit := s.maxBytesPerSnapshotSession(); limit > 0 && nextBytes > limit {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "byte_count", Actual: nextBytes, Limit: limit}
				}
				batch = append(batch, snapshotMaterializedRow{rowOrdinal: nextRows, tableID: info.tableID, keyBytes: append([]byte(nil), keyBytes...), bundleSeq: bundleSeq, payloadWire: append([]byte(nil), payloadWire...), wireByteCount: wireBytes})
				batchBytes += wireBytes
			}
			if err := liveRows.Err(); err != nil {
				liveRows.Close()
				return 0, 0, highWater, fmt.Errorf("iterate snapshot page: %w", err)
			}
			liveRows.Close()
			if len(batch) == 0 {
				break
			}
			copyRows := make([][]any, len(batch))
			for i := range batch {
				r := &batch[i]
				copyRows[i] = []any{snapshotID, r.rowOrdinal, r.tableID, r.keyBytes, r.bundleSeq, string(r.payloadWire), r.wireByteCount}
			}
			if s.snapshotHooks != nil && s.snapshotHooks.beforeSnapshotCopy != nil {
				if err := s.snapshotHooks.beforeSnapshotCopy(ctx); err != nil {
					return 0, 0, highWater, err
				}
			}
			written, err := tx.CopyFrom(ctx, pgx.Identifier{"sync", "snapshot_session_rows"}, []string{"snapshot_id", "row_ordinal", "table_id", "key_bytes", "bundle_seq", "payload_wire", "wire_byte_count"}, pgx.CopyFromRows(copyRows))
			if err != nil {
				return 0, 0, highWater, fmt.Errorf("COPY snapshot rows: %w", err)
			}
			if written != int64(len(batch)) {
				return 0, 0, highWater, fmt.Errorf("COPY snapshot rows wrote %d, want %d", written, len(batch))
			}
			s.snapshotMetrics.materializationCopyBatches.Add(1)
			highWater.batches++
			rowCount += written
			byteCount += batchBytes
			lastKey = append(lastKey[:0], batch[len(batch)-1].keyBytes...)
			if len(batch) > highWater.rows {
				highWater.rows = len(batch)
			}
			if batchBytes > highWater.bytes {
				highWater.bytes = batchBytes
			}
			if len(batch) < s.config.SnapshotMaterializationBatchRows && batchBytes < s.config.SnapshotMaterializationBatchBytes {
				continue
			}
		}
	}
	var stateCount int64
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state WHERE user_pk=$1 AND deleted=FALSE`, userPK).Scan(&stateCount); err != nil {
		return 0, 0, highWater, fmt.Errorf("count snapshot row_state: %w", err)
	}
	if rowCount != stateCount {
		return 0, 0, highWater, fmt.Errorf("snapshot integrity mismatch: materialized %d rows but live row_state contains %d", rowCount, stateCount)
	}
	// Each registered table count must also be represented by the exact row_state join.
	var allBusinessRows int64
	for _, info := range tableInfos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		var n int64
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s=$1`, tableIdent, ownerIdent), userID).Scan(&n); err != nil {
			return 0, 0, highWater, err
		}
		allBusinessRows += n
	}
	if rowCount != allBusinessRows {
		return 0, 0, highWater, fmt.Errorf("snapshot integrity mismatch: materialized %d rows but business tables contain %d", rowCount, allBusinessRows)
	}
	return rowCount, byteCount, highWater, nil
}

func (s *SyncService) GetSnapshotChunk(ctx context.Context, actor Actor, snapshotID string, afterRowOrdinal int64, maxRows int, maxBytes int64) (_ *SnapshotChunkResponse, err error) {
	if err := rejectRetryableCallbackContext(ctx, "GetSnapshotChunk"); err != nil {
		return nil, err
	}
	response, release, err := s.getSnapshotChunkWithLease(ctx, actor, snapshotID, afterRowOrdinal, maxRows, maxBytes)
	if release != nil {
		defer release()
	}
	return response, err
}

type snapshotChunkObservation struct {
	requestedMaxRows       int
	requestedMaxBytes      int64
	effectiveMaxRows       int
	effectiveMaxBytes      int64
	returnedRows           int64
	returnedBytes          int64
	selectedRowsHighWater  int64
	selectedBytesHighWater int64
	retainedRowsHighWater  int64
	retainedBytesHighWater int64
	selectionStartedAt     time.Time
	admitted               bool
}

func (o *snapshotChunkObservation) finish(s *SyncService, actor Actor, snapshotID string, err error) {
	admissionOutcome := "rejected"
	outcome := "rejected"
	selectionDuration := time.Duration(0)
	if !o.selectionStartedAt.IsZero() {
		selectionDuration = time.Since(o.selectionStartedAt)
		observeAtomicHighWater(&s.snapshotMetrics.chunkSelectionDurationHighWater, selectionDuration.Nanoseconds())
	}
	if o.admitted {
		admissionOutcome = "admitted"
		if err == nil {
			outcome = "completed"
			s.snapshotMetrics.chunkCompletions.Add(1)
		} else {
			outcome = "failed"
			s.snapshotMetrics.chunkFailures.Add(1)
		}
	} else {
		s.snapshotMetrics.chunkRejections.Add(1)
	}
	observeAtomicHighWater(&s.snapshotMetrics.chunkRowsHighWater, o.selectedRowsHighWater)
	observeAtomicHighWater(&s.snapshotMetrics.chunkBytesHighWater, o.selectedBytesHighWater)
	observeAtomicHighWater(&s.snapshotMetrics.chunkRetainedRowsHighWater, o.retainedRowsHighWater)
	observeAtomicHighWater(&s.snapshotMetrics.chunkRetainedBytesHighWater, o.retainedBytesHighWater)

	attrs := []any{
		"user_id", actor.UserID,
		"snapshot_id", snapshotID,
		"requested_max_rows", o.requestedMaxRows,
		"requested_max_bytes", o.requestedMaxBytes,
		"effective_max_rows", o.effectiveMaxRows,
		"effective_max_bytes", o.effectiveMaxBytes,
		"returned_rows", o.returnedRows,
		"returned_bytes", o.returnedBytes,
		"selected_rows_high_water", o.selectedRowsHighWater,
		"selected_bytes_high_water", o.selectedBytesHighWater,
		"retained_rows_high_water", o.retainedRowsHighWater,
		"retained_bytes_high_water", o.retainedBytesHighWater,
		"selection_duration", selectionDuration,
		"admission_outcome", admissionOutcome,
		"outcome", outcome,
		"active_chunk_count_high_water", s.snapshotMetrics.chunkHighWater.Load(),
	}
	if outcome == "failed" {
		attrs = append(attrs, "error", err)
	}
	s.logger.Info("Snapshot chunk selection completed", attrs...)
}

func (s *SyncService) getSnapshotChunkWithLease(ctx context.Context, actor Actor, snapshotID string, afterRowOrdinal int64, maxRows int, maxBytes int64) (_ *SnapshotChunkResponse, release func(), err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, nil, err
	}
	permitAcquired := false
	var releaseOnce sync.Once
	release = func() {
		releaseOnce.Do(func() {
			if permitAcquired {
				s.releaseSnapshotChunk()
			}
			done()
		})
	}
	succeeded := false
	defer func() {
		if !succeeded {
			release()
		}
	}()
	if err := s.validateClientActor(actor, false); err != nil {
		return nil, release, err
	}
	if snapshotID == "" {
		return nil, release, &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}
	if afterRowOrdinal < 0 {
		return nil, release, &SnapshotChunkInvalidError{Message: "after_row_ordinal must be >= 0"}
	}
	if maxRows <= 0 {
		return nil, release, &SnapshotChunkInvalidError{Message: "max_rows must be > 0"}
	}
	if maxBytes <= 0 {
		return nil, release, &SnapshotChunkInvalidError{Message: "max_bytes must be > 0"}
	}
	effectiveMaxRows := maxRows
	if effectiveMaxRows > s.maxRowsPerSnapshotChunk() {
		effectiveMaxRows = s.maxRowsPerSnapshotChunk()
	}
	effectiveMaxBytes := maxBytes
	if effectiveMaxBytes > s.maxBytesPerSnapshotChunk() {
		effectiveMaxBytes = s.maxBytesPerSnapshotChunk()
	}
	observation := snapshotChunkObservation{
		requestedMaxRows:  maxRows,
		requestedMaxBytes: maxBytes,
		effectiveMaxRows:  effectiveMaxRows,
		effectiveMaxBytes: effectiveMaxBytes,
	}
	s.snapshotMetrics.chunkRequests.Add(1)
	defer func() { observation.finish(s, actor, snapshotID, err) }()
	if !s.tryAcquireSnapshotChunk() {
		return nil, release, &SnapshotCapacityError{Operation: "chunk"}
	}
	permitAcquired = true
	observation.admitted = true
	if s.snapshotHooks != nil && s.snapshotHooks.afterChunkPermit != nil {
		if err := s.snapshotHooks.afterChunkPermit(ctx); err != nil {
			return nil, release, err
		}
	}
	observation.selectionStartedAt = time.Now()

	var resp *SnapshotChunkResponse
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly, DeferrableMode: pgx.NotDeferrable}, func(tx pgx.Tx) error {
		var (
			sessionUserID     string
			snapshotBundleSeq int64
			expired           bool
		)
		if err := tx.QueryRow(ctx, `
			SELECT us.user_id, ss.snapshot_bundle_seq, ss.expires_at <= transaction_timestamp()
			FROM sync.snapshot_sessions AS ss
			JOIN sync.user_state AS us ON us.user_pk = ss.user_pk
			WHERE ss.snapshot_id = $1::uuid
		`, snapshotID).Scan(&sessionUserID, &snapshotBundleSeq, &expired); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session: %w", err)
		}
		if sessionUserID != actor.UserID {
			return &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
		}
		if expired {
			return &SnapshotSessionExpiredError{SnapshotID: snapshotID}
		}
		if s.snapshotHooks != nil && s.snapshotHooks.afterChunkSessionRead != nil {
			if err := s.snapshotHooks.afterChunkSessionRead(ctx); err != nil {
				return err
			}
		}

		queryRows, err := tx.Query(ctx, `
			SELECT row_ordinal, table_id, key_bytes, bundle_seq, payload_wire, wire_byte_count
			FROM sync.snapshot_session_rows
			WHERE snapshot_id = $1::uuid
			  AND row_ordinal > $2
			ORDER BY row_ordinal
			LIMIT $3
		`, snapshotID, afterRowOrdinal, effectiveMaxRows+1)
		if err != nil {
			return fmt.Errorf("query snapshot chunk rows: %w", err)
		}
		s.snapshotMetrics.chunkQueries.Add(1)
		defer queryRows.Close()

		chunkRows := make([]SnapshotRow, 0, effectiveMaxRows)
		var chunkBytes int64
		hasMore := false
		nextRowOrdinal := afterRowOrdinal
		for queryRows.Next() {
			var (
				row           SnapshotRow
				tableID       int32
				keyBytes      []byte
				rowOrdinal    int64
				wireByteCount int64
			)
			if err := queryRows.Scan(&rowOrdinal, &tableID, &keyBytes, &row.RowVersion, &row.Payload, &wireByteCount); err != nil {
				return fmt.Errorf("scan snapshot chunk row: %w", err)
			}
			candidateRetainedRows := int64(len(chunkRows) + 1)
			candidateRetainedBytes := chunkBytes + wireByteCount
			if candidateRetainedRows > observation.retainedRowsHighWater {
				observation.retainedRowsHighWater = candidateRetainedRows
			}
			if candidateRetainedBytes > observation.retainedBytesHighWater {
				observation.retainedBytesHighWater = candidateRetainedBytes
			}
			if len(chunkRows) >= effectiveMaxRows || chunkBytes+wireByteCount > effectiveMaxBytes {
				hasMore = true
				if len(chunkRows) == 0 {
					return &SnapshotChunkTooSmallError{RequiredByteCount: wireByteCount}
				}
				break
			}
			info, err := s.tableInfoForID(tableID)
			if err != nil {
				return err
			}
			row.Schema = info.schemaName
			row.Table = info.tableName
			row.Key, err = wireSyncKeyFromBytes(info, keyBytes)
			if err != nil {
				return fmt.Errorf("decode snapshot chunk row key: %w", err)
			}
			chunkRows = append(chunkRows, row)
			chunkBytes += wireByteCount
			observation.selectedRowsHighWater = int64(len(chunkRows))
			observation.selectedBytesHighWater = chunkBytes
			nextRowOrdinal = rowOrdinal
		}
		if err := queryRows.Err(); err != nil {
			return fmt.Errorf("iterate snapshot chunk rows: %w", err)
		}

		resp = &SnapshotChunkResponse{
			SnapshotID:        snapshotID,
			SnapshotBundleSeq: snapshotBundleSeq,
			Rows:              chunkRows,
			NextRowOrdinal:    nextRowOrdinal,
			HasMore:           hasMore,
			ByteCount:         chunkBytes,
		}
		return nil
	})
	if err != nil {
		return nil, release, err
	}
	observation.returnedRows = int64(len(resp.Rows))
	observation.returnedBytes = resp.ByteCount
	succeeded = true
	return resp, release, nil
}

func (s *SyncService) DeleteSnapshotSession(ctx context.Context, actor Actor, snapshotID string) (err error) {
	if err := rejectRetryableCallbackContext(ctx, "DeleteSnapshotSession"); err != nil {
		return err
	}
	done, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	if err := s.validateClientActor(actor, false); err != nil {
		return err
	}
	if snapshotID == "" {
		return &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}

	err = pgx.BeginTxFunc(ctx, s.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		var sessionUserID string
		if err := tx.QueryRow(ctx, `
			SELECT us.user_id
			FROM sync.snapshot_sessions AS ss
			JOIN sync.user_state AS us ON us.user_pk = ss.user_pk
			WHERE ss.snapshot_id = $1::uuid
		`, snapshotID).Scan(&sessionUserID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session for delete: %w", err)
		}
		if sessionUserID != actor.UserID {
			return &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
		}
		if _, err := tx.Exec(ctx, `UPDATE sync.snapshot_sessions SET expires_at=LEAST(expires_at, transaction_timestamp()) WHERE snapshot_id=$1::uuid`, snapshotID); err != nil {
			return fmt.Errorf("retire snapshot session: %w", err)
		}
		return nil
	})
	if err == nil {
		s.requestSnapshotCleanup("explicit_delete")
	}
	return err
}
