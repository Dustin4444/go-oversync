// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const adoptionSourcePrefix = "oversync-adoption:"

type PopulatedTableAdoptionError struct {
	Reason           string
	UserID           string
	Table            string
	BusinessRowCount int64
	RowStateCount    int64
	Detail           string
}

func (e *PopulatedTableAdoptionError) Error() string {
	if e == nil {
		return "populated-table adoption failed"
	}
	location := ""
	if e.UserID != "" {
		location = " for scope " + e.UserID
	}
	if e.Table != "" {
		location += " at " + e.Table
	}
	return fmt.Sprintf(
		"populated-table adoption %s%s (business_rows=%d row_state=%d): %s",
		e.Reason,
		location,
		e.BusinessRowCount,
		e.RowStateCount,
		e.Detail,
	)
}

type adoptionBusinessRow struct {
	tableInfo   registeredTableRuntimeInfo
	keyText     string
	keyBytes    []byte
	payloadWire []byte
}

type adoptionScopeState struct {
	found                bool
	userPK               int64
	nextBundleSeq        int64
	retainedBundleFloor  int64
	scopeStateCode       sql.NullInt16
	rowStateCount        int64
	liveRowStateCount    int64
	bundleCount          int64
	maxBundleSeq         sql.NullInt64
	sourceCount          int64
	pushSessionCount     int64
	snapshotSessionCount int64
	captureStageCount    int64
}

type adoptionRetainedBundle struct {
	bundleSeq  int64
	rowCount   int64
	byteCount  int64
	bundleHash []byte
}

type adoptionRetainedRow struct {
	op          string
	payloadWire []byte
}

type adoptionRetainedEvidence struct {
	count int64
	row   adoptionRetainedRow
}

type adoptionCurrentRowState struct {
	tableID   int32
	keyBytes  []byte
	bundleSeq int64
	deleted   bool
}

func (s *SyncService) lockRegisteredTablesForAdoption(ctx context.Context, tx pgx.Tx) error {
	if s == nil || s.config == nil {
		return nil
	}
	targetsByKey := make(map[string]registeredTableTriggerTarget)
	for _, table := range s.config.RegisteredTables {
		targets, err := loadRegisteredTableTriggerTargets(ctx, tx, table)
		if err != nil {
			return err
		}
		for _, target := range targets {
			targetsByKey[Key(target.schemaName, target.tableName)] = target
		}
	}
	targets := make([]registeredTableTriggerTarget, 0, len(targetsByKey))
	for _, target := range targetsByKey {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].schemaName == targets[j].schemaName {
			return targets[i].tableName < targets[j].tableName
		}
		return targets[i].schemaName < targets[j].schemaName
	})
	for _, target := range targets {
		ident := pgx.Identifier{target.schemaName, target.tableName}.Sanitize()
		if _, err := tx.Exec(ctx, fmt.Sprintf(`LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE`, ident)); err != nil {
			return fmt.Errorf("lock registered table %s.%s for adoption: %w", target.schemaName, target.tableName, err)
		}
	}
	return nil
}

func (s *SyncService) adoptPopulatedRegisteredTables(ctx context.Context, tx pgx.Tx) error {
	userIDs, err := s.loadAdoptionUserIDs(ctx, tx)
	if err != nil {
		return err
	}
	var businessRowCount int64
	adoptedScopeCount := 0
	for _, userID := range userIDs {
		rows, err := s.scanRegisteredBusinessRowsForAdoption(ctx, tx, userID)
		if err != nil {
			return err
		}
		businessRowCount += int64(len(rows))
		state, err := loadAdoptionScopeState(ctx, tx, userID)
		if err != nil {
			return err
		}
		businessRows := int64(len(rows))
		adopt, err := s.classifyAdoptionScope(ctx, tx, userID, businessRows, state)
		if err != nil {
			return err
		}
		if !adopt {
			continue
		}
		if err := s.persistAdoptionBaseline(ctx, tx, userID, rows); err != nil {
			return err
		}
		adoptedScopeCount++

		state, err = loadAdoptionScopeState(ctx, tx, userID)
		if err != nil {
			return err
		}
		adopt, err = s.classifyAdoptionScope(ctx, tx, userID, businessRows, state)
		if err != nil {
			return err
		}
		if adopt {
			return &PopulatedTableAdoptionError{
				Reason:           "partial_sync_state",
				UserID:           userID,
				BusinessRowCount: businessRows,
				RowStateCount:    state.rowStateCount,
				Detail:           "scope still requires adoption after baseline persistence",
			}
		}
	}

	s.logger.Info("Populated registered-table adoption validated",
		"registered_table_count", len(s.registeredTableByID),
		"scope_count", len(userIDs),
		"adopted_scope_count", adoptedScopeCount,
		"business_row_count", businessRowCount)
	return nil
}

func (s *SyncService) scanRegisteredBusinessRowsForAdoption(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
) ([]adoptionBusinessRow, error) {
	rows := make([]adoptionBusinessRow, 0)
	seen := make(map[string]struct{})
	infos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].tableID < infos[j].tableID })

	for _, info := range infos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		keyIdent := pgx.Identifier{info.syncKeyColumn}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		query := fmt.Sprintf(`
			SELECT CAST(src.%s AS text), to_jsonb(src) - '_sync_scope_id'
			FROM %s AS src
			WHERE src.%s = $1
			ORDER BY CAST(src.%s AS text)
		`, keyIdent, tableIdent, ownerIdent, keyIdent)
		businessRows, err := tx.Query(ctx, query, userID)
		if err != nil {
			return nil, fmt.Errorf("scan registered rows for %s.%s adoption scope %s: %w", info.schemaName, info.tableName, userID, err)
		}
		for businessRows.Next() {
			var keyText sql.NullString
			var payloadDB []byte
			if err := businessRows.Scan(&keyText, &payloadDB); err != nil {
				businessRows.Close()
				return nil, fmt.Errorf("scan registered row for %s.%s adoption scope %s: %w", info.schemaName, info.tableName, userID, err)
			}
			if !keyText.Valid {
				businessRows.Close()
				return nil, &PopulatedTableAdoptionError{
					Reason: "identity_unrepresentable",
					UserID: userID,
					Table:  Key(info.schemaName, info.tableName),
					Detail: "registered row has a NULL sync key",
				}
			}
			keyBytes, _, err := encodeKeyBytes(info.syncKeyType, keyText.String)
			if err != nil {
				businessRows.Close()
				return nil, &PopulatedTableAdoptionError{
					Reason: "identity_unrepresentable",
					UserID: userID,
					Table:  Key(info.schemaName, info.tableName),
					Detail: err.Error(),
				}
			}
			logicalKey := snapshotLogicalRowKey(info.tableID, keyBytes)
			if _, duplicate := seen[logicalKey]; duplicate {
				businessRows.Close()
				return nil, &PopulatedTableAdoptionError{
					Reason: "duplicate_identity",
					UserID: userID,
					Table:  Key(info.schemaName, info.tableName),
					Detail: "duplicate logical scope/table/key identity",
				}
			}
			seen[logicalKey] = struct{}{}
			payloadWire, err := s.canonicalizeWirePayload(info.schemaName, info.tableName, payloadDB)
			if err != nil {
				businessRows.Close()
				return nil, &PopulatedTableAdoptionError{
					Reason: "baseline_unrepresentable",
					UserID: userID,
					Table:  Key(info.schemaName, info.tableName),
					Detail: err.Error(),
				}
			}
			rows = append(rows, adoptionBusinessRow{
				tableInfo:   info,
				keyText:     keyText.String,
				keyBytes:    append([]byte(nil), keyBytes...),
				payloadWire: append([]byte(nil), payloadWire...),
			})
		}
		if err := businessRows.Err(); err != nil {
			businessRows.Close()
			return nil, fmt.Errorf("iterate registered rows for %s.%s adoption scope %s: %w", info.schemaName, info.tableName, userID, err)
		}
		businessRows.Close()
	}
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		if left.tableInfo.tableID != right.tableInfo.tableID {
			return left.tableInfo.tableID < right.tableInfo.tableID
		}
		return bytes.Compare(left.keyBytes, right.keyBytes) < 0
	})
	return rows, nil
}

func (s *SyncService) loadAdoptionUserIDs(
	ctx context.Context,
	tx pgx.Tx,
) ([]string, error) {
	userSet := make(map[string]struct{})
	infos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].tableID < infos[j].tableID })
	for _, info := range infos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		rows, err := tx.Query(ctx, fmt.Sprintf(`
			SELECT DISTINCT CAST(src.%s AS text)
			FROM %s AS src
			ORDER BY CAST(src.%s AS text)
		`, ownerIdent, tableIdent, ownerIdent))
		if err != nil {
			return nil, fmt.Errorf("discover adoption scopes for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		for rows.Next() {
			var userID sql.NullString
			if err := rows.Scan(&userID); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan adoption scope for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			if !userID.Valid || userID.String == "" {
				rows.Close()
				return nil, &PopulatedTableAdoptionError{
					Reason: "identity_unrepresentable",
					Table:  Key(info.schemaName, info.tableName),
					Detail: "registered row has a NULL or empty scope",
				}
			}
			userSet[userID.String] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate adoption scopes for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		rows.Close()
	}
	rows, err := tx.Query(ctx, `SELECT user_id FROM sync.user_state ORDER BY user_pk`)
	if err != nil {
		return nil, fmt.Errorf("load existing sync users for adoption: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, fmt.Errorf("scan existing sync user for adoption: %w", err)
		}
		userSet[userID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing sync users for adoption: %w", err)
	}
	userIDs := make([]string, 0, len(userSet))
	for userID := range userSet {
		userIDs = append(userIDs, userID)
	}
	sort.Strings(userIDs)
	return userIDs, nil
}

func loadAdoptionScopeState(ctx context.Context, tx pgx.Tx, userID string) (adoptionScopeState, error) {
	var state adoptionScopeState
	err := tx.QueryRow(ctx, `
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = $1
		FOR UPDATE
	`, userID).Scan(&state.userPK, &state.nextBundleSeq, &state.retainedBundleFloor)
	if err != nil {
		if err == pgx.ErrNoRows {
			return state, nil
		}
		return state, fmt.Errorf("load user_state for adoption scope %s: %w", userID, err)
	}
	state.found = true
	err = tx.QueryRow(ctx, `SELECT state_code FROM sync.scope_state WHERE user_pk = $1 FOR UPDATE`, state.userPK).Scan(&state.scopeStateCode)
	if err != nil && err != pgx.ErrNoRows {
		return state, fmt.Errorf("load scope_state for adoption scope %s: %w", userID, err)
	}
	err = tx.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = $1),
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = $1 AND NOT deleted),
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = $1),
			(SELECT MAX(bundle_seq) FROM sync.bundle_log WHERE user_pk = $1),
			(SELECT COUNT(*) FROM sync.source_state WHERE user_pk = $1),
			(SELECT COUNT(*) FROM sync.push_sessions WHERE user_pk = $1),
			(SELECT COUNT(*) FROM sync.snapshot_sessions WHERE user_pk = $1),
			(SELECT COUNT(*) FROM sync.bundle_capture_stage WHERE user_pk = $1)
	`, state.userPK).Scan(
		&state.rowStateCount,
		&state.liveRowStateCount,
		&state.bundleCount,
		&state.maxBundleSeq,
		&state.sourceCount,
		&state.pushSessionCount,
		&state.snapshotSessionCount,
		&state.captureStageCount,
	)
	if err != nil {
		return state, fmt.Errorf("load adoption state counts for scope %s: %w", userID, err)
	}
	return state, nil
}

func (s *SyncService) classifyAdoptionScope(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	businessRowCount int64,
	state adoptionScopeState,
) (bool, error) {
	hasBusinessRows := businessRowCount > 0
	if !state.found {
		return hasBusinessRows, nil
	}
	if state.pushSessionCount > 0 || state.snapshotSessionCount > 0 || state.captureStageCount > 0 {
		return false, adoptionStateError("active_initialization_or_session", userID, businessRowCount, state, "scope has active push, snapshot, or capture-stage state")
	}
	zeroEnvelope := state.nextBundleSeq == 1 && state.retainedBundleFloor == 0 &&
		state.rowStateCount == 0 && state.bundleCount == 0 && state.sourceCount == 0
	if zeroEnvelope {
		if state.scopeStateCode.Valid && state.scopeStateCode.Int16 == scopeStateCodeInitializing {
			return false, adoptionStateError("active_initialization_or_session", userID, businessRowCount, state, "scope is initializing")
		}
		if state.scopeStateCode.Valid && state.scopeStateCode.Int16 != scopeStateCodeUninitialized && state.scopeStateCode.Int16 != scopeStateCodeInitialized {
			return false, adoptionStateError("partial_sync_state", userID, businessRowCount, state, "scope has an unsupported zero-state code")
		}
		return hasBusinessRows, nil
	}
	if !state.scopeStateCode.Valid || state.scopeStateCode.Int16 != scopeStateCodeInitialized {
		return false, adoptionStateError("partial_sync_state", userID, businessRowCount, state, "non-zero sync state is not initialized")
	}
	if err := s.validateCoherentAdoptionScope(ctx, tx, userID, state); err != nil {
		return false, err
	}
	return false, nil
}

func adoptionStateError(
	reason string,
	userID string,
	businessRowCount int64,
	state adoptionScopeState,
	detail string,
) error {
	return &PopulatedTableAdoptionError{
		Reason:           reason,
		UserID:           userID,
		BusinessRowCount: businessRowCount,
		RowStateCount:    state.rowStateCount,
		Detail:           detail,
	}
}

func (s *SyncService) validateCoherentAdoptionScope(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	state adoptionScopeState,
) error {
	highest := state.nextBundleSeq - 1
	if highest <= 0 || state.retainedBundleFloor < 0 || state.retainedBundleFloor > highest {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "user sequence or retained floor is invalid")
	}
	expectedRetainedBundleCount := highest - state.retainedBundleFloor
	if state.bundleCount != expectedRetainedBundleCount {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "retained bundle count does not form a contiguous history window")
	}
	if expectedRetainedBundleCount == 0 {
		if state.maxBundleSeq.Valid {
			return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "retained history exists at or below the retained floor")
		}
	} else if !state.maxBundleSeq.Valid || state.maxBundleSeq.Int64 != highest {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "latest retained bundle does not match next_bundle_seq")
	}
	currentRows, err := s.loadAdoptionCurrentRowState(ctx, tx, userID, state, highest)
	if err != nil {
		return err
	}
	retainedRows, err := s.validateRetainedAdoptionBundles(ctx, tx, userID, state, currentRows)
	if err != nil {
		return err
	}
	var sourceMismatchCount int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log AS bundle
		LEFT JOIN sync.source_state AS source
		  ON source.user_pk = bundle.user_pk AND source.source_id = bundle.source_id
		WHERE bundle.user_pk = $1
		  AND (source.user_pk IS NULL OR source.max_committed_source_bundle_id < bundle.source_bundle_id)
	`, state.userPK).Scan(&sourceMismatchCount); err != nil {
		return fmt.Errorf("validate source watermark for adoption scope %s: %w", userID, err)
	}
	if sourceMismatchCount != 0 {
		return adoptionStateError("source_watermark_mismatch", userID, state.liveRowStateCount, state, "committed bundle source watermark is missing or behind")
	}
	rows, _, err := s.materializeSnapshotRows(ctx, tx, userID, state.userPK)
	if err != nil {
		return &PopulatedTableAdoptionError{
			Reason:           "business_row_state_mismatch",
			UserID:           userID,
			BusinessRowCount: int64(len(rows)),
			RowStateCount:    state.rowStateCount,
			Detail:           err.Error(),
		}
	}
	if int64(len(rows)) != state.liveRowStateCount {
		return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, "live business and row_state counts differ")
	}
	liveRows := make(map[string]snapshotMaterializedRow, len(rows))
	for _, row := range rows {
		liveRows[snapshotLogicalRowKey(row.tableID, row.keyBytes)] = row
	}
	for _, current := range currentRows {
		if current.bundleSeq <= state.retainedBundleFloor {
			continue
		}
		logicalKey := snapshotLogicalRowKey(current.tableID, current.keyBytes)
		evidence := retainedRows[logicalKey]
		rowKind := "live row"
		if current.deleted {
			rowKind = "tombstone"
		}
		if evidence.count != 1 {
			return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, fmt.Sprintf("%s at version %d has %d matching retained bundle rows", rowKind, current.bundleSeq, evidence.count))
		}
		if current.deleted {
			if evidence.row.op != OpDelete {
				return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, fmt.Sprintf("tombstone at version %d matches a retained %s", current.bundleSeq, evidence.row.op))
			}
			continue
		}
		if evidence.row.op == OpDelete {
			return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, fmt.Sprintf("live row at version %d matches a retained delete", current.bundleSeq))
		}
		liveRow, ok := liveRows[logicalKey]
		if !ok {
			return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, fmt.Sprintf("live row_state at version %d is missing a materialized business row", current.bundleSeq))
		}
		livePayload, err := canonicalJSON(liveRow.payloadWire)
		if err != nil {
			return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, fmt.Sprintf("canonicalize live payload at version %d: %v", current.bundleSeq, err))
		}
		retainedPayload, err := canonicalJSON(evidence.row.payloadWire)
		if err != nil {
			return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, fmt.Sprintf("canonicalize retained payload at version %d: %v", current.bundleSeq, err))
		}
		if !bytes.Equal(livePayload, retainedPayload) {
			return adoptionStateError("business_row_state_mismatch", userID, int64(len(rows)), state, fmt.Sprintf("live payload differs from retained bundle row at version %d", current.bundleSeq))
		}
	}
	return nil
}

func (s *SyncService) loadAdoptionCurrentRowState(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	state adoptionScopeState,
	highest int64,
) ([]adoptionCurrentRowState, error) {
	rows, err := tx.Query(ctx, `
		SELECT table_id, key_bytes, bundle_seq, deleted
		FROM sync.row_state
		WHERE user_pk = $1
		ORDER BY table_id, key_bytes
	`, state.userPK)
	if err != nil {
		return nil, fmt.Errorf("query row_state for adoption scope %s: %w", userID, err)
	}
	defer rows.Close()

	currentRows := make([]adoptionCurrentRowState, 0, state.rowStateCount)
	var liveCount int64
	for rows.Next() {
		var current adoptionCurrentRowState
		if err := rows.Scan(&current.tableID, &current.keyBytes, &current.bundleSeq, &current.deleted); err != nil {
			return nil, fmt.Errorf("scan row_state for adoption scope %s: %w", userID, err)
		}
		if current.bundleSeq <= 0 || current.bundleSeq > highest {
			return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "row_state contains a non-positive or future version")
		}
		info, err := s.tableInfoForID(current.tableID)
		if err != nil {
			return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, fmt.Sprintf("row_state has invalid table identity: %v", err))
		}
		if _, err := wireSyncKeyFromBytes(info, current.keyBytes); err != nil {
			return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, fmt.Sprintf("row_state has invalid key for table_id %d: %v", current.tableID, err))
		}
		current.keyBytes = append([]byte(nil), current.keyBytes...)
		currentRows = append(currentRows, current)
		if !current.deleted {
			liveCount++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate row_state for adoption scope %s: %w", userID, err)
	}
	if int64(len(currentRows)) != state.rowStateCount || liveCount != state.liveRowStateCount {
		return nil, adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, "row_state counts changed during adoption validation")
	}
	return currentRows, nil
}

func (s *SyncService) validateRetainedAdoptionBundles(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	state adoptionScopeState,
	currentRows []adoptionCurrentRowState,
) (map[string]adoptionRetainedEvidence, error) {
	historyMismatch := func(detail string) error {
		return adoptionStateError("history_mismatch", userID, state.liveRowStateCount, state, detail)
	}
	targets := make(map[int64]map[string]struct{})
	for _, current := range currentRows {
		if current.bundleSeq <= state.retainedBundleFloor {
			continue
		}
		if targets[current.bundleSeq] == nil {
			targets[current.bundleSeq] = make(map[string]struct{})
		}
		targets[current.bundleSeq][snapshotLogicalRowKey(current.tableID, current.keyBytes)] = struct{}{}
	}
	retainedRows := make(map[string]adoptionRetainedEvidence, len(currentRows))
	rowRows, err := tx.Query(ctx, `
		SELECT
			bundle.bundle_seq, bundle.row_count, bundle.byte_count, bundle.bundle_hash,
			rows.row_ordinal, rows.table_id, rows.key_bytes, rows.op_code, rows.payload_wire
		FROM sync.bundle_log AS bundle
		LEFT JOIN sync.bundle_rows AS rows
		  ON rows.user_pk = bundle.user_pk AND rows.bundle_seq = bundle.bundle_seq
		WHERE bundle.user_pk = $1
		ORDER BY bundle.bundle_seq, rows.row_ordinal
	`, state.userPK)
	if err != nil {
		return nil, fmt.Errorf("query retained bundles for adoption scope %s: %w", userID, err)
	}
	defer rowRows.Close()

	expectedBundleSeq := state.retainedBundleFloor + 1
	var (
		currentBundle *adoptionRetainedBundle
		bundleRows    []BundleRow
		bundleCount   int64
	)
	finishBundle := func() error {
		if currentBundle == nil {
			return nil
		}
		if int64(len(bundleRows)) != currentBundle.rowCount {
			return historyMismatch(fmt.Sprintf("bundle %d row count does not match stored history", currentBundle.bundleSeq))
		}
		bundleHash, byteCount, err := computeCommittedBundleHash(bundleRows)
		if err != nil {
			return historyMismatch(fmt.Sprintf("recompute bundle %d hash: %v", currentBundle.bundleSeq, err))
		}
		if !bytes.Equal(bundleHash, currentBundle.bundleHash) {
			return historyMismatch(fmt.Sprintf("bundle %d hash does not match stored history", currentBundle.bundleSeq))
		}
		if byteCount != currentBundle.byteCount {
			return historyMismatch(fmt.Sprintf("bundle %d byte count does not match stored history", currentBundle.bundleSeq))
		}
		return nil
	}
	for rowRows.Next() {
		var (
			bundle     adoptionRetainedBundle
			rowOrdinal sql.NullInt64
			tableID    sql.NullInt64
			opCode     sql.NullInt64
			keyBytes   []byte
			payload    []byte
		)
		if err := rowRows.Scan(
			&bundle.bundleSeq,
			&bundle.rowCount,
			&bundle.byteCount,
			&bundle.bundleHash,
			&rowOrdinal,
			&tableID,
			&keyBytes,
			&opCode,
			&payload,
		); err != nil {
			return nil, fmt.Errorf("scan retained bundle for adoption scope %s: %w", userID, err)
		}
		if currentBundle == nil || bundle.bundleSeq != currentBundle.bundleSeq {
			if err := finishBundle(); err != nil {
				return nil, err
			}
			if bundle.bundleSeq != expectedBundleSeq {
				return nil, historyMismatch(fmt.Sprintf("retained history expected bundle %d but found %d", expectedBundleSeq, bundle.bundleSeq))
			}
			bundle.bundleHash = append([]byte(nil), bundle.bundleHash...)
			currentBundle = &bundle
			bundleRows = bundleRows[:0]
			bundleCount++
			expectedBundleSeq++
		}
		if !rowOrdinal.Valid {
			if tableID.Valid || opCode.Valid || keyBytes != nil || payload != nil {
				return nil, historyMismatch(fmt.Sprintf("bundle %d has a malformed empty row", bundle.bundleSeq))
			}
			continue
		}
		if !tableID.Valid || !opCode.Valid || keyBytes == nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d is incomplete", bundle.bundleSeq, rowOrdinal.Int64))
		}
		expectedOrdinal := int64(len(bundleRows) + 1)
		if rowOrdinal.Int64 != expectedOrdinal {
			return nil, historyMismatch(fmt.Sprintf("bundle %d expected row ordinal %d but found %d", bundle.bundleSeq, expectedOrdinal, rowOrdinal.Int64))
		}
		if tableID.Int64 < 1 || tableID.Int64 > int64(^uint32(0)>>1) {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid table identity %d", bundle.bundleSeq, rowOrdinal.Int64, tableID.Int64))
		}
		tableIDValue := int32(tableID.Int64)
		info, err := s.tableInfoForID(tableIDValue)
		if err != nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid table identity: %v", bundle.bundleSeq, rowOrdinal.Int64, err))
		}
		key, err := wireSyncKeyFromBytes(info, keyBytes)
		if err != nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid key: %v", bundle.bundleSeq, rowOrdinal.Int64, err))
		}
		if opCode.Int64 < 1 || opCode.Int64 > int64(^uint16(0)>>1) {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid operation %d", bundle.bundleSeq, rowOrdinal.Int64, opCode.Int64))
		}
		op, err := opStringFromCode(int16(opCode.Int64))
		if err != nil {
			return nil, historyMismatch(fmt.Sprintf("bundle %d row %d has invalid operation: %v", bundle.bundleSeq, rowOrdinal.Int64, err))
		}
		bundleRows = append(bundleRows, BundleRow{
			Schema:     info.schemaName,
			Table:      info.tableName,
			Key:        key,
			Op:         op,
			RowVersion: bundle.bundleSeq,
			Payload:    append([]byte(nil), payload...),
		})
		logicalKey := snapshotLogicalRowKey(tableIDValue, keyBytes)
		if _, wanted := targets[bundle.bundleSeq][logicalKey]; wanted {
			evidence := retainedRows[logicalKey]
			evidence.count++
			if evidence.count == 1 {
				evidence.row = adoptionRetainedRow{
					op:          op,
					payloadWire: append([]byte(nil), payload...),
				}
			}
			retainedRows[logicalKey] = evidence
		}
	}
	if err := rowRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retained bundles for adoption scope %s: %w", userID, err)
	}
	if err := finishBundle(); err != nil {
		return nil, err
	}
	if bundleCount != state.bundleCount {
		return nil, historyMismatch("retained bundle metadata changed during validation")
	}
	return retainedRows, nil
}

func (s *SyncService) persistAdoptionBaseline(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	rows []adoptionBusinessRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	if err := ensureScopeStateExistsWithExec(ctx, tx, userID); err != nil {
		return err
	}
	scope, err := loadScopeStateForUpdate(ctx, tx, userID)
	if err != nil {
		return err
	}
	if scope.State == scopeStateInitializing {
		return &PopulatedTableAdoptionError{
			Reason:           "active_initialization_or_session",
			UserID:           userID,
			BusinessRowCount: int64(len(rows)),
			Detail:           "scope is initializing",
		}
	}
	bundleSeq, err := reserveUserBundleSeq(ctx, tx, scope.UserPK)
	if err != nil {
		return err
	}
	if bundleSeq != 1 {
		return &PopulatedTableAdoptionError{
			Reason:           "history_mismatch",
			UserID:           userID,
			BusinessRowCount: int64(len(rows)),
			Detail:           fmt.Sprintf("adoption baseline reserved unexpected bundle_seq %d", bundleSeq),
		}
	}

	storageRows := make([]committedBundleStorageRow, 0, len(rows))
	bundleRows := make([]BundleRow, 0, len(rows))
	requestRows := make([]PushRequestRow, 0, len(rows))
	for _, row := range rows {
		key := SyncKey{row.tableInfo.syncKeyColumn: row.keyText}
		storageRows = append(storageRows, committedBundleStorageRow{
			tableID:     row.tableInfo.tableID,
			keyBytes:    append([]byte(nil), row.keyBytes...),
			opCode:      opCodeInsert,
			payloadWire: append([]byte(nil), row.payloadWire...),
		})
		bundleRows = append(bundleRows, BundleRow{
			Schema:     row.tableInfo.schemaName,
			Table:      row.tableInfo.tableName,
			Key:        key,
			Op:         OpInsert,
			RowVersion: bundleSeq,
			Payload:    append([]byte(nil), row.payloadWire...),
		})
		requestRows = append(requestRows, PushRequestRow{
			Schema:         row.tableInfo.schemaName,
			Table:          row.tableInfo.tableName,
			Key:            key,
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        append([]byte(nil), row.payloadWire...),
		})
	}
	requestHash, err := computeCanonicalPushRequestHash(requestRows)
	if err != nil {
		return &PopulatedTableAdoptionError{Reason: "baseline_unrepresentable", UserID: userID, BusinessRowCount: int64(len(rows)), Detail: err.Error()}
	}
	bundleHash, byteCount, err := computeCommittedBundleHash(bundleRows)
	if err != nil {
		return &PopulatedTableAdoptionError{Reason: "baseline_unrepresentable", UserID: userID, BusinessRowCount: int64(len(rows)), Detail: err.Error()}
	}
	var sourceID string
	for {
		sourceID = adoptionSourcePrefix + uuid.NewString()
		var collision bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sync.source_state WHERE user_pk = $1 AND source_id = $2)`, scope.UserPK, sourceID).Scan(&collision); err != nil {
			return fmt.Errorf("check adoption source collision for scope %s: %w", userID, err)
		}
		if !collision {
			break
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_log (
			user_pk, bundle_seq, source_id, source_bundle_id, row_count,
			byte_count, bundle_hash, canonical_request_hash, committed_at
		) VALUES ($1, $2, $3, 1, $4, $5, $6, $7, now())
	`, scope.UserPK, bundleSeq, sourceID, len(bundleRows), byteCount, bundleHash, requestHash); err != nil {
		return fmt.Errorf("insert adoption baseline bundle for scope %s: %w", userID, err)
	}
	if err := persistCommittedBundleRows(ctx, tx, scope.UserPK, bundleSeq, storageRows); err != nil {
		return err
	}
	if err := activateSourceState(ctx, tx, scope.UserPK, userID, sourceID, 1); err != nil {
		return err
	}
	if scope.State != scopeStateInitialized {
		if err := transitionScopeToInitialized(ctx, tx, userID, sourceID); err != nil {
			return err
		}
	}
	if err := s.emitBundleChangeNotify(ctx, tx, scope.UserPK, BundleChangeEvent{
		BundleSeq:      bundleSeq,
		SourceID:       sourceID,
		SourceBundleID: 1,
	}); err != nil {
		return err
	}
	s.logger.Info("Adopted populated registered-table scope",
		"user_id", userID,
		"row_count", len(rows),
		"bundle_seq", bundleSeq)
	return nil
}
