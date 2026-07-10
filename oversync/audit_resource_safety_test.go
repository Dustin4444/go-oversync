//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const auditResourceSafetyPressureCount = 12

type auditResourceSafetyFixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	svc        *SyncService
	schemaName string
	userID     string
	writer     Actor
	reader     Actor
}

func newAuditResourceSafetyFixture(
	t *testing.T,
	scenario string,
	maxConns int32,
	configure func(*ServiceConfig),
) *auditResourceSafetyFixture {
	t.Helper()

	ctx := context.Background()
	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_resource_" + strings.ReplaceAll(scenario, "_", "") + "_" + suffix
	if maxConns <= 0 {
		maxConns = 4
	}
	pool := newAuditConfiguredPool(t, ctx, databaseURL, "audit-resource-"+scenario+"-"+suffix, maxConns)
	require.NoError(t, resetTestSyncSchema(ctx, pool))
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestSchema(cleanupCtx, pool, schemaName); err != nil {
			t.Errorf("drop audit resource-safety schema: %v", err)
		}
		if !managed {
			if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
				t.Errorf("reset caller-managed audit resource-safety database: %v", err)
			}
		}
	})

	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "audit-resource-safety-" + scenario,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}
	if configure != nil {
		configure(config)
	}
	svc := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	userID := "audit-resource-" + scenario + "-" + suffix

	return &auditResourceSafetyFixture{
		ctx:        ctx,
		pool:       pool,
		svc:        svc,
		schemaName: schemaName,
		userID:     userID,
		writer:     Actor{UserID: userID, SourceID: "writer"},
		reader:     Actor{UserID: userID, SourceID: "reader"},
	}
}

func (f *auditResourceSafetyFixture) userRow(t *testing.T, id uuid.UUID, name string) PushRequestRow {
	t.Helper()

	payload, err := json.Marshal(map[string]string{
		"id":    id.String(),
		"name":  name,
		"email": strings.ToLower(name) + "@example.com",
	})
	require.NoError(t, err)
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        payload,
	}
}

func (f *auditResourceSafetyFixture) pushRows(
	t *testing.T,
	actor Actor,
	sourceBundleID int64,
	rows []PushRequestRow,
) *Bundle {
	t.Helper()

	bundle, err := pushRowsViaSession(t, f.ctx, f.svc, actor, sourceBundleID, rows)
	require.NoError(t, err)
	require.NotNil(t, bundle)
	return bundle
}

func (f *auditResourceSafetyFixture) count(t *testing.T, query string, args ...any) int64 {
	t.Helper()

	var count int64
	require.NoError(t, f.pool.QueryRow(f.ctx, query, args...).Scan(&count))
	return count
}

func TestAuditResourceSafety_RequestBodySizeCharacterization(t *testing.T) {
	fixture, initializationID := newAuditCreatePushSessionHTTPFixture(t)
	body := strings.Repeat(" ", 16<<10) + fmt.Sprintf(
		`{"source_bundle_id":1,"planned_row_count":1,"initialization_id":%q}`,
		initializationID,
	)

	startedAt := time.Now()
	recorder := runAuditCreatePushSessionRequest(t, fixture, body, "application/json")
	elapsed := time.Since(startedAt)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Less(t, elapsed, 5*time.Second)
	var sessionCount int64
	require.NoError(t, fixture.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM sync.push_sessions`).Scan(&sessionCount))
	require.Equal(t, int64(1), sessionCount)
	requireAuditMetadataIntegrity(t, context.Background(), fixture.pool)
	t.Logf("accepted request body bytes=%d elapsed=%s; handlers have no shared configured body bound", len(body), elapsed)
}

func TestAuditResourceSafety_ChunkLimitRejectsBeforePoolAcquireAndLeavesNoResidue(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{maxRowsPerPushChunk: 1})
	session := fixture.createSession(t, ctx, 1, 2)
	beforeAcquireCount := fixture.pool.Stat().AcquireCount()

	startedAt := time.Now()
	_, err := fixture.svc.UploadPushChunk(ctx, fixture.writer, session.PushID, &PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows: []PushRequestRow{
			fixture.userRow(uuid.New(), "Alpha"),
			fixture.userRow(uuid.New(), "Bravo"),
		},
	})
	elapsed := time.Since(startedAt)

	var invalidErr *PushChunkInvalidError
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, invalidErr.Error(), "exceeds max_rows_per_push_chunk 1")
	require.Equal(t, beforeAcquireCount, fixture.pool.Stat().AcquireCount(), "chunk limit must reject before pool acquisition")
	require.Less(t, elapsed, time.Second)

	var nextExpectedRowOrdinal, stagedRowCount int64
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT session.next_expected_row_ordinal, COUNT(rows.row_ordinal)
		FROM sync.push_sessions AS session
		LEFT JOIN sync.push_session_rows AS rows ON rows.push_id = session.push_id
		WHERE session.push_id = $1::uuid
		GROUP BY session.push_id
	`, session.PushID).Scan(&nextExpectedRowOrdinal, &stagedRowCount))
	require.Zero(t, nextExpectedRowOrdinal)
	require.Zero(t, stagedRowCount)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	t.Logf("rejected rows=2 configured_max=1 elapsed=%s without a pool acquire", elapsed)
}

func TestAuditResourceSafety_StagedAndSnapshotSessionPressureExpiresAndCleansUp(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "session_pressure", 4, nil)
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")
	fixture.pushRows(t, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(t, uuid.New(), "Seed"),
	})

	for i := 0; i < auditResourceSafetyPressureCount; i++ {
		actor := Actor{UserID: fixture.userID, SourceID: fmt.Sprintf("staged-%02d", i)}
		created, err := fixture.svc.CreatePushSession(fixture.ctx, actor, &PushSessionCreateRequest{
			SourceBundleID:       1,
			PlannedRowCount:      1,
			CanonicalRequestHash: strings.Repeat("0", 64),
		})
		require.NoError(t, err)
		_, err = fixture.svc.UploadPushChunk(fixture.ctx, actor, created.PushID, &PushSessionChunkRequest{
			StartRowOrdinal: 0,
			Rows: []PushRequestRow{
				fixture.userRow(t, uuid.New(), fmt.Sprintf("Staged%02d", i)),
			},
		})
		require.NoError(t, err)

		_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
		require.NoError(t, err)
	}

	require.Equal(t, int64(auditResourceSafetyPressureCount), fixture.count(t, `SELECT COUNT(*) FROM sync.push_sessions`))
	require.Equal(t, int64(auditResourceSafetyPressureCount), fixture.count(t, `SELECT COUNT(*) FROM sync.push_session_rows`))
	require.Equal(t, int64(auditResourceSafetyPressureCount), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_sessions`))
	require.Equal(t, int64(auditResourceSafetyPressureCount), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)

	_, err := fixture.pool.Exec(fixture.ctx, `UPDATE sync.push_sessions SET expires_at = now() - interval '1 second'`)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at = now() - interval '1 second'`)
	require.NoError(t, err)

	startedAt := time.Now()
	postCleanup, err := fixture.svc.CreatePushSession(fixture.ctx, Actor{
		UserID:   fixture.userID,
		SourceID: "post-cleanup",
	}, &PushSessionCreateRequest{SourceBundleID: 1, PlannedRowCount: 1, CanonicalRequestHash: strings.Repeat("0", 64)})
	require.NoError(t, err)
	require.Equal(t, "staging", postCleanup.Status)
	postCleanupSnapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	require.NoError(t, err)
	elapsed := time.Since(startedAt)

	require.Equal(t, int64(1), fixture.count(t, `SELECT COUNT(*) FROM sync.push_sessions`))
	require.Zero(t, fixture.count(t, `SELECT COUNT(*) FROM sync.push_session_rows`))
	require.Equal(t, int64(1), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_sessions`))
	require.Equal(t, int64(1), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	require.Equal(t, int64(1), postCleanupSnapshot.RowCount)
	require.Less(t, elapsed, 5*time.Second)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf(
		"created and lazily reclaimed push_sessions=%d snapshot_sessions=%d elapsed=%s; no active-session quota is configured",
		auditResourceSafetyPressureCount,
		auditResourceSafetyPressureCount,
		elapsed,
	)
}

func TestAuditResourceSafety_RetainedStorageAndFetchChunksStayWithinConfiguredBounds(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "retained_storage", 4, func(config *ServiceConfig) {
		config.RetainedBundlesPerUser = 2
		config.RetentionPruneBatchSize = 100
		config.MaxRowsPerCommittedBundleChunk = 2
		config.MaxRowsPerSnapshotChunk = 2
	})
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")

	var latest *Bundle
	for sourceBundleID := int64(1); sourceBundleID <= 4; sourceBundleID++ {
		rowCount := 1
		if sourceBundleID == 4 {
			rowCount = 3
		}
		rows := make([]PushRequestRow, 0, rowCount)
		for rowIndex := 0; rowIndex < rowCount; rowIndex++ {
			rows = append(rows, fixture.userRow(
				t,
				uuid.New(),
				fmt.Sprintf("Bundle%dRow%d", sourceBundleID, rowIndex+1),
			))
		}
		latest = fixture.pushRows(t, fixture.writer, sourceBundleID, rows)
	}
	require.NotNil(t, latest)
	require.Equal(t, int64(4), latest.BundleSeq)

	var retainedFloor, retainedBundleCount, firstRetainedBundle, lastRetainedBundle int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT users.retained_bundle_floor,
			COUNT(bundle.bundle_seq),
			COALESCE(MIN(bundle.bundle_seq), 0),
			COALESCE(MAX(bundle.bundle_seq), 0)
		FROM sync.user_state AS users
		LEFT JOIN sync.bundle_log AS bundle ON bundle.user_pk = users.user_pk
		WHERE users.user_id = $1
		GROUP BY users.user_pk
	`, fixture.userID).Scan(
		&retainedFloor,
		&retainedBundleCount,
		&firstRetainedBundle,
		&lastRetainedBundle,
	))
	require.Equal(t, int64(2), retainedFloor)
	require.Equal(t, int64(2), retainedBundleCount)
	require.Equal(t, int64(3), firstRetainedBundle)
	require.Equal(t, int64(4), lastRetainedBundle)
	require.Equal(t, int64(4), fixture.count(t, `SELECT COUNT(*) FROM sync.bundle_rows`))

	committedPage, err := fixture.svc.GetCommittedBundleRows(fixture.ctx, fixture.reader, latest.BundleSeq, nil, 100)
	require.NoError(t, err)
	require.Len(t, committedPage.Rows, 2)
	require.True(t, committedPage.HasMore)
	committedAfter := committedPage.NextRowOrdinal
	committedTail, err := fixture.svc.GetCommittedBundleRows(
		fixture.ctx,
		fixture.reader,
		latest.BundleSeq,
		&committedAfter,
		100,
	)
	require.NoError(t, err)
	require.Len(t, committedTail.Rows, 1)
	require.False(t, committedTail.HasMore)

	snapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	require.NoError(t, err)
	require.Equal(t, int64(6), snapshot.RowCount)
	require.Equal(t, int64(6), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	snapshotPage, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, snapshot.SnapshotID, 0, 100)
	require.NoError(t, err)
	require.Len(t, snapshotPage.Rows, 2)
	require.True(t, snapshotPage.HasMore)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf(
		"retained_floor=%d retained_bundles=%d retained_bundle_rows=%d committed_page_rows=%d snapshot_rows=%d snapshot_page_rows=%d",
		retainedFloor,
		retainedBundleCount,
		fixture.count(t, `SELECT COUNT(*) FROM sync.bundle_rows`),
		len(committedPage.Rows),
		snapshot.RowCount,
		len(snapshotPage.Rows),
	)
}

func TestAuditResourceSafety_SnapshotSessionLimitRejectsAtomicallyWithBoundedState(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "snapshot_limit", 2, func(config *ServiceConfig) {
		config.MaxRowsPerSnapshotSession = 1
	})
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")
	fixture.pushRows(t, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(t, uuid.New(), "Alpha"),
		fixture.userRow(t, uuid.New(), "Bravo"),
	})

	startedAt := time.Now()
	_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader)
	elapsed := time.Since(startedAt)

	var limitErr *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "row_count", limitErr.Dimension)
	require.Equal(t, int64(2), limitErr.Actual)
	require.Equal(t, int64(1), limitErr.Limit)
	require.Zero(t, fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_sessions`))
	require.Zero(t, fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	require.Zero(t, fixture.pool.Stat().AcquiredConns())
	require.Less(t, elapsed, 5*time.Second)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf("snapshot rows=%d configured_max=%d rejected atomically in %s after current full materialization", limitErr.Actual, limitErr.Limit, elapsed)
}

func TestAuditResourceSafety_PoolExhaustionCancelsAndRetryRecovers(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "pool_exhaustion", 2, nil)
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")
	fixture.pushRows(t, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(t, uuid.New(), "Alpha"),
	})

	first, err := fixture.pool.Acquire(fixture.ctx)
	require.NoError(t, err)
	firstHeld := true
	t.Cleanup(func() {
		if firstHeld {
			first.Release()
		}
	})
	second, err := fixture.pool.Acquire(fixture.ctx)
	require.NoError(t, err)
	secondHeld := true
	t.Cleanup(func() {
		if secondHeld {
			second.Release()
		}
	})
	require.Equal(t, int32(2), fixture.pool.Stat().MaxConns())
	require.Equal(t, int32(2), fixture.pool.Stat().AcquiredConns())
	canceledBefore := fixture.pool.Stat().CanceledAcquireCount()

	blockedCtx, cancelBlocked := context.WithTimeout(fixture.ctx, 200*time.Millisecond)
	startedAt := time.Now()
	_, err = fixture.svc.ProcessPull(blockedCtx, fixture.reader, 0, 1, 0)
	elapsed := time.Since(startedAt)
	cancelBlocked()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	canceledAfter := fixture.pool.Stat().CanceledAcquireCount()
	require.GreaterOrEqual(t, canceledAfter, canceledBefore+1)
	require.Less(t, elapsed, 2*time.Second)

	firstHeld = false
	first.Release()
	retryCtx, cancelRetry := context.WithTimeout(fixture.ctx, 2*time.Second)
	retry, err := fixture.svc.ProcessPull(retryCtx, fixture.reader, 0, 1, 0)
	cancelRetry()
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Len(t, retry.Bundles, 1)

	secondHeld = false
	second.Release()
	require.Zero(t, fixture.pool.Stat().AcquiredConns())
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf(
		"pool max_conns=2 canceled_acquire_delta=%d exhausted acquire after %s and recovered on one-slot release",
		canceledAfter-canceledBefore,
		elapsed,
	)
}
