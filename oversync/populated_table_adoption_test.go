package oversync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type adoptionIntegrityFixture struct {
	t          *testing.T
	ctx        context.Context
	pool       *pgxpool.Pool
	schemaName string
	userID     string
	rowID      uuid.UUID
	config     *ServiceConfig
	service    *SyncService
}

func newAdoptionIntegrityFixture(t *testing.T) *adoptionIntegrityFixture {
	t.Helper()

	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_integrity_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	userID := "integrity-owner-" + suffix
	rowID := uuid.New()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), userID, rowID)
	require.NoError(t, err)

	config := &ServiceConfig{AppName: "adoption-integrity", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}}
	service := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	return &adoptionIntegrityFixture{
		t:          t,
		ctx:        ctx,
		pool:       pool,
		schemaName: schemaName,
		userID:     userID,
		rowID:      rowID,
		config:     config,
		service:    service,
	}
}

func (f *adoptionIntegrityFixture) pushName(sourceBundleID, baseBundleSeq int64, name string) *Bundle {
	f.t.Helper()

	bundle, err := pushRowsViaSession(f.t, f.ctx, f.service, Actor{UserID: f.userID, SourceID: "integrity-writer"}, sourceBundleID, []PushRequestRow{{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": f.rowID.String()},
		Op:             OpUpdate,
		BaseRowVersion: baseBundleSeq,
		Payload:        []byte(fmt.Sprintf(`{"id":"%s","name":%q,"email":"existing@example.com"}`, f.rowID, name)),
	}})
	require.NoError(f.t, err)
	require.NotNil(f.t, bundle)
	return bundle
}

func (f *adoptionIntegrityFixture) newService(appName string) *SyncService {
	f.t.Helper()

	config := *f.config
	config.AppName = appName
	service, err := NewRuntimeService(f.pool, &config, integrationTestLogger(slog.LevelWarn))
	require.NoError(f.t, err)
	f.t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}

func (f *adoptionIntegrityFixture) requireRejected(service *SyncService, reason string) *PopulatedTableAdoptionError {
	f.t.Helper()

	err := service.Bootstrap(f.ctx)
	var adoptionErr *PopulatedTableAdoptionError
	require.ErrorAs(f.t, err, &adoptionErr)
	require.Equal(f.t, reason, adoptionErr.Reason)
	_, connectErr := service.Connect(f.ctx, Actor{UserID: f.userID, SourceID: "reader"}, &ConnectRequest{})
	require.ErrorIs(f.t, connectErr, errServiceNotReady)
	return adoptionErr
}

func TestBootstrap_PopulatedAdoptionReadiness(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "adopt_ready_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		AppName: "populated-adoption-readiness",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	actor := Actor{UserID: "readiness-user", SourceID: "reader"}

	_, err = svc.Connect(ctx, actor, &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)
	status, err := svc.GetStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, "unhealthy", status.Status)
	require.False(t, status.AcceptingOperations)

	handlers := NewHTTPSyncHandlers(svc, integrationTestLogger(slog.LevelWarn))
	req := httptest.NewRequest(http.MethodPost, "/sync/connect", bytes.NewBufferString(`{"has_local_pending_rows":false}`))
	req = req.WithContext(ContextWithActor(req.Context(), actor))
	rec := httptest.NewRecorder()
	handlers.HandleConnect(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), `"service_unavailable"`)

	require.NoError(t, svc.Bootstrap(ctx))
	status, err = svc.GetStatus(ctx)
	require.NoError(t, err)
	require.True(t, status.AcceptingOperations)
	connect, err := svc.Connect(ctx, actor, &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "initialize_empty", connect.Resolution)
}

func TestBootstrap_AdoptsPopulatedRegisteredTablesAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()

	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, tableIdent))
	require.NoError(t, err)

	userID := "existing-user-" + suffix
	rowID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name)
		VALUES ($1, $2, $3)
	`, tableIdent), userID, rowID, "Existing")
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "populated-table-adoption",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	require.NoError(t, svc.Bootstrap(ctx))

	actor := Actor{UserID: userID, SourceID: "reader"}
	connect, err := svc.Connect(ctx, actor, &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)

	pull, err := svc.ProcessPull(ctx, actor, 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), pull.StableBundleSeq)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	require.True(t, strings.HasPrefix(pull.Bundles[0].SourceID, adoptionSourcePrefix))
	require.Equal(t, int64(1), pull.Bundles[0].SourceBundleID)
	require.Equal(t, int64(1), pull.Bundles[0].Rows[0].RowVersion)
	require.Equal(t, rowID.String(), pull.Bundles[0].Rows[0].Key["id"])
	require.JSONEq(t, fmt.Sprintf(`{"id":"%s","name":"Existing"}`, rowID), string(pull.Bundles[0].Rows[0].Payload))
	expectedRequestHash, err := computeCanonicalPushRequestHash([]PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        pull.Bundles[0].Rows[0].Payload,
	}})
	require.NoError(t, err)
	require.Equal(t, expectedRequestHash, pull.Bundles[0].CanonicalRequestHash)
	expectedBundleHash, _, err := computeCommittedBundleHash(pull.Bundles[0].Rows)
	require.NoError(t, err)
	require.Equal(t, renderBundleHash(expectedBundleHash), pull.Bundles[0].BundleHash)

	session, err := svc.CreateSnapshotSession(ctx, actor)
	require.NoError(t, err)
	require.Equal(t, int64(1), session.SnapshotBundleSeq)
	require.Equal(t, int64(1), session.RowCount)

	chunk, err := svc.GetSnapshotChunk(ctx, actor, session.SnapshotID, 0, 10)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, int64(1), chunk.Rows[0].RowVersion)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
	require.JSONEq(t, fmt.Sprintf(`{"id":"%s","name":"Existing"}`, rowID), string(chunk.Rows[0].Payload))

	var (
		nextBundleSeq       int64
		retainedBundleFloor int64
		scopeStateCode      int16
		rowStateCount       int64
		sourceStateCount    int64
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT users.next_bundle_seq, users.retained_bundle_floor, scope.state_code
		FROM sync.user_state AS users
		JOIN sync.scope_state AS scope ON scope.user_pk = users.user_pk
		WHERE users.user_id = $1
	`, userID).Scan(&nextBundleSeq, &retainedBundleFloor, &scopeStateCode))
	require.Equal(t, int64(2), nextBundleSeq)
	require.Zero(t, retainedBundleFloor)
	require.Equal(t, scopeStateCodeInitialized, scopeStateCode)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.row_state AS state
		JOIN sync.user_state AS users ON users.user_pk = state.user_pk
		WHERE users.user_id = $1 AND state.bundle_seq = 1 AND NOT state.deleted
	`, userID).Scan(&rowStateCount))
	require.Equal(t, int64(1), rowStateCount)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.source_state AS source
		JOIN sync.user_state AS users ON users.user_pk = source.user_pk
		WHERE users.user_id = $1
		  AND source.source_id LIKE 'oversync-adoption:%'
		  AND source.state = 'active'
		  AND source.max_committed_source_bundle_id = 1
	`, userID).Scan(&sourceStateCount))
	require.Equal(t, int64(1), sourceStateCount)
}

func TestBootstrap_PopulatedAdoptionStateMatrix(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_matrix_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	userA := "owner-a-" + suffix
	userB := "owner-b-" + suffix
	userAID := uuid.New()
	userAPostID := uuid.New()
	userBID := uuid.New()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Alpha', 'alpha@example.com'), ($3, $4, 'Bravo', 'bravo@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), userA, userAID, userB, userBID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.posts (_sync_scope_id, id, title, content, author_id)
		VALUES ($1, $3, 'Existing', 'Body', $2)
	`, pgx.Identifier{schemaName}.Sanitize()), userA, userAID, userAPostID)
	require.NoError(t, err)

	config := &ServiceConfig{
		AppName: "populated-adoption-state-matrix",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		},
	}
	svc, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	require.NoError(t, svc.Bootstrap(ctx))

	assertAdoptedScope := func(userID string, expectedRows int64) {
		t.Helper()
		var bundleCount, rowCount, nextSeq int64
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
				(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk AND NOT deleted),
				users.next_bundle_seq
			FROM sync.user_state AS users WHERE users.user_id = $1
		`, userID).Scan(&bundleCount, &rowCount, &nextSeq))
		require.Equal(t, int64(1), bundleCount)
		require.Equal(t, expectedRows, rowCount)
		require.Equal(t, int64(2), nextSeq)
	}
	assertAdoptedScope(userA, 2)
	assertAdoptedScope(userB, 1)

	var beforeBundles int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&beforeBundles))
	require.NoError(t, svc.Bootstrap(ctx), "a coherent populated database must be an idempotent no-op")
	var afterBundles int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&afterBundles))
	require.Equal(t, beforeBundles, afterBundles)
}

func TestBootstrap_PopulatedAdoptionAcceptsInitializedEmptyEnvelope(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_zero_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "zero-owner-" + suffix
	config := &ServiceConfig{AppName: "zero-envelope-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}}
	first := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	connect, err := first.Connect(ctx, Actor{UserID: userID, SourceID: "original-initializer"}, &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "initialize_empty", connect.Resolution)
	var initializedAt time.Time
	var initializedBy string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT scope.initialized_at, scope.initialized_by_source_id
		FROM sync.scope_state AS scope
		JOIN sync.user_state AS users ON users.user_pk = scope.user_pk
		WHERE users.user_id = $1
	`, userID).Scan(&initializedAt, &initializedBy))
	require.NoError(t, first.Close(ctx))

	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()
	_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	rowID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, tableIdent), userID, rowID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)

	second, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	require.NoError(t, second.Bootstrap(ctx))
	connect, err = second.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)
	var actualInitializedAt time.Time
	var actualInitializedBy string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT scope.initialized_at, scope.initialized_by_source_id
		FROM sync.scope_state AS scope
		JOIN sync.user_state AS users ON users.user_pk = scope.user_pk
		WHERE users.user_id = $1
	`, userID).Scan(&actualInitializedAt, &actualInitializedBy))
	require.Equal(t, initializedAt.UTC(), actualInitializedAt.UTC())
	require.Equal(t, initializedBy, actualInitializedBy)
}

func TestBootstrap_PopulatedAdoptionRejectsPartialStateAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_partial_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "partial-owner-" + suffix
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New())
	require.NoError(t, err)
	config := &ServiceConfig{AppName: "partial-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}}
	first := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, first.Close(ctx))
	_, err = pool.Exec(ctx, `
		DELETE FROM sync.row_state
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID)
	require.NoError(t, err)

	second, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	err = second.Bootstrap(ctx)
	var adoptionErr *PopulatedTableAdoptionError
	require.ErrorAs(t, err, &adoptionErr)
	require.Equal(t, "business_row_state_mismatch", adoptionErr.Reason)
	var bundleCount, rowStateCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state`).Scan(&rowStateCount))
	require.Equal(t, int64(1), bundleCount, "rejection must not append replacement history")
	require.Zero(t, rowStateCount, "rejection must roll back attempted repair")
	_, connectErr := second.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, &ConnectRequest{})
	require.ErrorIs(t, connectErr, errServiceNotReady)
}

func TestBootstrap_PopulatedAdoptionRejectsRetainedHistoryGap(t *testing.T) {
	fixture := newAdoptionIntegrityFixture(t)
	require.Equal(t, int64(2), fixture.pushName(1, 1, "Second").BundleSeq)
	require.Equal(t, int64(3), fixture.pushName(2, 2, "Third").BundleSeq)
	require.NoError(t, fixture.service.Close(fixture.ctx))

	_, err := fixture.pool.Exec(fixture.ctx, `
		DELETE FROM sync.bundle_log
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
		  AND bundle_seq = 2
	`, fixture.userID)
	require.NoError(t, err)

	second := fixture.newService("adoption-integrity-gap")
	fixture.requireRejected(second, "history_mismatch")

	var bundleCount int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, fixture.userID).Scan(&bundleCount))
	require.Equal(t, int64(2), bundleCount, "rejection must not replace missing history")

	require.NoError(t, pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		if _, txErr := tx.Exec(fixture.ctx, `
			UPDATE sync.user_state
			SET retained_bundle_floor = 2
			WHERE user_id = $1
		`, fixture.userID); txErr != nil {
			return txErr
		}
		_, txErr := tx.Exec(fixture.ctx, `
			DELETE FROM sync.bundle_log
			WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
			  AND bundle_seq <= 2
		`, fixture.userID)
		return txErr
	}))
	require.NoError(t, second.Bootstrap(fixture.ctx), "a coherent retained suffix must retry cleanly")
}

func TestBootstrap_PopulatedAdoptionRejectsRetainedBundleHashMismatch(t *testing.T) {
	fixture := newAdoptionIntegrityFixture(t)
	fixture.pushName(1, 1, "Second")
	fixture.pushName(2, 2, "Third")
	require.NoError(t, fixture.service.Close(fixture.ctx))

	var originalPayload []byte
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT rows.payload_wire
		FROM sync.bundle_rows AS rows
		JOIN sync.user_state AS users ON users.user_pk = rows.user_pk
		WHERE users.user_id = $1 AND rows.bundle_seq = 2
	`, fixture.userID).Scan(&originalPayload))
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.bundle_rows AS rows
		SET payload_wire = jsonb_set(rows.payload_wire::jsonb, '{name}', '"Corrupted"'::jsonb)::json
		FROM sync.user_state AS users
		WHERE users.user_pk = rows.user_pk
		  AND users.user_id = $1
		  AND rows.bundle_seq = 2
	`, fixture.userID)
	require.NoError(t, err)

	second := fixture.newService("adoption-integrity-hash")
	fixture.requireRejected(second, "history_mismatch")

	var businessName string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, fmt.Sprintf(`
		SELECT name FROM %s.users WHERE _sync_scope_id = $1 AND id = $2
	`, pgx.Identifier{fixture.schemaName}.Sanitize()), fixture.userID, fixture.rowID).Scan(&businessName))
	require.Equal(t, "Third", businessName, "rejection must not overwrite authoritative business data")

	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.bundle_rows AS rows
		SET payload_wire = $2::json
		FROM sync.user_state AS users
		WHERE users.user_pk = rows.user_pk
		  AND users.user_id = $1
		  AND rows.bundle_seq = 2
	`, fixture.userID, string(originalPayload))
	require.NoError(t, err)
	require.NoError(t, second.Bootstrap(fixture.ctx), "restored retained history must retry cleanly")
}

func TestBootstrap_PopulatedAdoptionRejectsRetainedBundleByteCountMismatch(t *testing.T) {
	fixture := newAdoptionIntegrityFixture(t)
	require.NoError(t, fixture.service.Close(fixture.ctx))

	var originalByteCount int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT bundle.byte_count
		FROM sync.bundle_log AS bundle
		JOIN sync.user_state AS users ON users.user_pk = bundle.user_pk
		WHERE users.user_id = $1 AND bundle.bundle_seq = 1
	`, fixture.userID).Scan(&originalByteCount))
	_, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.bundle_log AS bundle
		SET byte_count = byte_count + 1
		FROM sync.user_state AS users
		WHERE users.user_pk = bundle.user_pk
		  AND users.user_id = $1
		  AND bundle.bundle_seq = 1
	`, fixture.userID)
	require.NoError(t, err)

	second := fixture.newService("adoption-integrity-byte-count")
	adoptionErr := fixture.requireRejected(second, "history_mismatch")
	require.Contains(t, adoptionErr.Detail, "byte count")

	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.bundle_log AS bundle
		SET byte_count = $2
		FROM sync.user_state AS users
		WHERE users.user_pk = bundle.user_pk
		  AND users.user_id = $1
		  AND bundle.bundle_seq = 1
	`, fixture.userID, originalByteCount)
	require.NoError(t, err)
	require.NoError(t, second.Bootstrap(fixture.ctx), "restored retained byte count must retry cleanly")
}

func TestBootstrap_PopulatedAdoptionRejectsLivePayloadDrift(t *testing.T) {
	fixture := newAdoptionIntegrityFixture(t)
	require.NoError(t, fixture.service.Close(fixture.ctx))
	tableIdent := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()

	_, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`
		UPDATE %s SET name = 'Drifted' WHERE _sync_scope_id = $1 AND id = $2
	`, tableIdent), fixture.userID, fixture.rowID)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)

	second := fixture.newService("adoption-integrity-live-drift")
	fixture.requireRejected(second, "business_row_state_mismatch")

	var bundleCount int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, fixture.userID).Scan(&bundleCount))
	require.Equal(t, int64(1), bundleCount, "rejection must not append repair history")

	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`
		UPDATE %s SET name = 'Existing' WHERE _sync_scope_id = $1 AND id = $2
	`, tableIdent), fixture.userID, fixture.rowID)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	require.NoError(t, second.Bootstrap(fixture.ctx), "restored business data must retry cleanly")
}

func TestBootstrap_PopulatedAdoptionAcceptsCoherentPrunedCurrentVersion(t *testing.T) {
	fixture := newAdoptionIntegrityFixture(t)
	require.NoError(t, fixture.service.Close(fixture.ctx))

	require.NoError(t, pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(fixture.ctx, `
			UPDATE sync.user_state
			SET retained_bundle_floor = 1
			WHERE user_id = $1
		`, fixture.userID); err != nil {
			return err
		}
		_, err := tx.Exec(fixture.ctx, `
			DELETE FROM sync.bundle_log
			WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
			  AND bundle_seq <= 1
		`, fixture.userID)
		return err
	}))

	second := fixture.newService("adoption-integrity-pruned")
	require.NoError(t, second.Bootstrap(fixture.ctx))
	connect, err := second.Connect(fixture.ctx, Actor{UserID: fixture.userID, SourceID: "reader"}, &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)
	snapshot, err := second.CreateSnapshotSession(fixture.ctx, Actor{UserID: fixture.userID, SourceID: "reader"})
	require.NoError(t, err)
	require.Equal(t, int64(1), snapshot.SnapshotBundleSeq)
	require.Equal(t, int64(1), snapshot.RowCount)
}

func TestBootstrap_PopulatedAdoptionRejectsTombstoneWithoutRetainedDelete(t *testing.T) {
	fixture := newAdoptionIntegrityFixture(t)
	require.NoError(t, fixture.service.Close(fixture.ctx))
	tableIdent := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()

	_, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE _sync_scope_id = $1 AND id = $2
	`, tableIdent), fixture.userID, fixture.rowID)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.row_state
		SET deleted = TRUE
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, fixture.userID)
	require.NoError(t, err)

	second := fixture.newService("adoption-integrity-tombstone-insert")
	adoptionErr := fixture.requireRejected(second, "business_row_state_mismatch")
	require.Contains(t, adoptionErr.Detail, "retained "+OpInsert)

	var businessRowCount, bundleCount int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, fmt.Sprintf(`
		SELECT COUNT(*) FROM %s WHERE _sync_scope_id = $1
	`, tableIdent), fixture.userID).Scan(&businessRowCount))
	require.Zero(t, businessRowCount, "rejection must not recreate authoritative business data")
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, fixture.userID).Scan(&bundleCount))
	require.Equal(t, int64(1), bundleCount, "rejection must not append repair history")

	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, tableIdent), fixture.userID, fixture.rowID)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER USER`, tableIdent))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.row_state
		SET deleted = FALSE
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, fixture.userID)
	require.NoError(t, err)
	require.NoError(t, second.Bootstrap(fixture.ctx), "restored live state must retry cleanly")
}

func TestBootstrap_PopulatedAdoptionAcceptsCoherentPrunedTombstone(t *testing.T) {
	fixture := newAdoptionIntegrityFixture(t)
	bundle, err := pushRowsViaSession(t, fixture.ctx, fixture.service, Actor{
		UserID:   fixture.userID,
		SourceID: "integrity-writer",
	}, 1, []PushRequestRow{{
		Schema:         fixture.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": fixture.rowID.String()},
		Op:             OpDelete,
		BaseRowVersion: 1,
	}})
	require.NoError(t, err)
	require.Equal(t, int64(2), bundle.BundleSeq)
	require.NoError(t, fixture.service.Close(fixture.ctx))
	unpruned := fixture.newService("adoption-integrity-retained-tombstone")
	require.NoError(t, unpruned.Bootstrap(fixture.ctx), "a coherent retained delete must remain acceptable")
	require.NoError(t, unpruned.Close(fixture.ctx))

	require.NoError(t, pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		if _, txErr := tx.Exec(fixture.ctx, `
			UPDATE sync.user_state
			SET retained_bundle_floor = 2
			WHERE user_id = $1
		`, fixture.userID); txErr != nil {
			return txErr
		}
		_, txErr := tx.Exec(fixture.ctx, `
			DELETE FROM sync.bundle_log
			WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
			  AND bundle_seq <= 2
		`, fixture.userID)
		return txErr
	}))

	second := fixture.newService("adoption-integrity-pruned-tombstone")
	require.NoError(t, second.Bootstrap(fixture.ctx))
	snapshot, err := second.CreateSnapshotSession(fixture.ctx, Actor{UserID: fixture.userID, SourceID: "reader"})
	require.NoError(t, err)
	require.Equal(t, int64(2), snapshot.SnapshotBundleSeq)
	require.Zero(t, snapshot.RowCount)
}

func TestBootstrap_CloseWaitsForActiveBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_close_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	blocker, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer blocker.Release()
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock($1)`, syncBootstrapLockKey)
	require.NoError(t, err)
	locked := true
	defer func() {
		if locked {
			_, _ = blocker.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey)
		}
	}()

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		AppName: "bootstrap-close-drain",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	bootstrapResult := make(chan error, 1)
	go func() { bootstrapResult <- svc.Bootstrap(ctx) }()
	require.Eventually(t, func() bool {
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return svc.bootstrapReadiness == bootstrapReadinessBootstrapping
	}, 5*time.Second, 10*time.Millisecond)

	closeResult := make(chan error, 1)
	go func() { closeResult <- svc.Close(ctx) }()
	var earlyCloseErr error
	earlyClose := false
	select {
	case earlyCloseErr = <-closeResult:
		earlyClose = true
	case <-time.After(150 * time.Millisecond):
	}

	var unlocked bool
	require.NoError(t, blocker.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey).Scan(&unlocked))
	require.True(t, unlocked)
	locked = false
	require.NoError(t, <-bootstrapResult)
	if !earlyClose {
		earlyCloseErr = <-closeResult
	}
	require.False(t, earlyClose, "Close returned while bootstrap could still mutate the database")
	require.NoError(t, earlyCloseErr)
	lifecycle, inFlight, accepting := svc.lifecycleSnapshot()
	require.Equal(t, serviceLifecycleClosed, lifecycle)
	require.Zero(t, inFlight)
	require.False(t, accepting)
}

func TestBootstrap_PopulatedAdoptionConcurrentInstances(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_parallel_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "parallel-owner-" + suffix
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New())
	require.NoError(t, err)
	config := &ServiceConfig{AppName: "parallel-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}}
	services := make([]*SyncService, 2)
	for i := range services {
		services[i], err = NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
		require.NoError(t, err)
		t.Cleanup(func() { _ = services[i].Close(context.Background()) })
	}
	start := make(chan struct{})
	errs := make(chan error, len(services))
	var wg sync.WaitGroup
	for _, svc := range services {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- svc.Bootstrap(ctx)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var bundleCount, sourceCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.source_state`).Scan(&sourceCount))
	require.Equal(t, int64(1), bundleCount)
	require.Equal(t, int64(1), sourceCount)
}

func TestBootstrap_PopulatedAdoptionConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_lock_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "lock-owner-" + suffix
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()

	writerTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	writerDone := false
	t.Cleanup(func() {
		if !writerDone {
			_ = writerTx.Rollback(context.Background())
		}
	})
	_, err = writerTx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Before Lock', 'before@example.com')
	`, tableIdent), userID, uuid.New())
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{AppName: "locking-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	bootstrapResult := make(chan error, 1)
	go func() { bootstrapResult <- svc.Bootstrap(ctx) }()

	require.Eventually(t, func() bool {
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return svc.bootstrapReadiness == bootstrapReadinessBootstrapping
	}, 5*time.Second, 10*time.Millisecond)
	_, err = svc.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)

	require.NoError(t, writerTx.Commit(ctx))
	writerDone = true
	require.NoError(t, <-bootstrapResult)
	connect, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, &ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)
	var rowStateCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state`).Scan(&rowStateCount))
	require.Equal(t, int64(1), rowStateCount)

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Direct', 'direct@example.com')
	`, tableIdent), userID, uuid.New())
	require.Error(t, err, "a writer waking after adoption must encounter the owner guard")

	managedID := uuid.New()
	bundle, err := pushRowsViaSession(t, ctx, svc, Actor{UserID: userID, SourceID: "managed-writer"}, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": managedID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        []byte(fmt.Sprintf(`{"id":"%s","name":"Managed","email":"managed@example.com"}`, managedID)),
	}})
	require.NoError(t, err)
	require.Equal(t, int64(2), bundle.BundleSeq, "the first post-adoption managed write must be contiguous")
}

func TestBootstrap_PopulatedAdoptionRollbackAndRetry(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "adopt_retry_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	userID := "retry-owner-" + suffix
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Existing', 'existing@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), userID, uuid.New())
	require.NoError(t, err)

	eventName := "reject_adoption_trigger_" + suffix
	functionName := "reject_adoption_trigger_fn_" + suffix
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION public.%s()
		RETURNS event_trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'synthetic adoption trigger failure'; END;
		$$;
		CREATE EVENT TRIGGER %s ON ddl_command_start
		WHEN TAG IN ('CREATE TRIGGER') EXECUTE FUNCTION public.%s();
	`, pgx.Identifier{functionName}.Sanitize(), pgx.Identifier{eventName}.Sanitize(), pgx.Identifier{functionName}.Sanitize()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP EVENT TRIGGER IF EXISTS "+pgx.Identifier{eventName}.Sanitize())
		_, _ = pool.Exec(context.Background(), "DROP FUNCTION IF EXISTS public."+pgx.Identifier{functionName}.Sanitize()+"()")
	})

	svc, err := NewRuntimeService(pool, &ServiceConfig{AppName: "retry-adoption", RegisteredTables: []RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	err = svc.Bootstrap(ctx)
	require.ErrorContains(t, err, "synthetic adoption trigger failure")
	var syncSchemaExists bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&syncSchemaExists))
	require.False(t, syncSchemaExists, "failed adoption must roll back schema and state")
	_, connectErr := svc.Connect(ctx, Actor{UserID: userID, SourceID: "reader"}, &ConnectRequest{})
	require.True(t, errors.Is(connectErr, errServiceNotReady))

	_, err = pool.Exec(ctx, "DROP EVENT TRIGGER "+pgx.Identifier{eventName}.Sanitize())
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(ctx))
	var bundleCount int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleCount))
	require.Equal(t, int64(1), bundleCount)
}
