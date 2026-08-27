//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	auditRestartPostgresLabel      = "com.mobiletoly.go-oversync.audit.restart=true"
	auditRestartBusinessDMLLockKey = int64(2026071004)
	auditRestartPruningLockKey     = int64(2026071005)
	auditRestartSnapshotRowLockKey = int64(2026071006)
)

type auditRestartPostgres struct {
	containerID   string
	containerName string
	dataDir       string
	server        *managedIntegrationPostgresServer

	cleanupOnce sync.Once
	cleanupErr  error
}

func newAuditRestartPostgres(t *testing.T) *auditRestartPostgres {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("find docker CLI for restart audit: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := fmt.Sprintf("%d-%s", os.Getpid(), strings.ReplaceAll(uuid.NewString(), "-", "")[:12])
	containerName := "go-oversync-audit-restart-" + suffix
	dataDir, err := os.MkdirTemp("", "go-oversync-audit-restart-postgres-")
	if err != nil {
		t.Fatalf("create restart-audit PostgreSQL data directory: %v", err)
	}
	if err := os.Chmod(dataDir, 0o777); err != nil {
		_ = os.RemoveAll(dataDir)
		t.Fatalf("prepare restart-audit PostgreSQL data directory: %v", err)
	}
	portReservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = os.RemoveAll(dataDir)
		t.Fatalf("reserve restart-audit PostgreSQL host port: %v", err)
	}
	host, port, err := net.SplitHostPort(portReservation.Addr().String())
	if err != nil {
		_ = portReservation.Close()
		_ = os.RemoveAll(dataDir)
		t.Fatalf("parse reserved restart-audit PostgreSQL host port: %v", err)
	}
	if err := portReservation.Close(); err != nil {
		_ = os.RemoveAll(dataDir)
		t.Fatalf("release reserved restart-audit PostgreSQL host port: %v", err)
	}

	harness := &auditRestartPostgres{
		containerName: containerName,
		dataDir:       dataDir,
		server: &managedIntegrationPostgresServer{
			host: host,
			port: port,
		},
	}
	completed := false
	defer func() {
		if !completed {
			_ = harness.cleanup()
		}
	}()

	if err := harness.startContainer(ctx); err != nil {
		t.Fatalf("start restart-audit PostgreSQL: %v", err)
	}
	if err := harness.server.waitUntilReady(ctx); err != nil {
		logs, _ := dockerOutput(context.Background(), "logs", "--tail", "100", harness.containerID)
		t.Fatalf("wait for restart-audit PostgreSQL: %v\n%s", err, logs)
	}

	t.Cleanup(func() {
		if err := harness.cleanup(); err != nil {
			t.Errorf("clean up restart-audit PostgreSQL: %v", err)
		}
	})
	completed = true
	return harness
}

func (p *auditRestartPostgres) startContainer(ctx context.Context) error {
	containerID, err := dockerOutput(ctx,
		"run",
		"--detach",
		"--name", p.containerName,
		"--label", auditRestartPostgresLabel,
		"--publish", net.JoinHostPort(p.server.host, p.server.port)+":5432",
		"--mount", "type=bind,source="+p.dataDir+",target=/var/lib/postgresql/data",
		"--env", "POSTGRES_USER=postgres",
		"--env", "POSTGRES_PASSWORD="+managedIntegrationPostgresPassword,
		"--env", "POSTGRES_DB=postgres",
		"--health-cmd", "pg_isready --username postgres --dbname postgres",
		"--health-interval", "250ms",
		"--health-timeout", "5s",
		"--health-retries", "120",
		managedIntegrationPostgresImage,
	)
	if err != nil {
		return err
	}
	p.containerID = strings.TrimSpace(containerID)
	p.server.containerID = p.containerID

	portOutput, err := dockerOutput(ctx, "port", p.containerID, "5432/tcp")
	if err != nil {
		return fmt.Errorf("query published port: %w", err)
	}
	host, port, err := parseDockerPublishedPort(portOutput)
	if err != nil {
		return err
	}
	if host != p.server.host || port != p.server.port {
		return fmt.Errorf(
			"published address changed: got %s, expected %s",
			net.JoinHostPort(host, port),
			net.JoinHostPort(p.server.host, p.server.port),
		)
	}
	return nil
}

func (p *auditRestartPostgres) databaseURL() string {
	return p.server.databaseURL("postgres")
}

func (p *auditRestartPostgres) crashAndRestart(t *testing.T) time.Duration {
	t.Helper()

	startedAt := time.Now()
	killCtx, cancelKill := context.WithTimeout(context.Background(), 30*time.Second)
	_, err := dockerOutput(killCtx, "kill", "--signal", "KILL", p.containerID)
	cancelKill()
	require.NoError(t, err, "SIGKILL restart-audit PostgreSQL")

	inspectCtx, cancelInspect := context.WithTimeout(context.Background(), 10*time.Second)
	state, err := dockerOutput(
		inspectCtx,
		"inspect",
		"--format", "{{.State.Running}} {{.State.ExitCode}}",
		p.containerID,
	)
	cancelInspect()
	require.NoError(t, err, "inspect crashed restart-audit PostgreSQL")
	require.True(t, strings.HasPrefix(state, "false "), "SIGKILL must leave PostgreSQL stopped, state=%q", state)

	removeCtx, cancelRemove := context.WithTimeout(context.Background(), 30*time.Second)
	_, err = dockerOutput(removeCtx, "rm", "--force", p.containerID)
	cancelRemove()
	require.NoError(t, err, "remove SIGKILLed restart-audit PostgreSQL container while retaining its test-owned data directory")
	p.containerID = ""
	p.server.containerID = ""

	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	err = p.startContainer(startCtx)
	cancelStart()
	require.NoError(t, err, "recreate restart-audit PostgreSQL against the same test-owned data directory")

	readyCtx, cancelReady := context.WithTimeout(context.Background(), 90*time.Second)
	err = p.server.waitUntilReady(readyCtx)
	cancelReady()
	if err != nil {
		logs, _ := dockerOutput(context.Background(), "logs", "--tail", "100", p.containerID)
		t.Fatalf("wait for restart-audit PostgreSQL after SIGKILL, pre-start state=%q: %v\n%s", state, err, logs)
	}

	elapsed := time.Since(startedAt)
	t.Logf("restart-audit PostgreSQL recovered from SIGKILL in %s; stopped state=%s", elapsed, state)
	return elapsed
}

func (p *auditRestartPostgres) cleanup() error {
	p.cleanupOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		var cleanupErrors []error
		if p.containerID != "" {
			if _, err := dockerOutput(ctx, "rm", "--force", "--volumes", p.containerID); err != nil && !auditDockerResourceMissing(err) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove container: %w", err))
			}
		}
		if p.dataDir != "" {
			if err := os.RemoveAll(p.dataDir); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove test-owned data directory: %w", err))
			}
		}
		p.cleanupErr = errors.Join(cleanupErrors...)
	})
	return p.cleanupErr
}

func auditDockerResourceMissing(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such container") || strings.Contains(message, "no such volume")
}

func newAuditRestartPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)
	config.MaxConns = 4
	config.MinConns = 1
	config.HealthCheckPeriod = 100 * time.Millisecond

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(context.Background()))
	return pool
}

func requireAuditRestartPoolReady(t *testing.T, pool *pgxpool.Pool) time.Duration {
	t.Helper()

	startedAt := time.Now()
	deadline := startedAt.Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		attemptCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		lastErr = pool.Ping(attemptCtx)
		cancel()
		if lastErr == nil {
			elapsed := time.Since(startedAt)
			t.Logf("existing pgxpool recovered in %s without Pool.Reset", elapsed)
			return elapsed
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("existing pgxpool did not recover within 30s: %v", lastErr)
	return 0
}

func newAuditRestartPushFixture(
	t *testing.T,
	postgres *auditRestartPostgres,
	scenario string,
	configure func(*ServiceConfig),
) *pushSessionFixture {
	t.Helper()

	ctx := context.Background()
	pool := newAuditRestartPool(t, postgres.databaseURL())
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_restart_" + scenario + "_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))

	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "audit-postgres-restart-" + scenario,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}
	if configure != nil {
		configure(config)
	}
	service, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.NoError(t, service.Bootstrap(ctx))

	userID := "audit-restart-" + scenario + "-user-" + suffix
	return &pushSessionFixture{
		pool:       pool,
		svc:        service,
		schemaName: schemaName,
		writer:     Actor{UserID: userID, SourceID: "writer"},
		reader:     Actor{UserID: userID, SourceID: "reader"},
	}
}

type auditRestartTableBlocker struct {
	conn *pgxpool.Conn
	tx   pgx.Tx
	once sync.Once
}

func newAuditRestartTableBlocker(t *testing.T, pool *pgxpool.Pool, table pgx.Identifier) *auditRestartTableBlocker {
	t.Helper()

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		t.Fatalf("begin restart-audit table blocker transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE "+table.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		_ = tx.Rollback(context.Background())
		conn.Release()
		t.Fatalf("lock restart-audit table %s: %v", table.Sanitize(), err)
	}
	blocker := &auditRestartTableBlocker{conn: conn, tx: tx}
	t.Cleanup(blocker.Close)
	return blocker
}

func (b *auditRestartTableBlocker) PID() uint32 {
	return b.conn.Conn().PgConn().PID()
}

func (b *auditRestartTableBlocker) Close() {
	b.once.Do(func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = b.tx.Rollback(rollbackCtx)
		cancel()
		b.conn.Release()
	})
}

type auditRestartAdvisoryBlocker struct {
	conn *pgxpool.Conn
	key  int64
	once sync.Once
}

func newAuditRestartAdvisoryBlocker(t *testing.T, pool *pgxpool.Pool, key int64) *auditRestartAdvisoryBlocker {
	t.Helper()

	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	if _, err := conn.Exec(context.Background(), `SELECT pg_advisory_lock($1)`, key); err != nil {
		conn.Release()
		t.Fatalf("acquire restart-audit advisory blocker %d: %v", key, err)
	}
	blocker := &auditRestartAdvisoryBlocker{conn: conn, key: key}
	t.Cleanup(blocker.Close)
	return blocker
}

func (b *auditRestartAdvisoryBlocker) PID() uint32 {
	return b.conn.Conn().PgConn().PID()
}

func (b *auditRestartAdvisoryBlocker) Close() {
	b.once.Do(func() {
		if !b.conn.Conn().IsClosed() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, _ = b.conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, b.key)
			cancel()
		}
		b.conn.Release()
	})
}

func requireAuditRestartWaiter(
	t *testing.T,
	pool *pgxpool.Pool,
	description string,
	query string,
	args ...any,
) uint32 {
	t.Helper()

	var waiterPID uint32
	require.Eventually(t, func() bool {
		queryCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		return pool.QueryRow(queryCtx, query, args...).Scan(&waiterPID) == nil
	}, 5*time.Second, 10*time.Millisecond, "%s", description)
	require.NotZero(t, waiterPID)
	return waiterPID
}

func requireAuditRestartRelationWaiter(
	t *testing.T,
	pool *pgxpool.Pool,
	schemaName string,
	tableName string,
	holderPID uint32,
) uint32 {
	t.Helper()

	return requireAuditRestartWaiter(
		t,
		pool,
		fmt.Sprintf("operation did not block reading %s.%s", schemaName, tableName),
		`
			SELECT locks.pid
			FROM pg_locks AS locks
			JOIN pg_class AS relations ON relations.oid = locks.relation
			JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
			WHERE locks.locktype = 'relation'
			  AND locks.mode = 'AccessShareLock'
			  AND NOT locks.granted
			  AND locks.pid <> $1
			  AND namespaces.nspname = $2
			  AND relations.relname = $3
			LIMIT 1
		`,
		holderPID,
		schemaName,
		tableName,
	)
}

func requireAuditRestartAdvisoryWaiter(
	t *testing.T,
	pool *pgxpool.Pool,
	key int64,
	holderPID uint32,
) uint32 {
	t.Helper()

	return requireAuditRestartWaiter(
		t,
		pool,
		fmt.Sprintf("operation did not block on advisory lock %d", key),
		`
			SELECT pid
			FROM pg_locks
			WHERE locktype = 'advisory'
			  AND classid = 0
			  AND objid = $1::oid
			  AND objsubid = 1
			  AND NOT granted
			  AND pid <> $2
			LIMIT 1
		`,
		key,
		holderPID,
	)
}

func TestAuditPostgresRestart_LoggedStateStagingPoolAndListenerRecover(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	pool := newAuditRestartPool(t, postgres.databaseURL())

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_restart_logged_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))

	service, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "audit-postgres-restart-logged",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled:           true,
			HeartbeatInterval: time.Second,
		},
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.NoError(t, service.Bootstrap(ctx))

	fixture := &pushSessionFixture{
		pool:       pool,
		svc:        service,
		schemaName: schemaName,
		writer:     Actor{UserID: "audit-restart-user-" + suffix, SourceID: "writer"},
		reader:     Actor{UserID: "audit-restart-user-" + suffix, SourceID: "reader"},
	}
	firstRowID := uuid.New()
	firstRows := []PushRequestRow{fixture.userRow(firstRowID, "BeforeRestart")}
	firstBundle, err := pushRowsViaSession(t, ctx, service, fixture.writer, 1, firstRows)
	require.NoError(t, err)
	require.Equal(t, int64(1), firstBundle.BundleSeq)

	stagedRowID := uuid.New()
	staged := fixture.createSession(t, ctx, 2, 1)
	fixture.uploadChunk(t, ctx, staged.PushID, 0, fixture.userRow(stagedRowID, "StagedAcrossRestart"))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, staged.PushID))

	snapshot, err := service.CreateSnapshotSession(ctx, fixture.reader)
	require.NoError(t, err)
	require.Equal(t, int64(1), snapshot.RowCount)
	require.Equal(t, firstBundle.BundleSeq, snapshot.SnapshotBundleSeq)
	requireAuditMetadataIntegrity(t, ctx, pool)

	startBundleChangeListenerForTest(t, service)
	require.Eventually(t, func() bool {
		return pool.Stat().AcquiredConns() >= 1
	}, 5*time.Second, 10*time.Millisecond, "bundle-change listener did not acquire its dedicated connection")
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	events, err := service.SubscribeBundleChanges(watchCtx, fixture.reader, firstBundle.BundleSeq)
	require.NoError(t, err)

	staleConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	var staleBackendPID uint32
	require.NoError(t, staleConn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&staleBackendPID))
	var serverStartedBefore time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT pg_postmaster_start_time()`).Scan(&serverStartedBefore))

	postgres.crashAndRestart(t)

	staleCtx, cancelStale := context.WithTimeout(ctx, time.Second)
	_, staleErr := staleConn.Exec(staleCtx, `SELECT 1`)
	cancelStale()
	require.Error(t, staleErr, "backend %d from before SIGKILL must be unusable", staleBackendPID)
	staleConn.Release()
	requireAuditRestartPoolReady(t, pool)

	var serverStartedAfter time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT pg_postmaster_start_time()`).Scan(&serverStartedAfter))
	require.True(t, serverStartedAfter.After(serverStartedBefore), "PostgreSQL postmaster start time must advance across SIGKILL/start")
	require.Equal(t, 1, fixture.businessUserCount(t, ctx))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, staged.PushID))

	snapshotRows, snapshotBundleSeq := collectSnapshotChunkRows(t, ctx, service, fixture.reader, snapshot.SnapshotID, 10)
	require.Equal(t, firstBundle.BundleSeq, snapshotBundleSeq)
	require.Len(t, snapshotRows, 1)
	require.Equal(t, firstRowID.String(), snapshotRows[0].Key["id"])

	replay, err := service.CreatePushSession(ctx, fixture.writer, &PushSessionCreateRequest{
		SourceBundleID:       1,
		PlannedRowCount:      1,
		CanonicalRequestHash: mustCanonicalPushRequestHash(t, firstRows),
	})
	require.NoError(t, err)
	require.Equal(t, "already_committed", replay.Status)
	require.Equal(t, firstBundle.BundleSeq, replay.BundleSeq)
	require.Equal(t, firstBundle.BundleHash, replay.BundleHash)
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, staged.PushID), "resolving a committed tuple must not disturb another staged tuple")
	requireAuditMetadataIntegrity(t, ctx, pool)

	require.Eventually(t, func() bool {
		return pool.Stat().AcquiredConns() >= 1
	}, 10*time.Second, 10*time.Millisecond, "bundle-change listener did not reacquire a connection after PostgreSQL restart")
	secondCommit := fixture.commitSession(t, ctx, staged.PushID)
	require.Equal(t, int64(2), secondCommit.BundleSeq)
	require.Equal(t, 2, fixture.businessUserCount(t, ctx))

	event := receiveBundleChangeEvent(t, events)
	require.Equal(t, secondCommit.BundleSeq, event.BundleSeq)

	var (
		bundleCount       int64
		liveRowStateCount int64
		maxSourceBundleID int64
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk AND NOT deleted),
			(SELECT max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = users.user_pk AND source_id = $2)
		FROM sync.user_state AS users
		WHERE users.user_id = $1
	`, fixture.writer.UserID, fixture.writer.SourceID).Scan(&bundleCount, &liveRowStateCount, &maxSourceBundleID))
	require.Equal(t, int64(2), bundleCount)
	require.Equal(t, int64(2), liveRowStateCount)
	require.Equal(t, int64(2), maxSourceBundleID)
	requireAuditMetadataIntegrity(t, ctx, pool)
}

func loadAuditFastAttachDurableState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
) map[string]string {
	t.Helper()
	relations := []string{pgx.Identifier{schemaName, "users"}.Sanitize()}
	for _, tableName := range []string{
		"user_state", "scope_state", "source_state", "row_state", "bundle_capture_stage", "bundle_log", "bundle_rows",
		"push_sessions", "push_session_rows", "snapshot_sessions", "snapshot_session_rows", "meta", "table_catalog",
	} {
		relations = append(relations, pgx.Identifier{"sync", tableName}.Sanitize())
	}
	state := make(map[string]string, len(relations))
	for _, relation := range relations {
		var encoded string
		require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT COALESCE(jsonb_agg(to_jsonb(snapshot) ORDER BY to_jsonb(snapshot)::text)::text, '[]')
			FROM %s AS snapshot
		`, relation)).Scan(&encoded))
		state[relation] = encoded
	}
	return state
}

func TestAuditPostgresRestart_FastAttachPreservesCommittedSupportedWrite(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	pool := newAuditRestartPool(t, postgres.databaseURL())
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_fast_attach_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "audit-postgres-fast-attach",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}
	service, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(ctx))

	actor := Actor{UserID: "fast-attach-owner-" + suffix, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, service, actor.UserID, "seed")
	rowID := uuid.New()
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()
	require.NoError(t, service.WithinSyncBundle(ctx, Actor{UserID: actor.UserID}, BundleSource{SourceID: "server-writer", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, name, email)
			VALUES ($1, 'Durable Ω', 'durable@example.com')
		`, tableIdent), rowID)
		return execErr
	}))

	pullBefore, err := service.ProcessPull(ctx, actor, 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), pullBefore.StableBundleSeq)
	require.Len(t, pullBefore.Bundles, 1)
	require.Len(t, pullBefore.Bundles[0].Rows, 1)
	pullBeforeJSON, err := json.Marshal(pullBefore)
	require.NoError(t, err)
	snapshotBefore, err := service.CreateSnapshotSession(ctx, actor)
	require.NoError(t, err)
	snapshotRowsBefore, snapshotSeqBefore := collectSnapshotChunkRows(t, ctx, service, actor, snapshotBefore.SnapshotID, 10)
	require.Equal(t, pullBefore.StableBundleSeq, snapshotSeqBefore)
	require.Len(t, snapshotRowsBefore, 1)
	require.Equal(t, rowID.String(), snapshotRowsBefore[0].Key["id"])
	require.NoError(t, service.DeleteSnapshotSession(ctx, actor, snapshotBefore.SnapshotID))
	durableBefore := loadAuditFastAttachDurableState(t, ctx, pool, schemaName)
	require.NoError(t, service.Close(ctx))

	postgres.crashAndRestart(t)
	requireAuditRestartPoolReady(t, pool)
	restarted, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	require.NoError(t, restarted.Bootstrap(ctx))
	require.Equal(t, durableBefore, loadAuditFastAttachDurableState(t, ctx, pool, schemaName))

	pullAfter, err := restarted.ProcessPull(ctx, actor, 0, 10, 0)
	require.NoError(t, err)
	pullAfterJSON, err := json.Marshal(pullAfter)
	require.NoError(t, err)
	require.JSONEq(t, string(pullBeforeJSON), string(pullAfterJSON))
	snapshotAfter, err := restarted.CreateSnapshotSession(ctx, actor)
	require.NoError(t, err)
	snapshotRowsAfter, snapshotSeqAfter := collectSnapshotChunkRows(t, ctx, restarted, actor, snapshotAfter.SnapshotID, 10)
	require.Equal(t, snapshotSeqBefore, snapshotSeqAfter)
	require.Equal(t, snapshotRowsBefore, snapshotRowsAfter)
	require.NoError(t, restarted.DeleteSnapshotSession(ctx, actor, snapshotAfter.SnapshotID))
}

func TestAuditPostgresRestart_InFlightCommitRollsBackAndSamePushIDRetriesOnce(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	pool := newAuditRestartPool(t, postgres.databaseURL())

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_restart_commit_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))

	service, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "audit-postgres-restart-in-flight-commit",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.NoError(t, service.Bootstrap(ctx))

	fixture := &pushSessionFixture{
		pool:       pool,
		svc:        service,
		schemaName: schemaName,
		writer:     Actor{UserID: "audit-restart-commit-user-" + suffix, SourceID: "writer"},
		reader:     Actor{UserID: "audit-restart-commit-user-" + suffix, SourceID: "reader"},
	}
	staged := fixture.createSession(t, ctx, 1, 1)
	fixture.uploadChunk(t, ctx, staged.PushID, 0, fixture.userRow(uuid.New(), "BlockedDuringBusinessDML"))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, staged.PushID))
	requireAuditMetadataIntegrity(t, ctx, pool)

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s.audit_restart_block_business_dml()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $audit$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d);
			RETURN NEW;
		END
		$audit$
	`, schemaIdent, auditRestartBusinessDMLLockKey))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER zz_audit_restart_block_business_dml
		BEFORE INSERT ON %s.users
		FOR EACH ROW
		EXECUTE FUNCTION %s.audit_restart_block_business_dml()
	`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	blocker, err := pool.Acquire(ctx)
	require.NoError(t, err)
	blockerReleased := false
	defer func() {
		if blockerReleased {
			return
		}
		if !blocker.Conn().IsClosed() {
			unlockCtx, cancelUnlock := context.WithTimeout(context.Background(), time.Second)
			_, _ = blocker.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, auditRestartBusinessDMLLockKey)
			cancelUnlock()
		}
		blocker.Release()
	}()
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock($1)`, auditRestartBusinessDMLLockKey)
	require.NoError(t, err)
	blockerPID := blocker.Conn().PgConn().PID()

	type commitResult struct {
		response *PushSessionCommitResponse
		err      error
	}
	commitCtx, cancelCommit := context.WithTimeout(ctx, 45*time.Second)
	defer cancelCommit()
	commitDone := make(chan commitResult, 1)
	go func() {
		response, commitErr := service.CommitPushSession(commitCtx, fixture.writer, staged.PushID)
		commitDone <- commitResult{response: response, err: commitErr}
	}()

	waiterPID := requireAuditRestartAdvisoryWaiter(t, pool, auditRestartBusinessDMLLockKey, blockerPID)
	t.Logf("in-flight commit backend %d is blocked behind advisory-lock holder %d before PostgreSQL SIGKILL", waiterPID, blockerPID)

	postgres.crashAndRestart(t)
	blocker.Release()
	blockerReleased = true

	var interrupted commitResult
	select {
	case interrupted = <-commitDone:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight commit did not return after PostgreSQL SIGKILL")
	}
	require.Error(t, interrupted.err)
	require.Nil(t, interrupted.response)
	t.Logf("in-flight commit returned after PostgreSQL SIGKILL with error: %v", interrupted.err)

	requireAuditRestartPoolReady(t, pool)
	require.Equal(t, 0, fixture.businessUserCount(t, ctx))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, staged.PushID))

	var (
		bundleCount       int64
		bundleRowCount    int64
		rowStateCount     int64
		captureStageCount int64
		sourceStateCount  int64
		pushSessionCount  int64
		nextBundleSeq     int64
		scopeStateCode    int16
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.bundle_log),
			(SELECT COUNT(*) FROM sync.bundle_rows),
			(SELECT COUNT(*) FROM sync.row_state),
			(SELECT COUNT(*) FROM sync.bundle_capture_stage),
			(SELECT COUNT(*) FROM sync.source_state),
			(SELECT COUNT(*) FROM sync.push_sessions WHERE push_id = $1::uuid),
			users.next_bundle_seq,
			scopes.state_code
		FROM sync.user_state AS users
		JOIN sync.scope_state AS scopes ON scopes.user_pk = users.user_pk
		WHERE users.user_id = $2
	`, staged.PushID, fixture.writer.UserID).Scan(
		&bundleCount,
		&bundleRowCount,
		&rowStateCount,
		&captureStageCount,
		&sourceStateCount,
		&pushSessionCount,
		&nextBundleSeq,
		&scopeStateCode,
	))
	require.Zero(t, bundleCount)
	require.Zero(t, bundleRowCount)
	require.Zero(t, rowStateCount)
	require.Zero(t, captureStageCount)
	require.Zero(t, sourceStateCount)
	require.Equal(t, int64(1), pushSessionCount)
	require.Equal(t, int64(1), nextBundleSeq)
	require.Equal(t, int16(1), scopeStateCode)
	requireAuditMetadataIntegrity(t, ctx, pool)

	retried, err := service.CommitPushSession(ctx, fixture.writer, staged.PushID)
	require.NoError(t, err)
	require.NotNil(t, retried)
	require.Equal(t, int64(1), retried.BundleSeq)
	require.Equal(t, fixture.writer.SourceID, retried.SourceID)
	require.Equal(t, int64(1), retried.SourceBundleID)
	require.Equal(t, 1, fixture.businessUserCount(t, ctx))

	var (
		committedBundles int64
		committedRows    int64
		liveRows         int64
		stagedSessions   int64
		stagedRows       int64
		watermark        int64
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk AND NOT deleted),
			(SELECT COUNT(*) FROM sync.push_sessions WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.push_session_rows AS rows JOIN sync.push_sessions AS sessions USING (push_id) WHERE sessions.user_pk = users.user_pk),
			(SELECT max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = users.user_pk AND source_id = $2)
		FROM sync.user_state AS users
		WHERE users.user_id = $1
	`, fixture.writer.UserID, fixture.writer.SourceID).Scan(
		&committedBundles,
		&committedRows,
		&liveRows,
		&stagedSessions,
		&stagedRows,
		&watermark,
	))
	require.Equal(t, int64(1), committedBundles)
	require.Equal(t, int64(1), committedRows)
	require.Equal(t, int64(1), liveRows)
	require.Zero(t, stagedSessions)
	require.Zero(t, stagedRows)
	require.Equal(t, int64(1), watermark)
	requireAuditMetadataIntegrity(t, ctx, pool)

	require.Eventually(t, func() bool {
		return pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "in-flight crash/retry leaked a pool connection")
}

func TestAuditPostgresRestart_InFlightPullFailsWithoutMutationAndExactRequestRetries(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	fixture := newAuditRestartPushFixture(t, postgres, "pull", nil)

	firstBundle, err := pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(uuid.New(), "PullBeforeRestartOne"),
	})
	require.NoError(t, err)
	secondBundle, err := pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 2, []PushRequestRow{
		fixture.userRow(uuid.New(), "PullBeforeRestartTwo"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstBundle.BundleSeq)
	require.Equal(t, int64(2), secondBundle.BundleSeq)

	const (
		afterBundleSeq = int64(0)
		maxBundles     = 10
	)
	targetBundleSeq := secondBundle.BundleSeq
	expected, err := fixture.svc.ProcessPull(ctx, fixture.reader, afterBundleSeq, maxBundles, targetBundleSeq)
	require.NoError(t, err)
	require.Len(t, expected.Bundles, 2)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	blocker := newAuditRestartTableBlocker(t, fixture.pool, pgx.Identifier{"sync", "bundle_log"})
	type pullResult struct {
		response *PullResponse
		err      error
	}
	pullCtx, cancelPull := context.WithTimeout(ctx, 45*time.Second)
	defer cancelPull()
	pullDone := make(chan pullResult, 1)
	go func() {
		response, pullErr := fixture.svc.ProcessPull(
			pullCtx,
			fixture.reader,
			afterBundleSeq,
			maxBundles,
			targetBundleSeq,
		)
		pullDone <- pullResult{response: response, err: pullErr}
	}()

	waiterPID := requireAuditRestartRelationWaiter(t, fixture.pool, "sync", "bundle_log", blocker.PID())
	t.Logf("in-flight pull backend %d is blocked reading sync.bundle_log before PostgreSQL SIGKILL", waiterPID)
	postgres.crashAndRestart(t)
	blocker.Close()

	var interrupted pullResult
	select {
	case interrupted = <-pullDone:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight pull did not return after PostgreSQL SIGKILL")
	}
	require.Error(t, interrupted.err)
	require.Nil(t, interrupted.response)
	t.Logf("in-flight pull returned after PostgreSQL SIGKILL with error: %v", interrupted.err)

	requireAuditRestartPoolReady(t, fixture.pool)
	require.Equal(t, 2, fixture.businessUserCount(t, ctx))
	var (
		bundleCount    int64
		bundleRowCount int64
		rowStateCount  int64
		watermark      int64
	)
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk AND NOT deleted),
			(SELECT max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = users.user_pk AND source_id = $2)
		FROM sync.user_state AS users
		WHERE users.user_id = $1
	`, fixture.writer.UserID, fixture.writer.SourceID).Scan(
		&bundleCount,
		&bundleRowCount,
		&rowStateCount,
		&watermark,
	))
	require.Equal(t, int64(2), bundleCount)
	require.Equal(t, int64(2), bundleRowCount)
	require.Equal(t, int64(2), rowStateCount)
	require.Equal(t, int64(2), watermark)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	retried, err := fixture.svc.ProcessPull(ctx, fixture.reader, afterBundleSeq, maxBundles, targetBundleSeq)
	require.NoError(t, err)
	require.Equal(t, expected, retried)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	require.Eventually(t, func() bool {
		return fixture.pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "in-flight pull crash/retry leaked a pool connection")
}

func TestAuditPostgresRestart_InFlightSnapshotPartialInsertionRollsBackAndRetries(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	fixture := newAuditRestartPushFixture(t, postgres, "snapshot", nil)

	firstRowID := uuid.New()
	secondRowID := uuid.New()
	bundle, err := pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(firstRowID, "SnapshotBeforeRestartOne"),
		fixture.userRow(secondRowID, "SnapshotBeforeRestartTwo"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), bundle.BundleSeq)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	_, err = fixture.pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION sync.audit_restart_block_second_snapshot_row()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $audit$
		BEGIN
			IF NEW.row_ordinal = 2 THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END
		$audit$
	`, auditRestartSnapshotRowLockKey))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(ctx, `
		CREATE TRIGGER zz_audit_restart_block_second_snapshot_row
		BEFORE INSERT ON sync.snapshot_session_rows
		FOR EACH ROW
		EXECUTE FUNCTION sync.audit_restart_block_second_snapshot_row()
	`)
	require.NoError(t, err)
	blocker := newAuditRestartAdvisoryBlocker(t, fixture.pool, auditRestartSnapshotRowLockKey)
	type snapshotResult struct {
		response *SnapshotSession
		err      error
	}
	snapshotCtx, cancelSnapshot := context.WithTimeout(ctx, 45*time.Second)
	defer cancelSnapshot()
	snapshotDone := make(chan snapshotResult, 1)
	go func() {
		response, snapshotErr := fixture.svc.CreateSnapshotSessionWithRequest(snapshotCtx, fixture.reader, nil)
		snapshotDone <- snapshotResult{response: response, err: snapshotErr}
	}()

	waiterPID := requireAuditRestartAdvisoryWaiter(t, fixture.pool, auditRestartSnapshotRowLockKey, blocker.PID())
	t.Logf("in-flight snapshot backend %d inserted row ordinal 1 and is blocked inserting row ordinal 2 before PostgreSQL SIGKILL", waiterPID)
	postgres.crashAndRestart(t)
	blocker.Close()

	var interrupted snapshotResult
	select {
	case interrupted = <-snapshotDone:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight snapshot creation did not return after PostgreSQL SIGKILL")
	}
	require.Error(t, interrupted.err)
	require.Nil(t, interrupted.response)
	t.Logf("in-flight snapshot creation returned after PostgreSQL SIGKILL with error: %v", interrupted.err)

	requireAuditRestartPoolReady(t, fixture.pool)
	var (
		snapshotSessionCount int64
		snapshotRowCount     int64
	)
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.snapshot_sessions),
			(SELECT COUNT(*) FROM sync.snapshot_session_rows)
	`).Scan(&snapshotSessionCount, &snapshotRowCount))
	require.Zero(t, snapshotSessionCount)
	require.Zero(t, snapshotRowCount)
	require.Equal(t, 2, fixture.businessUserCount(t, ctx))
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	retried, err := fixture.svc.CreateSnapshotSessionWithRequest(ctx, fixture.reader, nil)
	require.NoError(t, err)
	require.NotNil(t, retried)
	require.Equal(t, bundle.BundleSeq, retried.SnapshotBundleSeq)
	require.Equal(t, int64(2), retried.RowCount)
	rows, snapshotBundleSeq := collectSnapshotChunkRows(t, ctx, fixture.svc, fixture.reader, retried.SnapshotID, 10)
	require.Equal(t, bundle.BundleSeq, snapshotBundleSeq)
	require.Len(t, rows, 2)
	require.ElementsMatch(
		t,
		[]any{firstRowID.String(), secondRowID.String()},
		[]any{rows[0].Key["id"], rows[1].Key["id"]},
	)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	require.Eventually(t, func() bool {
		return fixture.pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "in-flight snapshot crash/retry leaked a pool connection")
}

func TestAuditPostgresRestart_InFlightPruningRollsBackFloorAndHistoryThenRetries(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	fixture := newAuditRestartPushFixture(t, postgres, "pruning", func(config *ServiceConfig) {
		config.RetainedBundlesPerUser = 1
		config.RetentionPruneBatchSize = 100
	})

	firstRowID := uuid.New()
	firstBundle, err := pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(firstRowID, "PruningBeforeRestart"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstBundle.BundleSeq)
	var initialFloor int64
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = $1
	`, fixture.writer.UserID).Scan(&initialFloor))
	require.Zero(t, initialFloor)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	_, err = fixture.pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION sync.audit_restart_block_pruning_delete()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $audit$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d);
			RETURN OLD;
		END
		$audit$
	`, auditRestartPruningLockKey))
	require.NoError(t, err)
	_, err = fixture.pool.Exec(ctx, `
		CREATE TRIGGER zz_audit_restart_block_pruning_delete
		BEFORE DELETE ON sync.bundle_log
		FOR EACH ROW
		EXECUTE FUNCTION sync.audit_restart_block_pruning_delete()
	`)
	require.NoError(t, err)

	secondRowID := uuid.New()
	staged := fixture.createSession(t, ctx, 2, 1)
	fixture.uploadChunk(t, ctx, staged.PushID, 0, fixture.userRow(secondRowID, "PruningBlockedAcrossRestart"))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, staged.PushID))

	blocker := newAuditRestartAdvisoryBlocker(t, fixture.pool, auditRestartPruningLockKey)
	type commitResult struct {
		response *PushSessionCommitResponse
		err      error
	}
	commitCtx, cancelCommit := context.WithTimeout(ctx, 45*time.Second)
	defer cancelCommit()
	commitDone := make(chan commitResult, 1)
	go func() {
		response, commitErr := fixture.svc.CommitPushSession(commitCtx, fixture.writer, staged.PushID)
		commitDone <- commitResult{response: response, err: commitErr}
	}()

	waiterPID := requireAuditRestartAdvisoryWaiter(t, fixture.pool, auditRestartPruningLockKey, blocker.PID())
	t.Logf("in-flight commit backend %d is blocked deleting pruned bundle history before PostgreSQL SIGKILL", waiterPID)
	postgres.crashAndRestart(t)
	blocker.Close()

	var interrupted commitResult
	select {
	case interrupted = <-commitDone:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight pruning commit did not return after PostgreSQL SIGKILL")
	}
	require.Error(t, interrupted.err)
	require.Nil(t, interrupted.response)
	t.Logf("in-flight pruning commit returned after PostgreSQL SIGKILL with error: %v", interrupted.err)

	requireAuditRestartPoolReady(t, fixture.pool)
	require.Equal(t, 1, fixture.businessUserCount(t, ctx))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, staged.PushID))

	var (
		floorAfterFault         int64
		nextBundleSeqAfterFault int64
		bundleCountAfterFault   int64
		minBundleSeqAfterFault  int64
		maxBundleSeqAfterFault  int64
		bundleRowsAfterFault    int64
		rowStatesAfterFault     int64
		captureRowsAfterFault   int64
		watermarkAfterFault     int64
		pushSessionsAfterFault  int64
	)
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT
			users.retained_bundle_floor,
			users.next_bundle_seq,
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COALESCE(MIN(bundle_seq), 0) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COALESCE(MAX(bundle_seq), 0) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk AND NOT deleted),
			(SELECT COUNT(*) FROM sync.bundle_capture_stage WHERE user_pk = users.user_pk),
			(SELECT max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = users.user_pk AND source_id = $2),
			(SELECT COUNT(*) FROM sync.push_sessions WHERE user_pk = users.user_pk)
		FROM sync.user_state AS users
		WHERE users.user_id = $1
	`, fixture.writer.UserID, fixture.writer.SourceID).Scan(
		&floorAfterFault,
		&nextBundleSeqAfterFault,
		&bundleCountAfterFault,
		&minBundleSeqAfterFault,
		&maxBundleSeqAfterFault,
		&bundleRowsAfterFault,
		&rowStatesAfterFault,
		&captureRowsAfterFault,
		&watermarkAfterFault,
		&pushSessionsAfterFault,
	))
	require.Zero(t, floorAfterFault)
	require.Equal(t, int64(2), nextBundleSeqAfterFault)
	require.Equal(t, int64(1), bundleCountAfterFault)
	require.Equal(t, int64(1), minBundleSeqAfterFault)
	require.Equal(t, int64(1), maxBundleSeqAfterFault)
	require.Equal(t, int64(1), bundleRowsAfterFault)
	require.Equal(t, int64(1), rowStatesAfterFault)
	require.Zero(t, captureRowsAfterFault)
	require.Equal(t, int64(1), watermarkAfterFault)
	require.Equal(t, int64(1), pushSessionsAfterFault)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	retried, err := fixture.svc.CommitPushSession(ctx, fixture.writer, staged.PushID)
	require.NoError(t, err)
	require.NotNil(t, retried)
	require.Equal(t, int64(2), retried.BundleSeq)
	require.Equal(t, int64(2), retried.SourceBundleID)
	require.Equal(t, 2, fixture.businessUserCount(t, ctx))

	var (
		floorAfterRetry         int64
		nextBundleSeqAfterRetry int64
		bundleCountAfterRetry   int64
		minBundleSeqAfterRetry  int64
		maxBundleSeqAfterRetry  int64
		bundleRowsAfterRetry    int64
		rowStatesAfterRetry     int64
		captureRowsAfterRetry   int64
		watermarkAfterRetry     int64
		pushSessionsAfterRetry  int64
	)
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT
			users.retained_bundle_floor,
			users.next_bundle_seq,
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COALESCE(MIN(bundle_seq), 0) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COALESCE(MAX(bundle_seq), 0) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk AND NOT deleted),
			(SELECT COUNT(*) FROM sync.bundle_capture_stage WHERE user_pk = users.user_pk),
			(SELECT max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = users.user_pk AND source_id = $2),
			(SELECT COUNT(*) FROM sync.push_sessions WHERE user_pk = users.user_pk)
		FROM sync.user_state AS users
		WHERE users.user_id = $1
	`, fixture.writer.UserID, fixture.writer.SourceID).Scan(
		&floorAfterRetry,
		&nextBundleSeqAfterRetry,
		&bundleCountAfterRetry,
		&minBundleSeqAfterRetry,
		&maxBundleSeqAfterRetry,
		&bundleRowsAfterRetry,
		&rowStatesAfterRetry,
		&captureRowsAfterRetry,
		&watermarkAfterRetry,
		&pushSessionsAfterRetry,
	))
	require.Equal(t, int64(1), floorAfterRetry)
	require.Equal(t, int64(3), nextBundleSeqAfterRetry)
	require.Equal(t, int64(1), bundleCountAfterRetry)
	require.Equal(t, int64(2), minBundleSeqAfterRetry)
	require.Equal(t, int64(2), maxBundleSeqAfterRetry)
	require.Equal(t, int64(1), bundleRowsAfterRetry)
	require.Equal(t, int64(2), rowStatesAfterRetry)
	require.Zero(t, captureRowsAfterRetry)
	require.Equal(t, int64(2), watermarkAfterRetry)
	require.Zero(t, pushSessionsAfterRetry)

	snapshot, err := fixture.svc.CreateSnapshotSession(ctx, fixture.reader)
	require.NoError(t, err)
	require.Equal(t, int64(2), snapshot.RowCount)
	rows, snapshotBundleSeq := collectSnapshotChunkRows(t, ctx, fixture.svc, fixture.reader, snapshot.SnapshotID, 10)
	require.Equal(t, retried.BundleSeq, snapshotBundleSeq)
	require.Len(t, rows, 2)
	require.ElementsMatch(t, []any{firstRowID.String(), secondRowID.String()}, []any{rows[0].Key["id"], rows[1].Key["id"]})
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	require.Eventually(t, func() bool {
		return fixture.pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "in-flight pruning crash/retry leaked a pool connection")
}

func TestAuditPostgresRestart_ManagedLayoutDriftPersistsUntilExactRepair(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	pool := newAuditRestartPool(t, postgres.databaseURL())
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_restart_layout_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	rowID := uuid.New()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'Durable', 'durable@example.com')
	`, pgx.Identifier{schemaName}.Sanitize()), "restart-layout-owner", rowID)
	require.NoError(t, err)
	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "audit-restart-managed-layout",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}
	service, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(ctx))
	requireAuditMetadataIntegrity(t, ctx, pool)
	var businessBefore, bundleBefore, rowStateBefore int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users`, pgx.Identifier{schemaName}.Sanitize())).Scan(&businessBefore))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleBefore))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state`).Scan(&rowStateBefore))
	require.NoError(t, service.Close(ctx))
	_, err = pool.Exec(ctx, `DROP INDEX sync.rs_user_live_snapshot_idx`)
	require.NoError(t, err)
	pool.Close()

	postgres.crashAndRestart(t)
	restartedPool := newAuditRestartPool(t, postgres.databaseURL())
	restarted, err := NewRuntimeService(restartedPool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	err = restarted.Bootstrap(ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "sync.rs_user_live_snapshot_idx")
	var businessAfter, bundleAfter, rowStateAfter int64
	require.NoError(t, restartedPool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users`, pgx.Identifier{schemaName}.Sanitize())).Scan(&businessAfter))
	require.NoError(t, restartedPool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleAfter))
	require.NoError(t, restartedPool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state`).Scan(&rowStateAfter))
	require.Equal(t, []int64{businessBefore, bundleBefore, rowStateBefore}, []int64{businessAfter, bundleAfter, rowStateAfter})
	requireAuditMetadataIntegrity(t, ctx, restartedPool)

	_, err = restartedPool.Exec(ctx, `CREATE INDEX rs_user_live_snapshot_idx ON sync.row_state(user_pk, table_id, key_bytes) WHERE deleted = FALSE`)
	require.NoError(t, err)
	require.NoError(t, restarted.Bootstrap(ctx))
	require.NoError(t, restarted.validateManagedLayout(ctx, restartedPool))
	requireAuditMetadataIntegrity(t, ctx, restartedPool)
}

func TestAuditPostgresRestart_UnloggedAcknowledgedRowMustNotDisappearWhileMetadataSurvives(t *testing.T) {
	ctx := context.Background()
	postgres := newAuditRestartPostgres(t)
	pool := newAuditRestartPool(t, postgres.databaseURL())

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_restart_unlogged_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, "CREATE SCHEMA "+schemaIdent)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s.records (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent))
	require.NoError(t, err)

	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "audit-postgres-restart-unlogged",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}},
		},
	}
	unloggedService, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)

	bootstrapErr := unloggedService.Bootstrap(ctx)
	require.Error(t, bootstrapErr)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, bootstrapErr, &schemaErr)
	require.Contains(t, bootstrapErr.Error(), schemaName+".records")
	require.Contains(t, strings.ToLower(bootstrapErr.Error()), "unlogged")
	require.Contains(t, bootstrapErr.Error(), `relpersistence="u"`)
	require.NoError(t, unloggedService.Close(context.Background()))

	var syncSchemaExists bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&syncSchemaExists))
	require.False(t, syncSchemaExists, "unlogged rejection must happen before sync layout creation")

	_, err = pool.Exec(ctx, "DROP SCHEMA "+schemaIdent+" CASCADE")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "CREATE SCHEMA "+schemaIdent)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.records (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			body TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, schemaIdent))
	require.NoError(t, err)

	service, err := NewRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.NoError(t, service.Bootstrap(ctx))

	rowID := uuid.NewString()
	writer := Actor{UserID: "audit-unlogged-user-" + suffix, SourceID: "writer"}
	reader := Actor{UserID: writer.UserID, SourceID: "reader"}
	bundle, err := pushRowsViaSession(t, ctx, service, writer, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "records",
		Key:            SyncKey{"id": rowID},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","body":"acknowledged"}`, rowID)),
	}})
	require.NoError(t, err)
	require.NotNil(t, bundle)
	require.Equal(t, int64(1), bundle.BundleSeq)
	requireAuditMetadataIntegrity(t, ctx, pool)

	var (
		persistence        string
		businessRowsBefore int64
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT c.relpersistence::text
		FROM pg_class AS c
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = 'records'
	`, schemaName).Scan(&persistence))
	require.Equal(t, "p", persistence)
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.records`, schemaIdent)).Scan(&businessRowsBefore))
	require.Equal(t, int64(1), businessRowsBefore)

	postgres.crashAndRestart(t)
	requireAuditRestartPoolReady(t, pool)

	var (
		businessRowsAfter   int64
		liveRowStateCount   int64
		bundleCount         int64
		bundleRowCount      int64
		maxSourceBundleID   int64
		persistedBundleHash string
	)
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.records`, schemaIdent)).Scan(&businessRowsAfter))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk AND NOT deleted),
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = users.user_pk),
			(SELECT max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = users.user_pk AND source_id = $2),
			(SELECT encode(bundle_hash, 'hex') FROM sync.bundle_log WHERE user_pk = users.user_pk AND bundle_seq = 1)
		FROM sync.user_state AS users
		WHERE users.user_id = $1
	`, writer.UserID, writer.SourceID).Scan(
		&liveRowStateCount,
		&bundleCount,
		&bundleRowCount,
		&maxSourceBundleID,
		&persistedBundleHash,
	))

	pull, err := service.ProcessPull(ctx, reader, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	require.Equal(t, bundle.BundleSeq, pull.Bundles[0].BundleSeq)
	require.Equal(t, bundle.BundleHash, pull.Bundles[0].BundleHash)
	require.Len(t, pull.Bundles[0].Rows, 1)

	snapshot, err := service.CreateSnapshotSession(ctx, reader)
	require.NoError(t, err)
	snapshotRows, snapshotBundleSeq := collectSnapshotChunkRows(t, ctx, service, reader, snapshot.SnapshotID, 10)
	require.Equal(t, bundle.BundleSeq, snapshotBundleSeq)
	require.Len(t, snapshotRows, 1)
	require.Equal(t, rowID, snapshotRows[0].Key["id"])

	t.Logf(
		"OS-AUD-018 corrected state after PostgreSQL SIGKILL/start: relpersistence=%q business_rows_before=%d business_rows_after=%d live_row_state=%d bundle_log=%d bundle_rows=%d max_source_bundle_id=%d bundle_hash=%s snapshot_rows=%d",
		persistence,
		businessRowsBefore,
		businessRowsAfter,
		liveRowStateCount,
		bundleCount,
		bundleRowCount,
		maxSourceBundleID,
		persistedBundleHash,
		len(snapshotRows),
	)

	require.Equal(t, int64(1), liveRowStateCount)
	require.Equal(t, int64(1), bundleCount)
	require.Equal(t, int64(1), bundleRowCount)
	require.Equal(t, int64(1), maxSourceBundleID)
	require.Equal(t, bundle.BundleHash, persistedBundleHash)
	requireAuditMetadataIntegrity(t, ctx, pool)

	require.Equal(
		t,
		int64(1),
		businessRowsAfter,
		"OS-AUD-018: an acknowledged authoritative row disappeared while its logged synchronization metadata survived",
	)
}
