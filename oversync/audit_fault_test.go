//go:build oversync_audit

package oversync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAuditFault_TerminatedUploadConnectionReleasesLockAndPoolSlot(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})

	conn, releaseConn, err := fixture.svc.acquireUserUploadConn(ctx, fixture.writer.UserID)
	require.NoError(t, err)

	var terminated bool
	require.NoError(t, fixture.pool.QueryRow(ctx,
		`SELECT pg_terminate_backend($1)`,
		conn.Conn().PgConn().PID(),
	).Scan(&terminated))
	require.True(t, terminated)

	_, err = conn.Exec(ctx, `SELECT 1`)
	require.Error(t, err, "the terminated backend must no longer accept queries")
	releaseConn()

	require.Eventually(t, func() bool {
		return fixture.pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "terminated upload connection leaked a pool slot")
	require.NoError(t, fixture.pool.Ping(ctx))

	bundle, err := pushRowsViaSession(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(uuid.New(), "Recovered"),
	})
	require.NoError(t, err)
	require.NotNil(t, bundle)
	require.Equal(t, int64(1), bundle.BundleSeq)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}

func TestAuditFault_CancelledAdvisoryLockWaitLeavesRetryableState(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})
	initializationID := resolveConnectForPushSession(t, ctx, fixture.svc, fixture.writer, true)

	lockConn, err := fixture.pool.Acquire(ctx)
	require.NoError(t, err)
	lockHeld := true
	lockConnReleased := false
	defer func() {
		if lockHeld {
			_, _ = lockConn.Exec(context.Background(),
				`SELECT pg_advisory_unlock(hashtextextended($1, 0))`,
				fixture.writer.UserID,
			)
		}
		if !lockConnReleased {
			lockConn.Release()
		}
	}()

	_, err = lockConn.Exec(ctx,
		`SELECT pg_advisory_lock(hashtextextended($1, 0))`,
		fixture.writer.UserID,
	)
	require.NoError(t, err)

	waitCtx, cancelWait := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, createErr := fixture.svc.CreatePushSession(waitCtx, fixture.writer, &PushSessionCreateRequest{
			SourceBundleID:       1,
			PlannedRowCount:      1,
			CanonicalRequestHash: strings.Repeat("0", 64),
			InitializationID:     initializationID,
		})
		result <- createErr
	}()

	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := fixture.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_locks
				WHERE locktype = 'advisory'
				  AND NOT granted
				  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			)
		`).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond, "push session never reached the advisory lock wait")

	cancelWait()
	select {
	case createErr := <-result:
		require.Error(t, createErr)
		require.Contains(t, strings.ToLower(createErr.Error()), "cancel")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled advisory lock wait did not return")
	}

	_, err = lockConn.Exec(ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1, 0))`,
		fixture.writer.UserID,
	)
	require.NoError(t, err)
	lockHeld = false
	lockConn.Release()
	lockConnReleased = true

	require.Eventually(t, func() bool {
		return fixture.pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "cancelled lock waiter leaked a pool slot")

	row := fixture.userRow(uuid.New(), "Retry")
	session, err := fixture.svc.CreatePushSession(ctx, fixture.writer, &PushSessionCreateRequest{
		SourceBundleID:       1,
		PlannedRowCount:      1,
		CanonicalRequestHash: mustCanonicalPushRequestHash(t, []PushRequestRow{row}),
		InitializationID:     initializationID,
	})
	require.NoError(t, err)
	require.Equal(t, "staging", session.Status)
	fixture.uploadChunk(t, ctx, session.PushID, 0, row)
	commit := fixture.commitSession(t, ctx, session.PushID)
	require.Equal(t, int64(1), commit.BundleSeq)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}
