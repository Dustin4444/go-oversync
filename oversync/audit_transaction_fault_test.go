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

func TestAuditTransactionFault_CancelDuringMetadataFinalizationRollsBackAndRetries(t *testing.T) {
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})
	rowID := uuid.New()
	session := fixture.createSession(t, ctx, 1, 1)
	fixture.uploadChunk(t, ctx, session.PushID, 0, fixture.userRow(rowID, "MetadataGate"))

	const gateKey int64 = 732026071004
	_, err := fixture.pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION sync.audit_wait_before_bundle_log_insert()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $function$
		BEGIN
			PERFORM pg_advisory_xact_lock(732026071004);
			RETURN NEW;
		END
		$function$;

		CREATE TRIGGER audit_wait_before_bundle_log_insert
		BEFORE INSERT ON sync.bundle_log
		FOR EACH ROW
		EXECUTE FUNCTION sync.audit_wait_before_bundle_log_insert()
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = fixture.pool.Exec(context.Background(), `
			DROP TRIGGER IF EXISTS audit_wait_before_bundle_log_insert ON sync.bundle_log;
			DROP FUNCTION IF EXISTS sync.audit_wait_before_bundle_log_insert()
		`)
	})

	gateConn, err := fixture.pool.Acquire(ctx)
	require.NoError(t, err)
	gateHeld := true
	gateReleased := false
	defer func() {
		if gateHeld {
			_, _ = gateConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, gateKey)
		}
		if !gateReleased {
			gateConn.Release()
		}
	}()
	_, err = gateConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, gateKey)
	require.NoError(t, err)

	commitCtx, cancelCommit := context.WithCancel(ctx)
	commitDone := make(chan error, 1)
	go func() {
		_, commitErr := fixture.svc.CommitPushSession(commitCtx, fixture.writer, session.PushID)
		commitDone <- commitErr
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
	}, 5*time.Second, 10*time.Millisecond, "commit never reached metadata-finalization gate")

	cancelCommit()
	select {
	case commitErr := <-commitDone:
		require.Error(t, commitErr)
		require.Contains(t, strings.ToLower(commitErr.Error()), "cancel")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled metadata-finalization commit did not return")
	}

	require.Equal(t, 0, fixture.businessUserCount(t, ctx))
	require.Equal(t, 1, fixture.pushSessionRowCount(t, ctx, session.PushID))
	var bundleCount, rowStateCount, sourceCount int
	require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log`).Scan(&bundleCount))
	require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state`).Scan(&rowStateCount))
	require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.source_state`).Scan(&sourceCount))
	require.Zero(t, bundleCount)
	require.Zero(t, rowStateCount)
	require.Zero(t, sourceCount)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	var unlocked bool
	require.NoError(t, gateConn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, gateKey).Scan(&unlocked))
	require.True(t, unlocked)
	gateHeld = false
	gateConn.Release()
	gateReleased = true

	commit := fixture.commitSession(t, ctx, session.PushID)
	require.Equal(t, int64(1), commit.BundleSeq)
	require.Equal(t, 1, fixture.businessUserCount(t, ctx))
	require.Equal(t, 0, fixture.pushSessionRowCount(t, ctx, session.PushID))
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}
