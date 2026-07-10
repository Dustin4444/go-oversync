//go:build oversync_audit

package oversync

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestAuditRetryFault_PoolExhaustionBeforeTransactionLeavesRetryableState(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditMultiServiceFixture(t, ctx, "pool_exhaustion", 1, 0)
	actor := fixture.actor("pool-exhaustion", "writer")
	mustInitializeEmptyScope(t, ctx, fixture.services[0], actor.UserID, actor.SourceID)

	heldConn, err := fixture.servicePools[0].Acquire(ctx)
	require.NoError(t, err)
	heldConnReleased := false
	t.Cleanup(func() {
		if !heldConnReleased {
			heldConn.Release()
		}
	})

	waitCtx, cancelWait := context.WithTimeout(ctx, 150*time.Millisecond)
	response, err := fixture.services[0].CreatePushSession(waitCtx, actor, &PushSessionCreateRequest{
		SourceBundleID:       1,
		PlannedRowCount:      1,
		CanonicalRequestHash: strings.Repeat("0", 64),
	})
	cancelWait()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, response)

	var stagedCount int64
	require.NoError(t, fixture.controlPool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_sessions AS session
		JOIN sync.user_state AS users ON users.user_pk = session.user_pk
		WHERE users.user_id = $1
	`, actor.UserID).Scan(&stagedCount))
	require.Zero(t, stagedCount)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)

	heldConn.Release()
	heldConnReleased = true
	require.Eventually(t, func() bool {
		return fixture.servicePools[0].Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "released exhausted pool slot remained acquired")

	bundle, err := pushRowsViaSession(
		t,
		ctx,
		fixture.services[0],
		actor,
		1,
		[]PushRequestRow{fixture.userRow(t, uuid.New(), "Pool Retry")},
	)
	require.NoError(t, err)
	require.NotNil(t, bundle)
	fixture.requireBusinessRowCount(t, ctx, actor.UserID, 1)
	requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, actor, 1, bundle.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
}

func TestAuditRetryFault_RowLockTimeoutRollsBackAndExplicitRetryCommitsOnce(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditMultiServiceFixture(t, ctx, "row_lock_timeout", 2, 40*time.Millisecond)
	actor := fixture.actor("row-lock-timeout", "writer")
	mustInitializeEmptyScope(t, ctx, fixture.services[0], actor.UserID, actor.SourceID)
	session := fixture.stagePush(
		t,
		ctx,
		fixture.services[0],
		actor,
		fixture.userRow(t, uuid.New(), "Lock Timeout"),
	)

	lockTx, err := fixture.controlPool.Begin(ctx)
	require.NoError(t, err)
	lockTxOpen := true
	t.Cleanup(func() {
		if lockTxOpen {
			_ = lockTx.Rollback(context.Background())
		}
	})
	var lockedPushID string
	require.NoError(t, lockTx.QueryRow(ctx, `
		SELECT push_id::text
		FROM sync.push_sessions
		WHERE push_id = $1::uuid
		FOR UPDATE
	`, session.PushID).Scan(&lockedPushID))
	require.Equal(t, session.PushID, lockedPushID)

	commitCtx, cancelCommit := context.WithTimeout(ctx, 5*time.Second)
	failedResponse, err := fixture.services[0].CommitPushSession(commitCtx, actor, session.PushID)
	cancelCommit()
	require.Nil(t, failedResponse)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "55P03", pgErr.Code)

	fixture.requireBusinessRowCount(t, ctx, actor.UserID, 0)
	var sessionCount, stagedRowCount int64
	require.NoError(t, fixture.controlPool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_sessions
		WHERE push_id = $1::uuid
	`, session.PushID).Scan(&sessionCount))
	require.Equal(t, int64(1), sessionCount)
	require.NoError(t, fixture.controlPool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_session_rows
		WHERE push_id = $1::uuid
	`, session.PushID).Scan(&stagedRowCount))
	require.Equal(t, int64(1), stagedRowCount)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)

	require.NoError(t, lockTx.Rollback(ctx))
	lockTxOpen = false
	committed, err := fixture.services[0].CommitPushSession(ctx, actor, session.PushID)
	require.NoError(t, err)
	require.NotNil(t, committed)
	require.Equal(t, int64(1), committed.BundleSeq)
	fixture.requireBusinessRowCount(t, ctx, actor.UserID, 1)
	requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, actor, 1, committed.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
}

func TestAuditRetryFault_ServerReportedRetryableTransactionFailureCommitsOnce(t *testing.T) {
	testCases := []struct {
		name    string
		code    string
		message string
	}{
		{name: "serialization_failure", code: "40001", message: "serialization failure"},
		{name: "deadlock_detected", code: "40P01", message: "deadlock detected"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newAuditMultiServiceFixture(t, ctx, "retry_"+testCase.code, 2, 0)
			actor := fixture.actor("retryable-"+testCase.code, "writer")
			mustInitializeEmptyScope(t, ctx, fixture.services[0], actor.UserID, actor.SourceID)
			sequenceIdent := installAuditRetryOnceTrigger(t, ctx, fixture, testCase.code, testCase.message)

			session := fixture.stagePush(
				t,
				ctx,
				fixture.services[0],
				actor,
				fixture.userRow(t, uuid.New(), "Retry "+testCase.code),
			)
			committed, err := fixture.services[0].CommitPushSession(ctx, actor, session.PushID)
			require.NoError(t, err)
			require.NotNil(t, committed)
			require.Equal(t, int64(1), committed.BundleSeq)

			var lastValue int64
			var isCalled bool
			require.NoError(t, fixture.controlPool.QueryRow(ctx,
				fmt.Sprintf(`SELECT last_value, is_called FROM %s`, sequenceIdent),
			).Scan(&lastValue, &isCalled))
			require.True(t, isCalled)
			require.Equal(t, int64(2), lastValue, "retryable transaction must execute exactly two attempts")

			fixture.requireBusinessRowCount(t, ctx, actor.UserID, 1)
			requireAuditCommittedSourceTuple(t, ctx, fixture.controlPool, actor, 1, committed.BundleHash)
			requireAuditMetadataIntegrity(t, ctx, fixture.controlPool)
		})
	}
}

func installAuditRetryOnceTrigger(
	t *testing.T,
	ctx context.Context,
	fixture *auditMultiServiceFixture,
	code string,
	message string,
) string {
	t.Helper()

	sequenceIdent := pgx.Identifier{fixture.schemaName, "audit_retry_sequence_" + code}.Sanitize()
	functionIdent := pgx.Identifier{fixture.schemaName, "audit_retry_function_" + code}.Sanitize()
	tableIdent := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()
	triggerIdent := pgx.Identifier{"audit_retry_trigger_" + code}.Sanitize()

	_, err := fixture.controlPool.Exec(ctx, fmt.Sprintf(`CREATE SEQUENCE %s START WITH 1`, sequenceIdent))
	require.NoError(t, err)
	_, err = fixture.controlPool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $audit$
		BEGIN
			IF nextval('%s'::regclass) = 1 THEN
				RAISE EXCEPTION 'audit injected %s' USING ERRCODE = '%s';
			END IF;
			RETURN NEW;
		END
		$audit$
	`, functionIdent, sequenceIdent, message, code))
	require.NoError(t, err)
	_, err = fixture.controlPool.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s()
	`, triggerIdent, tableIdent, functionIdent))
	require.NoError(t, err)
	return sequenceIdent
}
