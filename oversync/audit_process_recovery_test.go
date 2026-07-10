//go:build oversync_audit

package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const auditBusinessDMLGateKey int64 = 72340172838076673

type auditProcessHTTPResult struct {
	statusCode int
	body       []byte
	err        error
}

type auditHTTPStagedPush struct {
	userID         string
	sourceID       string
	sourceBundleID int64
	pushID         string
	rowID          uuid.UUID
	createRequest  PushSessionCreateRequest
}

type auditCommittedTupleState struct {
	businessRowCount     int64
	rowStateCount        int64
	bundleCount          int64
	bundleRowCount       int64
	sourceStateCount     int64
	pushSessionCount     int64
	bundleSeq            int64
	sourceWatermark      int64
	bundleHash           string
	distinctBundleHashes int64
}

type auditStagedTupleState struct {
	businessRowCount int64
	rowStateCount    int64
	bundleCount      int64
	sourceStateCount int64
	pushSessionCount int64
	stagedRowCount   int64
}

func newAuditProcessRequest(
	t *testing.T,
	method string,
	requestURL string,
	userID string,
	sourceID string,
	body any,
) *http.Request {
	t.Helper()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, requestURL, reader)
	require.NoError(t, err)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if userID != "" {
		request.Header.Set(auditHelperUserIDHeader, userID)
	}
	if sourceID != "" {
		request.Header.Set(SourceIDHeader, sourceID)
	}
	return request
}

func runAuditProcessRequest(client *http.Client, request *http.Request) auditProcessHTTPResult {
	response, err := client.Do(request)
	if err != nil {
		return auditProcessHTTPResult{err: err}
	}
	defer response.Body.Close()

	body, readErr := io.ReadAll(response.Body)
	return auditProcessHTTPResult{
		statusCode: response.StatusCode,
		body:       body,
		err:        readErr,
	}
}

func doAuditProcessRequest(
	t *testing.T,
	client *http.Client,
	method string,
	requestURL string,
	userID string,
	sourceID string,
	body any,
) auditProcessHTTPResult {
	t.Helper()
	result := runAuditProcessRequest(client, newAuditProcessRequest(t, method, requestURL, userID, sourceID, body))
	require.NoError(t, result.err)
	return result
}

func startAuditProcessRequest(
	t *testing.T,
	client *http.Client,
	method string,
	requestURL string,
	userID string,
	sourceID string,
	body any,
) <-chan auditProcessHTTPResult {
	t.Helper()
	request := newAuditProcessRequest(t, method, requestURL, userID, sourceID, body)
	result := make(chan auditProcessHTTPResult, 1)
	go func() {
		result <- runAuditProcessRequest(client, request)
	}()
	return result
}

func newAuditProcessAssertionPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(context.Background()))
	return pool
}

func initializeAuditProcessScope(
	t *testing.T,
	client *http.Client,
	baseURL string,
	userID string,
	sourceID string,
) string {
	t.Helper()
	result := doAuditProcessRequest(
		t,
		client,
		http.MethodPost,
		baseURL+"/sync/connect",
		userID,
		sourceID,
		ConnectRequest{HasLocalPendingRows: false},
	)
	require.Equal(t, http.StatusOK, result.statusCode, string(result.body))

	var response ConnectResponse
	require.NoError(t, json.Unmarshal(result.body, &response))
	require.Contains(t, []string{"initialize_empty", "remote_authoritative"}, response.Resolution)
	return response.InitializationID
}

func stageAuditHTTPPush(
	t *testing.T,
	client *http.Client,
	baseURL string,
	schemaName string,
	userID string,
	sourceID string,
	sourceBundleID int64,
	rowID uuid.UUID,
) auditHTTPStagedPush {
	t.Helper()
	initializationID := initializeAuditProcessScope(t, client, baseURL, userID, sourceID)
	row := PushRequestRow{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(
			`{"id":%q,"name":"Process Recovery","email":"process-recovery@example.com"}`,
			rowID.String(),
		)),
	}
	createRequest := PushSessionCreateRequest{
		SourceBundleID:       sourceBundleID,
		PlannedRowCount:      1,
		CanonicalRequestHash: mustCanonicalPushRequestHash(t, []PushRequestRow{row}),
		InitializationID:     initializationID,
	}
	createResult := doAuditProcessRequest(
		t,
		client,
		http.MethodPost,
		baseURL+"/sync/push-sessions",
		userID,
		sourceID,
		createRequest,
	)
	require.Equal(t, http.StatusOK, createResult.statusCode, string(createResult.body))

	var createResponse PushSessionCreateResponse
	require.NoError(t, json.Unmarshal(createResult.body, &createResponse))
	require.Equal(t, "staging", createResponse.Status)
	require.NotEmpty(t, createResponse.PushID)

	chunkRequest := PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows:            []PushRequestRow{row},
	}
	chunkResult := doAuditProcessRequest(
		t,
		client,
		http.MethodPost,
		fmt.Sprintf("%s/sync/push-sessions/%s/chunks", baseURL, createResponse.PushID),
		userID,
		sourceID,
		chunkRequest,
	)
	require.Equal(t, http.StatusOK, chunkResult.statusCode, string(chunkResult.body))

	var chunkResponse PushSessionChunkResponse
	require.NoError(t, json.Unmarshal(chunkResult.body, &chunkResponse))
	require.Equal(t, int64(1), chunkResponse.NextExpectedRowOrdinal)

	return auditHTTPStagedPush{
		userID:         userID,
		sourceID:       sourceID,
		sourceBundleID: sourceBundleID,
		pushID:         createResponse.PushID,
		rowID:          rowID,
		createRequest:  createRequest,
	}
}

func auditCommitGateStatus(client *http.Client, baseURL string) (auditCommitResponseGateStatus, error) {
	request, err := http.NewRequest(http.MethodGet, baseURL+auditCommitGateStatusPath, nil)
	if err != nil {
		return auditCommitResponseGateStatus{}, err
	}
	result := runAuditProcessRequest(client, request)
	if result.err != nil {
		return auditCommitResponseGateStatus{}, result.err
	}
	if result.statusCode != http.StatusOK {
		return auditCommitResponseGateStatus{}, fmt.Errorf("commit gate status returned HTTP %d: %s", result.statusCode, result.body)
	}
	var status auditCommitResponseGateStatus
	if err := json.Unmarshal(result.body, &status); err != nil {
		return auditCommitResponseGateStatus{}, err
	}
	return status, nil
}

func postAuditCommitGateControl(t *testing.T, client *http.Client, baseURL string, path string) {
	t.Helper()
	result := doAuditProcessRequest(t, client, http.MethodPost, baseURL+path, "", "", nil)
	require.Equal(t, http.StatusOK, result.statusCode, string(result.body))
}

func tryReleaseAuditCommitGate(client *http.Client, baseURL string) {
	request, err := http.NewRequest(http.MethodPost, baseURL+auditCommitGateReleasePath, nil)
	if err != nil {
		return
	}
	response, err := client.Do(request)
	if err == nil {
		_ = response.Body.Close()
	}
}

func loadAuditCommittedTupleState(
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
	push auditHTTPStagedPush,
) (auditCommittedTupleState, error) {
	var state auditCommittedTupleState
	userPK, err := lookupUserPK(ctx, pool, push.userID)
	if err != nil {
		return state, err
	}

	businessTable := pgx.Identifier{schemaName, "users"}.Sanitize()
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s
		WHERE _sync_scope_id = $1
		  AND id = $2::uuid
	`, businessTable), push.userID, push.rowID).Scan(&state.businessRowCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.row_state AS row
		JOIN sync.table_catalog AS catalog ON catalog.table_id = row.table_id
		WHERE row.user_pk = $1
		  AND catalog.schema_name = $2
		  AND catalog.table_name = 'users'
		  AND row.deleted = FALSE
	`, userPK, schemaName).Scan(&state.rowStateCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*),
			COALESCE(MIN(bundle_seq), 0),
			COALESCE(MIN(encode(bundle_hash, 'hex')), ''),
			COUNT(DISTINCT encode(bundle_hash, 'hex'))
		FROM sync.bundle_log
		WHERE user_pk = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
	`, userPK, push.sourceID, push.sourceBundleID).Scan(
		&state.bundleCount,
		&state.bundleSeq,
		&state.bundleHash,
		&state.distinctBundleHashes,
	); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_rows AS row
		JOIN sync.bundle_log AS bundle
		  ON bundle.user_pk = row.user_pk
		 AND bundle.bundle_seq = row.bundle_seq
		WHERE bundle.user_pk = $1
		  AND bundle.source_id = $2
		  AND bundle.source_bundle_id = $3
	`, userPK, push.sourceID, push.sourceBundleID).Scan(&state.bundleRowCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(MAX(max_committed_source_bundle_id), 0)
		FROM sync.source_state
		WHERE user_pk = $1
		  AND source_id = $2
	`, userPK, push.sourceID).Scan(&state.sourceStateCount, &state.sourceWatermark); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_sessions
		WHERE user_pk = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
	`, userPK, push.sourceID, push.sourceBundleID).Scan(&state.pushSessionCount); err != nil {
		return state, err
	}
	return state, nil
}

func requireAuditCommittedTupleState(t *testing.T, state auditCommittedTupleState, sourceBundleID int64) {
	t.Helper()
	require.Equal(t, int64(1), state.businessRowCount)
	require.Equal(t, int64(1), state.rowStateCount)
	require.Equal(t, int64(1), state.bundleCount)
	require.Equal(t, int64(1), state.bundleRowCount)
	require.Equal(t, int64(1), state.sourceStateCount)
	require.Equal(t, int64(0), state.pushSessionCount)
	require.Equal(t, sourceBundleID, state.sourceWatermark)
	require.Positive(t, state.bundleSeq)
	require.NotEmpty(t, state.bundleHash)
	require.Equal(t, int64(1), state.distinctBundleHashes)
}

func loadAuditStagedTupleState(
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
	push auditHTTPStagedPush,
) (auditStagedTupleState, error) {
	var state auditStagedTupleState
	userPK, err := lookupUserPK(ctx, pool, push.userID)
	if err != nil {
		return state, err
	}

	businessTable := pgx.Identifier{schemaName, "users"}.Sanitize()
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s
		WHERE _sync_scope_id = $1
		  AND id = $2::uuid
	`, businessTable), push.userID, push.rowID).Scan(&state.businessRowCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.row_state AS row
		JOIN sync.table_catalog AS catalog ON catalog.table_id = row.table_id
		WHERE row.user_pk = $1
		  AND catalog.schema_name = $2
		  AND catalog.table_name = 'users'
	`, userPK, schemaName).Scan(&state.rowStateCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_pk = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
	`, userPK, push.sourceID, push.sourceBundleID).Scan(&state.bundleCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.source_state
		WHERE user_pk = $1
		  AND source_id = $2
	`, userPK, push.sourceID).Scan(&state.sourceStateCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_sessions
		WHERE user_pk = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
	`, userPK, push.sourceID, push.sourceBundleID).Scan(&state.pushSessionCount); err != nil {
		return state, err
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.push_session_rows
		WHERE push_id = $1::uuid
	`, push.pushID).Scan(&state.stagedRowCount); err != nil {
		return state, err
	}
	return state, nil
}

func requireAuditStagedTupleState(t *testing.T, state auditStagedTupleState) {
	t.Helper()
	require.Zero(t, state.businessRowCount)
	require.Zero(t, state.rowStateCount)
	require.Zero(t, state.bundleCount)
	require.Zero(t, state.sourceStateCount)
	require.Equal(t, int64(1), state.pushSessionCount)
	require.Equal(t, int64(1), state.stagedRowCount)
}

func commitAuditHTTPPush(
	t *testing.T,
	client *http.Client,
	baseURL string,
	push auditHTTPStagedPush,
) PushSessionCommitResponse {
	t.Helper()
	result := doAuditProcessRequest(
		t,
		client,
		http.MethodPost,
		fmt.Sprintf("%s/sync/push-sessions/%s/commit", baseURL, push.pushID),
		push.userID,
		push.sourceID,
		nil,
	)
	require.Equal(t, http.StatusOK, result.statusCode, string(result.body))
	var response PushSessionCommitResponse
	require.NoError(t, json.Unmarshal(result.body, &response))
	return response
}

func TestAuditProcessRecovery_AmbiguousCommitResolvesToExactlyOneCommittedTuple(t *testing.T) {
	ctx := context.Background()
	databaseURL, schemaName := newAuditHelperDatabase(t)
	pool := newAuditProcessAssertionPool(t, databaseURL)
	process := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	proxy, err := newAuditTCPProxy("127.0.0.1:0", process.address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	directClient := newAuditHTTPClient()
	t.Cleanup(directClient.CloseIdleConnections)
	proxyClient := newAuditHTTPClient()
	t.Cleanup(proxyClient.CloseIdleConnections)

	push := stageAuditHTTPPush(
		t,
		directClient,
		process.URL(),
		schemaName,
		"audit-ambiguous-user",
		"audit-ambiguous-source",
		1,
		uuid.New(),
	)
	postAuditCommitGateControl(t, directClient, process.URL(), auditCommitGateArmPath)
	gateReleased := false
	defer func() {
		if !gateReleased {
			tryReleaseAuditCommitGate(directClient, process.URL())
		}
	}()

	commitResult := startAuditProcessRequest(
		t,
		proxyClient,
		http.MethodPost,
		fmt.Sprintf("http://%s/sync/push-sessions/%s/commit", proxy.Address(), push.pushID),
		push.userID,
		push.sourceID,
		nil,
	)
	require.Eventually(t, func() bool {
		status, statusErr := auditCommitGateStatus(directClient, process.URL())
		return statusErr == nil && status.Waiting
	}, 5*time.Second, 10*time.Millisecond, "commit handler did not reach the post-commit response gate")
	require.Eventually(t, func() bool {
		state, stateErr := loadAuditCommittedTupleState(ctx, pool, schemaName, push)
		return stateErr == nil && state.bundleCount == 1 && state.businessRowCount == 1 && state.sourceWatermark == 1
	}, 5*time.Second, 10*time.Millisecond, "committed source tuple did not become visible before response interruption")
	require.Eventually(t, func() bool {
		return proxy.ActiveConnections() == 1
	}, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, 1, proxy.Interrupt())
	select {
	case result := <-commitResult:
		require.Error(t, result.err, "interrupted commit response must be ambiguous to the client")
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted commit client did not return")
	}

	committedBeforeRecovery, err := loadAuditCommittedTupleState(ctx, pool, schemaName, push)
	require.NoError(t, err)
	requireAuditCommittedTupleState(t, committedBeforeRecovery, push.sourceBundleID)
	requireAuditMetadataIntegrity(t, ctx, pool)

	postAuditCommitGateControl(t, directClient, process.URL(), auditCommitGateReleasePath)
	gateReleased = true
	require.Eventually(t, func() bool {
		status, statusErr := auditCommitGateStatus(directClient, process.URL())
		return statusErr == nil && !status.Armed && !status.Waiting
	}, 5*time.Second, 10*time.Millisecond)

	recoveryResult := doAuditProcessRequest(
		t,
		directClient,
		http.MethodPost,
		process.URL()+"/sync/push-sessions",
		push.userID,
		push.sourceID,
		push.createRequest,
	)
	require.Equal(t, http.StatusOK, recoveryResult.statusCode, string(recoveryResult.body))
	var recovery PushSessionCreateResponse
	require.NoError(t, json.Unmarshal(recoveryResult.body, &recovery))
	require.Equal(t, "already_committed", recovery.Status)
	require.Equal(t, committedBeforeRecovery.bundleSeq, recovery.BundleSeq)
	require.Equal(t, committedBeforeRecovery.bundleHash, recovery.BundleHash)
	require.Equal(t, push.sourceID, recovery.SourceID)
	require.Equal(t, push.sourceBundleID, recovery.SourceBundleID)
	require.Equal(t, int64(1), recovery.RowCount)

	committedAfterRecovery, err := loadAuditCommittedTupleState(ctx, pool, schemaName, push)
	require.NoError(t, err)
	require.Equal(t, committedBeforeRecovery, committedAfterRecovery)
	requireAuditCommittedTupleState(t, committedAfterRecovery, push.sourceBundleID)
	requireAuditMetadataIntegrity(t, ctx, pool)
}

func TestAuditProcessRecovery_StagedSessionSurvivesHelperKillAndRestart(t *testing.T) {
	ctx := context.Background()
	databaseURL, schemaName := newAuditHelperDatabase(t)
	pool := newAuditProcessAssertionPool(t, databaseURL)
	firstProcess := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	client := newAuditHTTPClient()
	t.Cleanup(client.CloseIdleConnections)

	push := stageAuditHTTPPush(
		t,
		client,
		firstProcess.URL(),
		schemaName,
		"audit-staged-restart-user",
		"audit-staged-restart-source",
		1,
		uuid.New(),
	)
	stagedBeforeKill, err := loadAuditStagedTupleState(ctx, pool, schemaName, push)
	require.NoError(t, err)
	requireAuditStagedTupleState(t, stagedBeforeKill)
	requireAuditMetadataIntegrity(t, ctx, pool)

	require.NoError(t, firstProcess.Kill())
	stagedAfterKill, err := loadAuditStagedTupleState(ctx, pool, schemaName, push)
	require.NoError(t, err)
	require.Equal(t, stagedBeforeKill, stagedAfterKill)
	requireAuditStagedTupleState(t, stagedAfterKill)
	requireAuditMetadataIntegrity(t, ctx, pool)

	secondProcess := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	commit := commitAuditHTTPPush(t, client, secondProcess.URL(), push)
	require.Equal(t, push.sourceBundleID, commit.SourceBundleID)
	require.NotEmpty(t, commit.BundleHash)

	committed, err := loadAuditCommittedTupleState(ctx, pool, schemaName, push)
	require.NoError(t, err)
	requireAuditCommittedTupleState(t, committed, push.sourceBundleID)
	require.Equal(t, committed.bundleSeq, commit.BundleSeq)
	require.Equal(t, committed.bundleHash, commit.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, pool)
}

func installAuditBusinessDMLGate(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
) func() {
	t.Helper()
	functionIdent := pgx.Identifier{schemaName, "audit_block_users_insert"}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d::bigint);
			RETURN NEW;
		END;
		$$;
		DROP TRIGGER IF EXISTS audit_block_users_insert ON %s;
		CREATE TRIGGER audit_block_users_insert
		BEFORE INSERT ON %s
		FOR EACH ROW
		EXECUTE FUNCTION %s();
	`, functionIdent, auditBusinessDMLGateKey, tableIdent, tableIdent, functionIdent))
	require.NoError(t, err)

	gateConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = gateConn.Exec(ctx, `SELECT pg_advisory_lock($1::bigint)`, auditBusinessDMLGateKey)
	require.NoError(t, err)
	gateHeld := true
	release := func() {
		if !gateHeld {
			return
		}
		var unlocked bool
		require.NoError(t, gateConn.QueryRow(context.Background(), `SELECT pg_advisory_unlock($1::bigint)`, auditBusinessDMLGateKey).Scan(&unlocked))
		require.True(t, unlocked)
		gateHeld = false
		gateConn.Release()
	}
	t.Cleanup(func() {
		if !gateHeld {
			return
		}
		_, _ = gateConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1::bigint)`, auditBusinessDMLGateKey)
		gateConn.Release()
	})
	return release
}

func auditBusinessDMLGateHasWaiter(ctx context.Context, pool *pgxpool.Pool) bool {
	var waiting bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_locks
			WHERE locktype = 'advisory'
			  AND NOT granted
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			  AND classid::bigint = ($1::bigint >> 32)
			  AND objid::bigint = ($1::bigint & 4294967295::bigint)
			  AND objsubid = 1
		)
	`, auditBusinessDMLGateKey).Scan(&waiting)
	return err == nil && waiting
}

func TestAuditProcessRecovery_HelperKillDuringBusinessDMLRollsBackAndRetryCommits(t *testing.T) {
	ctx := context.Background()
	databaseURL, schemaName := newAuditHelperDatabase(t)
	pool := newAuditProcessAssertionPool(t, databaseURL)
	firstProcess := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	client := newAuditHTTPClient()
	t.Cleanup(client.CloseIdleConnections)

	push := stageAuditHTTPPush(
		t,
		client,
		firstProcess.URL(),
		schemaName,
		"audit-dml-kill-user",
		"audit-dml-kill-source",
		1,
		uuid.New(),
	)
	releaseGate := installAuditBusinessDMLGate(t, ctx, pool, schemaName)
	commitResult := startAuditProcessRequest(
		t,
		client,
		http.MethodPost,
		fmt.Sprintf("%s/sync/push-sessions/%s/commit", firstProcess.URL(), push.pushID),
		push.userID,
		push.sourceID,
		nil,
	)
	require.Eventually(t, func() bool {
		return auditBusinessDMLGateHasWaiter(ctx, pool)
	}, 5*time.Second, 10*time.Millisecond, "commit did not block inside the business INSERT trigger")

	require.NoError(t, firstProcess.Kill())
	select {
	case result := <-commitResult:
		require.Error(t, result.err, "killed in-flight helper must not return a successful commit response")
	case <-time.After(5 * time.Second):
		t.Fatal("client did not observe killed in-flight helper")
	}
	releaseGate()

	stagedAfterKill, err := loadAuditStagedTupleState(ctx, pool, schemaName, push)
	require.NoError(t, err)
	requireAuditStagedTupleState(t, stagedAfterKill)
	requireAuditMetadataIntegrity(t, ctx, pool)

	secondProcess := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	commit := commitAuditHTTPPush(t, client, secondProcess.URL(), push)
	require.Equal(t, push.sourceBundleID, commit.SourceBundleID)
	require.NotEmpty(t, commit.BundleHash)

	committed, err := loadAuditCommittedTupleState(ctx, pool, schemaName, push)
	require.NoError(t, err)
	requireAuditCommittedTupleState(t, committed, push.sourceBundleID)
	require.Equal(t, committed.bundleSeq, commit.BundleSeq)
	require.Equal(t, committed.bundleHash, commit.BundleHash)
	requireAuditMetadataIntegrity(t, ctx, pool)
}
