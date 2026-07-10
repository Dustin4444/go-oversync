//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type auditHTTPRecoveryFixture struct {
	assertionPool *pgxpool.Pool
	process       *auditHTTPHelperProcess
	proxy         *auditTCPProxy
	directClient  *http.Client
	proxyClient   *http.Client
	schemaName    string
}

func newAuditHTTPRecoveryFixture(t *testing.T) *auditHTTPRecoveryFixture {
	t.Helper()
	databaseURL, schemaName := newAuditHelperDatabase(t)
	process := startAuditHTTPHelperProcess(t, databaseURL, schemaName)
	proxy, err := newAuditTCPProxy("127.0.0.1:0", process.address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })

	directClient := newAuditHTTPClient()
	proxyClient := newAuditHTTPClient()
	t.Cleanup(directClient.CloseIdleConnections)
	t.Cleanup(proxyClient.CloseIdleConnections)
	assertionPool := newAuditProcessAssertionPool(t, databaseURL)

	return &auditHTTPRecoveryFixture{
		assertionPool: assertionPool,
		process:       process,
		proxy:         proxy,
		directClient:  directClient,
		proxyClient:   proxyClient,
		schemaName:    schemaName,
	}
}

func auditResponseGateStatus(client *http.Client, baseURL string) (auditCommitResponseGateStatus, error) {
	request, err := http.NewRequest(http.MethodGet, baseURL+auditResponseGateStatusPath, nil)
	if err != nil {
		return auditCommitResponseGateStatus{}, err
	}
	result := runAuditProcessRequest(client, request)
	if result.err != nil {
		return auditCommitResponseGateStatus{}, result.err
	}
	if result.statusCode != http.StatusOK {
		return auditCommitResponseGateStatus{}, fmt.Errorf("response gate status returned HTTP %d: %s", result.statusCode, result.body)
	}
	var status auditCommitResponseGateStatus
	if err := json.Unmarshal(result.body, &status); err != nil {
		return auditCommitResponseGateStatus{}, err
	}
	return status, nil
}

func interruptAuditHTTPResponseAfterEffect(
	t *testing.T,
	fixture *auditHTTPRecoveryFixture,
	operation string,
	method string,
	requestURL string,
	userID string,
	sourceID string,
	body any,
	effectVisible func() bool,
) {
	t.Helper()

	armURL := fmt.Sprintf("%s%s?operation=%s", fixture.process.URL(), auditResponseGateArmPath, operation)
	arm := doAuditProcessRequest(t, fixture.directClient, http.MethodPost, armURL, "", "", nil)
	require.Equal(t, http.StatusOK, arm.statusCode, string(arm.body))
	released := false
	defer func() {
		if released {
			return
		}
		request, _ := http.NewRequest(http.MethodPost, fixture.process.URL()+auditResponseGateReleasePath, nil)
		if request != nil {
			response, err := fixture.directClient.Do(request)
			if err == nil {
				_ = response.Body.Close()
			}
		}
	}()

	result := startAuditProcessRequest(
		t,
		fixture.proxyClient,
		method,
		requestURL,
		userID,
		sourceID,
		body,
	)
	require.Eventually(t, func() bool {
		status, err := auditResponseGateStatus(fixture.directClient, fixture.process.URL())
		return err == nil && status.Waiting && status.Operation == operation
	}, 5*time.Second, 10*time.Millisecond, "operation %s did not reach its post-handler response gate", operation)
	require.Eventually(t, effectVisible, 5*time.Second, 10*time.Millisecond, "operation %s effect did not become visible", operation)
	require.Eventually(t, func() bool {
		return fixture.proxy.ActiveConnections() == 1
	}, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, 1, fixture.proxy.Interrupt())
	select {
	case interrupted := <-result:
		require.Error(t, interrupted.err, "operation %s must have an ambiguous client response", operation)
	case <-time.After(5 * time.Second):
		t.Fatalf("operation %s client did not observe the interrupted response", operation)
	}

	release := doAuditProcessRequest(
		t,
		fixture.directClient,
		http.MethodPost,
		fixture.process.URL()+auditResponseGateReleasePath,
		"",
		"",
		nil,
	)
	require.Equal(t, http.StatusOK, release.statusCode, string(release.body))
	released = true
	require.Eventually(t, func() bool {
		status, err := auditResponseGateStatus(fixture.directClient, fixture.process.URL())
		return err == nil && !status.Armed && !status.Waiting && status.Operation == ""
	}, 5*time.Second, 10*time.Millisecond)
}

func TestAuditHTTPRecovery_PushCreateChunkDeleteAndPullAfterConnectionLoss(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditHTTPRecoveryFixture(t)
	userID := "audit-http-retry-user"
	sourceID := "audit-http-retry-source"
	initializeAuditProcessScope(t, fixture.directClient, fixture.process.URL(), userID, sourceID)
	userPK, err := lookupUserPK(ctx, fixture.assertionPool, userID)
	require.NoError(t, err)

	rowID := uuid.New()
	chunkRequest := PushSessionChunkRequest{
		StartRowOrdinal: 0,
		Rows: []PushRequestRow{{
			Schema:         fixture.schemaName,
			Table:          "users",
			Key:            SyncKey{"id": rowID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"HTTP Retry","email":"http-retry@example.com"}`, rowID.String())),
		}},
	}
	createRequest := PushSessionCreateRequest{
		SourceBundleID:       1,
		PlannedRowCount:      1,
		CanonicalRequestHash: mustCanonicalPushRequestHash(t, chunkRequest.Rows),
	}
	interruptAuditHTTPResponseAfterEffect(
		t,
		fixture,
		auditResponseGatePushCreate,
		http.MethodPost,
		"http://"+fixture.proxy.Address()+"/sync/push-sessions",
		userID,
		sourceID,
		createRequest,
		func() bool {
			var count int64
			return fixture.assertionPool.QueryRow(ctx, `
				SELECT COUNT(*) FROM sync.push_sessions
				WHERE user_pk = $1 AND source_id = $2 AND source_bundle_id = 1
			`, userPK, sourceID).Scan(&count) == nil && count == 1
		},
	)

	var lostCreatePushID string
	require.NoError(t, fixture.assertionPool.QueryRow(ctx, `
		SELECT push_id::text FROM sync.push_sessions
		WHERE user_pk = $1 AND source_id = $2 AND source_bundle_id = 1
	`, userPK, sourceID).Scan(&lostCreatePushID))
	retryCreate := doAuditProcessRequest(
		t,
		fixture.directClient,
		http.MethodPost,
		fixture.process.URL()+"/sync/push-sessions",
		userID,
		sourceID,
		createRequest,
	)
	require.Equal(t, http.StatusOK, retryCreate.statusCode, string(retryCreate.body))
	var createResponse PushSessionCreateResponse
	require.NoError(t, json.Unmarshal(retryCreate.body, &createResponse))
	require.Equal(t, "staging", createResponse.Status)
	require.NotEqual(t, lostCreatePushID, createResponse.PushID)
	var pushSessionCount, stagedRowCount int64
	require.NoError(t, fixture.assertionPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM sync.push_sessions
		WHERE user_pk = $1 AND source_id = $2 AND source_bundle_id = 1
	`, userPK, sourceID).Scan(&pushSessionCount))
	require.Equal(t, int64(1), pushSessionCount)
	require.NoError(t, fixture.assertionPool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.push_session_rows`).Scan(&stagedRowCount))
	require.Zero(t, stagedRowCount)
	requireAuditMetadataIntegrity(t, ctx, fixture.assertionPool)

	chunkURL := fmt.Sprintf("/sync/push-sessions/%s/chunks", createResponse.PushID)
	interruptAuditHTTPResponseAfterEffect(
		t,
		fixture,
		auditResponseGatePushChunk,
		http.MethodPost,
		"http://"+fixture.proxy.Address()+chunkURL,
		userID,
		sourceID,
		chunkRequest,
		func() bool {
			var cursor, count int64
			err := fixture.assertionPool.QueryRow(ctx, `
				SELECT session.next_expected_row_ordinal, COUNT(row.row_ordinal)
				FROM sync.push_sessions AS session
				LEFT JOIN sync.push_session_rows AS row ON row.push_id = session.push_id
				WHERE session.push_id = $1::uuid
				GROUP BY session.next_expected_row_ordinal
			`, createResponse.PushID).Scan(&cursor, &count)
			return err == nil && cursor == 1 && count == 1
		},
	)
	retryChunk := doAuditProcessRequest(
		t,
		fixture.directClient,
		http.MethodPost,
		fixture.process.URL()+chunkURL,
		userID,
		sourceID,
		chunkRequest,
	)
	require.Equal(t, http.StatusConflict, retryChunk.statusCode, string(retryChunk.body))
	var chunkError ErrorResponse
	require.NoError(t, json.Unmarshal(retryChunk.body, &chunkError))
	require.Equal(t, "push_chunk_out_of_order", chunkError.Error)
	requireAuditMetadataIntegrity(t, ctx, fixture.assertionPool)

	commitResult := doAuditProcessRequest(
		t,
		fixture.directClient,
		http.MethodPost,
		fmt.Sprintf("%s/sync/push-sessions/%s/commit", fixture.process.URL(), createResponse.PushID),
		userID,
		sourceID,
		nil,
	)
	require.Equal(t, http.StatusOK, commitResult.statusCode, string(commitResult.body))
	var commit PushSessionCommitResponse
	require.NoError(t, json.Unmarshal(commitResult.body, &commit))

	secondCreate := doAuditProcessRequest(
		t,
		fixture.directClient,
		http.MethodPost,
		fixture.process.URL()+"/sync/push-sessions",
		userID,
		sourceID,
		PushSessionCreateRequest{SourceBundleID: 2, PlannedRowCount: 1, CanonicalRequestHash: strings.Repeat("0", 64)},
	)
	require.Equal(t, http.StatusOK, secondCreate.statusCode, string(secondCreate.body))
	var secondSession PushSessionCreateResponse
	require.NoError(t, json.Unmarshal(secondCreate.body, &secondSession))
	deletePath := fmt.Sprintf("/sync/push-sessions/%s", secondSession.PushID)
	interruptAuditHTTPResponseAfterEffect(
		t,
		fixture,
		auditResponseGatePushDelete,
		http.MethodDelete,
		"http://"+fixture.proxy.Address()+deletePath,
		userID,
		sourceID,
		nil,
		func() bool {
			var count int64
			return fixture.assertionPool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.push_sessions WHERE push_id = $1::uuid`, secondSession.PushID).Scan(&count) == nil && count == 0
		},
	)
	retryDelete := doAuditProcessRequest(t, fixture.directClient, http.MethodDelete, fixture.process.URL()+deletePath, userID, sourceID, nil)
	require.Equal(t, http.StatusNotFound, retryDelete.statusCode, string(retryDelete.body))
	requireAuditMetadataIntegrity(t, ctx, fixture.assertionPool)

	pullPath := fmt.Sprintf("/sync/pull?after_bundle_seq=0&max_bundles=10&target_bundle_seq=%d", commit.BundleSeq)
	controlPull := doAuditProcessRequest(t, fixture.directClient, http.MethodGet, fixture.process.URL()+pullPath, userID, sourceID, nil)
	require.Equal(t, http.StatusOK, controlPull.statusCode, string(controlPull.body))
	interruptAuditHTTPResponseAfterEffect(
		t,
		fixture,
		auditResponseGatePull,
		http.MethodGet,
		"http://"+fixture.proxy.Address()+pullPath,
		userID,
		sourceID,
		nil,
		func() bool { return true },
	)
	retryPull := doAuditProcessRequest(t, fixture.directClient, http.MethodGet, fixture.process.URL()+pullPath, userID, sourceID, nil)
	require.Equal(t, http.StatusOK, retryPull.statusCode, string(retryPull.body))
	require.JSONEq(t, string(controlPull.body), string(retryPull.body))
	requireAuditMetadataIntegrity(t, ctx, fixture.assertionPool)
}

func TestAuditHTTPRecovery_SnapshotCreateReadAndDeleteAfterConnectionLoss(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditHTTPRecoveryFixture(t)
	userID := "audit-http-snapshot-retry-user"
	sourceID := "audit-http-snapshot-retry-source"
	push := stageAuditHTTPPush(t, fixture.directClient, fixture.process.URL(), fixture.schemaName, userID, sourceID, 1, uuid.New())
	commitAuditHTTPPush(t, fixture.directClient, fixture.process.URL(), push)
	userPK, err := lookupUserPK(ctx, fixture.assertionPool, userID)
	require.NoError(t, err)

	snapshotCreatePath := "/sync/snapshot-sessions"
	interruptAuditHTTPResponseAfterEffect(
		t,
		fixture,
		auditResponseGateSnapshotCreate,
		http.MethodPost,
		"http://"+fixture.proxy.Address()+snapshotCreatePath,
		userID,
		sourceID,
		SnapshotSessionCreateRequest{},
		func() bool {
			var sessions, rows int64
			err := fixture.assertionPool.QueryRow(ctx, `
				SELECT COUNT(*), COALESCE(SUM(snapshot.row_count), 0)
				FROM sync.snapshot_sessions AS snapshot
				WHERE snapshot.user_pk = $1
			`, userPK).Scan(&sessions, &rows)
			return err == nil && sessions == 1 && rows == 1
		},
	)

	var lostSnapshotID string
	require.NoError(t, fixture.assertionPool.QueryRow(ctx, `
		SELECT snapshot_id::text FROM sync.snapshot_sessions WHERE user_pk = $1
	`, userPK).Scan(&lostSnapshotID))
	retryCreate := doAuditProcessRequest(
		t,
		fixture.directClient,
		http.MethodPost,
		fixture.process.URL()+snapshotCreatePath,
		userID,
		sourceID,
		SnapshotSessionCreateRequest{},
	)
	require.Equal(t, http.StatusOK, retryCreate.statusCode, string(retryCreate.body))
	var retrySnapshot SnapshotSession
	require.NoError(t, json.Unmarshal(retryCreate.body, &retrySnapshot))
	require.NotEqual(t, lostSnapshotID, retrySnapshot.SnapshotID)

	var snapshotIDs []string
	rows, err := fixture.assertionPool.Query(ctx, `
		SELECT snapshot_id::text FROM sync.snapshot_sessions
		WHERE user_pk = $1 ORDER BY snapshot_id
	`, userPK)
	require.NoError(t, err)
	for rows.Next() {
		var snapshotID string
		require.NoError(t, rows.Scan(&snapshotID))
		snapshotIDs = append(snapshotIDs, snapshotID)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	sort.Strings(snapshotIDs)
	require.ElementsMatch(t, []string{lostSnapshotID, retrySnapshot.SnapshotID}, snapshotIDs)
	t.Logf("OS-AUD-011 ambiguous snapshot-create response left two coherent TTL-bounded sessions: %v", snapshotIDs)
	requireAuditMetadataIntegrity(t, ctx, fixture.assertionPool)

	getPath := fmt.Sprintf("/sync/snapshot-sessions/%s?max_rows=10", lostSnapshotID)
	controlGet := doAuditProcessRequest(t, fixture.directClient, http.MethodGet, fixture.process.URL()+getPath, userID, sourceID, nil)
	require.Equal(t, http.StatusOK, controlGet.statusCode, string(controlGet.body))
	interruptAuditHTTPResponseAfterEffect(
		t,
		fixture,
		auditResponseGateSnapshotGet,
		http.MethodGet,
		"http://"+fixture.proxy.Address()+getPath,
		userID,
		sourceID,
		nil,
		func() bool { return true },
	)
	retryGet := doAuditProcessRequest(t, fixture.directClient, http.MethodGet, fixture.process.URL()+getPath, userID, sourceID, nil)
	require.Equal(t, http.StatusOK, retryGet.statusCode, string(retryGet.body))
	require.JSONEq(t, string(controlGet.body), string(retryGet.body))

	deletePath := "/sync/snapshot-sessions/" + lostSnapshotID
	interruptAuditHTTPResponseAfterEffect(
		t,
		fixture,
		auditResponseGateSnapshotDelete,
		http.MethodDelete,
		"http://"+fixture.proxy.Address()+deletePath,
		userID,
		sourceID,
		nil,
		func() bool {
			var count int64
			return fixture.assertionPool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.snapshot_sessions WHERE snapshot_id = $1::uuid`, lostSnapshotID).Scan(&count) == nil && count == 0
		},
	)
	retryDelete := doAuditProcessRequest(t, fixture.directClient, http.MethodDelete, fixture.process.URL()+deletePath, userID, sourceID, nil)
	require.Equal(t, http.StatusNotFound, retryDelete.statusCode, string(retryDelete.body))

	cleanupRetrySnapshot := doAuditProcessRequest(
		t,
		fixture.directClient,
		http.MethodDelete,
		fixture.process.URL()+"/sync/snapshot-sessions/"+retrySnapshot.SnapshotID,
		userID,
		sourceID,
		nil,
	)
	require.Equal(t, http.StatusNoContent, cleanupRetrySnapshot.statusCode, string(cleanupRetrySnapshot.body))
	requireAuditMetadataIntegrity(t, ctx, fixture.assertionPool)
}
