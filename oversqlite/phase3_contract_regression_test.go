package oversqlite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type phase3TrackingBody struct {
	io.Reader
	closed bool
}

type phase3RetryableHostileError struct{}

func (phase3RetryableHostileError) Error() string {
	return "private-source-sentinel connection reset by peer"
}

func (phase3RetryableHostileError) Unwrap() error { return syscall.ECONNRESET }

func (b *phase3TrackingBody) Close() error {
	b.closed = true
	return nil
}

func TestPhase3RetryClassificationUsesTypedCausesOnly(t *testing.T) {
	require.False(t, isRetryableOperationError(context.Background(), errors.New("connection reset by peer")))
	require.False(t, isRetryableOperationError(context.Background(), errors.New("broken pipe")))
	require.False(t, isRetryableOperationError(context.Background(), &retryHTTPError{
		Operation:  "phase3",
		StatusCode: http.StatusBadRequest,
	}))

	require.True(t, isRetryableOperationError(context.Background(), syscall.ECONNRESET))
	require.True(t, isRetryableOperationError(context.Background(), syscall.EPIPE))
	require.True(t, isRetryableOperationError(context.Background(), io.ErrUnexpectedEOF))
}

func TestPhase3BufferedControlResponseIsBoundedAndClosed(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.sourceID = "phase3-source"
	body := &phase3TrackingBody{Reader: strings.NewReader(strings.Repeat("x", int(snapshotControlBodyLimit+1)))}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       body,
		}, nil
	})}

	_, err := client.createPushSession(context.Background(), 1, 0, strings.Repeat("a", 64))
	var tooLarge *SnapshotResponseBodyTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, "push_session_create", tooLarge.Operation)
	require.True(t, body.closed)
}

func TestPhase3SourceRetiredPresenceDistinctions(t *testing.T) {
	tests := []struct {
		name string
		body string
		ok   bool
	}{
		{name: "absent", body: `{"error":"source_retired","message":"retired","source_id":"source-a"}`, ok: true},
		{name: "present null", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":null}`},
		{name: "present empty", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":""}`},
		{name: "valid same", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-a"}`, ok: true},
		{name: "valid different", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-b"}`, ok: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, ok := decodeSourceRetiredResponse([]byte(testCase.body), "source-a")
			require.Equal(t, testCase.ok, ok)
		})
	}
}

func TestPhase3SnapshotRetirementMustNameFailedRequestSource(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.sourceID = "requested-source"
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return rawJSONResponse(
			http.StatusConflict,
			`{"error":"source_retired","message":"hostile","source_id":"different-source"}`,
		), nil
	})}

	_, err := client.createSnapshotSession(context.Background(), nil)
	require.EqualError(t, err, "invalid source_retired response")
	require.NotContains(t, err.Error(), "requested-source")
	require.NotContains(t, err.Error(), "different-source")
}

func TestPhase3RemoteErrorMessagesAndBodiesAreNotRetained(t *testing.T) {
	sentinel := "private-remote-sentinel"
	require.Equal(t, "invalid_error_response", decodeServerErrorBody([]byte(sentinel)))
	require.NotContains(t, decodeServerErrorBody([]byte(sentinel)), sentinel)

	body := []byte(`{"error":"history_pruned","message":"private-remote-sentinel"}`)
	require.Equal(t, "history_pruned", decodeServerErrorBody(body))
	decoded, ok := decodeServerErrorResponse(body)
	require.True(t, ok)
	require.Empty(t, decoded.Message)

	retired, ok := decodeSourceRetiredResponse(
		[]byte(`{"error":"source_retired","message":"private-remote-sentinel","source_id":"source-a"}`),
		"source-a",
	)
	require.True(t, ok)
	require.Empty(t, retired.Message)
}

func TestPhase3SnapshotSessionLimitErrorIsStructuredAndRedacted(t *testing.T) {
	const sentinel = "private-snapshot-limit-sentinel"
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return rawJSONResponse(
			http.StatusConflict,
			`{"error":"snapshot_session_limit_exceeded","message":"`+sentinel+`","dimension":"byte_count","actual":257,"limit":256}`,
		), nil
	})}

	_, err := client.createSnapshotSession(context.Background(), nil)
	var limitErr *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "byte_count", limitErr.Dimension)
	require.EqualValues(t, 257, limitErr.Actual)
	require.EqualValues(t, 256, limitErr.Limit)
	require.NotContains(t, err.Error(), sentinel)
	require.Equal(
		t, "snapshot_session_limit_exceeded",
		decodeServerErrorBody([]byte(`{"error":"snapshot_session_limit_exceeded","message":"`+sentinel+`"}`)),
	)
}

func TestPhase3SnapshotSessionLimitErrorRejectsMalformedDetails(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "unknown dimension",
			body: `{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"other","actual":2,"limit":1}`,
		},
		{
			name: "actual does not exceed limit",
			body: `{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"row_count","actual":1,"limit":1}`,
		},
		{
			name: "non-positive limit",
			body: `{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"row_byte_count","actual":1,"limit":0}`,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return rawJSONResponse(http.StatusConflict, testCase.body), nil
			})}

			_, err := client.createSnapshotSession(context.Background(), nil)
			require.EqualError(t, err, "invalid snapshot_session_limit_exceeded response")
			require.NotContains(t, err.Error(), "hostile")
		})
	}
}

func TestPhase3PublicRetryAndRecoveryErrorsDoNotRetainHostileCauses(t *testing.T) {
	const hostile = "private-source-sentinel connection reset by peer"
	policy := normalizedRetryPolicy{enabled: true, maxAttempts: 1}
	_, err := withRetryValue(context.Background(), policy, "phase3", func() (struct{}, error) {
		return struct{}{}, phase3RetryableHostileError{}
	})
	var retryErr *RetryExhaustedError
	require.ErrorAs(t, err, &retryErr)
	require.NotContains(t, retryErr.Error(), hostile)
	require.Nil(t, retryErr.LastErr)
	require.Nil(t, errors.Unwrap(retryErr))

	blocked := &CheckpointRecoveryBlockedError{
		Reason: CheckpointRecoveryBlockedPushFailed,
		Cause:  errors.New(hostile),
	}
	require.NotContains(t, blocked.Error(), hostile)
	require.Nil(t, errors.Unwrap(blocked))
}
