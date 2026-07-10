//go:build oversync_audit

package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func newAuditCreatePushSessionHTTPFixture(t *testing.T) (*pushSessionFixture, string) {
	t.Helper()
	ctx := context.Background()
	fixture := newPushSessionFixture(t, ctx, pushSessionFixtureOptions{})
	initializationID := resolveConnectForPushSession(t, ctx, fixture.svc, fixture.writer, true)
	require.NotEmpty(t, initializationID)
	return fixture, initializationID
}

func runAuditCreatePushSessionRequest(
	t *testing.T,
	fixture *pushSessionFixture,
	body string,
	contentType string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(http.MethodPost, "/sync/push-sessions", strings.NewReader(body))
	request = request.WithContext(ContextWithActor(request.Context(), fixture.writer))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	recorder := httptest.NewRecorder()
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	handlers.HandleCreatePushSession(recorder, request)
	return recorder
}

func TestAuditHTTPCreatePushSession_RejectsUnknownFields(t *testing.T) {
	fixture, initializationID := newAuditCreatePushSessionHTTPFixture(t)
	body := fmt.Sprintf(
		`{"source_bundle_id":1,"planned_row_count":1,"initialization_id":%q,"unknown":true}`,
		initializationID,
	)

	recorder := runAuditCreatePushSessionRequest(t, fixture, body, "application/json")
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestAuditHTTPCreatePushSession_RejectsTrailingJSONDocument(t *testing.T) {
	fixture, initializationID := newAuditCreatePushSessionHTTPFixture(t)
	body := fmt.Sprintf(
		`{"source_bundle_id":1,"planned_row_count":1,"initialization_id":%q} {"extra":true}`,
		initializationID,
	)

	recorder := runAuditCreatePushSessionRequest(t, fixture, body, "application/json")
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestAuditHTTPCreatePushSession_RejectsUnsupportedContentType(t *testing.T) {
	fixture, initializationID := newAuditCreatePushSessionHTTPFixture(t)
	body := fmt.Sprintf(
		`{"source_bundle_id":1,"planned_row_count":1,"initialization_id":%q}`,
		initializationID,
	)

	recorder := runAuditCreatePushSessionRequest(t, fixture, body, "text/plain")
	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code)
}
