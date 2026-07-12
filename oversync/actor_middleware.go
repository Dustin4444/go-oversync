// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

const SourceIDHeader = "Oversync-Source-ID"

// ActorMiddlewareConfig configures the standard HTTP actor middleware.
type ActorMiddlewareConfig struct {
	UserIDFromContext func(context.Context) (string, error)
}

// ActorMiddleware injects the runtime actor from trusted auth context plus sync source header.
func ActorMiddleware(cfg ActorMiddlewareConfig) func(http.Handler) http.Handler {
	if cfg.UserIDFromContext == nil {
		panic("oversync.ActorMiddleware requires UserIDFromContext")
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID, err := cfg.UserIDFromContext(r.Context())
			if err != nil || strings.TrimSpace(userID) == "" {
				writeMiddlewareError(w, http.StatusUnauthorized, "authentication_failed", "authenticated user_id not found in request context")
				return
			}

			sourceID := r.Header.Get(SourceIDHeader)
			if err := sourceid.Validate(sourceID); err != nil {
				writeMiddlewareError(w, http.StatusBadRequest, "invalid_request", SourceIDHeader+" header must be a non-empty visible ASCII token")
				return
			}

			actor := Actor{
				UserID:   strings.TrimSpace(userID),
				SourceID: sourceID,
			}
			next.ServeHTTP(w, r.WithContext(ContextWithActor(r.Context(), actor)))
		})
	}
}

func writeMiddlewareError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Error:   code,
		Message: message,
	})
}
