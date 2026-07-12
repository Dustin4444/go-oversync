// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

type actorContextKey struct{}

// Actor represents the authenticated runtime identity for sync operations.
type Actor struct {
	UserID   string `json:"user_id"`
	SourceID string `json:"source_id,omitempty"`
}

func (a Actor) validate(requireSource bool) error {
	if a.UserID == "" {
		return fmt.Errorf("actor user_id is required")
	}
	if requireSource {
		if err := sourceid.Validate(a.SourceID); err != nil {
			return fmt.Errorf("actor source_id is invalid: %w", err)
		}
	} else if err := sourceid.ValidateOptional(a.SourceID); err != nil {
		return fmt.Errorf("actor source_id is invalid: %w", err)
	}
	return nil
}

// ContextWithActor stores the runtime actor in context for transport/runtime handoff.
func ContextWithActor(ctx context.Context, actor Actor) context.Context {
	return context.WithValue(ctx, actorContextKey{}, actor)
}

// ActorFromContext retrieves the runtime actor from context.
func ActorFromContext(ctx context.Context) (Actor, bool) {
	actor, ok := ctx.Value(actorContextKey{}).(Actor)
	return actor, ok
}
