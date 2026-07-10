// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var errHistoryPruned = errors.New("requested checkpoint is older than retained history")

type HistoryPrunedError struct {
	UserID        string
	Field         string
	ProvidedSeq   int64
	RetainedFloor int64
}

func (e *HistoryPrunedError) Error() string {
	field := e.Field
	if field == "" {
		field = "checkpoint"
	}
	return fmt.Sprintf(
		"requested %s %d is older than retained history floor %d for user %s",
		field,
		e.ProvidedSeq,
		e.RetainedFloor,
		e.UserID,
	)
}

func (e *HistoryPrunedError) Is(target error) bool {
	return target == errHistoryPruned
}

type CheckpointAheadError struct {
	UserID           string
	Field            string
	ProvidedSeq      int64
	CurrentBundleSeq int64
}

func (e *CheckpointAheadError) Error() string {
	field := e.Field
	if field == "" {
		field = "checkpoint"
	}
	return fmt.Sprintf(
		"requested %s %d is ahead of current committed bundle sequence %d for user %s",
		field,
		e.ProvidedSeq,
		e.CurrentBundleSeq,
		e.UserID,
	)
}

type InvalidPullRequestError struct {
	Message string
}

func (e *InvalidPullRequestError) Error() string {
	return e.Message
}

type retainedHistoryState struct {
	UserPK        int64
	NextBundleSeq int64
	RetainedFloor int64
}

func (s retainedHistoryState) highestBundleSeq() int64 {
	if s.NextBundleSeq <= 0 {
		return 0
	}
	return s.NextBundleSeq - 1
}

func loadRetainedHistoryStateByUserID(ctx context.Context, q userStateQuerier, userID string) (*retainedHistoryState, error) {
	state, err := scanRetainedHistoryState(q.QueryRow(ctx, `
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = $1
	`, userID))
	if err != nil {
		return nil, fmt.Errorf("query retained history state for %q: %w", userID, err)
	}
	return state, nil
}

func loadRetainedHistoryStateByUserPK(ctx context.Context, q userStateQuerier, userPK int64) (*retainedHistoryState, error) {
	state, err := scanRetainedHistoryState(q.QueryRow(ctx, `
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_pk = $1
	`, userPK))
	if err != nil {
		return nil, fmt.Errorf("query retained history state for user_pk %d: %w", userPK, err)
	}
	return state, nil
}

func scanRetainedHistoryState(row pgx.Row) (*retainedHistoryState, error) {
	var state retainedHistoryState
	if err := row.Scan(&state.UserPK, &state.NextBundleSeq, &state.RetainedFloor); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &state, nil
}

func enforceRetainedBundleFloor(userID string, providedSeq int64, retainedFloor int64) error {
	if providedSeq < retainedFloor {
		return &HistoryPrunedError{
			UserID:        userID,
			ProvidedSeq:   providedSeq,
			RetainedFloor: retainedFloor,
		}
	}
	return nil
}

func enforceCommittedBundleRetention(userID string, providedSeq int64, retainedFloor int64) error {
	if providedSeq <= retainedFloor {
		return &HistoryPrunedError{
			UserID:        userID,
			ProvidedSeq:   providedSeq,
			RetainedFloor: retainedFloor,
		}
	}
	return nil
}

func enforcePullSequenceBoundary(userID string, field string, providedSeq int64, state retainedHistoryState) error {
	if err := enforceRetainedBundleFloor(userID, providedSeq, state.RetainedFloor); err != nil {
		var prunedErr *HistoryPrunedError
		if errors.As(err, &prunedErr) {
			prunedErr.Field = field
		}
		return err
	}
	currentBundleSeq := state.highestBundleSeq()
	if providedSeq > currentBundleSeq {
		return &CheckpointAheadError{
			UserID:           userID,
			Field:            field,
			ProvidedSeq:      providedSeq,
			CurrentBundleSeq: currentBundleSeq,
		}
	}
	return nil
}
