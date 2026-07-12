// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"time"
)

type StageTiming struct {
	Operation string
	Stage     string
	Duration  time.Duration
	Count     int
	Attempt   int
	Error     bool
}

type StageMetricsRecorder interface {
	ObserveStage(ctx context.Context, timing StageTiming)
}

type StageMetricsRecorderFunc func(ctx context.Context, timing StageTiming)

func (f StageMetricsRecorderFunc) ObserveStage(ctx context.Context, timing StageTiming) {
	f(ctx, timing)
}

func (s *SyncService) stageTimingEnabled() bool {
	if s == nil || s.config == nil {
		return false
	}
	return s.config.StageMetrics != nil || s.config.LogStageTimings
}

func (s *SyncService) stageStart() time.Time {
	if !s.stageTimingEnabled() {
		return time.Time{}
	}
	return time.Now()
}

func (s *SyncService) observeStage(ctx context.Context, op, stage string, start time.Time, count, attempt int, hadError bool) {
	if start.IsZero() || s == nil || s.config == nil {
		return
	}
	s.observeStageDuration(ctx, op, stage, time.Since(start), count, attempt, hadError)
}

func (s *SyncService) observeStageDuration(ctx context.Context, op, stage string, duration time.Duration, count, attempt int, hadError bool) {
	if duration < 0 || s == nil || s.config == nil || !s.stageTimingEnabled() {
		return
	}

	timing := StageTiming{
		Operation: op,
		Stage:     stage,
		Duration:  duration,
		Count:     count,
		Attempt:   attempt,
		Error:     hadError,
	}

	if s.config.StageMetrics != nil {
		s.config.StageMetrics.ObserveStage(ctx, timing)
	}
	if s.config.LogStageTimings && s.logger != nil {
		s.logger.Debug("Stage timing",
			"op", timing.Operation,
			"stage", timing.Stage,
			"duration", timing.Duration,
			"count", timing.Count,
			"attempt", timing.Attempt,
			"error", timing.Error,
		)
	}
}

func (s *SyncService) observeStageErr(ctx context.Context, op, stage string, start time.Time, count, attempt int, err error) {
	s.observeStage(ctx, op, stage, start, count, attempt, err != nil && !errors.Is(err, errServiceShuttingDown))
}

const bootstrapProgressInterval = 5 * time.Second

type bootstrapProgress struct {
	service   *SyncService
	total     int
	lastLogAt time.Time
	now       func() time.Time
	interval  time.Duration
}

func newBootstrapProgress(service *SyncService, total int) *bootstrapProgress {
	progress := &bootstrapProgress{service: service, total: total}
	if service == nil || service.config == nil || !service.config.LogStageTimings || service.logger == nil {
		return progress
	}
	progress.now = service.bootstrapProgressNow
	if progress.now == nil {
		progress.now = time.Now
	}
	progress.interval = service.bootstrapProgressInterval
	if progress.interval <= 0 {
		progress.interval = bootstrapProgressInterval
	}
	progress.lastLogAt = progress.now()
	return progress
}

func (p *bootstrapProgress) maybeLog(stage string, completed int, businessRows int64, adopted int) {
	if p == nil || p.now == nil || p.interval <= 0 {
		return
	}
	now := p.now()
	if now.Sub(p.lastLogAt) < p.interval {
		return
	}
	p.lastLogAt = now
	p.service.logger.Debug("Sync bootstrap adoption progress",
		"op", "bootstrap",
		"stage", stage,
		"completed_scope_count", completed,
		"total_scope_count", p.total,
		"business_row_count", businessRows,
		"adopted_scope_count", adopted,
	)
}
