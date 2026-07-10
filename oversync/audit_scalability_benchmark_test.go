//go:build oversync_audit

package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	auditScalabilityFullScaleEnv = "OVERSYNC_AUDIT_FULL_SCALE"
	auditScalabilityWarmups      = 5
	auditScalabilitySamples      = 10
)

type auditScalabilityOperationSample struct {
	duration       time.Duration
	allocatedBytes uint64
	mallocs        uint64
}

// These are fixed-sample capacity profiles rather than adaptive Go microbenchmarks.
// Invoke them with -benchtime=1x so each named profile performs exactly five warmups
// and ten measured samples. Scales above the smallest point additionally require
// OVERSYNC_AUDIT_FULL_SCALE=1.
func BenchmarkAuditScalabilityConcurrentPushes(b *testing.B) {
	topologies := []struct {
		name           string
		distinctScopes bool
	}{
		{name: "same_scope"},
		{name: "distinct_scopes", distinctScopes: true},
	}
	for _, topology := range topologies {
		for _, workerCount := range []int{1, 8, 32, 128} {
			b.Run(fmt.Sprintf("%s/workers_%d", topology.name, workerCount), func(b *testing.B) {
				auditRequireScalabilityProfile(b, workerCount, 1)
				fixture := newAuditDatabaseBenchmarkFixture(
					b,
					fmt.Sprintf("concurrent_%s_%d", topology.name, workerCount),
				)
				actors := auditScalabilityActors(b, fixture, workerCount, topology.distinctScopes)

				for round := 0; round < auditScalabilityWarmups; round++ {
					auditRunConcurrentPushRound(b, fixture, actors, round)
				}
				beforePool := fixture.pool.Stat()
				var beforeMemory runtime.MemStats
				runtime.ReadMemStats(&beforeMemory)
				b.ResetTimer()
				measuredStart := time.Now()
				latencies := make([]time.Duration, 0, workerCount*auditScalabilitySamples)
				roundDurations := make([]time.Duration, 0, auditScalabilitySamples)
				for round := auditScalabilityWarmups; round < auditScalabilityWarmups+auditScalabilitySamples; round++ {
					roundStarted := time.Now()
					roundLatencies := auditRunConcurrentPushRound(b, fixture, actors, round)
					roundDurations = append(roundDurations, time.Since(roundStarted))
					latencies = append(latencies, roundLatencies...)
				}
				measuredElapsed := time.Since(measuredStart)
				b.StopTimer()
				var afterMemory runtime.MemStats
				runtime.ReadMemStats(&afterMemory)

				afterPool := fixture.pool.Stat()
				operationCount := float64(workerCount * auditScalabilitySamples)
				auditReportScalabilityMetrics(
					b,
					latencies,
					roundDurations,
					operationCount,
					measuredElapsed,
					"pushes/s",
				)
				auditReportScalabilityMemoryDelta(b, beforeMemory, afterMemory, operationCount, "push")
				b.ReportMetric(float64(afterPool.MaxConns()), "pool-max-conns")
				b.ReportMetric(
					float64((afterPool.EmptyAcquireWaitTime()-beforePool.EmptyAcquireWaitTime()).Nanoseconds())/operationCount,
					"pool-wait-ns/push",
				)
				b.ReportMetric(
					float64(afterPool.EmptyAcquireCount()-beforePool.EmptyAcquireCount())/operationCount,
					"empty-acquires/push",
				)
				if acquired := fixture.pool.Stat().AcquiredConns(); acquired != 0 {
					b.Fatalf("concurrent push profile leaked %d acquired pool connections", acquired)
				}
			})
		}
	}
}

func BenchmarkAuditScalabilityWatchSubscribers(b *testing.B) {
	modes := []struct {
		name           string
		reconnectChurn bool
	}{
		{name: "slow_clients"},
		{name: "reconnect_churn", reconnectChurn: true},
	}
	for _, subscriberCount := range []int{100, 1_000, 10_000} {
		for _, mode := range modes {
			b.Run(fmt.Sprintf("subscribers_%d/%s", subscriberCount, mode.name), func(b *testing.B) {
				auditRequireScalabilityProfile(b, subscriberCount, 100)
				runtime.GC()
				var beforeMemory runtime.MemStats
				runtime.ReadMemStats(&beforeMemory)
				beforeGoroutines := runtime.NumGoroutine()

				hub := newBundleChangeHub()
				subscribers := auditCreateWatchProfileSubscribers(hub, subscriberCount)
				b.Cleanup(func() {
					auditUnsubscribeWatchProfileSubscribers(subscribers)
				})
				runtime.GC()
				var afterSetupMemory runtime.MemStats
				runtime.ReadMemStats(&afterSetupMemory)
				afterSetupGoroutines := runtime.NumGoroutine()
				churnPerRound := 0
				if mode.reconnectChurn {
					churnPerRound = subscriberCount / 100
					if churnPerRound < 1 {
						churnPerRound = 1
					}
				}

				lastBundleSeq := int64(0)
				for round := 0; round < auditScalabilityWarmups; round++ {
					lastBundleSeq++
					auditRunWatchProfileRound(b, hub, subscribers, lastBundleSeq, round, churnPerRound)
				}
				var afterWarmupMemory runtime.MemStats
				runtime.ReadMemStats(&afterWarmupMemory)
				b.ResetTimer()
				measuredStart := time.Now()
				latencies := make([]time.Duration, 0, auditScalabilitySamples)
				for round := 0; round < auditScalabilitySamples; round++ {
					lastBundleSeq++
					latencies = append(
						latencies,
						auditRunWatchProfileRound(b, hub, subscribers, lastBundleSeq, round, churnPerRound),
					)
				}
				measuredElapsed := time.Since(measuredStart)
				b.StopTimer()
				var afterMeasuredMemory runtime.MemStats
				runtime.ReadMemStats(&afterMeasuredMemory)

				auditAssertSlowSubscribersBounded(b, subscribers, lastBundleSeq)
				cleanupStart := time.Now()
				auditUnsubscribeWatchProfileSubscribers(subscribers)
				cleanupElapsed := time.Since(cleanupStart)
				if remaining := hub.subscriberCount(1); remaining != 0 {
					b.Fatalf("watch profile cleanup left %d subscribers", remaining)
				}
				subscribers = nil

				eventCount := float64(subscriberCount * auditScalabilitySamples)
				auditReportScalabilityMetrics(b, latencies, latencies, eventCount, measuredElapsed, "events/s")
				auditReportScalabilityMemoryDelta(
					b,
					afterWarmupMemory,
					afterMeasuredMemory,
					eventCount,
					"event",
				)
				b.ReportMetric(1, "buffer-slots/subscriber")
				reconnectCount := churnPerRound * auditScalabilitySamples
				b.ReportMetric(float64(reconnectCount), "reconnects")
				if reconnectCount > 0 && afterMeasuredMemory.TotalAlloc >= afterWarmupMemory.TotalAlloc {
					b.ReportMetric(
						float64(afterMeasuredMemory.TotalAlloc-afterWarmupMemory.TotalAlloc)/float64(reconnectCount),
						"workload-alloc-bytes/reconnect",
					)
				}
				if cleanupElapsed > 0 {
					b.ReportMetric(float64(subscriberCount)/cleanupElapsed.Seconds(), "disconnects/s")
				}
				if afterSetupMemory.HeapAlloc >= beforeMemory.HeapAlloc {
					b.ReportMetric(
						float64(afterSetupMemory.HeapAlloc-beforeMemory.HeapAlloc)/float64(subscriberCount),
						"heap-bytes/subscriber",
					)
				}
				b.ReportMetric(float64(afterSetupGoroutines-beforeGoroutines), "subscriber-goroutines")

				runtime.GC()
				deadline := time.Now().Add(time.Second)
				for runtime.NumGoroutine() > beforeGoroutines && time.Now().Before(deadline) {
					runtime.Gosched()
					time.Sleep(time.Millisecond)
				}
				var afterCleanupMemory runtime.MemStats
				runtime.ReadMemStats(&afterCleanupMemory)
				b.ReportMetric(
					float64(int64(afterCleanupMemory.HeapAlloc)-int64(beforeMemory.HeapAlloc)),
					"post-cleanup-heap-bytes",
				)
				b.ReportMetric(
					float64(runtime.NumGoroutine()-beforeGoroutines),
					"post-cleanup-goroutines",
				)
			})
		}
	}
}

func BenchmarkAuditScalabilityBootstrapRegisteredTables(b *testing.B) {
	for _, tableCount := range []int{1, 10, 100, 1_000} {
		b.Run(fmt.Sprintf("tables_%d", tableCount), func(b *testing.B) {
			auditRequireScalabilityProfile(b, tableCount, 1)
			fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("bootstrap_%d", tableCount))
			if err := fixture.svc.Close(context.Background()); err != nil {
				b.Fatalf("close bootstrap setup service: %v", err)
			}
			if err := resetTestSyncSchema(fixture.ctx, fixture.pool); err != nil {
				b.Fatalf("reset sync schema before bootstrap profile: %v", err)
			}
			registeredTables := auditCreateBootstrapProfileTables(b, fixture, tableCount)
			config := &ServiceConfig{
				MaxSupportedSchemaVersion: 1,
				AppName:                   fmt.Sprintf("audit-scalability-bootstrap-%d", tableCount),
				RegisteredTables:          registeredTables,
			}

			latencies, allocatedBytes, mallocs := auditRunFixedScalabilitySamples(b, func(measured bool) auditScalabilityOperationSample {
				return auditRunBootstrapProfileSample(b, fixture, config, measured)
			})

			measuredElapsed := auditDurationSum(latencies)
			workUnits := float64(tableCount * auditScalabilitySamples)
			auditReportScalabilityMetrics(
				b,
				latencies,
				latencies,
				workUnits,
				measuredElapsed,
				"tables/s",
			)
			auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, "table")
		})
	}
}

func BenchmarkAuditScalabilityExpiredSessionCleanup(b *testing.B) {
	cleanupKinds := []string{"push", "snapshot"}
	for _, sessionCount := range []int{100, 10_000} {
		for _, cleanupKind := range cleanupKinds {
			b.Run(fmt.Sprintf("%s/sessions_%d", cleanupKind, sessionCount), func(b *testing.B) {
				auditRequireScalabilityProfile(b, sessionCount, 100)
				fixture := newAuditDatabaseBenchmarkFixture(
					b,
					fmt.Sprintf("cleanup_%s_%d", cleanupKind, sessionCount),
				)
				userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.writer.UserID)
				if err != nil {
					b.Fatalf("lookup cleanup profile user: %v", err)
				}
				var tableID int32
				if err := fixture.pool.QueryRow(fixture.ctx, `
					SELECT table_id
					FROM sync.table_catalog
					WHERE schema_name = $1 AND table_name = 'users'
				`, fixture.schemaName).Scan(&tableID); err != nil {
					b.Fatalf("lookup cleanup profile table: %v", err)
				}

				latencies, allocatedBytes, mallocs := auditRunFixedScalabilitySamples(b, func(measured bool) auditScalabilityOperationSample {
					return auditRunExpiredCleanupProfileSample(
						b,
						fixture,
						cleanupKind,
						sessionCount,
						userPK,
						tableID,
						measured,
					)
				})

				measuredElapsed := auditDurationSum(latencies)
				workUnits := float64(sessionCount * auditScalabilitySamples)
				auditReportScalabilityMetrics(
					b,
					latencies,
					latencies,
					workUnits,
					measuredElapsed,
					"sessions/s",
				)
				auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, "session")
			})
		}
	}
}

func BenchmarkAuditScalabilityRetentionPruning(b *testing.B) {
	const retainedBundles = 10
	for _, historyCount := range []int{100, 10_000} {
		b.Run(fmt.Sprintf("history_%d", historyCount), func(b *testing.B) {
			auditRequireScalabilityProfile(b, historyCount, 100)
			fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("pruning_%d", historyCount))
			fixture.svc.config.RetainedBundlesPerUser = retainedBundles
			fixture.svc.config.RetentionPruneBatchSize = int64(historyCount)
			userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.writer.UserID)
			if err != nil {
				b.Fatalf("lookup pruning profile user: %v", err)
			}
			var tableID int32
			if err := fixture.pool.QueryRow(fixture.ctx, `
				SELECT table_id
				FROM sync.table_catalog
				WHERE schema_name = $1 AND table_name = 'users'
			`, fixture.schemaName).Scan(&tableID); err != nil {
				b.Fatalf("lookup pruning profile table: %v", err)
			}

			latencies, allocatedBytes, mallocs := auditRunFixedScalabilitySamples(b, func(measured bool) auditScalabilityOperationSample {
				return auditRunPruningProfileSample(
					b,
					fixture,
					historyCount,
					retainedBundles,
					userPK,
					tableID,
					measured,
				)
			})

			measuredElapsed := auditDurationSum(latencies)
			workUnits := float64((historyCount - retainedBundles) * auditScalabilitySamples)
			auditReportScalabilityMetrics(
				b,
				latencies,
				latencies,
				workUnits,
				measuredElapsed,
				"bundles/s",
			)
			auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, "bundle")
		})
	}
}

type auditConcurrentPushResult struct {
	duration time.Duration
	err      error
}

func auditScalabilityActors(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	workerCount int,
	distinctScopes bool,
) []Actor {
	b.Helper()
	actors := make([]Actor, workerCount)
	for worker := range workerCount {
		userID := fixture.writer.UserID
		if distinctScopes {
			userID = fmt.Sprintf("%s-scope-%04d", fixture.writer.UserID, worker)
		}
		actor := Actor{UserID: userID, SourceID: fmt.Sprintf("worker-%04d", worker)}
		response, err := fixture.svc.Connect(fixture.ctx, actor, &ConnectRequest{HasLocalPendingRows: false})
		if err != nil {
			b.Fatalf("initialize concurrency actor %d: %v", worker, err)
		}
		if response.Resolution != "initialize_empty" && response.Resolution != "remote_authoritative" {
			b.Fatalf("initialize concurrency actor %d returned %q", worker, response.Resolution)
		}
		actors[worker] = actor
	}
	return actors
}

func auditRunConcurrentPushRound(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	actors []Actor,
	round int,
) []time.Duration {
	b.Helper()
	results := make(chan auditConcurrentPushResult, len(actors))
	start := make(chan struct{})
	roundCtx, cancelRound := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
	defer cancelRound()
	var workers sync.WaitGroup
	workers.Add(len(actors))
	for worker, actor := range actors {
		go func() {
			defer workers.Done()
			<-start
			startedAt := time.Now()
			rowSeed := int64(round*len(actors) + worker + 1)
			_, err := auditBenchmarkPush(
				roundCtx,
				fixture.svc,
				actor,
				int64(round+1),
				auditBenchmarkRows(fixture.schemaName, rowSeed, 1),
			)
			results <- auditConcurrentPushResult{duration: time.Since(startedAt), err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	latencies := make([]time.Duration, 0, len(actors))
	for result := range results {
		if result.err != nil {
			b.Fatalf("concurrent push round %d failed: %v", round, result.err)
		}
		latencies = append(latencies, result.duration)
	}
	return latencies
}

func auditUnsubscribeWatchProfileSubscribers(subscribers []auditWatchProfileSubscriber) {
	for _, subscriber := range subscribers {
		if subscriber.unsubscribe != nil {
			subscriber.unsubscribe()
		}
	}
}

type auditWatchProfileSubscriber struct {
	events      <-chan BundleChangeEvent
	unsubscribe func()
	slow        bool
}

func auditCreateWatchProfileSubscribers(hub *bundleChangeHub, subscriberCount int) []auditWatchProfileSubscriber {
	subscribers := make([]auditWatchProfileSubscriber, subscriberCount)
	for index := range subscriberCount {
		events, unsubscribe := hub.subscribe(context.Background(), 1, 0)
		subscribers[index] = auditWatchProfileSubscriber{
			events:      events,
			unsubscribe: unsubscribe,
			slow:        index%10 == 0,
		}
	}
	return subscribers
}

func auditRunWatchProfileRound(
	b *testing.B,
	hub *bundleChangeHub,
	subscribers []auditWatchProfileSubscriber,
	bundleSeq int64,
	round int,
	churnCount int,
) time.Duration {
	b.Helper()
	startedAt := time.Now()
	roundTimer := time.NewTimer(30 * time.Second)
	defer roundTimer.Stop()
	for offset := 0; offset < churnCount; offset++ {
		index := (round*churnCount + offset) % len(subscribers)
		subscribers[index].unsubscribe()
		events, unsubscribe := hub.subscribe(context.Background(), 1, bundleSeq-1)
		subscribers[index].events = events
		subscribers[index].unsubscribe = unsubscribe
	}
	hub.publish(1, BundleChangeEvent{BundleSeq: bundleSeq})
	for index := range subscribers {
		if subscribers[index].slow {
			continue
		}
		var event BundleChangeEvent
		var ok bool
		select {
		case event, ok = <-subscribers[index].events:
		case <-roundTimer.C:
			b.Fatalf("timed out waiting for fast watch subscriber %d during profile", index)
		}
		if !ok {
			b.Fatalf("fast watch subscriber %d closed during profile", index)
		}
		if event.BundleSeq != bundleSeq {
			b.Fatalf("fast watch subscriber %d received bundle_seq %d, want %d", index, event.BundleSeq, bundleSeq)
		}
	}
	return time.Since(startedAt)
}

func auditAssertSlowSubscribersBounded(
	b *testing.B,
	subscribers []auditWatchProfileSubscriber,
	wantBundleSeq int64,
) {
	b.Helper()
	for index := range subscribers {
		if !subscribers[index].slow {
			continue
		}
		if buffered := len(subscribers[index].events); buffered != 1 {
			b.Fatalf("slow watch subscriber %d buffered %d events, want exactly 1", index, buffered)
		}
		event, ok := <-subscribers[index].events
		if !ok {
			b.Fatalf("slow watch subscriber %d closed during profile", index)
		}
		if event.BundleSeq != wantBundleSeq {
			b.Fatalf("slow watch subscriber %d retained bundle_seq %d, want latest %d", index, event.BundleSeq, wantBundleSeq)
		}
	}
}

func auditCreateBootstrapProfileTables(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	tableCount int,
) []RegisteredTable {
	b.Helper()
	schemaName := fmt.Sprintf("audit_scale_bootstrap_%d_%d", tableCount, managedIntegrationDatabaseSequence.Add(1))
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	if _, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent)); err != nil {
		b.Fatalf("create bootstrap profile schema: %v", err)
	}
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestSchema(cleanupCtx, fixture.pool, schemaName); err != nil {
			b.Errorf("drop bootstrap profile schema: %v", err)
		}
	})

	registeredTables := make([]RegisteredTable, tableCount)
	err := pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		var batch pgx.Batch
		for index := range tableCount {
			tableName := fmt.Sprintf("table_%04d", index)
			tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
			batch.Queue(fmt.Sprintf(`
				CREATE TABLE %s (
					_sync_scope_id TEXT NOT NULL,
					id UUID NOT NULL,
					payload TEXT NOT NULL,
					PRIMARY KEY (_sync_scope_id, id)
				)
			`, tableIdent))
			registeredTables[index] = RegisteredTable{
				Schema:         schemaName,
				Table:          tableName,
				SyncKeyColumns: []string{"id"},
			}
		}
		results := tx.SendBatch(fixture.ctx, &batch)
		for index := range tableCount {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("create registered table %d: %w", index, err)
			}
		}
		return results.Close()
	})
	if err != nil {
		b.Fatalf("create bootstrap profile tables: %v", err)
	}
	return registeredTables
}

func auditRunBootstrapProfileSample(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	config *ServiceConfig,
	measured bool,
) auditScalabilityOperationSample {
	b.Helper()
	if err := resetTestSyncSchema(fixture.ctx, fixture.pool); err != nil {
		b.Fatalf("reset sync schema for bootstrap sample: %v", err)
	}
	service, err := NewRuntimeService(fixture.pool, config, integrationTestLogger(slog.LevelWarn))
	if err != nil {
		b.Fatalf("create bootstrap sample service: %v", err)
	}
	var beforeMemory runtime.MemStats
	if measured {
		runtime.ReadMemStats(&beforeMemory)
		b.StartTimer()
	}
	startedAt := time.Now()
	err = service.Bootstrap(fixture.ctx)
	elapsed := time.Since(startedAt)
	var afterMemory runtime.MemStats
	if measured {
		b.StopTimer()
		runtime.ReadMemStats(&afterMemory)
	}
	if closeErr := service.Close(context.Background()); closeErr != nil {
		b.Fatalf("close bootstrap sample service: %v", closeErr)
	}
	if err != nil {
		b.Fatalf("bootstrap sample failed: %v", err)
	}
	return auditScalabilitySampleWithMemory(elapsed, measured, beforeMemory, afterMemory)
}

func auditRunExpiredCleanupProfileSample(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	cleanupKind string,
	sessionCount int,
	userPK int64,
	tableID int32,
	measured bool,
) auditScalabilityOperationSample {
	b.Helper()
	if err := auditSeedExpiredSessions(fixture.ctx, fixture.pool, cleanupKind, sessionCount, userPK, tableID); err != nil {
		b.Fatalf("seed %s cleanup sample: %v", cleanupKind, err)
	}
	var beforeMemory runtime.MemStats
	if measured {
		runtime.ReadMemStats(&beforeMemory)
		b.StartTimer()
	}
	startedAt := time.Now()
	var err error
	switch cleanupKind {
	case "push":
		err = cleanupExpiredPushSessionsQuerier(fixture.ctx, fixture.pool)
	case "snapshot":
		err = cleanupExpiredSnapshotSessionsQuerier(fixture.ctx, fixture.pool)
	default:
		b.Fatalf("unknown cleanup profile kind %q", cleanupKind)
	}
	elapsed := time.Since(startedAt)
	var afterMemory runtime.MemStats
	if measured {
		b.StopTimer()
		runtime.ReadMemStats(&afterMemory)
	}
	if err != nil {
		b.Fatalf("cleanup %s sessions: %v", cleanupKind, err)
	}

	var remaining int64
	query := `
		SELECT
			(SELECT COUNT(*) FROM sync.push_sessions) +
			(SELECT COUNT(*) FROM sync.push_session_rows)
	`
	if cleanupKind == "snapshot" {
		query = `
			SELECT
				(SELECT COUNT(*) FROM sync.snapshot_sessions) +
				(SELECT COUNT(*) FROM sync.snapshot_session_rows)
		`
	}
	if err := fixture.pool.QueryRow(fixture.ctx, query).Scan(&remaining); err != nil {
		b.Fatalf("count remaining %s sessions: %v", cleanupKind, err)
	}
	if remaining != 0 {
		b.Fatalf("cleanup left %d %s session or staged-row records", remaining, cleanupKind)
	}
	return auditScalabilitySampleWithMemory(elapsed, measured, beforeMemory, afterMemory)
}

func auditSeedExpiredSessions(
	ctx context.Context,
	pool *pgxpool.Pool,
	cleanupKind string,
	sessionCount int,
	userPK int64,
	tableID int32,
) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		switch cleanupKind {
		case "push":
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.push_sessions (
					push_id, user_pk, source_id, source_bundle_id,
					planned_row_count, next_expected_row_ordinal,
					initialization_id, expires_at
				)
				SELECT md5('audit-expired-push-' || item::text)::uuid,
					$1,
					'audit-expired-' || item::text,
					1,
					1,
					1,
					NULL,
					now() - interval '1 second'
				FROM generate_series(1, $2) AS item
			`, userPK, sessionCount); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.push_session_rows (
					push_id, row_ordinal, table_id, key_bytes,
					op_code, base_bundle_seq, payload_apply
				)
				SELECT md5('audit-expired-push-' || item::text)::uuid,
					0,
					$1,
					decode(md5('audit-expired-push-key-' || item::text), 'hex'),
					1,
					0,
					'{}'::json
				FROM generate_series(1, $2) AS item
			`, tableID, sessionCount); err != nil {
				return err
			}
		case "snapshot":
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.snapshot_sessions (
					snapshot_id, user_pk, snapshot_bundle_seq,
					row_count, byte_count, expires_at
				)
				SELECT md5('audit-expired-snapshot-' || item::text)::uuid,
					$1,
					0,
					1,
					2,
					now() - interval '1 second'
				FROM generate_series(1, $2) AS item
			`, userPK, sessionCount); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.snapshot_session_rows (
					snapshot_id, row_ordinal, table_id, key_bytes,
					bundle_seq, payload_wire
				)
				SELECT md5('audit-expired-snapshot-' || item::text)::uuid,
					0,
					$1,
					decode(md5('audit-expired-snapshot-key-' || item::text), 'hex'),
					0,
					'{}'::json
				FROM generate_series(1, $2) AS item
			`, tableID, sessionCount); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown cleanup profile kind %q", cleanupKind)
		}
		return nil
	})
}

func auditRunPruningProfileSample(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	historyCount int,
	retainedBundles int,
	userPK int64,
	tableID int32,
	measured bool,
) auditScalabilityOperationSample {
	b.Helper()
	if err := auditSeedPruningHistory(fixture.ctx, fixture.pool, historyCount, userPK, tableID); err != nil {
		b.Fatalf("seed pruning sample: %v", err)
	}
	var beforeMemory runtime.MemStats
	if measured {
		runtime.ReadMemStats(&beforeMemory)
		b.StartTimer()
	}
	startedAt := time.Now()
	err := pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		return fixture.svc.applyRetentionPolicyForUser(fixture.ctx, tx, userPK)
	})
	elapsed := time.Since(startedAt)
	var afterMemory runtime.MemStats
	if measured {
		b.StopTimer()
		runtime.ReadMemStats(&afterMemory)
	}
	if err != nil {
		b.Fatalf("apply retention policy: %v", err)
	}

	var retainedFloor, remaining int64
	if err := fixture.pool.QueryRow(fixture.ctx, `
		SELECT users.retained_bundle_floor, COUNT(bundle.bundle_seq)
		FROM sync.user_state AS users
		LEFT JOIN sync.bundle_log AS bundle ON bundle.user_pk = users.user_pk
		WHERE users.user_pk = $1
		GROUP BY users.retained_bundle_floor
	`, userPK).Scan(&retainedFloor, &remaining); err != nil {
		b.Fatalf("verify pruning sample: %v", err)
	}
	if wantFloor := int64(historyCount - retainedBundles); retainedFloor != wantFloor {
		b.Fatalf("retained floor = %d, want %d", retainedFloor, wantFloor)
	}
	if remaining != int64(retainedBundles) {
		b.Fatalf("retained bundle count = %d, want %d", remaining, retainedBundles)
	}
	return auditScalabilitySampleWithMemory(elapsed, measured, beforeMemory, afterMemory)
}

func auditScalabilitySampleWithMemory(
	duration time.Duration,
	measured bool,
	before runtime.MemStats,
	after runtime.MemStats,
) auditScalabilityOperationSample {
	sample := auditScalabilityOperationSample{duration: duration}
	if !measured {
		return sample
	}
	if after.TotalAlloc >= before.TotalAlloc {
		sample.allocatedBytes = after.TotalAlloc - before.TotalAlloc
	}
	if after.Mallocs >= before.Mallocs {
		sample.mallocs = after.Mallocs - before.Mallocs
	}
	return sample
}

func auditSeedPruningHistory(
	ctx context.Context,
	q interface {
		Begin(context.Context) (pgx.Tx, error)
	},
	historyCount int,
	userPK int64,
	tableID int32,
) error {
	return pgx.BeginFunc(ctx, q, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_log WHERE user_pk = $1`, userPK); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sync.user_state
			SET next_bundle_seq = $2 + 1,
				retained_bundle_floor = 0
			WHERE user_pk = $1
		`, userPK, historyCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.source_state (
				user_pk, source_id, state, max_committed_source_bundle_id,
				replaced_by_source_id, retirement_reason
			)
			VALUES ($1, 'audit-prune', 'active', $2, '', '')
			ON CONFLICT (user_pk, source_id) DO UPDATE
			SET state = 'active',
				max_committed_source_bundle_id = EXCLUDED.max_committed_source_bundle_id,
				replaced_by_source_id = '',
				retirement_reason = ''
		`, userPK, historyCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.bundle_log (
				user_pk, bundle_seq, source_id, source_bundle_id,
				row_count, byte_count, bundle_hash
			)
			SELECT $1, item, 'audit-prune', item, 1, 2, decode(md5(item::text), 'hex')
			FROM generate_series(1, $2) AS item
		`, userPK, historyCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.bundle_rows (
				user_pk, bundle_seq, row_ordinal, table_id, key_bytes, op_code, payload_wire
			)
			SELECT $1, item, 1, $3, decode(md5('key-' || item::text), 'hex'), 1, '{}'::json
			FROM generate_series(1, $2) AS item
		`, userPK, historyCount, tableID); err != nil {
			return err
		}
		return nil
	})
}

func auditRequireScalabilityProfile(b *testing.B, scale, smallestScale int) {
	b.Helper()
	if b.N != 1 {
		b.Fatalf("fixed-sample scalability profiles require -benchtime=1x (benchmark N=%d)", b.N)
	}
	if scale > smallestScale && os.Getenv(auditScalabilityFullScaleEnv) != "1" {
		b.Skipf("scale %d requires %s=1", scale, auditScalabilityFullScaleEnv)
	}
}

func auditRunFixedScalabilitySamples(
	b *testing.B,
	runSample func(measured bool) auditScalabilityOperationSample,
) ([]time.Duration, uint64, uint64) {
	b.Helper()
	b.StopTimer()
	for range auditScalabilityWarmups {
		runSample(false)
	}
	b.ResetTimer()
	b.StopTimer()
	latencies := make([]time.Duration, 0, auditScalabilitySamples)
	var allocatedBytes uint64
	var mallocs uint64
	for range auditScalabilitySamples {
		sample := runSample(true)
		latencies = append(latencies, sample.duration)
		allocatedBytes += sample.allocatedBytes
		mallocs += sample.mallocs
	}
	return latencies, allocatedBytes, mallocs
}

func auditReportScalabilityAllocations(
	b *testing.B,
	allocatedBytes uint64,
	mallocs uint64,
	workUnits float64,
	unit string,
) {
	b.Helper()
	if workUnits <= 0 {
		b.Fatalf("scalability allocation normalization requires positive work units, got %f", workUnits)
	}
	b.ReportMetric(float64(allocatedBytes)/workUnits, fmt.Sprintf("alloc-bytes/%s", unit))
	b.ReportMetric(float64(mallocs)/workUnits, fmt.Sprintf("allocs/%s", unit))
}

func auditReportScalabilityMemoryDelta(
	b *testing.B,
	before runtime.MemStats,
	after runtime.MemStats,
	workUnits float64,
	unit string,
) {
	b.Helper()
	var allocatedBytes uint64
	if after.TotalAlloc >= before.TotalAlloc {
		allocatedBytes = after.TotalAlloc - before.TotalAlloc
	}
	var mallocs uint64
	if after.Mallocs >= before.Mallocs {
		mallocs = after.Mallocs - before.Mallocs
	}
	auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, unit)
}

func auditReportScalabilityMetrics(
	b *testing.B,
	latencies []time.Duration,
	reproducibilitySamples []time.Duration,
	workUnits float64,
	measuredElapsed time.Duration,
	throughputUnit string,
) {
	b.Helper()
	if len(latencies) == 0 {
		b.Fatal("scalability profile produced no latency samples")
	}
	if len(reproducibilitySamples) == 0 {
		b.Fatal("scalability profile produced no reproducibility samples")
	}
	ordered := append([]time.Duration(nil), latencies...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	mean := auditDurationMean(ordered)
	b.ReportMetric(float64(mean.Nanoseconds()), "ns/op")
	b.ReportMetric(float64(auditDurationPercentile(ordered, 0.50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(auditDurationPercentile(ordered, 0.95).Nanoseconds()), "p95-ns")
	b.ReportMetric(float64(auditDurationPercentile(ordered, 0.99).Nanoseconds()), "p99-ns")
	b.ReportMetric(auditDurationCoefficientOfVariation(reproducibilitySamples)*100, "cv-percent")
	b.ReportMetric(float64(len(ordered)), "samples")
	b.ReportMetric(float64(len(reproducibilitySamples)), "cv-samples")
	if measuredElapsed > 0 {
		b.ReportMetric(workUnits/measuredElapsed.Seconds(), throughputUnit)
	}
}

func auditDurationPercentile(ordered []time.Duration, percentile float64) time.Duration {
	index := int(math.Ceil(percentile*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

func auditDurationMean(samples []time.Duration) time.Duration {
	return auditDurationSum(samples) / time.Duration(len(samples))
}

func auditDurationSum(samples []time.Duration) time.Duration {
	var total time.Duration
	for _, sample := range samples {
		total += sample
	}
	return total
}

func auditDurationCoefficientOfVariation(samples []time.Duration) float64 {
	mean := float64(auditDurationMean(samples))
	if mean == 0 {
		return 0
	}
	var squaredDifferenceSum float64
	for _, sample := range samples {
		difference := float64(sample) - mean
		squaredDifferenceSum += difference * difference
	}
	return math.Sqrt(squaredDifferenceSum/float64(len(samples))) / mean
}
