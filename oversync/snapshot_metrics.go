package oversync

import "sync/atomic"

type snapshotRuntimeMetrics struct {
	activeBuilds                       atomic.Int64
	buildHighWater                     atomic.Int64
	activeChunks                       atomic.Int64
	chunkHighWater                     atomic.Int64
	materializationBatchRowsHighWater  atomic.Int64
	materializationBatchBytesHighWater atomic.Int64
	materializationPageQueries         atomic.Int64
	materializationCopyBatches         atomic.Int64
	chunkRequests                      atomic.Int64
	chunkCompletions                   atomic.Int64
	chunkRejections                    atomic.Int64
	chunkFailures                      atomic.Int64
	chunkSelectionDurationHighWater    atomic.Int64
	chunkQueries                       atomic.Int64
	chunkRowsHighWater                 atomic.Int64
	chunkBytesHighWater                atomic.Int64
	chunkRetainedRowsHighWater         atomic.Int64
	chunkRetainedBytesHighWater        atomic.Int64
	activeCleanupRuns                  atomic.Int64
	cleanupRunHighWater                atomic.Int64
	cleanupRuns                        atomic.Int64
	cleanupBatches                     atomic.Int64
	cleanupFailures                    atomic.Int64
	cleanupCancellations               atomic.Int64
	cleanupBatchDurationHighWater      atomic.Int64
	cleanupOldestExpiryAgeHighWater    atomic.Int64
	cleanupCandidateSessionsHighWater  atomic.Int64
	cleanupDeletedRowsHighWater        atomic.Int64
	cleanupDeletedSessionsHighWater    atomic.Int64
}

type snapshotRuntimeMetricsSnapshot struct {
	ActiveBuilds                       int64
	BuildHighWater                     int64
	ActiveChunks                       int64
	ChunkHighWater                     int64
	MaterializationBatchRowsHighWater  int64
	MaterializationBatchBytesHighWater int64
	MaterializationPageQueries         int64
	MaterializationCopyBatches         int64
	ChunkRequests                      int64
	ChunkCompletions                   int64
	ChunkRejections                    int64
	ChunkFailures                      int64
	ChunkSelectionDurationHighWater    int64
	ChunkQueries                       int64
	ChunkRowsHighWater                 int64
	ChunkBytesHighWater                int64
	ChunkRetainedRowsHighWater         int64
	ChunkRetainedBytesHighWater        int64
	ActiveCleanupRuns                  int64
	CleanupRunHighWater                int64
	CleanupRuns                        int64
	CleanupBatches                     int64
	CleanupFailures                    int64
	CleanupCancellations               int64
	CleanupBatchDurationHighWater      int64
	CleanupOldestExpiryAgeHighWater    int64
	CleanupCandidateSessionsHighWater  int64
	CleanupDeletedRowsHighWater        int64
	CleanupDeletedSessionsHighWater    int64
}

func observeAtomicHighWater(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func (s *SyncService) snapshotRuntimeMetricsSnapshot() snapshotRuntimeMetricsSnapshot {
	m := &s.snapshotMetrics
	return snapshotRuntimeMetricsSnapshot{
		ActiveBuilds: m.activeBuilds.Load(), BuildHighWater: m.buildHighWater.Load(),
		ActiveChunks: m.activeChunks.Load(), ChunkHighWater: m.chunkHighWater.Load(),
		MaterializationBatchRowsHighWater:  m.materializationBatchRowsHighWater.Load(),
		MaterializationBatchBytesHighWater: m.materializationBatchBytesHighWater.Load(),
		MaterializationPageQueries:         m.materializationPageQueries.Load(),
		MaterializationCopyBatches:         m.materializationCopyBatches.Load(),
		ChunkRequests:                      m.chunkRequests.Load(),
		ChunkCompletions:                   m.chunkCompletions.Load(),
		ChunkRejections:                    m.chunkRejections.Load(),
		ChunkFailures:                      m.chunkFailures.Load(),
		ChunkSelectionDurationHighWater:    m.chunkSelectionDurationHighWater.Load(),
		ChunkQueries:                       m.chunkQueries.Load(),
		ChunkRowsHighWater:                 m.chunkRowsHighWater.Load(), ChunkBytesHighWater: m.chunkBytesHighWater.Load(),
		ChunkRetainedRowsHighWater:        m.chunkRetainedRowsHighWater.Load(),
		ChunkRetainedBytesHighWater:       m.chunkRetainedBytesHighWater.Load(),
		ActiveCleanupRuns:                 m.activeCleanupRuns.Load(),
		CleanupRunHighWater:               m.cleanupRunHighWater.Load(),
		CleanupRuns:                       m.cleanupRuns.Load(),
		CleanupBatches:                    m.cleanupBatches.Load(),
		CleanupFailures:                   m.cleanupFailures.Load(),
		CleanupCancellations:              m.cleanupCancellations.Load(),
		CleanupBatchDurationHighWater:     m.cleanupBatchDurationHighWater.Load(),
		CleanupOldestExpiryAgeHighWater:   m.cleanupOldestExpiryAgeHighWater.Load(),
		CleanupCandidateSessionsHighWater: m.cleanupCandidateSessionsHighWater.Load(),
		CleanupDeletedRowsHighWater:       m.cleanupDeletedRowsHighWater.Load(),
		CleanupDeletedSessionsHighWater:   m.cleanupDeletedSessionsHighWater.Load(),
	}
}

func (s *SyncService) tryAcquireSnapshotBuild() bool {
	if !tryAcquireSnapshotPermit(s.snapshotBuildPermits) {
		return false
	}
	active := s.snapshotMetrics.activeBuilds.Add(1)
	observeAtomicHighWater(&s.snapshotMetrics.buildHighWater, active)
	return true
}
func (s *SyncService) releaseSnapshotBuild() {
	s.snapshotMetrics.activeBuilds.Add(-1)
	releaseSnapshotPermit(s.snapshotBuildPermits)
}
func (s *SyncService) tryAcquireSnapshotChunk() bool {
	if !tryAcquireSnapshotPermit(s.snapshotChunkPermits) {
		return false
	}
	active := s.snapshotMetrics.activeChunks.Add(1)
	observeAtomicHighWater(&s.snapshotMetrics.chunkHighWater, active)
	return true
}
func (s *SyncService) releaseSnapshotChunk() {
	s.snapshotMetrics.activeChunks.Add(-1)
	releaseSnapshotPermit(s.snapshotChunkPermits)
}
