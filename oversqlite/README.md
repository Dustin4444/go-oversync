# oversqlite

`oversqlite` is the SQLite client runtime for go-oversync's bundle-based sync protocol.

The client captures local writes with triggers, freezes one outbound bundle at a time, uploads that
bundle through push sessions, replays authoritative committed rows locally, pulls committed remote
bundles, and rebuilds from chunked snapshots when incremental history is unavailable or when a
durable rebuild gate is active.

## Managed Source Identity

`source_id` is first-class on the wire. Authenticated sync requests sent by `oversqlite` carry
`Oversync-Source-ID`. The host-facing contract is:

- `oversqlite` generates and persists source identity internally
- applications do not call `Open(ctx, sourceID)`
- applications do not rotate source identity manually
- applications do not provide replacement source ids during rebuild

The durable source/runtime state lives in:

- `_sync_source_state`
- `_sync_attachment_state.current_source_id`
- `_sync_operation_state`

## Public API

Lifecycle:

- `Open(ctx) error`
- `Attach(ctx, userID) (AttachResult, error)`
- `Detach(ctx) (DetachResult, error)`
- `PendingSyncStatus(ctx) (PendingSyncStatus, error)`
- `SyncStatus(ctx) (SyncStatus, error)`
- `SyncThenDetach(ctx) (SyncThenDetachResult, error)`
- `UninstallSync(ctx) error`

Sync:

- `PushPending(ctx) (PushReport, error)`
- `PullToStable(ctx) (RemoteSyncReport, error)`
- `Sync(ctx) (SyncReport, error)`
- `Rebuild(ctx) (RemoteSyncReport, error)`
- `LastBundleSeqSeen(ctx) (int64, error)`

Diagnostics:

- `SourceInfo(ctx) (SourceInfo, error)`

`SourceInfo` is debug-only. `CurrentSourceID` is opaque and must not be treated as a host-managed
lifecycle surface.

## Optional Watch Wakeups

`Config.BundleChangeWatchMode` defaults to `BundleChangeWatchOff`. Set
`BundleChangeWatchAuto` to let `Start(ctx)` use `/sync/watch` only when the server advertises
`bundle_change_watch=true`.

Watch events are wake-up hints only. `oversqlite` still applies data through `Sync(ctx)`,
`PullToStable(ctx)`, and snapshot recovery; the watch stream never mutates SQLite directly.

## Preconditions

Require no prior lifecycle step:

- `Open(ctx)`

Require successful `Open(ctx)`:

- `Attach(ctx, userID)`
- `PendingSyncStatus(ctx)`
- `SourceInfo(ctx)`
- `UninstallSync(ctx)`

Require successful `Open(ctx)` plus durable attached local state:

- `Detach(ctx)`

Require successful `Open(ctx)` plus successful `Attach(ctx, userID)`:

- `PushPending(ctx)`
- `PullToStable(ctx)`
- `Sync(ctx)`
- `Rebuild(ctx)`
- `SyncStatus(ctx)`
- `LastBundleSeqSeen(ctx)`
- `SyncThenDetach(ctx)`

## Recovery

- `PullToStable()` falls back to authoritative snapshot rebuild for pull-side `history_pruned` and
  `checkpoint_ahead`
- checkpoint recovery durably records `rebuild_required` before the snapshot and normal `Sync()` or
  `PullToStable()` automatically resumes it after interruption
- pending local work is frozen into the durable outbox and uploaded first; if it cannot be drained,
  recovery returns `CheckpointRecoveryBlockedError` without discarding the row, outbox, checkpoint,
  or rebuild gate
- the durable checkpoint changes only when authoritative snapshot apply completes atomically
- committed-remote replay pruned below the retained floor still uses keep-source rebuild behavior,
  advances local source sequencing past the already committed tuple, and clears committed-remote
  outbox state only after recovery succeeds
- stale/pruned, out-of-order, changed, and retired source cases still enter durable fail-closed
  source-recovery mode
- `PushPending()`, `PullToStable()`, and `Sync()` fail closed while source recovery is required;
  checkpoint recovery is automatically resumed by pull and sync
- `Rebuild(ctx)` remains explicit and attached/authenticated
- when source recovery is active, `Rebuild(ctx)` internally generates a fresh replacement source and
  preserves frozen outbox intent across the rotated rebuild

Source recovery reasons map to server errors:

- `history_pruned`
- `source_sequence_out_of_order`
- `source_sequence_changed`
- `source_retired`

Applications should not clear this state manually. Call `Rebuild(ctx)` after authentication and
let the client preserve pending outbox intent, rotate source identity when required, and resume
upload under the fresh source after snapshot recovery completes.

## Exact numeric columns and database compatibility

Oversqlite uses RFC 8785 JCS for canonical bytes. Configure exact numeric columns explicitly on
each `SyncTable`:

```go
NumericColumns: map[string]oversqlite.NumericColumnKind{
	"total_count": oversqlite.NumericColumnExactInt64, // SQLite INTEGER, JSON string
	"amount":      oversqlite.NumericColumnExactDecimal, // SQLite TEXT, JSON string
	"ratio":       oversqlite.NumericColumnApproximate, // SQLite REAL, JSON number
}
```

Exact integers must use canonical signed-64-bit text; exact decimals use the documented finite
decimal grammar and preserve their wire spelling. Unsupported grammar, range, or SQLite affinity
fails before row mutation. The client hashes the frozen request separately from the authoritative
committed bundle, so server-side field injection does not require comparing mutated rows with the
outbox.

This contract requires a fresh database. In-place migration, canonicalization or hash fallback,
and mixed-version operation are unsupported. When aligning an existing deployment with this
contract, recreate the client database and discard its outboxes, checkpoints, hashes, retry state,
and offline work.

## Quick Start

```go
db, err := sql.Open("sqlite3", "app.db")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
	{TableName: "users", SyncKeyColumnName: "id"},
	{TableName: "posts", SyncKeyColumnName: "id"},
})

tokenProvider := func(ctx context.Context) (string, error) {
	return "<jwt>", nil
}

client, err := oversqlite.NewClient(db, "http://localhost:8080", tokenProvider, cfg)
if err != nil {
	log.Fatal(err)
}
defer client.Close()

if err := client.Open(context.Background()); err != nil {
	log.Fatal(err)
}

attachResult, err := client.Attach(context.Background(), "user-123")
if err != nil {
	log.Fatal(err)
}
if attachResult.Status == oversqlite.AttachStatusRetryLater {
	log.Printf("attach pending; retry after %s", attachResult.RetryAfter)
	return
}

syncReport, err := client.Sync(context.Background())
if err != nil {
	log.Fatal(err)
}
log.Printf("sync outcomes: push=%s remote=%s", syncReport.PushOutcome, syncReport.RemoteOutcome)

if _, err := client.Rebuild(context.Background()); err != nil {
	log.Printf("rebuild failed: %v", err)
}
```

## Invariants

- `_sync_outbox_*` is durable and restart-resumable
- `PullToStable()` and `Rebuild(ctx)` fail closed while `_sync_outbox_*` exists
- `Sync()` fails closed while `_sync_attachment_state.rebuild_required = 1`
- `last_bundle_seq_seen` advances only after durable local apply
- exactly one `oversqlite.Client` should own one SQLite database at a time
- `Close()` releases client ownership of the SQLite database; it does not close the underlying
  `*sql.DB`
- `UninstallSync()` is more destructive than `Detach()` because it removes the installed sync
  metadata and triggers from the database
