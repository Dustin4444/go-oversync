# go-oversync

Sync local SQLite clients with a PostgreSQL-backed Go server.

go-oversync is a Go library suite for client-server database sync. It gives you the server
runtime, HTTP handlers, and SQLite client SDK pieces needed to replicate application data between
local SQLite databases and authoritative PostgreSQL business tables.

Use it when your app needs local database writes, reconnect-safe uploads, incremental downloads,
conflict handling, and snapshot recovery without hand-rolling the replication protocol.

## Why It Is Useful

go-oversync handles the sync machinery that is easy to underestimate:

- trigger-based dirty-row capture on SQLite clients
- staged push sessions for durable, retryable uploads
- authoritative PostgreSQL commits and replay back to clients
- complete committed-bundle pulls with stable checkpoints
- conflict reporting and client-side resolution hooks
- frozen snapshot rebuilds for fresh installs and retained-history recovery
- fail-closed schema validation so unsupported table shapes do not sync silently

Your application still owns its domain schema, auth, business logic, and HTTP server. go-oversync
owns the replication runtime around those pieces.

## What You Can Build

- Mobile apps that keep a local SQLite database and sync with server-side PostgreSQL.
- Desktop apps or local caches that replicate to a central Go backend.
- Multi-device account sync across phones, tablets, and laptops.
- Field tools that capture durable local writes and reconcile them when connectivity returns.

## Typical Architecture

```text
SQLite client database
  -> oversqlite client runtime (or your own over-HTTP implementation)
  -> your authenticated HTTP API
  -> oversync HTTP handlers
  -> PostgreSQL business tables plus sync metadata
```

PostgreSQL business tables are authoritative. Clients push local dirty rows, replay the committed
authoritative result, and pull complete committed bundles from the server. Fresh installs and
history-pruned clients rebuild from frozen server snapshots.

## Client Options

- Go clients can use `oversqlite` directly.
- Kotlin Multiplatform clients can use SQLiteNow KMP's optional OverSqlite sync component:
  <https://github.com/mobiletoly/sqlitenow-kmp>

SQLiteNow KMP is the mobile companion for Kotlin Multiplatform apps. This repository remains the
Go/PostgreSQL sync server foundation and includes a Go SQLite client SDK for Go-based clients and
simulators.

## Quick Start

Install the module:

```bash
go get github.com/mobiletoly/go-oversync
```

Run the fast checks:

```bash
go test ./oversync ./oversqlite
```

Start the reference server:

```bash
DATABASE_URL="postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable" \
JWT_SECRET="dev-secret" \
go run ./examples/nethttp_server
```

Run an implemented simulator scenario:

```bash
cd examples/mobile_flow
go run . --scenario=fresh-install --cleanup=false
```

## Server Integration

Register the PostgreSQL tables that participate in sync, bootstrap the runtime, and mount the
HTTP handlers behind your existing authentication middleware.

```go
cfg := &oversync.ServiceConfig{
    MaxSupportedSchemaVersion: 1,
    AppName:                   "my-sync-app",
    RegisteredTables: []oversync.RegisteredTable{
        {Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
        {Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
    },
}

svc, err := oversync.NewRuntimeService(pool, cfg, logger)
if err != nil {
    log.Fatal(err)
}
if err := svc.Bootstrap(ctx); err != nil {
    log.Fatal(err)
}

handlers := oversync.NewHTTPSyncHandlers(svc, logger)

syncActorMiddleware := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
    UserIDFromContext: func(ctx context.Context) (string, error) {
        return yourUserIDFromContext(ctx)
    },
})

// Wrap your auth middleware around the sync actor middleware, which is required
// to identify the sync scope for each request.
withSyncActor := func(next http.Handler) http.Handler {
    return yourAuthMiddleware(syncActorMiddleware(next))
}

mux := http.NewServeMux()
// Wire the handlers behind your auth and sync actor middleware.
// In most cases you can just copy these lines into your server setup
mux.Handle("POST /sync/connect", withSyncActor(http.HandlerFunc(handlers.HandleConnect)))
mux.Handle("POST /sync/push-sessions", withSyncActor(http.HandlerFunc(handlers.HandleCreatePushSession)))
mux.Handle("POST /sync/push-sessions/{push_id}/chunks", withSyncActor(http.HandlerFunc(handlers.HandlePushSessionChunk)))
mux.Handle("POST /sync/push-sessions/{push_id}/commit", withSyncActor(http.HandlerFunc(handlers.HandleCommitPushSession)))
mux.Handle("DELETE /sync/push-sessions/{push_id}", withSyncActor(http.HandlerFunc(handlers.HandleDeletePushSession)))
mux.Handle("GET /sync/committed-bundles/{bundle_seq}/rows", withSyncActor(http.HandlerFunc(handlers.HandleGetCommittedBundleRows)))
mux.Handle("GET /sync/pull", withSyncActor(http.HandlerFunc(handlers.HandlePull)))
mux.Handle("POST /sync/snapshot-sessions", withSyncActor(http.HandlerFunc(handlers.HandleCreateSnapshotSession)))
mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", withSyncActor(http.HandlerFunc(handlers.HandleGetSnapshotChunk)))
mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", withSyncActor(http.HandlerFunc(handlers.HandleDeleteSnapshotSession)))
mux.Handle("GET /sync/capabilities", withSyncActor(http.HandlerFunc(handlers.HandleCapabilities)))
mux.HandleFunc("GET /syncx/health", handlers.HandleHealth)
mux.HandleFunc("GET /syncx/status", handlers.HandleStatus)
```

`yourAuthMiddleware` must authenticate the request and expose trusted `user_id` in request context.
`oversync.ActorMiddleware(...)` reads `Oversync-Source-ID` from the client request and combines it
with that trusted user identity. The runtime derives user sync scope id from `Actor.UserID`.

## SQLite Client

Go clients can use `oversqlite` to install local metadata tables, capture local dirty rows, attach
an authenticated user, and run push/pull sync.

```go
cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
    {TableName: "users", SyncKeyColumnName: "id"},
    {TableName: "posts", SyncKeyColumnName: "id"},
})

client, err := oversqlite.NewClient(db, "http://localhost:8080", tokenProvider, cfg)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

if err := client.Open(ctx); err != nil {
    log.Fatal(err)
}

attachResult, err := client.Attach(ctx, "user-123")
if err != nil {
    log.Fatal(err)
}
if attachResult.Status == oversqlite.AttachStatusRetryLater {
    log.Printf("connect pending, retry after %s", attachResult.RetryAfter)
    return
}

syncReport, err := client.Sync(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("sync outcomes: push=%s remote=%s", syncReport.PushOutcome, syncReport.RemoteOutcome)

detachResult, err := client.Detach(ctx)
if err != nil {
    log.Fatal(err)
}
if detachResult.Outcome == oversqlite.DetachOutcomeBlockedUnsyncedData {
    log.Printf("detach blocked by %d pending rows", detachResult.PendingRowCount)
}
```

Kotlin Multiplatform clients should use SQLiteNow KMP's OverSqlite client instead of this Go SDK.
Both clients target the same go-oversync server protocol.

## How The Sync Model Works

The current contract is bundle-based:

- PostgreSQL business tables are authoritative.
- clients push one logical dirty set at a time through staged push sessions
- accepted pushes are replayed locally from the authoritative committed result
- clients pull complete committed bundles only
- fresh installs and prune recovery rebuild from frozen snapshot sessions

The goal is durable visibility at clear boundaries: a push either commits as one authoritative
bundle or fails, and a pull advances a local checkpoint only after complete committed bundles are
applied.

## Current Supported Envelope

Server-side registered tables are intentionally constrained:

- one sync key column per registered table
- visible sync key type must be `uuid` or `text` and the column must declare `NOT NULL`
- registered relations must be permanent logged PostgreSQL tables (`pg_class.relpersistence = 'p'`);
  `UNLOGGED` and temporary relations are unsupported
- registered PostgreSQL roots and every current partition/inheritance descendant must include
  `_sync_scope_id TEXT NOT NULL` and a non-null visible sync key
- registered PostgreSQL row identity must be scope-bound through `(_sync_scope_id, sync_key)`
- registered tables must be FK-closed
- registered-to-registered foreign keys must be scope-inclusive and `DEFERRABLE`
- unsupported key shapes or FK shapes fail during bootstrap

The SQLite client is likewise fail-closed:

- one configured remote schema per SQLite database
- one `oversqlite.Client` process owner per SQLite database
- managed local tables must be FK-closed
- the configured visible `TEXT`/`BLOB` primary-key column must declare `NOT NULL` explicitly

See `docs/getting-started.md` and `docs/documentation/server.md` for the full table requirements.

Oversync does not provide an in-place migration for deployments outside this supported envelope.
To reset one, stop every server and client, recreate PostgreSQL with permanent logged business
tables, recreate every client database, and deploy compatible server and client versions together.
This procedure discards business history, sync state, checkpoints, outboxes, and offline work;
mixed-version operation is unsupported.

Nullable identity declarations use a data-preserving operator migration rather than the destructive
reset above. Stop all writers and mixed-version instances, resolve existing NULL owner/key values
according to application policy, add explicit `NOT NULL` to every reported PostgreSQL
root/descendant or SQLite visible key, then retry bootstrap/initialization. Corrected binaries do not
change the wire, checkpoint, or snapshot contract, but older binaries do not enforce this invariant.

Registered-table `TRUNCATE` is always rejected with PostgreSQL SQLSTATE `55000`, including through
`CASCADE`, direct current-partition targets, and `WithinSyncBundle`. Administrative reset requires
the same stopped-process recreation of PostgreSQL and every client database; Oversync exposes no
runtime reset endpoint or bundle-context escape hatch.

Supported populated registered tables are adopted during `Bootstrap()` without rewriting their
business rows. Bootstrap locks registered writes and commits trigger installation plus one
replayable baseline bundle per populated scope atomically. Runtime operations remain unavailable
until bootstrap succeeds; inconsistent partial sync metadata fails closed. Existing populated
deployments require a coordinated stop, backup, upgraded bootstrap, readiness check, and restart—do
not run old and new server versions together during first adoption.

The PostgreSQL `sync` namespace is reserved for Oversync. Every startup validates the complete
`server_postgres_sync_v1` semantic layout—tables and columns, constraints and indexes, sequences,
managed functions, and reserved registered-table/partition triggers—before adoption or readiness.
A healthy marked layout is read-only during bootstrap, so managed function and trigger OIDs remain
stable. Missing, extra, disabled, invalid, or altered managed objects return a typed
`UnsupportedSchemaError` with expected/actual fingerprints and bounded field differences; bootstrap
does not recreate or repair them.

For a managed-layout rejection, stop every old and new server instance before changing DDL. Restore
the exact reported object while services remain stopped, or use the already-approved C2/C3
whole-database recreation procedure when exact repair is not trustworthy, then retry bootstrap.
Do not run an older binary during repair because it can recreate managed functions or triggers.
H4 changes no HTTP/wire shape, checkpoint, snapshot, bundle, or Go/KMP/Dart client durable state.

## Packages

- `oversync/`: PostgreSQL adapter, schema validation, bundle capture, HTTP handlers
- `oversqlite/`: SQLite client SDK with trigger-based dirty capture and sync loops
- `examples/nethttp_server/`: reference `net/http` server
- `examples/mobile_flow/`: end-to-end simulator for the current client/server contract
- `examples/samplesync_server/`: sample server for the KMP sample app
- `docs/`: Jekyll site content
- `swagger/two_way_sync.yaml`: OpenAPI description of the HTTP surface

## Documentation

- docs site: <https://mobiletoly.github.io/go-oversync/>
- getting started: `docs/getting-started.md`
- server reference: `docs/documentation/server.md`
- client reference: `docs/documentation/client.md`
- HTTP API reference: `docs/documentation/api.md`

## Examples

- `examples/nethttp_server/`: reference server with JWT auth and test helpers
- `examples/mobile_flow/`: simulator for implemented sync scenarios plus a small number of still-partial CLI entries
- `examples/samplesync_server/`: sample server used by the Kotlin sample app

## License

Apache 2.0. See `LICENSE` if present in your distribution or the source headers in this repository.
