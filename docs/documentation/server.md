---
layout: doc
title: Server
permalink: /documentation/server/
parent: Architecture
---

This page is the PostgreSQL/server runtime overview. It explains what the server owns, which sync
flows it serves, and which contracts host applications must satisfy. Detailed guidance for
server-originated writes lives on the separate
[Server-Originated Writes]({{ site.baseurl }}/documentation/server-originated-writes/) page.

## What The Server Owns

The server treats registered PostgreSQL business tables as authoritative state.

`oversync.SyncService` is the low-level PostgreSQL sync runtime. It owns:

- schema bootstrap and validation for the supported registered-table envelope
- first-connect lifecycle state
- staged push-session creation, chunk upload, and commit
- committed bundle capture from business-table transactions
- pull and snapshot serving
- optional process-local bundle change watch fanout backed by PostgreSQL `LISTEN`/`NOTIFY`
- source sequencing, retirement, and retained-history enforcement

## Runtime Tables

Layout ownership and registered-table catalog:

- `sync.meta`: one-row layout marker; startup fails closed if the layout name does not match the
  running build
- `sync.table_catalog`: deterministic registered-table catalog with compact `table_id`,
  visible sync-key column, and key kind

Authoritative replication state:

- `sync.user_state`: external `user_id`, internal `user_pk`, per-user bundle sequencing, and
  retained-floor tracking
- `sync.source_state`: durable per-source committed `source_bundle_id` watermark and source
  retirement/replacement state
- `sync.row_state`: authoritative current row version and tombstone state keyed by
  `(user_pk, table_id, key_bytes)`
- `sync.bundle_log`: one row per retained committed sync bundle, with the unique
  `(user_pk, source_id, source_bundle_id)` replay/idempotency key
- `sync.bundle_rows`: retained committed row effects using compact `table_id`, `key_bytes`, and
  `op_code`

Transport and lifecycle state:

- `sync.scope_state`: durable first-connect authority state keyed by `user_pk`
- `sync.push_sessions`: active staging-only push sessions
- `sync.push_session_rows`: compact staged rows for each push session
- `sync.snapshot_sessions`: active frozen snapshot session metadata
- `sync.snapshot_session_rows`: materialized rows for each snapshot session, ordered by compact
  table/key identity

Row-bearing server tables store compact internal identifiers (`user_pk`, `table_id`, `key_bytes`,
and `op_code`). Wire responses reconstruct visible `schema`, `table`, structured `key`, operation
strings, and payloads from `sync.table_catalog` and the stored row data.

Registered PostgreSQL integer, decimal, and floating business columns use canonical JSON strings
under `jcs_uniform_numeric_strings_v1`. Floating strings use the shortest finite RFC 8785 binary64
spelling; Boolean columns emit JSON Booleans. Bootstrap schema discovery owns this mapping, so
clients do not advertise or configure per-column numeric kinds.

## Main Flows

### First Connect

Clients call `POST /sync/connect` to resolve first authority for one scope.

Possible outcomes:

- `remote_authoritative`: the scope was already initialized, even if it is currently empty
- `initialize_local`: this source won the initialization lease and may seed server state from local
  pending rows
- `initialize_empty`: the scope was uninitialized and the server established authoritative empty
  state
- `retry_later`: another initializer currently owns the lease, or the server is applying a bounded
  empty-first deferral optimization

`retry_later` is a normal lifecycle result, not an auth failure.

### Push

The normal client write flow is:

- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{push_id}/chunks`
- `POST /sync/push-sessions/{push_id}/commit`

Accepted-push recovery fetches authoritative bundle rows through
`GET /sync/committed-bundles/{bundle_seq}/rows`.

Retained duplicate replay is resolved through `sync.bundle_log`, not through push-session state.
If the exact source tuple is older than retained bundle history but `sync.source_state` proves that
the source bundle id was already committed, the server returns `history_pruned` instead of
accepting it again. If a source sends a future bundle id, the server returns
`source_sequence_out_of_order`; commit revalidation can return `source_sequence_changed`.

### Pull And Snapshot

- `GET /sync/pull` returns complete committed bundles only
- `POST /sync/snapshot-sessions` materializes one frozen current after-image inside PostgreSQL
- `GET /sync/snapshot-sessions/{snapshot_id}` returns deterministic chunks from that frozen
  snapshot
- if a client checkpoint falls behind the retained bundle floor, the server returns
  `history_pruned`

Pull and committed-bundle replay are only guaranteed above `retained_bundle_floor`. Rows at or
below that floor are outside the retained-history contract even if physical pruning has not deleted
them yet. Snapshot creation reads live business tables plus `sync.row_state` from one PostgreSQL
transaction snapshot and fails closed if they disagree.
Materialization uses deterministic keyset pages and bounded PostgreSQL `COPY` batches. Snapshot
chunk reads are bounded by both row count and exact full-row JSON wire bytes. Non-blocking build
and chunk admission limits reject excess concurrency with HTTP `429` before opening a transaction.
Expired or explicitly retired sessions are reclaimed by each service's bounded cleanup worker.

### Bundle Change Watch

Servers can opt in to `BundleChangeWatch` to expose `GET /sync/watch`. Each process owns one
PostgreSQL listener connection plus a process-local subscriber hub. Bundle commits emit a small
`NOTIFY` payload inside the same transaction as `sync.bundle_log`; PostgreSQL delivers it only on
commit.

Watch events are not data delivery. They wake clients so they can run normal pull/sync paths.
`GET /sync/pull`, `sync.bundle_log`, and client checkpoints remain authoritative.

## Server-Originated Writes

If your application writes registered PostgreSQL tables outside client push handling, use:

- `ScopeManager.ExecWrite(...)` in the common case
- `WithinSyncBundle(...)` only when your application already manages exact
  `(user_id, source_id, source_bundle_id)` tuples directly

That topic has enough runtime detail to deserve its own page. See
[Server-Originated Writes]({{ site.baseurl }}/documentation/server-originated-writes/).

## Registered Table Requirements

Registered PostgreSQL tables must satisfy these rules before bootstrap:

- exactly one visible sync key column per registered table
- visible sync key type must be `uuid` or `text` and the column must declare `NOT NULL`
- every registered relation must be a permanent logged PostgreSQL table
  (`pg_class.relpersistence = 'p'`); `UNLOGGED` and temporary relations are unsupported
- every registered root and current partition/inheritance descendant must define
  `_sync_scope_id TEXT NOT NULL` and a non-null visible sync key
- `(_sync_scope_id, sync_key)` must be unique
- every unique constraint or unique index on a registered table must include `_sync_scope_id`
- registered table sets must be FK-closed
- registered-to-registered foreign keys must be scope-inclusive and `DEFERRABLE`
- supported `ON DELETE` / `ON UPDATE` actions are `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`,
  and `SET DEFAULT`
- supported `MATCH` options are empty, `NONE`, or `SIMPLE`
- `DEFERRABLE INITIALLY DEFERRED` is recommended
- `DEFERRABLE INITIALLY IMMEDIATE` is accepted
- partial, predicate, and expression unique indexes are unsupported on registered tables

Bootstrap fails closed with `UnsupportedSchemaError` when the registered schema is outside the
supported envelope. The persistence preflight runs before Oversync creates or accepts `sync.*`,
including for an already-marked layout.

Nullable identity admission is catalog-backed and fail-closed before `sync.*` or registered-trigger
mutation. Diagnostics list the logical root, physical relation, identity role, and column. Stop
writers, resolve NULL rows according to application policy, add explicit `NOT NULL` constraints,
and retry bootstrap. This coordinated migration is required before corrected binaries start because
older binaries can still accept nullable declarations; wire, checkpoint, pull, and snapshot meaning
does not change.

When no managed layout exists, Bootstrap atomically adopts supported registered tables that already
contain authoritative rows. It locks every registered root and current partition against writes,
installs the managed capture and TRUNCATE triggers, and creates one sequence-1 adoption baseline
bundle per populated scope in the same PostgreSQL transaction. The baseline records the state
observed at adoption time; it does not claim to recreate earlier modification history. Connect,
pull, push, and snapshot operations return `503 service_unavailable` until that transaction commits.

For the one-time adoption of an existing populated deployment, back up PostgreSQL, stop every old
Oversync server and managed writer, start the upgraded server and wait for healthy readiness, then
restore client traffic. Mixed old/new server operation during adoption is unsupported. Fresh
adoption never updates or deletes authoritative business rows and retains exact post-persistence
validation of row identity, payloads, history, hashes, counters, watermarks, and sessions.

An unsupported registered schema or managed definition is rejected without modifying the database.
Stop affected instances and restore a reviewed compatible backup or follow a separately reviewed
operator procedure. Oversync does not provide migration, repair, or rolling-upgrade behavior for
that database; mixed-version operation is unsupported.

Registered tables also have an unconditional statement-level `BEFORE TRUNCATE` guard. PostgreSQL
returns SQLSTATE `55000` with the registered `schema.table`; bundle context and `CASCADE` do not
bypass it. Current partitions are guarded individually because a directly targeted partition does
not fire its parent's statement trigger. Later partition or managed-trigger DDL is unsupported.
Administrative reset uses the same stopped-process, whole-database recreation procedure above;
there is no runtime reset endpoint or TRUNCATE-based reset.

## Managed `sync` layout validation

The entire PostgreSQL `sync` schema is reserved and server-managed. Do not add operator tables,
views, sequences, functions, rules, indexes, constraints, columns, or non-internal triggers there.
On application registered tables, only the three Oversync-reserved trigger names are managed;
application triggers and indexes with other names remain supported.

Every existing-layout `Bootstrap()` validates registered declarations, the marker and exact table
catalog, then compares the complete PostgreSQL 17.10 semantic manifest for
`server_postgres_sync_v1`. Validation covers relation
persistence/partition/RLS/replica identity, every column and default, PK/UNIQUE/CHECK/FK semantics,
constraint-backed and explicit indexes, identity and metric sequences, exact managed-function body
hashes and attributes, and the root/descendant trigger definitions and arguments. Expected and
actual facts are sorted and length-delimited before SHA-256 fingerprinting; field differences, not
hash equality alone, decide acceptance.

A coherent marked layout takes the trusted-database attachment path. It reads no registered business
rows or managed operational-state rows, takes no explicit business or managed data locks, and
performs no DML, DDL, adoption, canonicalization, hashing, cleanup, or repair. Drift fails closed
with `UnsupportedSchemaError` before readiness and preserves business rows, sync rows, bundles,
checkpoints, functions, and triggers. Diagnostics start with
`managed sync layout mismatch`, name the layout and both fingerprints, and include a bounded sorted
set of missing, unexpected, or changed attributes.

This fast path trusts PostgreSQL's committed durability. Ordinary direct registered-table DML
without bundle context and every registered-table `TRUNCATE` remain rejected, but privileged guard
bypass or direct managed-row edits are unsupported. Bootstrap does not detect or repair those data
mutations. Stop all Oversync instances and restore a known coherent backup or follow a separately
reviewed operator procedure. This validation adds no wire, checkpoint, snapshot, bundle, or client
durable-state version.

## Auth Contract

The handlers expect the host application to authenticate first and place
`oversync.Actor{UserID, SourceID}` into request context.

The built-in transport helper is `oversync.ActorMiddleware(...)`, which reads
`Oversync-Source-ID` after the host auth layer has already established trusted `user_id` in
request context.

`_sync_scope_id` is derived from `Actor.UserID`, enforced only on the authoritative PostgreSQL
side, and excluded from client-visible payloads, conflicts, pulls, and snapshots.

## Related Guides

- [Core Concepts]({{ site.baseurl }}/documentation/core-concepts/)
- [Server-Originated Writes]({{ site.baseurl }}/documentation/server-originated-writes/)
- [HTTP API]({{ site.baseurl }}/documentation/api/)
- [Performance]({{ site.baseurl }}/documentation/performance/)
