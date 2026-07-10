# Oversync Assurance Audit Report

Status: `complete`

Audit date: 2026-07-10

Execution spec: `specs/2026-07-10-oversync-assurance-audit.md`

Audit scope: `oversync/`

## Executive Summary

Phase 0 established the durable execution contract and evidence structure.
Phase 1 then replaced the manual database prerequisite with isolated,
test-owned PostgreSQL 17.10 provisioning. The unchanged default `oversync`
suite now passes without a pre-created database. No production behavior, public
API, wire shape, or runtime schema was changed.

Ordered local probes reproduce three Critical, nine High, four Medium, and one
Low confirmed finding, plus one unresolved Medium hypothesis. Audit-tagged
reproducers intentionally remain red for confirmed behaviors; the default
suite remains green. This report records evidence and later-fix requirements
only. No remediation is authorized in this audit.

Phase 8 completed the local Go 1.25.0 and 1.26.5 validation matrix, including
default and repository-wide tests, default and focused audit race lanes,
separated tagged green and expected-red groups, fuzz corpus replay, benchmark
smoke, vet, staticcheck, Drymint review, and direct document validation. The
audit is evidence-complete, not defect-free, and stops here for review.

Security research is explicitly excluded at the user's request on 2026-07-10.
This report does not assess vulnerabilities, exploitability, authentication,
authorization, database privileges, or security posture, and it must not be
read as a security certification.

The audit is also local-only by request on 2026-07-10. It adds no GitHub
Actions, CI, scheduled remote audit, or repository-hosted audit automation.

## Environment and Baseline

### Repository

| Field            | Baseline value                                                      |
|------------------|---------------------------------------------------------------------|
| Repository       | `github.com/mobiletoly/go-oversync`                                 |
| Branch           | `main`                                                              |
| Commit           | `1f1118f1749490daaa8f1f76a7d8516f5303a873`                          |
| Tag              | `v0.2.1` (`v0.2.1-0-g1f1118f`)                                      |
| Initial worktree | Modified `drymint.toml` and `drymint/triage.yaml`; no other changes |
| Audit scope      | `oversync/`                                                         |

The two Drymint files are pre-existing user changes and are outside the audit.
They must be preserved without modification.

### Toolchain and Database Matrix

| Field                           | Baseline value        |
|---------------------------------|-----------------------|
| Module Go directive             | `go 1.25.0`           |
| Active Go toolchain             | `go1.26.5`            |
| Active platform                 | `darwin/arm64`        |
| `GOTOOLCHAIN`                   | `auto`                |
| Workspace file                  | None (`GOWORK` empty) |
| PostgreSQL certification target | PostgreSQL 17.10      |
| Local `psql` client             | PostgreSQL 18.4       |
| Docker server                   | 29.2.1                |

PostgreSQL 17.10 is the locked certification target, not a completed baseline.
Phase 0 did not provision or validate a PostgreSQL 17.10 server.

### Existing Test Inventory

- 184 top-level `func Test...` declarations across 21 test files in
  `oversync/`.
- 22 `*_test.go` files total, including the shared integration helper file.
- No `func Benchmark...` declarations in `oversync/`.
- No `func Fuzz...` declarations in `oversync/`.
- Count method: `rg -c '^func Test' oversync -g '*_test.go'`, summed across the
  reported files.

### Baseline Commands

| Command                                                                                            | Result        | Evidence                                                                                                                                                                                     |
|----------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `go vet ./oversync`                                                                                | PASS          | Exit 0 with no diagnostics on 2026-07-10                                                                                                                                                     |
| `go test ./oversync`                                                                               | BLOCKED       | Exit 1; DB-backed tests cannot connect because database `clisync_test` does not exist (`SQLSTATE 3D000`)                                                                                     |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.25.0 go test -count=1 ./oversync`                        | PASS          | Local Go 1.25.0 lane; PostgreSQL 17.10 was provisioned automatically; package passed in 23.989s                                                                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./oversync`                        | PASS          | Local Go 1.26.5 lane; PostgreSQL 17.10 was provisioned automatically; package passed in 24.219s                                                                                              |
| Focused Phase 1 audit-tagged harness on Go 1.25.0 and 1.26.5                                       | PASS          | Helper process, TCP proxy, integrity oracle, and 30-operation multi-scope reference model passed in 3.819s and 3.916s                                                                        |
| `staticcheck ./oversync`                                                                           | FINDINGS      | Exit 1; 13 existing unused declarations (`U1000`), recorded as Low maintenance evidence                                                                                                      |
| `go vet ./oversync` and `go vet -tags=oversync_audit ./oversync`                                   | PASS          | Phase 2 local Go 1.26.5 runs exited 0 without diagnostics                                                                                                                                    |
| `staticcheck -tags=oversync_audit ./oversync`                                                      | FINDINGS      | Phase 2 reported the same 13 `U1000` declarations as the default build; by Phase 8 the audit harness exercised `preparePushRows`, leaving 12 from that same set and no audit-file diagnostic |
| Ruby/Psych parse of `/sync/pull` OpenAPI parameters                                                | FINDING       | Parsed list contains only query parameters; duplicate YAML key discards the source-header declaration (`OS-AUD-014`)                                                                         |
| Audit exactness/retention/token reproducers                                                        | EXPECTED FAIL | Four deterministic failures mapped to `OS-AUD-001`, `OS-AUD-003`, and `OS-AUD-008`                                                                                                           |
| Audit HTTP strictness reproducers                                                                  | EXPECTED FAIL | Unknown fields, trailing JSON, and unsupported content type each returned HTTP 200; mapped to `OS-AUD-009`                                                                                   |
| `TestAuditIntegrityOracle_AfterCommittedAndStagedWork`                                             | PASS          | Bundle/source/session/snapshot metadata oracle passed after one committed bundle and one partial staged session                                                                              |
| Audit canonical JSON fuzzing                                                                       | EXPECTED FAIL | Minimized valid JSON input `1e700` cannot be represented by the current `float64` decode path                                                                                                |
| Canonical JSON benchmark, five samples                                                             | PASS          | 2410-2445 ns/op, 1873 B/op, 43 allocs/op on Apple M2 Max / Go 1.26.5                                                                                                                         |
| Phase 3 `go test -count=1 ./oversync`                                                              | PASS          | Local Go 1.26.5 / PostgreSQL 17.10 package passed in 21.173s after Phase 2 closeout                                                                                                          |
| Phase 3 exactness/token/retention contract group                                                   | EXPECTED FAIL | Exact JSON numbers rounded, fractional `Int64Field` accepted, checkpoint 0 accepted below floor, and exact actor tokens trimmed; 0.403s                                                      |
| Phase 3 PostgreSQL contract group                                                                  | EXPECTED FAIL | Row/byte limits, populated adoption, nullable keys, TRUNCATE, marked-layout drift, and checkpoint 0 each reproduced; 4.361s                                                                  |
| Phase 3 integrity oracle and seed `20260710` reference model                                       | PASS          | One staged/committed scenario plus 30 multi-scope operations passed all business-row, metadata, pull, snapshot, and cleanup comparisons in 3.411s                                            |
| `FuzzAuditCanonicalJSON_Idempotent` baseline corpus                                                | EXPECTED FAIL | Minimized corpus value `1e700` fails before fuzz mutation because canonicalization requires `float64` representation                                                                         |
| Phase 3 exact-value characterization group                                                         | PASS          | Nested/exponent JSON, Unicode non-normalization/common ordering, bytea round-trip, and deterministic logical hashing passed in 0.507s                                                        |
| Phase 3 exact-value contract group                                                                 | EXPECTED FAIL | Int64/decimal/nested rounding, `1e700`, RFC 8785 UTF-16 order, adjacent-number hash collision, and PostgreSQL bundle/pull corruption reproduced in 1.492s                                    |
| Phase 3 bootstrap atomicity probes                                                                 | PASS          | Parallel bootstrap convergence plus cancelled lock-wait rollback/retry passed; ten repeats and focused race run also passed                                                                  |
| Phase 3 supported schema-edge group                                                                | PASS          | Partitioned parent, reserved identifiers, mixed-case rejection, and view rejection were coherent and atomic; 1.836s                                                                          |
| Phase 3 generated/unlogged schema group                                                            | EXPECTED FAIL | Generated after-image replay fails with `428C9`; unlogged authoritative table is accepted with `relpersistence='u'`; 1.498s                                                                  |
| Phase 3 checkpoint boundary and post-prune snapshot group                                          | MIXED         | Snapshot reconstructs all three authoritative rows; checkpoint 0/equality/future-target boundaries fail as recorded in `OS-AUD-003`; 2.472s                                                  |
| Final Phase 3 green audit subset                                                                   | PASS          | Bootstrap atomicity, exact-value characterization, coherent schema edges, integrity oracle, seeded model, and post-prune snapshot passed together in 5.514s; tagged vet passed               |
| Prescribed Phase 4 baseline                                                                        | PASS          | Go 1.26.5/PostgreSQL 17.10 helper, proxy, terminated connection, and cancelled advisory-lock probes passed in 3.891s                                                                         |
| Phase 4 consolidated green group                                                                   | PASS          | Go 1.26.5 process, HTTP-response, transaction, two-service concurrency, retry, and PostgreSQL restart probes passed in 13.398s; Go 1.25.0 passed in 13.770s                                  |
| Phase 4 focused race group                                                                         | PASS          | Go 1.26.5 bootstrap, shutdown, watch/listener, process/proxy, concurrency, retry, and restart group passed in 23.781s                                                                        |
| Phase 4 expected-contract group                                                                    | EXPECTED FAIL | OS-AUD-018 and OS-AUD-019 failed independently while every integrity-oracle subcheck passed; package exited 1 in 3.401s                                                                      |
| Phase 4 lifecycle/concurrency disposition group                                                    | PASS          | Existing server-write, source-rotation, pruning/pull, snapshot/prune, cleanup, session-creation, and listener-reconnect tests passed in 2.648s                                               |
| Phase 5 consolidated green group                                                                   | PASS          | Go 1.26.5 protocol, pressure, SSE, lifecycle, and drift probes passed in 4.511s; Go 1.25.0 passed in 4.490s                                                                                  |
| Phase 5 focused race group                                                                         | PASS          | The consolidated Go 1.26.5 audit-tagged green group passed under race detection in 6.194s                                                                                                    |
| Phase 5 expected-contract group                                                                    | EXPECTED FAIL | Exact-token, configured-limit, strict-decoder, malformed-path, drift, and post-close response contracts failed only at the desired assertions in 2.778s                                      |
| Phase 5 protocol fuzz smoke                                                                        | PASS          | Six bounded pure-parser targets completed 100 executions each; canonical JSON remains owned by its existing expected-failing target and corpus                                               |
| `git diff --no-index --check -- /dev/null specs/2026-07-10-oversync-assurance-audit.md`            | PASS          | Expected new-file exit 1 with no whitespace diagnostics                                                                                                                                      |
| `git diff --no-index --check -- /dev/null engineering-docs/audit/2026-07-10-oversync-assurance.md` | PASS          | Expected new-file exit 1 with no whitespace diagnostics                                                                                                                                      |

The original package failure was an environment/prerequisite blocker, not a
production defect. Phase 1 now lazily starts `postgres:17.10`, creates a unique
database for each integration test, and removes all test-owned resources in
`TestMain`. A non-empty `TEST_DATABASE_URL` remains an exact caller-managed
override and is serialized in-process to preserve the previous shared-schema
behavior.

## Methodology

Phases 0 through 8 are complete. Assets produced in parallel did not complete
or advance a phase
until all earlier exit criteria are met and their evidence is re-run in order. The
audit combines:

- A reproducible PostgreSQL 17.10 test foundation.
- Static contract and implementation tracing.
- A SQL integrity oracle and deterministic reference model.
- Deterministic correctness and schema-edge probes.
- Process, connection, transaction, and PostgreSQL fault injection.
- Protocol-conformance, exact-token, configured-limit, malformed-input,
  lifecycle, and resource-pressure analysis for correctness and reliability.
- Benchmarks, query plans, resource profiles, and capacity curves.

For every command or experiment, this report records the exact environment,
seed, command, exit status, relevant output, and artifact location. A test
checkbox in the execution spec means evidence was captured, not that a defect
was remediated.

## Contract and Implementation Inventory

### Exported Operation Map

Every exported `SyncService` operation and the higher-level write/watch
surfaces were traced to their transaction and failure boundaries:

| Surface                                                                                                               | Transaction and ordering contract                                                                                                                       | Primary failure contract                                                                                          |
|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `NewRuntimeService`                                                                                                   | Constructs runtime state only; database work is deferred                                                                                                | Configuration validation only                                                                                     |
| `Bootstrap`                                                                                                           | Retryable schema transaction followed by trigger installation under the global bootstrap advisory lock                                                  | Unsupported registered schema, marked-layout drift, trigger install, or database failure                          |
| `Connect`                                                                                                             | Per-scope session advisory lock and one transaction locking `scope_state`                                                                               | Invalid transition, initialization lease state, lifecycle, or database failure                                    |
| `CreatePushSession`                                                                                                   | Per-scope advisory lock and up to three retryable transactions                                                                                          | Invalid request, scope/lease state, retained tuple, source sequence, lifecycle, or database failure               |
| `UploadPushChunk`                                                                                                     | Per-scope lock; retryable transaction; session row lock; `CopyFrom` plus cursor/expiry update                                                           | Invalid row/chunk, cursor mismatch, missing/expired session, stale initialization, lifecycle, or database failure |
| `CommitPushSession`                                                                                                   | Per-scope lock; retryable transaction atomically applies business DML, capture, bundle/hash metadata, source watermark, retention, and session deletion | Incomplete staging, conflict, sequence change, lease/session state, lifecycle, or any apply/finalization failure  |
| `GetCommittedBundleRows`                                                                                              | Repeatable-read transaction and deterministic bounded row page                                                                                          | Invalid cursor/limit, pruned/missing bundle, lifecycle, or database failure                                       |
| `DeletePushSession`                                                                                                   | Transaction plus session row lock; row storage cascades                                                                                                 | Invalid/missing/expired session, lifecycle, or database failure                                                   |
| `ProcessPull`                                                                                                         | Read-only repeatable-read transaction with a frozen stable bundle ceiling                                                                               | Invalid bounds, uninitialized scope, pruned checkpoint, bundle load, lifecycle, or database failure               |
| `CreateSnapshotSession`, `CreateSnapshotSessionWithRequest`                                                           | Repeatable-read transaction atomically validates, materializes, bounds, and persists a frozen snapshot                                                  | Scope/source state, live-row mismatch, configured bound, lifecycle, or database failure                           |
| `GetSnapshotChunk`, `DeleteSnapshotSession`                                                                           | Repeatable-read bounded fetch or transactional delete with cascading rows                                                                               | Invalid cursor/session, expiry, lifecycle, or database failure                                                    |
| `WithinSyncBundle`                                                                                                    | Per-scope advisory lock and one captured business transaction                                                                                           | Invalid source/callback, scope/source sequence, capture/finalization, lifecycle, or database failure              |
| `ScopeManager.ExecWrite`                                                                                              | Per-scope advisory lock and one captured transaction with optional empty-scope initialization                                                           | Invalid input, initializing scope, no captured changes, lifecycle, or database failure                            |
| `RunBundleChangeListener`, `SubscribeBundleChanges`                                                                   | Dedicated reconnecting `LISTEN` connection plus a short read-only subscription validation transaction                                                   | Disabled feature, invalid checkpoint/scope, cancellation, lifecycle, or listener/database failure                 |
| `Close`                                                                                                               | Rejects new operations, waits for tracked in-flight operations, and deliberately leaves the caller-owned pool open                                      | Context expiry while draining                                                                                     |
| `GetStatus`, `GetCapabilities`, `GetSchemaVersion`, `Pool`, `IsTableRegistered`, `BundleChangeSubscriberCountForTest` | Snapshot/getter behavior; only status and subscriber count query PostgreSQL                                                                             | Query failure where applicable                                                                                    |

The public helper surface was also reviewed. `ActorMiddleware`, payload and
binary conversion helpers, schema discovery, and configuration/model types are
ordinary in-process helpers. `DiscoverSchemaWithDependencyOverrides` is an
exported method whose parameter includes the unexported
`registeredTableRuntimeInfo` type, making meaningful external invocation
impractical; this is recorded under `OS-AUD-014`.

### HTTP Handler Map

All 14 handlers were traced through `oversync/http_handlers.go:114-862`:

| HTTP surface                                    | Service operation                  | Normal response            | Notable failure mapping                                                |
|-------------------------------------------------|------------------------------------|----------------------------|------------------------------------------------------------------------|
| `GET /syncx/health`                             | `GetStatus`                        | 200 healthy, 503 unhealthy | Status-query failure becomes 500 but is absent from OpenAPI            |
| `GET /syncx/status`                             | `GetStatus`                        | 200 status snapshot        | Status-query failure becomes 500 but is absent from OpenAPI            |
| `POST /sync/connect`                            | `Connect`                          | 200 resolution             | Decode/transition errors mapped to 400/409/410/500/503                 |
| `POST /sync/push-sessions`                      | `CreatePushSession`                | 200 staging or replay      | Typed request/scope/sequence/session mappings plus generic 500/503     |
| `POST /sync/push-sessions/{push_id}/chunks`     | `UploadPushChunk`                  | 200 next ordinal           | Typed validation/session mappings plus generic 500/503                 |
| `POST /sync/push-sessions/{push_id}/commit`     | `CommitPushSession`                | 200 authoritative commit   | Typed conflict/session/source mappings plus generic 500/503            |
| `DELETE /sync/push-sessions/{push_id}`          | `DeletePushSession`                | 204                        | Documentation advertises an unreachable committed-session error branch |
| `GET /sync/committed-bundles/{bundle_seq}/rows` | `GetCommittedBundleRows`           | 200 deterministic page     | 400/404/410 plus generic 500/503                                       |
| `GET /sync/pull`                                | `ProcessPull`                      | 200 frozen page            | Runtime emits `invalid_request`; API docs say `pull_invalid`           |
| `GET /sync/watch`                               | `SubscribeBundleChanges`           | 200 SSE wakeups/heartbeats | Documented responses omit implemented setup failures                   |
| `POST /sync/snapshot-sessions`                  | `CreateSnapshotSessionWithRequest` | 200 frozen session         | `SnapshotSessionLimitExceededError` falls through to 500               |
| `GET /sync/snapshot-sessions/{snapshot_id}`     | `GetSnapshotChunk`                 | 200 deterministic page     | Typed cursor/session mappings plus generic 500/503                     |
| `DELETE /sync/snapshot-sessions/{snapshot_id}`  | `DeleteSnapshotSession`            | 204                        | Typed session mappings plus generic 500/503                            |
| `GET /sync/capabilities`                        | `GetCapabilities`                  | 200 advertised surface     | Encoding/lifecycle policy only                                         |

### Managed PostgreSQL Inventory

`initializeSchemaInTx` owns 13 tables: `meta`, `table_catalog`, `user_state`,
`scope_state`, `source_state`, `row_state`, `bundle_capture_stage`, `bundle_log`,
`bundle_rows`, `push_sessions`, `push_session_rows`, `snapshot_sessions`, and
`snapshot_session_rows`. The layout also owns:

- Three metric sequences: `accepted_push_replay_seq`,
  `rejected_registered_write_seq`, and `history_pruned_error_seq`.
- Two PL/pgSQL functions for registered-row owner consistency and row capture.
- Two triggers per registered table: a `BEFORE` owner guard and an `AFTER`
  capture trigger for row-level INSERT/UPDATE/DELETE.
- Five explicit indexes for capture ordering, live snapshot lookup, session
  expiry, and snapshot logical identity. The logical-identity index duplicates
  an existing unique constraint and is part of `OS-AUD-011`.
- One global transaction advisory lock for bootstrap and one session advisory
  lock keyed by exact scope identity for serialized writers.

Cleanup is lazy and transactional. Push creation removes expired push sessions;
restart, commit, and explicit delete remove staged storage; snapshot creation
removes expired snapshots; retention deletes bundle logs and cascades rows;
capture finalization clears staging; and watch cancellation unregisters the
process-local subscription. Service close drains operations and listener state
but leaves the caller-owned pool open by contract.

### Critical Data-Path Trace

| Path                     | Observed implementation                                                                                                                          | Audit disposition                                                                                                                          |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| Exact identities         | Actor middleware, `ScopeManager`, snapshot source replacement, and selected validation paths use `TrimSpace`                                     | Exact-token aliasing is `OS-AUD-008`                                                                                                       |
| JSON request decode      | Create, chunk, and connect use one unbounded `Decode`; snapshot disallows unknown fields but does not enforce EOF                                | Parser and bound inconsistency is `OS-AUD-009`                                                                                             |
| JSON persistence/hash    | Untyped `json.Unmarshal` converts numbers to `float64`; canonicalization and committed hash repeat the conversion                                | Exact-number corruption is `OS-AUD-001`                                                                                                    |
| Binary conversion        | Payload maps convert registered `bytea` fields to/from canonical base64 before wire canonicalization                                             | Phase 3 binary round-trip and exact-value characterization passed                                                                          |
| Bundle limits            | `MaxRowsPerBundle` and `MaxBytesPerBundle` are advertised but not enforced; chunk/page/snapshot limits are enforced after decode/materialization | Missing early bounds are `OS-AUD-004` and `009`                                                                                            |
| Retry and isolation      | Bootstrap and push-session mutations retry selected PostgreSQL transaction errors; pull and chunk reads use repeatable read                      | Phase 4 proves rollback/retry for connection loss, lock timeout, and injected `40001`/`40P01`; natural lock-graph formation is not claimed |
| Locking                  | Global bootstrap advisory lock, per-scope session advisory lock, row locks, optional local `lock_timeout`                                        | Cancellation cleanup passes; pool-wide waiter starvation is `OS-AUD-019`                                                                   |
| Clocks                   | Session/lease creation and validation use application `time.Now`; lazy cleanup uses database `now()`                                             | Multi-instance skew candidate is `OS-AUD-020`                                                                                              |
| Pull loading             | One query lists bundle IDs, followed by repeated user, metadata, and row queries per bundle                                                      | Query amplification is `OS-AUD-011`                                                                                                        |
| Snapshot materialization | All rows are held in Go memory, bounds are checked afterward, then rows are inserted one statement at a time                                     | Resource and scaling issue is `OS-AUD-011`                                                                                                 |

### Executable Invariant Assertions

- **Atomic commit:** business state, row state, bundle log/rows, source
  watermark, retention state, and notification visibility must either all
  reflect one bundle sequence or all remain unchanged.
- **Idempotent replay:** each exact `(scope, source, source_bundle_id)` tuple
  maps to at most one immutable committed bundle and repeated commit returns
  that same metadata without another business effect.
- **Source sequencing:** committed source watermarks are monotonic; gaps,
  conflicting stale tuples, and retired-source commits produce no state change.
- **Scope identity consistency:** byte-for-byte distinct scope/source tokens
  never share business rows, metadata, locks, sessions, snapshots, or watches.
- **Row-state equivalence:** every live business row has one non-deleted
  `row_state` entry with the same key and latest bundle sequence; every live
  state has one business row; tombstones have no business row.
- **Bundle integrity:** logged row count equals contiguous ordinals and stored
  rows; payloads, keys, operations, row versions, byte count, and hash are
  deterministic and complete.
- **Retention safety:** any checkpoint below a positive retained floor fails
  with `history_pruned`; no successful response silently omits required
  history; authoritative row/source state survives pruning.
- **Snapshot completeness:** the frozen snapshot contains each live row exactly
  once at or below its stable bundle sequence, excludes tombstones, remains
  chunk-stable, and survives allowed history pruning.
- **Bootstrap safety:** bootstrap is repeatable and atomic, validates the full
  managed layout, and either adopts populated registered rows coherently or
  fails without readiness or partial metadata.
- **Resource cleanup and shutdown:** cancellation, expiry, explicit deletion,
  connection/process failure, and close release locks, pool slots, staged rows,
  listeners, subscriptions, goroutines, and helper resources within bounded
  time.

## Invariant Coverage

| Invariant                  | Normal                                                                         | Concurrent                                                                                          | Failure-injected                                                                                      | Status                                         | Evidence                                        |
|----------------------------|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|------------------------------------------------|-------------------------------------------------|
| Atomic commit              | Existing rollback/capture tests pass                                           | Two-service same-scope serialization passes                                                         | Cancellation, process kill, and PostgreSQL crash at DML/finalization/pruning roll back coherently     | Passed within audited logged-table envelope    | Phase 4 fault/restart group                     |
| Idempotent replay          | Existing duplicate-commit tests pass                                           | Existing concurrent creation tests pass                                                             | Ambiguous commit resolves to one tuple/hash/effect                                                    | Passed within retained-history envelope        | Phase 4 process recovery                        |
| Source sequencing          | Existing stale/out-of-order/rotation tests pass                                | Two-service independent sources serialize coherently                                                | Staged/restarted and retry paths remain monotonic                                                     | Passed except recorded boundary/token findings | Phase 3/4 groups                                |
| Scope identity consistency | Existing exact-token behavior partially covered                                | Existing scope concurrency tests pass                                                               | Normalization reproducer fails                                                                        | Failed                                         | `OS-AUD-006`, `008`                             |
| Row-state equivalence      | Existing stress/cascade and seeded model pass                                  | Existing mixed-operation stress passes                                                              | TRUNCATE and unlogged crash recovery fail                                                             | Failed                                         | `OS-AUD-002`, `018`                             |
| Bundle integrity           | Ordinary deterministic/hash vectors pass                                       | Existing ordering stress passes                                                                     | Exact numeric/hash vectors fail                                                                       | Failed                                         | `OS-AUD-001`                                    |
| Retention safety           | Post-prune snapshot is complete                                                | Pull/prune repeatable-read test passes                                                              | Zero/equality/future-target boundaries fail                                                           | Failed                                         | `OS-AUD-003`                                    |
| Snapshot completeness      | Frozen/chunk and post-prune creation pass                                      | Existing prune/session tests pass                                                                   | Populated-table adoption fails                                                                        | Failed                                         | `OS-AUD-005`, `011`                             |
| Bootstrap safety           | Partitioned/reserved shapes are coherent; unsupported shapes reject atomically | Parallel and cancelled/retried bootstrap pass, including race                                       | Populated, nullable, damaged, generated, and unlogged shapes expose findings                          | Failed                                         | `OS-AUD-005`, `006`, `007`, `017`, `018`        |
| Resource cleanup           | Existing close/watch cleanup tests pass                                        | Bounded session/subscriber pressure cleans up; one blocked waiter leaves an independent slot usable | Pool/listener restart recovery passes; waiter saturation, missing quotas, and post-close mapping fail | Failed                                         | `OS-AUD-004`, `009`, `011`, `013`, `015`, `019` |

### Phase 4 Durability, Concurrency, and Recovery Evidence

All Phase 4 tests use the `oversync_audit` build tag. The PostgreSQL restart
harness ignores `TEST_DATABASE_URL`, owns a named data volume, sends SIGKILL,
recreates PostgreSQL 17.10 against the same volume and host port, and removes
both container and volume deterministically.

The failure-stage matrix produced these outcomes:

- Cancellation while acquiring the per-scope advisory lock proves the
  pre-transaction path releases its pool slot. A test trigger blocking
  `sync.bundle_log` insertion proves cancellation during metadata finalization
  rolls back business DML, capture rows, bundle/source state, and session
  deletion before the same `push_id` commits once.
- Killing the helper while business DML is blocked leaves the durable staged
  session intact and no partial business or metadata effect. Restarting the
  real helper and retrying the same `push_id` commits once.
- A response gate buffers the real commit result after PostgreSQL commit. The
  proxy then drops the client connection before response delivery. Database
  polling proves one business row, row-state row, bundle, bundle row, source
  watermark, and hash; recreating the same exact source tuple returns
  `already_committed` with that sequence and hash.
- Lost-response retries are deterministic. Create replaces the unknown empty
  staging session with one new session; an accepted chunk retry returns
  `push_chunk_out_of_order` with the advanced cursor and leaves the accepted row
  intact; committed tuples resolve through create; successful delete retries
  return not found; fixed-target pull and snapshot reads are identical; and
  snapshot delete retries return not found. An ambiguous snapshot-create retry
  leaves two coherent TTL-bounded sessions, extending the quota concern in
  `OS-AUD-011` rather than creating inconsistent state.
- SIGKILL during a blocked commit, pull, second snapshot-row insertion, and
  pruning DELETE returns a connection error and leaves no partial transaction.
  The exact commit/pull/snapshot/prune request then succeeds after the original
  pool reconnects. Staged rows and frozen snapshots survive; listener catch-up
  wakes the subscriber after reconnect.
- Two independently pooled `SyncService` instances serialize same-scope pushes
  into bundle sequences 1 and 2 while different scopes each reach sequence 1.
  With `MaxConns=2`, one blocked scope uses one slot and an independent scope
  completes; cancellation releases the slot and retry succeeds. Two blocked
  waiters, however, pin both slots and reproduce `OS-AUD-019`.
- Pool exhaustion before transaction start, a real `55P03` row-lock timeout,
  and a terminated connection all leave retryable state. Test triggers that
  raise PostgreSQL `40001` and `40P01` prove the configured classifier retries
  exactly once and commits once. These are deterministic SQLSTATE injections,
  not natural serialization-conflict or deadlock lock graphs; natural graph
  formation is explicitly not claimed.

Existing focused tests disposition the remaining concurrency combinations:
`ScopeManager.ExecWrite` concurrent server writes, pull versus prune under
repeatable read, active snapshot versus pruning, equivalent source rotation,
expired-snapshot cleanup, concurrent session creation, multiple-service watch
delivery, and listener reconnect all pass. They share the same per-scope lock,
transaction, source-state, retention, and cleanup owners exercised by the new
two-service faults; Phase 4 does not claim exhaustive scheduler interleavings.
The integrity oracle runs after each injected fault and retry. The consolidated
Go 1.26.5 green group passed in 13.398s, the Go 1.25.0 group in 13.770s, and the
focused Go 1.26.5 race group in 23.781s.

The baseline-backed Drymint change review required its emitted one-time index
refresh, then attributed all seven Phase 4 Go paths. Its only inspect item was
the two-line `auditMultiServiceFixture.actor` helper matching the equivalent
fixture-local watch helper. Detailed source and caller inspection classified it
as low-substance, low-value test-fixture duplication with no shared owner;
refactoring or triage mutation was not warranted.

Exact Phase 4 command evidence on 2026-07-10:

| Command                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Exit/result                                                                                                                                                          |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditFault_\|TestAuditHarness_HelperProcessStartsServesRealHandlersAndStops\|TestAuditTCPProxy_)' -count=1 -v`                                                                                                                                                                                                                                                                                                                                                                                              | Exit 0; 3.891s                                                                                                                                                       |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditTransactionFault_CancelDuringMetadataFinalizationRollsBackAndRetries$' -count=1 -v`                                                                                                                                                                                                                                                                                                                                                                                                                     | Exit 0; 3.452s                                                                                                                                                       |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditHTTPRecovery_' -count=1 -timeout=2m -v`                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Exit 0; 1.895s                                                                                                                                                       |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditFault_\|TestAuditHarness_HelperProcessStartsServesRealHandlersAndStops\|TestAuditTCPProxy_\|TestAuditProcessRecovery_\|TestAuditHTTPRecovery_\|TestAuditTransactionFault_\|TestAuditConcurrency_(TwoServices\|BlockedScopeAllows)\|TestAuditRetryFault_\|TestAuditPostgresRestart_(LoggedState\|InFlight))' -count=1 -timeout=10m -v`                                                                                                                                                                  | Exit 0; 13.398s                                                                                                                                                      |
| Same consolidated green command with `GOTOOLCHAIN=go1.25.0` and without `-v`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Exit 0; 13.770s                                                                                                                                                      |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditConcurrencyContract_BlockedScopeWaitersDoNotStarveIndependentScope\|TestAuditPostgresRestart_UnloggedAcknowledgedRowMustNotDisappearWhileMetadataSurvives)$' -count=1 -timeout=5m -v`                                                                                                                                                                                                                                                                                                                  | Expected exit 1; 3.401s; both desired-contract assertions failed and all oracle subtests passed                                                                      |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -race -tags=oversync_audit ./oversync -run '^(TestAuditBootstrapAtomicity_\|TestAuditFault_\|TestAuditHarness_HelperProcessStartsServesRealHandlersAndStops\|TestAuditTCPProxy_\|TestAuditProcessRecovery_\|TestAuditHTTPRecovery_\|TestAuditTransactionFault_\|TestAuditConcurrency_(TwoServices\|BlockedScopeAllows)\|TestAuditRetryFault_\|TestAuditPostgresRestart_(LoggedState\|InFlight)\|TestSyncService_CloseWaitsForInflightOperations\|TestRunBundleChangeListener_ReconnectCatchUpWakesActiveSubscribers)' -count=1 -timeout=15m`            | Exit 0; 23.781s                                                                                                                                                      |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestScopeManager_ExecWrite_ConcurrentUsage\|TestProcessPull_RepeatableReadSnapshotDoesNotMixWithConcurrentPrune\|TestSnapshotSessions_(ActiveSessionRemainsReadableAfterHistoryPrune\|RepeatedEquivalentRotationIsIdempotentForSourceState\|CleanupExpiredSessionsRemovesRows)\|TestPushSessions_ConcurrentSessionCreationLeavesOneActiveStagingSession\|TestAuditBootstrapAtomicity_ParallelBootstrapsConverge\|TestRunBundleChangeListener_ReconnectCatchUpWakesActiveSubscribers)$' -count=1 -timeout=5m -v` | Exit 0; seven matched lifecycle tests passed in 2.648s; the mistyped bootstrap selector matched no test, while the correct bootstrap prefix passed in the race group |

Two audit-harness development failures were retained as evidence rather than
hidden: the first restart harness attempt was interrupted after 145.79s because
Docker Desktop did not resume the original container on its anonymous port;
the final harness recreates the container over a named volume and fixed host
port. One intermediate snapshot/pruning run exited 1 because a strengthened
two-row fixture still asserted one post-crash business row; correcting that
audit assertion produced the final green and race results above.

### Phase 5 Protocol Robustness and Resource-Safety Evidence

All new probes remain audit-only behind `oversync_audit`. They use bounded
inputs to characterize admission, allocation, persistence, cleanup, and error
mapping without turning this phase into the capacity profiling reserved for
Phase 6.

The protocol and identity matrix produced these outcomes:

- Missing and blank actor IDs fail before handler dispatch. Case-distinct user
  and source tokens remain distinct, but whitespace is trimmed, and control
  characters plus 64 KiB actor IDs reach the handler. This extends
  `OS-AUD-008`; no identity grammar or size bound is currently configured.
- Malformed JSON and snapshot unknown fields fail, but create, chunk, and
  snapshot parsing is not one strict policy. Duplicate and case-colliding
  create fields return HTTP 200, duplicate and case-colliding row payloads
  persist one staged row, and snapshot creation accepts a second JSON document.
  Existing probes also reconfirm create unknown fields, trailing JSON, and
  `text/plain` return HTTP 200.
- A malformed push UUID maps to 404 and a malformed snapshot UUID reaches
  PostgreSQL `22P02` and maps to 500. Text sync keys containing NUL, payload
  columns absent from the discovered table shape, and 64-byte PostgreSQL
  identifiers pass early validation. These are parser/error-boundary evidence
  within `OS-AUD-009`; source tracing shows an overlength configured table name
  later fails exact metadata lookup, so the validator result is not classified
  as a separate live-object aliasing finding.

The bounded pressure matrix covers every Phase 5 resource class:

- A 16,487-byte leading-whitespace request is accepted because handlers have no
  shared body bound. A configured one-row chunk rejects two rows before a pool
  acquisition and leaves its staged cursor and rows unchanged.
- Twelve independent staged sessions and twelve complete snapshot sessions are
  admitted without a quota. After forced SQL expiry, one lazy create of each
  kind cascades the old rows and leaves one live session of each kind.
- A retained window of two preserves bundle sequences 3 and 4 while a snapshot
  reconstructs all six authoritative rows. Committed and snapshot page requests
  for 100 rows are capped to two. A one-row snapshot-session limit rejects an
  actual two-row snapshot atomically, but only after full materialization.
- A two-connection pool times out one pull after about 200 ms while both slots
  are held, increments the cancelled-acquire counter, and succeeds after one
  slot is released. The oracle remains green after every pressure/failure path.
- Sixty-four process-local subscribers each retain only the newest of 256
  wakeups and all unregister after cancellation. Sixteen slow HTTP clients
  unregister after disconnect. There is still no per-user/global admission
  quota or handler write deadline, strengthening `OS-AUD-011`.

Drift and lifecycle probes show that a marked layout accepts missing managed
indexes and constraints and a replaced capture function. The altered function
causes a later commit to fail atomically, while a drifted registered-table
trigger is deterministically dropped and recreated. Malformed notifications
are logged and ignored without stopping the listener. Two concurrent listener
loops coalesce duplicate sequences but each consumes a pool slot; terminating
the listener backend reconnects and catches up successfully.

Status and health map a closed-pool query failure consistently to HTTP 500 with
`status_failed` and `health_failed`. Response encoding and write failures are
contained and logged. Shutdown-in-progress create/watch requests map to 503,
but the same requests after `Close` return operation-specific 500 responses,
dynamically confirming `OS-AUD-015`. The status query still performs global
aggregates; Phase 6 measured its cardinality cost and confirmed
`OS-AUD-013`.

Six pure fuzz targets exercise schema, table, column, schema-table,
notification-channel, and sync-key parsing with a 4 KiB input cap. Each
completed 100 executions without a panic or non-deterministic result. JSON is
not duplicated: `FuzzAuditCanonicalJSON_Idempotent` and its minimized corpus
remain the owner and intentionally expose `OS-AUD-001`. Drymint's aggregate
review found only the three separately invokable identifier fuzz wrappers,
which intentionally share seed/assertion helpers so each required input class
has its own fuzz target; no refactor or triage mutation was warranted.

Security research, scanning, access-control analysis, exploitability work, and
database-privilege probes remained excluded. None of these results is a
security claim.

Exact final Phase 5 command evidence on 2026-07-10:

| Command                                                                                                                                                                                                                                                                                                                                                                                                           | Exit/result                                                                     |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test ./oversync -run '^(TestActorMiddleware_\|TestBundleChangeHub_\|TestHTTPSyncHandlers_HandleWatch\|TestBundleChangeWatch_\|TestRunBundleChangeListener_\|TestSyncService_GetStatus\|TestHTTPSyncHandlers_HandleStatus\|TestHTTPSyncHandlers_HandleHealth\|TestBundleChangeWatchConfig_)' -count=1 -v`                                                        | Exit 0; package 2.282s; wall 3.25s                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditProtocolGreen_' -count=1 -timeout=5m`                                                                                                                                                                                                                                                                      | Exit 0; 1.695s                                                                  |
| Same protocol-green command with `-v` during development                                                                                                                                                                                                                                                                                                                                                          | Exit 0; 1.446s                                                                  |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditProtocolContract_' -count=1 -timeout=5m`                                                                                                                                                                                                                                                                   | Expected exit 1; 1.923s                                                         |
| Same protocol-contract command with `-v` during development                                                                                                                                                                                                                                                                                                                                                       | Expected exit 1; 2.262s                                                         |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditResourceSafety_' -count=1 -v`                                                                                                                                                                                                                                                                              | Exit 0; 2.570s                                                                  |
| Same resource command with `-race` and without `-v`                                                                                                                                                                                                                                                                                                                                                               | Exit 0; 3.842s                                                                  |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestBundleChangeHub_DoesNotBlockOnSlowSubscriber\|TestBundleChangeHub_RemovesSubscriberOnContextCancel\|TestHTTPSyncHandlers_HandleWatchSendsHeartbeatAndCleansUp\|TestHTTPSyncHandlers_HandleWatchUnregistersSubscriberOnWriteFailure)$' -count=1 -v`                                                             | Exit 0; 1.851s                                                                  |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditSSELifecycle_\|TestAuditDriftResilience_)' -count=1 -v`                                                                                                                                                                                                                                                   | Exit 0; 3.144s                                                                  |
| Same SSE/drift command with `-count=5` and without `-v`                                                                                                                                                                                                                                                                                                                                                           | Exit 0; 10.355s                                                                 |
| Same SSE/drift command with `-race -count=1` and without `-v`                                                                                                                                                                                                                                                                                                                                                     | Exit 0; 4.665s                                                                  |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditProtocolGreen_\|TestAuditResourceSafety_\|TestAuditSSELifecycle_\|TestAuditDriftResilience_)' -count=1 -timeout=5m -v`                                                                                                                                                                                    | Exit 0; package 4.511s; wall 5.45s                                              |
| Same consolidated green command with `GOTOOLCHAIN=go1.25.0` and without `-v`                                                                                                                                                                                                                                                                                                                                      | Exit 0; package 4.490s; wall 6.30s                                              |
| Same Go 1.26.5 consolidated green command with `-race`, `-timeout=10m`, and without `-v`                                                                                                                                                                                                                                                                                                                          | Exit 0; package 6.194s; wall 8.81s                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditActorMiddleware_PreservesExactProtocolTokens\|TestAuditHTTPCreatePushSession_\|TestAuditMaxRowsPerBundle_RejectsOversizedBundle\|TestAuditMaxBytesPerBundle_RejectsOversizedBundle\|TestAuditProtocolContract_\|TestAuditExpectedDriftResilience_\|TestAuditExpectedLifecycle_)' -count=1 -timeout=5m -v` | Expected exit 1; package 2.778s; wall 3.83s; all desired assertions reproduced  |
| Six commands of the form `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^$' -fuzz '^FuzzAuditProtocol<Target>_Deterministic$' -fuzztime=100x -timeout=3m`, for `SchemaName`, `TableName`, `ColumnName`, `SchemaTableKey`, `NotificationChannel`, and `SyncKeyEncoding`                                                                                              | Exit 0 for all; 0.507s, 0.403s, 0.391s, 0.430s, 0.406s, and 0.380s respectively |
| The same six fuzz commands with `-fuzztime=1x` during development                                                                                                                                                                                                                                                                                                                                                 | Exit 0 for all; 0.465s, 0.331s, 0.460s, 0.351s, 0.357s, and 0.344s respectively |

One early expected-contract selector anchored the `TestAuditHTTPCreatePushSession_`
prefix as though it were a complete test name, so it ran only actor and bundle
limit reproducers: expected exit 1, package 1.668s, wall 2.81s. The corrected
HTTP-prefix command then ran all three intended HTTP tests: expected exit 1,
package 1.569s, wall 1.96s. Both commands and the selector correction are
retained as evidence rather than hidden.

## Findings

### Confirmed Findings

### Final Classification Registry

The confirmed registry contains 17 findings: three Critical, nine High, four
Medium, and one Low. IDs OS-AUD-010 and OS-AUD-012 were never assigned and are
intentionally left unused. OS-AUD-020 is an unresolved hypothesis, not a
confirmed finding.

| Severity | Ordered findings                                                                                                                                                                                                                                                                                                |
|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Critical | OS-AUD-001 exact-value corruption; OS-AUD-018 acknowledged crash loss; OS-AUD-002 uncaptured TRUNCATE loss                                                                                                                                                                                                      |
| High     | OS-AUD-003 retention boundaries; OS-AUD-005 populated adoption; OS-AUD-006 nullable identity; OS-AUD-007 managed-layout drift; OS-AUD-008 token aliasing; OS-AUD-004 unenforced bounds; OS-AUD-009 unbounded/ambiguous parsing; OS-AUD-011 resource amplification; OS-AUD-019 independent-scope pool starvation |
| Medium   | OS-AUD-017 generated-column replay; OS-AUD-015 closed-service mapping; OS-AUD-013 status scaling; OS-AUD-014 public contract drift                                                                                                                                                                              |
| Low      | OS-AUD-016 unused declarations                                                                                                                                                                                                                                                                                  |

OS-AUD-019 remains High even though the pool bounds its connection count: two
supported same-scope waiters can consume the whole deliberately small pool and
make unrelated initialized scopes unavailable. The classification is based on
complete service starvation through supported operations, not on integrity
loss or a security claim.

Affected versions for every confirmed row: known affected at tag `v0.2.1`,
baseline commit `1f1118f1749490daaa8f1f76a7d8516f5303a873`; earlier and later
versions were not audited. Each row below supplies the explicit evidence,
reproduction, impact, compatibility, regression, and residual-risk fields.
The detailed subsection for the same ID supplies the invariant and
compatible-first remediation.

| ID         | Severity / confidence | Evidence and reproduction                                                                                                                                              | Impact                                                                                                | Compatibility analysis and required regression                                                                                                                                   | Residual risk                                                                                   |
|------------|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| OS-AUD-001 | Critical / Confirmed  | Canonical/exact-value tests plus `FuzzAuditCanonicalJSON_Idempotent` corpus `1e700`; PostgreSQL exact-value group reproduces bundle/pull corruption and hash collision | Authoritative payload corruption, adjacent-number hash collision, failed BIGINT writes                | Internal exact parsing is compatible, but historical hashes may require a versioned alternative; promote the exact-value group and minimized corpus                              | Existing corrupted values/hashes, mixed-version peers, and unreconstructable historical lexemes |
| OS-AUD-002 | Critical / Confirmed  | `TestAuditRegisteredTable_TruncateFailsClosed` deletes a registered row without bundle/row-state movement                                                              | Acknowledged authoritative data can disappear behind live metadata                                    | Add a fail-closed TRUNCATE guard and explicit reset workflow; promote the reproducer and add the `WithinSyncBundle` path                                                         | Other administrative/out-of-band mutation needs an explicit consistency policy                  |
| OS-AUD-003 | High / Confirmed      | Retained-floor, zero-checkpoint, and full checkpoint-boundary tests                                                                                                    | Successful incomplete recovery, rejected valid floor, manufactured future checkpoint                  | Wire shape is unchanged, but boundary acceptance changes; promote the full boundary matrix                                                                                       | Previously poisoned client checkpoints may require forced snapshot recovery                     |
| OS-AUD-004 | High / Confirmed      | Row/byte expected-contract tests, Phase 5 body characterization, and Phase 6 5,000 x 64 KiB SQLSTATE `54000` boundary                                                  | Supported work allocates/persists beyond advertised limits and fails late                             | Enforce advertised planned/cumulative/body limits with stable errors; promote row/byte tests and add early 413/zero-residue cases                                                | Encoded/decoded accounting and aggregate concurrent pressure                                    |
| OS-AUD-005 | High / Confirmed      | `TestAuditBootstrap_AdoptsPopulatedRegisteredTableIntoSnapshot`                                                                                                        | Pre-existing authoritative rows are invisible and snapshots fail                                      | Atomic adoption/readiness can preserve APIs but changes internal state; promote reproducer plus concurrent-write/rollback/connect variants                                       | Historical provenance cannot be recreated; old clients may require a baseline snapshot          |
| OS-AUD-006 | High / Confirmed      | `TestAuditBootstrap_RejectsNullableScopeOrSyncKeyColumns`                                                                                                              | NULL owners/keys make scope and row identity ambiguous                                                | Fail-closed bootstrap needs diagnostics/migration for accepted legacy schemas; promote both subtests plus pre-existing NULL rows                                                 | Partition children, migrated constraints, and unresolved legacy NULLs                           |
| OS-AUD-007 | High / Confirmed      | Damaged-layout, marked-layout drift, and missing-constraint expected-contract tests                                                                                    | A layout reports ready while managed objects are missing or semantically replaced                     | Version-aware fingerprints and explicit migrations preserve public APIs but can fail readiness; promote full table/function/constraint/index drift matrix                        | Definition equivalence across PostgreSQL revisions and migration races                          |
| OS-AUD-008 | High / Confirmed      | `TestAuditActorMiddleware_PreservesExactProtocolTokens` plus positive case-distinct/bounds controls                                                                    | Byte-distinct owners/sources alias, damaging ownership and idempotency                                | Exact preservation changes whitespace-bearing legacy identity; detect collisions and use a versioned migration if necessary; add ScopeManager/replacement/session-token coverage | Existing aliased state and undefined Unicode/control/length grammar                             |
| OS-AUD-009 | High / Confirmed      | HTTP create tests and Phase 5 strict parser/malformed path/key/identifier expected-contract group                                                                      | Ambiguous input succeeds, invalid values fail late/as 500, and bodies allocate without a common bound | Strict bounded single-document decode keeps valid shapes but rejects tolerated malformed requests; promote the complete expected-contract group                                  | Streaming allocation, future decoder drift, and handler error consistency                       |
| OS-AUD-011 | High / Confirmed      | Pressure/SSE/session tests, ambiguous snapshot retry, Phase 6 pull/snapshot/watch profiles and plans                                                                   | Query, memory, TTL-session, listener, and subscriber amplification without quotas                     | Batching/index changes are internal; quotas/deadlines need configurable defaults and stable errors; retain bounded-work and profile budgets                                      | Multi-instance quotas, retry identity, and slow-client OS resources                             |
| OS-AUD-013 | Medium / Confirmed    | `TestAuditProfileMetrics_StatusScaling`: 0/1k/100k/1m with exact production SQL and plan                                                                               | Routine status/health work performs two history-sized scans and competes with sync                    | Preserve response fields using bounded counters/cached aggregates; retain cardinality/plan regression with a database-noise budget                                               | Counter drift, cache freshness, and other status aggregates                                     |
| OS-AUD-014 | Medium / Confirmed    | Ruby/Psych parse plus source/runtime/OpenAPI/document comparison                                                                                                       | Parsers/generated clients can omit a required header or implement wrong models/errors                 | Correct additively and add OpenAPI parse/response golden plus consumer compile tests                                                                                             | Generated-client behavior and future document drift remain incompletely measured                |
| OS-AUD-015 | Medium / Confirmed    | SSE lifecycle characterization and `TestAuditExpectedLifecycle_RequestsAfterCloseReturnStableUnavailable`                                                              | Closed service looks like internal failure, harming retry/monitoring behavior                         | Observable 500-to-503 change without a shape change; promote expected-contract test across every handler lifecycle state                                                         | Close races and handler-specific omissions                                                      |
| OS-AUD-016 | Low / Confirmed       | Final default `staticcheck` reports 13 `U1000` declarations; the tagged build reports 12 from the same set because the harness exercises `preparePushRows`             | Dead paths obscure supported behavior and increase audit/maintenance cost                             | Remove only proven internal dead code; require clean staticcheck in both build modes                                                                                             | Intended but unexercised hooks may be mistaken for dead code                                    |
| OS-AUD-017 | Medium / Confirmed    | `TestAuditSchemaEdge_GeneratedColumnRoundTripsOrBootstrapRejectsAtomically`                                                                                            | A supported schema emits an after-image that the same protocol cannot replay                          | Omit generated fields from assignments while retaining authoritative after-images; promote the current round-trip test                                                           | Generated keys/expressions and old clients that resend generated fields                         |
| OS-AUD-018 | Critical / Confirmed  | Unlogged bootstrap expected-contract test plus real PostgreSQL SIGKILL/restart reproducer                                                                              | Acknowledged row disappears while hash, row-state, bundle, and source watermark survive               | Rejecting existing unlogged registrations needs readiness diagnostics and operator migration; both current tests must become green                                               | Already-lost rows, ghost metadata, and existing unlogged deployments                            |
| OS-AUD-019 | High / Confirmed      | Small-pool expected-contract test plus cancellation/retry positive controls                                                                                            | Same-scope waiters exhaust the pool and starve unrelated scopes                                       | Try-lock/backoff is internal but changes fairness/timing; promote the small-pool test with cancellation and slot cleanup                                                         | Polling herd, unfairness, or indefinite scope starvation                                        |

Conditional versioned alternatives are limited to OS-AUD-001 (when stored hash
history cannot be reconciled under exact canonicalization) and OS-AUD-008
(when legacy trimmed identities collide). Every other confirmed finding has a
compatible-first path in its detailed entry and does not justify a breaking
wire/API alternative at this stage.

#### OS-AUD-001: JSON numbers are corrupted before persistence and hashing

Severity: Critical
Confidence: Confirmed
Invariant: Bundle integrity and row-state equivalence

`canonicalJSON` decodes through `any`/`float64`
(`oversync/upload_support.go:137`), push preparation repeats an untyped map
decode (`oversync/push.go:122`), and committed-bundle hashing reparses payloads
the same way (`oversync/bundle_capture.go:356`). The audit reproducer proves:

```text
1234567890.123456789 -> 1234567890.1234567
9007199254740993      -> 9007199254740992
-9223372036854775808  -> -9223372036854776000
9223372036854775807   -> 9223372036854776000
```

Fuzzing minimized a second valid input, `1e700`, which the implementation
rejects because it cannot fit in `float64`. This can mutate authoritative
payloads, make adjacent integer values hash identically, and violate the
Swagger RFC 8785 claim. The broader vector suite also proves incorrect UTF-16
property ordering for U+10000 versus U+E000 and an identical committed hash for
payload values `9007199254740992` and `9007199254740993`.

The PostgreSQL path confirms the defect in both directions. A native BIGINT
maximum is rounded upward before record population and fails with SQLSTATE
`22003`. When exact BIGINT/NUMERIC values are inserted through typed database
conversion, the business row and later snapshot remain exact, but the committed
bundle and pull payload round both values; the NUMERIC
`12345678901234567890.12345678901234567890` becomes
`12345678901234567000`.

Compatible-first remediation: parse with `json.Decoder.UseNumber`, preserve
number lexemes through payload normalization, use a standards-conformant
canonicalizer, and version hash behavior only if stored hashes cannot be
reconciled safely. Implement RFC 8785 UTF-16 property ordering, promote the
exact-value and PostgreSQL reproducers plus minimized fuzz corpus, and preserve
the existing exact snapshot behavior.

#### OS-AUD-002: TRUNCATE bypasses ownership and bundle capture

Severity: Critical
Confidence: Confirmed
Invariant: Atomic commit and row-state equivalence

Registered tables receive row-level triggers for INSERT, UPDATE, and DELETE
only (`oversync/bundle_capture.go:85`). PostgreSQL `TRUNCATE` does not fire
those triggers. A caller can therefore erase registered business rows without
updating `sync.row_state`, writing a bundle, or advancing source state,
including inside `WithinSyncBundle` where no captured events is accepted as a
nil bundle.

Compatible-first remediation: install a statement-level TRUNCATE guard that
always rejects TRUNCATE on registered tables and document an explicit
administrative reset path outside normal runtime. Required regression tests:
`TestRegisteredTableGuard_RejectsTruncateWithoutBundleContext` and
`TestWithinSyncBundle_TruncateCannotCommitUncapturedDataLoss`.

#### OS-AUD-003: Pull checkpoint boundaries are internally inconsistent

Severity: High
Confidence: Confirmed
Invariant: Retention safety

`enforceRetainedBundleFloor` accepts all checkpoints `<= 0`, but rejects a
checkpoint exactly equal to the retained floor; pull itself filters only rows
at or below that floor. A new or reset client can therefore receive successful
partial history, while a client at the first safe checkpoint is unnecessarily
rejected. The full PostgreSQL matrix confirms floor `2`, current sequence `3`,
and these outcomes: checkpoint `0` is accepted, `floor-1` is rejected, `floor`
is rejected, current/future checkpoints are empty, and a caller-supplied future
`target_bundle_seq=13` is echoed as a manufactured stable checkpoint above the
actual current sequence `3`.

Compatible-first remediation: when the retained floor is positive, reject
checkpoints strictly below it including zero, accept equality, and reject or
clamp target ceilings above the current committed sequence. Promote the full
boundary matrix as the permanent regression test.

#### OS-AUD-004: Advertised bundle and request bounds are not enforced

Severity: High
Confidence: Confirmed
Invariant: Resource cleanup and availability

`MaxRowsPerBundle` and `MaxBytesPerBundle` are advertised but have no runtime
enforcement. `planned_row_count` is unbounded, commit loads all staged rows,
and primary HTTP body decoders do not use `http.MaxBytesReader`. Per-chunk row
limits apply only after JSON allocation. PostgreSQL-backed reproducers commit
both a two-row bundle with a configured one-row maximum and a non-empty payload
with a configured one-byte maximum without error.

Compatible-first remediation: enforce planned, cumulative staged, committed,
and body-byte limits before allocation/persistence; reject atomically with
stable 4xx errors. Required tests cover planned rows, cumulative bytes, HTTP
413 behavior, and absence of staging residue.

#### OS-AUD-005: Populated registered tables are not adopted safely

Severity: High
Confidence: Confirmed
Invariant: Bootstrap safety and snapshot completeness

Bootstrap creates metadata and triggers but never scans or backfills existing
authoritative rows. Connect can then mark the scope `initialize_empty` without
checking those rows; snapshot creation later rejects live rows without
`row_state`. This violates the locked requirement that populated tables be
adoptable. The PostgreSQL reproducer starts with one owned business row, then
finds `user_state=0` and `row_state=0`; snapshot creation fails because the
scope is uninitialized.

Compatible-first remediation: add an explicit, atomic adoption phase that
locks registered writes, creates coherent user/scope/row/bundle state, and
becomes ready only after validation. Required tests cover populated adoption,
concurrent writes, rollback, connect, and snapshot coherence.

#### OS-AUD-006: Registered owner and sync-key nullability is not validated

Severity: High
Confidence: Confirmed
Invariant: Scope identity consistency and bootstrap safety

Column discovery reads names and types but not nullability. Bootstrap therefore
accepts nullable `_sync_scope_id` and visible keys despite the documented
`NOT NULL` contract. Existing NULL owners interact unsafely with SQL
three-valued `<>` checks in trigger functions. Both nullable-column variants
bootstrap successfully in the PostgreSQL reproducer instead of returning
`UnsupportedSchemaError`.

Compatible-first remediation: include nullability in schema discovery, reject
nullable owner/key columns before creating sync metadata, and explicitly guard
NULL owner values in trigger code. Required tests cover both nullable columns
and pre-existing NULL rows.

#### OS-AUD-007: Marked layouts skip managed-object validation

Severity: High
Confidence: Confirmed
Invariant: Bootstrap safety

Once `sync.meta` and `sync.table_catalog` match, initialization returns ready
without validating required tables, columns, constraints, sequences, indexes,
or function definitions. Missing or drifted objects can survive restart as an
apparently valid layout.
The dynamic probe confirms a dropped required index is silently accepted. A
dropped capture function is detected only later while reinstalling triggers and
returns a raw PostgreSQL error rather than a coherent marked-layout validation
failure.

Phase 5 additionally proves that a removed check constraint and a replaced
capture-function body are accepted. The replaced function makes a later commit
fail atomically because no effects are captured. Registered-table trigger drift
is repaired because those triggers are always recreated; that narrower repair
does not validate the rest of the marked layout.

Compatible-first remediation: fingerprint and validate every managed object,
fail closed on drift, and define explicit versioned migrations rather than
silently accepting or recreating state. Required tests remove or alter an
index, constraint, table, and trigger function before a second bootstrap.

#### OS-AUD-008: Protocol identifiers are silently normalized

Severity: High
Confidence: Confirmed
Invariant: Scope identity consistency, idempotency, and source sequencing

Middleware trims user and source IDs; ScopeManager and snapshot source
replacement trim additional protocol identities. The audit reproducer shows
`" audit-user "` becomes `"audit-user"`, aliasing otherwise distinct keys
contrary to the repository exact-token rule.

Case-distinct tokens remain distinct, but control-bearing and 64 KiB actor
tokens reach handlers without a defined grammar or size bound. This is exact
identity plus resource-bound evidence, not a security assessment.

Compatible-first remediation: preserve exact values and introduce explicit
grammar validation only where the protocol defines a grammar. Promote
`TestAuditActorMiddleware_PreservesExactProtocolTokens` and add ScopeManager,
source replacement, and session-token coverage.

#### OS-AUD-009: Protocol parsing is inconsistent and unbounded

Severity: High
Confidence: Confirmed
Invariant: Availability and deterministic protocol handling

Create, chunk, and connect handlers accept unknown fields and trailing JSON
documents. Snapshot decoding disallows unknown fields but accepts a second
valid document. No shared body bound, media-type policy, or duplicate-key
policy exists. Case-colliding payload keys can also fail late in PostgreSQL.

Three PostgreSQL-backed audit reproducers confirm that create-push-session
requests containing an unknown field, a trailing JSON document, or
`Content-Type: text/plain` all return HTTP 200 instead of the intended 400/415.
The snapshot-create handler also maps its deliberate configured row/byte-limit
error to generic HTTP 500 instead of a stable bound response.

Phase 5 broadens the dynamic evidence: duplicate and case-colliding create
fields succeed; ambiguous payload keys stage rows; snapshot accepts a second
document; malformed UUID paths map to 404/500; and NUL text keys, unknown
payload columns, and overlength PostgreSQL identifiers pass early validation.
A 16,487-byte request is accepted because no shared body bound exists.

Compatible-first remediation: one shared bounded, single-document decoder with
operation-specific byte limits and stable 400/413/415 errors; reject duplicate,
unknown, and case-colliding fields before staging, and map configured snapshot
bounds consistently.

#### OS-AUD-011: Snapshot and watch work can amplify resources without quotas

Severity: High
Confidence: Confirmed
Invariant: Availability and resource cleanup

Snapshot creation materializes an entire scope in Go memory before checking
configured limits and persists rows one statement at a time. Active snapshot
sessions are unbounded. Watch subscriptions have no global/per-user quota and
allocate channels and goroutines per connection; handler write duration relies
on host timeouts. Pull also repeats user, metadata, and row queries for every
bundle, and `snapshot_session_rows` has an explicit unique index that duplicates
its unique constraint. Phase 4 response-loss injection also shows that retrying
an ambiguous snapshot-create request creates a second complete TTL-bounded
session because the request has no replay identity; both sessions remain until
explicit deletion or lazy expiry.

Bounded pressure confirms 12 staged and 12 snapshot sessions are admitted
without quotas and later reclaimed lazily. Sixty-four subscribers and 16 slow
HTTP clients clean up correctly, but no subscriber quota or SSE write deadline
exists. Duplicate listener loops each pin a pool connection even though event
sequences coalesce.

Compatible-first remediation: stream/batch snapshot materialization with
incremental bounds, enforce active-session and subscriber quotas, add write
deadlines, and expose bounded metrics. Required load tests verify rejection
before large allocation and cleanup after slow/disconnected clients.

#### OS-AUD-013: Operational status scans retained bundle history twice

Severity: Medium
Confidence: Confirmed
Invariant: Availability and bounded operational work

`GetStatus` runs global bundle COUNT and byte SUM in separate scalar
subqueries. Phase 6 captured the exact runtime SQL and measured 0, 1,000,
100,000, and 1,000,000 retained bundles with five warmups and ten samples. The
median grew from 0.276 ms at zero to 8.315 ms at 100,000 and 81.809 ms at one
million. At one million the plan runs two parallel sequential scans of
`sync.bundle_log`, each with three workers. Closed-pool status/health failures
still map consistently to operation-specific 500 responses; the defect is the
history-sized work on routine operational probes.

Compatible-first remediation: preserve response fields while maintaining
bounded transactional counters or cached aggregates with a documented
freshness contract. Promote the cardinality/plan probe with a database-noise
budget and counter-coherence cases.

#### OS-AUD-014: Public protocol and documentation contracts drift from runtime

Severity: Medium
Confidence: Confirmed by source comparison
Invariant: Deterministic protocol handling and compatibility

The contract review found multiple independently reproducible mismatches:

- `/sync/pull` repeats the YAML `parameters` mapping key, allowing ordinary
  parsers to discard the source-header declaration.
- OpenAPI permits arbitrary JSON values in `SyncKey`, while runtime accepts
  only strings.
- Runtime pull bundles emit `row_count` and `bundle_hash`, but the OpenAPI
  `Bundle` schema and API example omit them.
- Delete-push documentation advertises an unreachable committed-session error;
  pull docs name a different 400 error code; watch docs omit implemented setup
  failures; and health/status omit their possible 500 response.
- Server table documentation omits `sync.bundle_capture_stage`, and one page
  contains checkout-specific absolute filesystem links.
- `DiscoverSchemaWithDependencyOverrides` is exported with an unexported value
  type in its parameter, making useful external calls impractical.

Compatible-first remediation: make the checked-in OpenAPI document the
canonical wire contract, add parser and response golden tests, reconcile the
documentation and runtime error codes, and either expose a usable discovery
input type or make the method internal in a separately reviewed compatibility
slice.

#### OS-AUD-015: Requests after service close map to HTTP 500

Severity: Medium
Confidence: Confirmed dynamically
Invariant: Shutdown and deterministic failure handling

`beginOperation` returns a generic `service is closed` error after close, while
handlers only map the separate shutting-down sentinel to 503. A request that
arrives after close therefore falls through to generic 500 instead of a stable
unavailable response.

The Phase 5 HTTP reproducer holds one operation while `Close` enters
`shutting_down`: create and watch return the intended 503. After the operation
drains and lifecycle becomes `closed`, create returns
`push_session_create_failed`/500 and watch returns
`bundle_change_watch_failed`/500. The desired-contract test expects 503 for
both and fails deterministically.

Compatible-first remediation: return one typed lifecycle error for both
shutting-down and closed states and map it consistently to 503. Required tests
exercise every handler before, during, and after close.

#### OS-AUD-016: Static analysis reports thirteen unused declarations

Severity: Low
Confidence: Confirmed
Invariant: Maintainability and auditability

`staticcheck ./oversync` reports 13 `U1000` declarations across capture, pull,
push, schema, sync-key, upload, and test-helper code. They do not establish a
runtime defect, but they obscure which paths remain supported and increase the
surface that future correctness changes must reason about. The final tagged
build reports 12 of those declarations because an audit test exercises
`preparePushRows`; it adds no audit-file diagnostic.

Compatible-first remediation: in a later maintenance slice, remove truly dead
code and promote any intended extension point to an exercised, documented
surface. Keep the exact staticcheck output as the regression baseline.

#### OS-AUD-017: Generated-column after-images cannot round-trip through push

Severity: Medium
Confidence: Confirmed
Invariant: Supported-schema consistency and idempotent replay

Bootstrap accepts a registered table with a stored generated column, and
committed bundle, pull, and snapshot payloads correctly include the generated
after-image. Reusing that authoritative after-image for the next update sends
the generated field back as an assignment; PostgreSQL rejects it with SQLSTATE
`428C9`. Business state and metadata roll back coherently, but the accepted
table shape emits a payload that the same protocol cannot replay.

Compatible-first remediation: discover generated columns and omit them from
client-applied INSERT/UPDATE assignments while retaining them in authoritative
after-images, or reject the table shape explicitly at bootstrap. Promote the
generated-column round-trip probe as the regression test.

#### OS-AUD-018: Unlogged registered tables violate the durability contract

Severity: Critical
Confidence: Confirmed by PostgreSQL SIGKILL/recovery
Invariant: Durability, bootstrap safety, and row-state equivalence

Bootstrap accepts an authoritative registered table whose PostgreSQL
`relpersistence` is `u`. Unlogged table contents are not crash durable, while
the synchronized metadata tables remain logged. The Phase 4 PostgreSQL 17.10
SIGKILL/recovery reproducer commits and acknowledges one real row, then records
this exact recovered state: business rows `1 -> 0`, live `row_state=1`,
`bundle_log=1`, `bundle_rows=1`, source watermark `1`, and the acknowledged hash
unchanged. Snapshot creation then fails because the live row-state entry points
to a missing authoritative business row. Every metadata-only oracle check
passes, demonstrating why business/live-state comparison is also required.

Compatible-first remediation: reject unlogged registered tables during schema
discovery before creating any sync layout. A later regression must cover both
bootstrap rejection and crash/restart integrity.

Required regression test:
`TestAuditPostgresRestart_UnloggedAcknowledgedRowMustNotDisappearWhileMetadataSurvives`
must become a green bootstrap-rejection and restart-coherence test.

Residual risk: previously bootstrapped deployments may already contain
unlogged registered tables and need a compatibility-safe readiness failure plus
an operator migration path before the schema can be accepted again.

#### OS-AUD-019: Advisory-lock waiters can exhaust the pool and starve unrelated scopes

Severity: High
Confidence: Confirmed
Invariant: Resource cleanup and independent-scope progress

`acquireUserUploadConn` acquires a pool connection before waiting on the
per-scope session advisory lock. With a service pool configured to
`MaxConns=2`, two requests waiting on the same externally blocked scope pin both
connections. A create request for a different initialized scope then fails
after 300ms with `acquire push connection: context deadline exceeded`. Cancelling
the waiters releases both connections, all metadata integrity checks pass, and
later retries succeed; the defect is starvation rather than state corruption.
`UploadLockTimeout` is transaction-local and starts only after the session lock
is acquired, so it does not bound this wait.

Compatible-first remediation: acquire a connection, attempt
`pg_try_advisory_lock`, and release the connection before bounded,
context-aware backoff when the lock is unavailable. Retain the connection only
after the lock succeeds, apply a documented wait bound, and add fairness/pool
wait observability. This preserves the public API, wire contract, and schema.

Required regression test:
`TestAuditConcurrencyContract_BlockedScopeWaitersDoNotStarveIndependentScope`
must become a permanent green small-pool test, including cancellation and pool
slot cleanup.

Residual risk: try-lock polling needs jitter and bounded fairness testing to
avoid replacing pool starvation with a thundering herd or indefinite scope
starvation.

### Unresolved Hypotheses

#### OS-AUD-020: Persisted expiry and lease decisions mix application and database clocks

Severity: Medium
Confidence: Medium
Invariant: Resource cleanup and multi-instance consistency

Evidence: push-session creation/refresh/checks, snapshot-session
creation/checks, and initialization leases use application `time.Now`; lazy
cleanup deletes with PostgreSQL `now()`. Source tracing confirms mixed clocks,
but the audit did not execute independently skewed application clocks against
one database.

Reproduction: unresolved. A later audit-only test needs two services with
independently controllable application clocks while PostgreSQL remains the
shared clock.

Impact: a clock-ahead instance may retain sessions/leases too long, while a
clock-behind instance may create timestamps another instance treats as already
expired. Takeover, staged recovery, and lazy cleanup can disagree.

Affected versions: source inspection shows the pattern at `v0.2.1`, baseline
commit `1f1118f1749490daaa8f1f76a7d8516f5303a873`; earlier and later versions
were not audited.

Compatibility analysis: configured TTLs and response fields can remain
unchanged if persisted timestamps and expiry decisions use the PostgreSQL
clock in the owning transaction. Existing skewed rows may need to age out or
be normalized during rollout.

Compatible-first remediation: compute expiry and lease timestamps in SQL from
one documented database-clock expression, return generated timestamps where
needed, and compare expiry in SQL or to a database-derived reference.

Required regression tests:
`TestSessionExpiry_UsesDatabaseClockAcrossSkewedServiceInstances` and
`TestInitializationLease_UsesDatabaseClockAcrossSkewedServiceInstances`.

Residual risk: existing host-skew timestamps, database wall-clock changes, and
mixed-version instances. This entry remains a hypothesis and is not counted in
the confirmed severity totals.

Generated-client impact for OS-AUD-014 remains residual risk rather than a
separate finding. All Phase 6 performance candidates were measured or given an
explicit non-baseline disposition.

### Intentional Behavior

- `PayloadExtractor.Int64Field` currently documents numeric conversion, but
  silent fractional truncation conflicts with exact-integer expectations. It
  remains part of `OS-AUD-001` pending the later compatibility decision.
- Bundle history pruning is allowed; successful incomplete reconstruction is
  not.

### Scanner Triage

- `staticcheck` reported 13 unused declarations. They are recorded as the Low
  maintenance finding `OS-AUD-016`, not as runtime defects.

### Security Exclusion

Security-specific scanning and database-privilege probing were discontinued
when the scope changed. No security-specific artifact or workflow job is part
of this audit, and no security conclusion will be drawn from its results.

### Out-of-scope Deployment Concerns

Physical storage corruption, backup and restore, point-in-time recovery,
regional disaster recovery, operator deployment architecture, and all security
research are outside the locked audit failure model.

### Finding Template

Each finding must use this structure:

```text
OS-AUD-###: <title>
Severity: Critical | High | Medium | Low
Confidence: Confirmed | High | Medium | Low
Invariant: <affected invariant>
Evidence: <commands, output, artifact paths>
Reproduction: <deterministic steps and seed>
Impact: <data, correctness, reliability, or performance impact>
Affected versions: <known range>
Compatibility analysis: <existing public behavior>
Compatible-first remediation: <preferred later fix>
Versioned alternative: <only if compatible remediation is insufficient>
Required regression test: <audit reproducer to promote>
Residual risk: <risk after proposed remediation>
```

## Performance and Scalability

Phase 6 ran locally on Apple M2 Max (`darwin/arm64`) with Go 1.26.5 and
test-owned PostgreSQL 17.10 databases. `TEST_DATABASE_URL` was unset for every
reported database command. Fixture creation and capacity seeding are outside
timed regions. Fixed scalability profiles run five warmups and ten measured
rounds internally. Commands using `-count=15` treat the first five process runs
as warmups and the final ten as measurements.

Microbenchmarks require CV at or below 5%; database workloads require CV at or
below 10%. Results above those limits remain capacity/noise evidence and are
not baselines. Accepted baselines receive a later regression budget of 10%,
which is larger than every accepted noise envelope. Top-tier single samples
receive no regression budget.

### Phase 6 Command Log

| Command                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Exit/result                                                                                                                  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 GOMAXPROCS=1 go test -tags=oversync_audit ./oversync -run '^$' -bench '^BenchmarkAuditMicro' -benchmem -benchtime=500ms -count=15 -cpu=1 -timeout=30m`                                                                                                                                                                                                                                                   | Exit 0; package 108.909s; wall 110.57s; ten leaves accepted, two predeclared for a longer rerun                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 GOMAXPROCS=1 go test -tags=oversync_audit ./oversync -run '^$' -bench '^(BenchmarkAuditMicroKeyEncoding                                                                                                                                                                                                                                                                                                  | BenchmarkAuditMicroPayloadPreparation)$/^(text_encode                                                                        |prepare_push_row)$' -benchmem -benchtime=1s -count=15 -cpu=1 -timeout=30m` | Exit 0; package 36.331s; wall 36.86s; both leaves accepted |
| The first focused expression, `-bench '^(BenchmarkAuditMicroKeyEncoding/text_encode                                                                                                                                                                                                                                                                                                                                                                     | BenchmarkAuditMicroPayloadPreparation/prepare_push_row)$'`, followed by the corrected selector with `-benchtime=1x -count=1` | Exit 0 but matched no benchmarks, package 0.500s/wall 1.90s; corrected smoke exit 0, package 0.337s. The empty-selector correction is retained as evidence. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 GOMAXPROCS=1 go test -tags=oversync_audit ./oversync -run '^$' -bench '^BenchmarkAuditCanonicalJSON$' -benchmem -benchtime=500ms -count=15 -cpu=1`                                                                                                                                                                                                                                                       | Exit 0; package 9.427s; wall 9.94s                                                                                           |
| Same environment with `-bench '^BenchmarkAuditCommittedBundleHash$' -benchmem -benchtime=500ms -count=15 -cpu=1`                                                                                                                                                                                                                                                                                                                                        | Exit 0; package 36.972s; wall 37.49s; the 100-row leaf was rejected for noise                                                |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditProfileMetrics_RepresentativeOperations$' -count=1 -timeout=10m -cpuprofile=engineering-docs/audit/artifacts/2026-07-10-phase6/representative.cpu.pprof -memprofile=engineering-docs/audit/artifacts/2026-07-10-phase6/representative.heap.pprof -blockprofile=engineering-docs/audit/artifacts/2026-07-10-phase6/representative.block.pprof -v` | Exit 0; package 3.469s; wall 4.77s                                                                                           |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditProfileMetrics_StatusScaling$' -count=1 -timeout=15m -v`                                                                                                                                                                                                                                                                                         | Exit 0; package 8.911s on the concise rerun; exact production SQL captured and planned at 0/1k/100k/1m bundles               |
| `/usr/bin/time -p env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 GOMAXPROCS=1 OVERSYNC_AUDIT_BENCHMARK_PROFILE=full OVERSYNC_AUDIT_BENCHMARK_OPERATION_TIMEOUT=10m go test -tags=oversync_audit ./oversync -run '^$' -bench '^(BenchmarkAuditDatabasePushCommit                                                                                                                                                                                          | BenchmarkAuditDatabaseServerCapture)$' -benchtime=1x -count=15 -benchmem -cpu=1 -timeout=2h`                                 | Exit 0; package 235.110s; wall 236.61s |
| Same full environment with `-bench '^BenchmarkAuditDatabasePull$'`                                                                                                                                                                                                                                                                                                                                                                                      | Exit 0; package 551.557s; wall 555.65s; fixed-bundle-count sparse/dense matrix                                               |
| Same full environment with `-bench '^BenchmarkAuditDatabasePullEqualRows$'`                                                                                                                                                                                                                                                                                                                                                                             | Exit 0; package 312.781s; wall 316.38s; equal-row many/few bundle matrix                                                     |
| Same environment with `OVERSYNC_AUDIT_BENCHMARK_PROFILE=smoke` and `-bench '^BenchmarkAuditDatabaseSnapshotCreate$'`                                                                                                                                                                                                                                                                                                                                    | Exit 0; package 9.957s; wall 11.47s                                                                                          |
| Same environment with `OVERSYNC_AUDIT_BENCHMARK_PROFILE=top`, `-bench '^(BenchmarkAuditDatabasePushCommit                                                                                                                                                                                                                                                                                                                                               | BenchmarkAuditDatabaseServerCapture)/json_64kib/rows_5000$'`, `-count=1`, and `-timeout=30m`                                 | Expected exit 1; package 31.260s; wall 32.91s; push hit PostgreSQL SQLSTATE `54000`, while capture completed |
| Same top environment with `-bench '^BenchmarkAuditDatabaseSnapshotCreate/(rows_100000                                                                                                                                                                                                                                                                                                                                                                   | rows_1000000)$'`, `-count=1`, and `-timeout=25m`                                                                             | Exit 0; package 384.329s; wall 385.75s |
| `/usr/bin/time -p env -u TEST_DATABASE_URL OVERSYNC_AUDIT_FULL_SCALE=1 GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^$' -bench '^BenchmarkAuditScalability' -benchtime=1x -count=1 -benchmem -timeout=2h`                                                                                                                                                                                                                         | Exit 0; package 106.702s; wall 107.10s; final corrected allocation/CV run                                                    |
| `go tool pprof -top -nodecount=20` for CPU/block and `go tool pprof -top -nodecount=20 -alloc_space` for heap, against the three recorded artifacts                                                                                                                                                                                                                                                                                                     | Exit 0 for all                                                                                                               |

Two compile-only commands failed while agents were still assembling separate
benchmark files (transient unused imports and a transient undefined helper).
The settled tagged package compiled, the final explicit-path Drymint review was
inspected, and the command table above is authoritative. Drymint's remaining
match is the intentional pair of Go benchmark entrypoints; their substantive
runner is shared, while their fixed-bundle and equal-row matrices are distinct.

### Stable Microbenchmarks

| Operation                                |                Final mean/range |                       CV |                              Allocation | Decision |
|------------------------------------------|--------------------------------:|-------------------------:|----------------------------------------:|----------|
| Canonical JSON                           |        2.980 us; 2.938-3.024 us |                   1.233% |                      1,872 B; 43 allocs | Accepted |
| UUID key encode/decode                   |             86.98 ns / 52.39 ns |          2.513% / 2.184% |                          80 B/3; 64 B/2 | Accepted |
| Text key encode/decode                   |             37.05 ns / 47.90 ns |          2.106% / 2.271% |                          48 B/2; 80 B/3 | Accepted |
| Base64-to-stored / stored-to-wire binary |             2.254 us / 1.975 us |          2.060% / 2.532% |                    7,584 B/6; 3,856 B/4 | Accepted |
| Prepare one push row / inject owner      |           12.833 us / 10.306 us |          0.948% / 1.562% |                 9,816 B/104; 6,040 B/96 | Accepted |
| Normalize 1 / 100 / 1,000 events         | 300 ns / 19.633 us / 208.693 us | 2.947% / 3.366% / 1.750% | 352 B/11; 26,144 B/707; 279,720 B/7,009 | Accepted |
| Render bundle hash                       |                        59.72 ns |                   2.538% |                         128 B; 2 allocs | Accepted |

Committed-bundle hashing is approximately linear and allocation-heavy:

|  Rows |      Mean |     CV |   Allocated/op | Allocations/op | Decision |
|------:|----------:|-------:|---------------:|---------------:|----------|
|     1 |  8.415 us | 1.551% |        4,648 B |            126 | Accepted |
|   100 |  820.1 us | 5.538% |      463,798 B |         12,084 | Rejected |
| 1,000 |  7.782 ms | 2.432% | about 4.51 MiB |        122,483 | Accepted |
| 5,000 | 43.080 ms | 1.878% |  22.3-22.6 MiB |  about 614,493 | Accepted |

The 100-row hash leaf remained noisy in a focused rerun and receives no
budget. This performance baseline does not weaken OS-AUD-001: the same JSON
path is fast but corrupts exact numbers.

### Push and Capture Capacity

Values are final-ten mean milliseconds / CV. `A` is accepted and `R` is
rejected.

| Path and payload      |              1 row |           100 rows |           1,000 rows |                    5,000 rows |
|-----------------------|-------------------:|-------------------:|---------------------:|------------------------------:|
| Push, JSON 1 KiB      |   noisy outlier, R |  42.915 / 5.713% A |   198.854 / 1.347% A |            835.483 / 1.127% A |
| Capture, JSON 1 KiB   |  15.270 / 8.255% A | 33.160 / 43.625% R |   120.036 / 2.308% A |            508.089 / 2.943% A |
| Push, JSON 64 KiB     |  31.408 / 4.765% A | 589.154 / 1.224% A | 5,337.092 / 0.697% A | SQLSTATE `54000`, no baseline |
| Capture, JSON 64 KiB  |  20.802 / 7.800% A | 308.711 / 1.255% A | 2,783.037 / 0.736% A |     14,642.587 ms, one sample |
| Push, binary 1 KiB    | 25.081 / 10.615% R |  54.988 / 3.463% A |   307.930 / 3.332% A |          1,334.925 / 5.190% A |
| Capture, binary 1 KiB |  16.865 / 7.505% A | 39.143 / 39.339% R |   164.416 / 2.444% A |            733.023 / 2.650% A |

Allocation grows with rows and payload bytes. JSON 1 KiB pushes allocated
about 6.76 MiB/41,017 objects at 100 rows, 62.61 MiB/401,562 at 1,000, and
336.50 MiB/2,005,656 at 5,000. JSON 64 KiB push allocated about 300.45 MiB at
100 rows and 2.881 GiB at 1,000; capture allocated about 180.6 MiB and 1.722
GiB. The successful top capture allocated 9,624,363,664 bytes and 1,105,546
objects. The paired push failed before timing because PostgreSQL's JSONB
aggregate exceeded 268,435,455 bytes. This extends OS-AUD-004 and OS-AUD-011:
the database hard limit is reached before an Oversync admission bound rejects
the supported request coherently.

### Pull and Snapshot Capacity

Holding bundle count fixed shows that bundle count dominates latency while row
density dominates allocation:

| Bundles x rows/bundle  |                   Mean |                CV |                               Allocation | Decision |
|------------------------|-----------------------:|------------------:|-----------------------------------------:|----------|
| 1 x 1 / 1 x 10         |       3.952 / 4.444 ms | 11.755% / 19.161% |               12,664 B/200; 29,672 B/303 | Rejected |
| 10 x 1 / 10 x 10       |     10.378 / 10.828 ms |   5.959% / 9.522% |            51,312 B/660; 221,536 B/1,690 | Accepted |
| 100 x 1 / 100 x 10     |     91.312 / 99.155 ms |   5.567% / 5.288% |      436,688 B/5,250; 2,139,072 B/15,550 | Accepted |
| 1,000 x 1 / 1,000 x 10 | 960.583 / 1,009.119 ms |   2.178% / 3.820% | 4,302,464 B/52,643; 21,326,464 B/155,643 | Accepted |

Holding total rows fixed isolates the per-bundle amplification:

| Total rows |    Many one-row bundles | Fewer ten-row bundles | Decision |
|-----------:|------------------------:|----------------------:|----------|
|          1 |    4.196 ms, CV 40.172% |  3.799 ms, CV 21.781% | Rejected |
|         10 |   10.767 ms, CV 12.832% |  4.750 ms, CV 27.175% | Rejected |
|        100 |    84.636 ms, CV 8.527% |  11.575 ms, CV 6.141% | Accepted |
|      1,000 | 1,032.948 ms, CV 7.059% | 109.089 ms, CV 5.163% | Accepted |

The instrumented 20-bundle/200-row pull issued 65 database operations, about
three per bundle plus fixed work. The equal-row 1,000-row pair differs by about
9.5x in latency. This dynamically confirms the pull-amplification part of
OS-AUD-011.

| Snapshot rows |                     Result |                         Allocation | Decision            |
|--------------:|---------------------------:|-----------------------------------:|---------------------|
|             0 |  mean 5.669 ms; CV 18.373% |               13,288 B; 209 allocs | Rejected            |
|         1,000 | mean 349.613 ms; CV 4.162% |         2,754,080 B; 52,003 allocs | Accepted            |
|       100,000 |      33.118s; 3,019 rows/s |    262,701,456 B; 5,200,773 allocs | One-sample boundary |
|     1,000,000 |     340.259s; 2,939 rows/s | 2,748,279,824 B; 52,012,308 allocs | One-sample boundary |

Repeating the million-row point for five warmups and ten samples would require
about 85 minutes of materialization, so it is explicitly not a baseline. The
100k/1m curve is linear but costly: about 52 allocations and 2.7 KiB per row.
The 200-row instrumented snapshot issued 208 operations, confirming the
one-insert-per-row amplification in OS-AUD-011.

### Instrumented Operations, Plans, and Profiles

| Operation                  |           p50 / mean |     CV | Queries | Other evidence                                                               | Decision |
|----------------------------|---------------------:|-------:|--------:|------------------------------------------------------------------------------|----------|
| Push 10 rows               |   15.820 / 15.684 ms | 11.15% |      51 | 18,178 approximate cluster WAL bytes; 247,114 allocated bytes; 4,506 mallocs | Rejected |
| Pull 20 bundles / 200 rows |   19.128 / 19.703 ms |  9.54% |      65 | 228,530 allocated bytes; 2,878 mallocs                                       | Accepted |
| Snapshot 200 rows          |   63.549 / 66.522 ms |  6.99% |     208 | 123,044 approximate WAL bytes; 438,891 allocated bytes; 9,486 mallocs        | Accepted |
| Status 20 bundles          | 309.292 / 310.629 us |  7.51% |       1 | 3,286 allocated bytes; 30 mallocs                                            | Accepted |

The process reported raw Darwin peak RSS 25,247,744 bytes, heap allocation
3,173,752 bytes, heap system memory 7,798,784 bytes, total allocation
17,774,888 bytes, 285,222 mallocs, and four goroutines. Peak RSS is a
process-lifetime representative-workload observation, not scale-specific RSS.
Forced pool and advisory-lock waits each lasted about 101 ms; cancellation
released resources and the lock operation retried successfully.

`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` showed an index-only
`bundle_log_pkey` pull page (20 rows, two shared hits, 0.041 ms), indexed
snapshot row-state lookup (200 rows, five hits, 0.038 ms), primary-key business
row loading plus 57 KiB in-memory quicksort (200 rows, eight hits, 0.346 ms),
and sequential status aggregates.

Status scaling captured the exact SQL emitted by `GetStatus`, then planned the
same query:

|   Bundles |      Median / mean |     CV | Plan execution | Decision |
|----------:|-------------------:|-------:|---------------:|----------|
|         0 |   0.276 / 0.293 ms | 15.45% |       0.036 ms | Rejected |
|     1,000 |   0.432 / 0.550 ms | 51.53% |       0.211 ms | Rejected |
|   100,000 |   8.315 / 8.377 ms |  6.40% |      20.268 ms | Accepted |
| 1,000,000 | 81.809 / 82.058 ms |  1.22% |      76.424 ms | Accepted |

At one million bundles, count and byte-sum execute as separate parallel
sequential scans with three workers processing about 333,333 rows each. This
confirms the status-scaling part of OS-AUD-013. Application/database clock
mixing remains separate unresolved candidate OS-AUD-020.

CPU samples were dominated by database/system waiting: `runtime.kevent` 37.5%,
raw syscalls 29.17%, and `madvise` plus pthread signaling 12.5% each. Heap
`alloc_space` leaders included `reflect.mapassign_faststr0` (3.073 MiB),
`loadSnapshotLiveRowState` (1.544 MiB), `loadCommittedBundle` (1.538 MiB flat,
3.586 MiB cumulative), wire sync-key conversion (1.536 MiB), and
`encoding/json.Unmarshal` (1.536 MiB flat, 6.657 MiB cumulative). The block
profile was dominated by test/runtime and pool health-check waits and is not
used to rank application bottlenecks.

### Concurrency, Watch, Bootstrap, Cleanup, and Pruning

Concurrency p50/p95/p99 use every request; CV uses ten independent round
durations. That distinction corrects an early harness result that measured
queue-position dispersion as noise.

| Topology/workers | Mean; p50 / p95=p99                 | Round CV | Throughput | Pool wait/push | Decision |
|------------------|-------------------------------------|---------:|-----------:|---------------:|----------|
| Same / 1         | 14.037; 14.149 / 16.619 ms          |   13.43% |    71.20/s |              0 | Rejected |
| Same / 8         | 66.497; 65.748 / 110.806 ms         |   10.77% |    87.96/s |              0 | Rejected |
| Same / 32        | 264.926; 256.000 / 549.577 ms       |   18.78% |    86.33/s |       139.7 ms | Rejected |
| Same / 128       | 1,060.278; 1,046.539 / 1,865.429 ms |   11.49% |    83.44/s |       920.2 ms | Rejected |
| Distinct / 1     | 15.252; 15.397 / 17.743 ms          |   8.552% |    65.51/s |              0 | Accepted |
| Distinct / 8     | 36.068; 35.168 / 41.590 ms          |   7.192% |    216.1/s |              0 | Accepted |
| Distinct / 32    | 90.155; 91.908 / 123.428 ms         |   3.631% |    272.6/s |        49.1 ms | Accepted |
| Distinct / 128   | 324.920; 325.874 / 461.777 ms       |   1.548% |    280.7/s |       282.8 ms | Accepted |

The final run allocated about 60-96 KiB and 1,111-1,117 Go objects per push.
Same-scope throughput saturates under advisory serialization; distinct scopes
scale until the 12-connection pool dominates. This reinforces OS-AUD-019.

The watch benchmark measures the in-process hub, not 10,000 real TCP/SSE
sockets. Every subscriber has one buffer slot and one goroutine; every final
sub-benchmark returned the hub count and post-cleanup goroutine delta to zero.

| Subscribers/mode        |                 Mean |              CV |               Throughput | Setup heap/subscriber | Decision            |
|-------------------------|---------------------:|----------------:|-------------------------:|----------------------:|---------------------|
| 100 slow / reconnect    |     7.704 / 8.829 us | 2.845% / 15.66% | 12.60M / 11.01M events/s |           about 410 B | Accepted / rejected |
| 1,000 slow / reconnect  |   78.866 / 87.500 us | 3.069% / 2.019% |        12.63M / 11.39M/s |         903 B / 479 B | Accepted / accepted |
| 10,000 slow / reconnect | 818.083 / 885.212 us | 15.55% / 3.043% |        12.22M / 11.29M/s |         942 B / 512 B | Rejected / accepted |

Reconnect figures additionally report amortized whole-workload allocation per
reconnect (not reconnect-attributable allocation): 648 B, 503 B, and 410 B at
100/1k/10k. Real HTTP cleanup and slow-client lifecycle are proven in Phase 5.

Bootstrap final results were 27.086 ms/CV 3.745% at one table (accepted),
58.887 ms/98.26% at ten (rejected), 1.836s/135.6% at 100 (rejected), and
2.113s/7.796% at 1,000 (accepted, 473.2 tables/s). Timed Bootstrap allocation
was about 7.2 KiB and 133 objects per table at 1,000 tables. No superlinear
claim is made from the noisy middle points.

Expired push/snapshot cleanup at 100 sessions averaged 0.702/0.675 ms with
6.085%/4.412% CV (accepted). At 10,000 sessions it averaged 34.797/34.048 ms
with 18.66%/3.464% CV (push rejected, snapshot accepted). Pruning 100 bundles
while retaining ten averaged 1.848 ms/CV 22.18% (rejected); pruning 10,000
averaged 30.247 ms/CV 3.533% and 330,281 bundles/s (accepted). Custom Go
allocation metrics cover only the timed SQL-owning operation, excluding
seeding and verification; they do not measure PostgreSQL server memory.

### Complexity and Bottleneck Ranking

1. Snapshot materialization is linear but dominant: one database insert per
   row, about 52 allocations per row, 2.75 GiB allocated and 340 seconds at one
   million rows (OS-AUD-011).
2. Push/capture and hashing are linear but allocation-heavy. Payload size
   multiplies copying/canonicalization cost, and 5,000 x 64 KiB push reaches a
   PostgreSQL aggregate ceiling before Oversync rejects it (OS-AUD-004/011).
3. Pull is linear in bundle count at about three database operations per
   bundle; row count primarily raises decode/allocation cost (OS-AUD-011).
4. Status is linear in retained global bundle cardinality and scans
   `bundle_log` twice (OS-AUD-013).
5. Same-scope concurrency serializes; distinct scopes scale until pool wait
   dominates (OS-AUD-019).
6. Hub publication is linear in subscribers and carries one goroutine plus
   per-subscriber memory. The 10k transport/socket capacity remains outside
   this process-local profile.
7. Bootstrap, cleanup, and pruning completed at every required largest point;
   noisy points are explicitly not baselines and show no proven superlinear
   behavior.

These are local capacity curves, not production SLOs, and the audit makes no
claim that performance is "great."

## Residual Risks

- Local Go 1.25.0 and Go 1.26.5 lanes pass against automatically provisioned
  PostgreSQL 17.10 across the recorded final matrix. This remains local
  runner evidence rather than a hosted or deployment certification.
- Deterministic SQLSTATE injection proves `40001`/`40P01` retry classification,
  but Phase 4 does not claim a naturally formed serialization-conflict or
  deadlock lock graph.
- Ten-thousand-subscriber capacity is an in-process hub profile, not a real
  10,000-socket HTTP/SSE load test; transport lifecycle has bounded Phase 5
  evidence only.
- Top 100k/1m snapshot and 5,000 x 64 KiB payload results are one-sample
  capacity boundaries, not stable regression baselines.
- OS-AUD-020 remains an unresolved application/database clock-skew hypothesis.
- Security posture remains unknown because security research is outside scope.
- Passing the default suite and `go vet` is not evidence that the confirmed
  audit findings are safe.

## Remediation Ordering

No remediation is authorized during this audit. Future work is ordered as
these independently approved slices:

1. Critical C1, OS-AUD-001: exact JSON/canonical hash correctness and legacy
   hash compatibility. Stop after promoted exact-value/fuzz regressions and a
   stored-history compatibility decision.
2. Critical C2, OS-AUD-018: reject unlogged registered tables with an operator
   migration/readiness path. Stop after bootstrap plus real crash/restart tests
   are green.
3. Critical C3, OS-AUD-002: fail-closed TRUNCATE guard and coherent
   administrative reset workflow. Stop after outside/inside-bundle tests pass.
4. High H1-H5, one finding per slice in this order: OS-AUD-003 retention
   boundaries, OS-AUD-005 populated adoption, OS-AUD-006 nullable identity,
   OS-AUD-007 managed-layout validation, and OS-AUD-008 exact protocol
   identity. Stop after each finding's registry regression group is green and
   its compatibility/migration notes are accepted.
5. High H6-H9, one finding per slice in this order: OS-AUD-004 admission
   bounds, OS-AUD-009 strict bounded decoding, OS-AUD-011 snapshot/pull/watch
   bounded work, and OS-AUD-019 advisory-lock/pool fairness. Stop after each
   slice's quota/error semantics, resource cleanup, and load regressions pass.
6. Medium M1-M4, one finding per slice in this order: OS-AUD-017 generated
   columns, OS-AUD-015 lifecycle error mapping, OS-AUD-013 bounded status
   metrics, and OS-AUD-014 OpenAPI/documentation compatibility. Stop after each
   promoted test and public compatibility review.
7. Low L1, OS-AUD-016: remove only proven dead declarations and stop when both
   staticcheck build modes are clean.
8. Investigate OS-AUD-020 separately. Do not authorize remediation or promote
   it into the confirmed ordering until a deterministic skewed-clock reproducer
   establishes the behavior.

Every slice requires separate approval, compatible-first design, promotion of
the named audit reproducer into a green regression, version/contract review,
and focused plus repository-wide validation before the next slice begins.

## Phase 8 Final Validation and Closeout

All Phase 8 commands ran on 2026-07-10 from the repository root on the Apple
M2 Max `darwin/arm64` runner. Database commands unset `TEST_DATABASE_URL` and
therefore used the test-owned PostgreSQL 17.10 lane. Unless a row says
otherwise, results include `/usr/bin/time -p` wall timing and a 30-minute test
timeout. Expected-contract failures were never mixed into a green result.

### Default, Tagged, Race, and Repository-Wide Tests

For readability, the command table names these three literal `-run` values:

- Green selector:
  `^(TestAuditBootstrapAtomicity_|TestAuditConcurrency_(TwoServices|BlockedScopeAllows)|TestAuditDriftResilience_|TestAuditExactValuesCharacterization_|TestAuditHarnessHelperProcess$|TestAuditHarness_HelperProcessStartsServesRealHandlersAndStops$|TestAuditFault_|TestAuditHTTPRecovery_|TestAuditIntegrityOracle_|TestAuditPostgresRestart_(LoggedState|InFlight)|TestAuditProcessRecovery_|TestAuditProfileMetrics_|TestAuditProtocolGreen_|TestAuditReferenceModel_|TestAuditResourceSafety_|TestAuditSnapshotAfterPruningReconstructsAuthoritativeState$|TestAuditRetryFault_|TestAuditSchemaEdge_(PartitionedRegisteredTableIsCoherentOrRejectedAtomically|ReservedIdentifiersRoundTrip|QuotedMixedCaseIdentifiersRejectWithoutPartialBootstrap|ViewRegistrationRejectsWithoutPartialBootstrap)|TestAuditSSELifecycle_|TestAuditTCPProxy_|TestAuditTransactionFault_)`
- Expected-red selector:
  `^(TestAuditCanonicalJSON_|TestAuditPayloadExtractor_|TestAuditRetainedFloor_|TestAuditActorMiddleware_PreservesExactProtocolTokens$|TestAuditMaxRowsPerBundle_|TestAuditMaxBytesPerBundle_|TestAuditBootstrap_(AdoptsPopulatedRegisteredTableIntoSnapshot|RejectsNullableScopeOrSyncKeyColumns|RejectsDamagedExistingLayout)|TestAuditRegisteredTable_TruncateFailsClosed$|TestAuditProcessPull_RejectsZeroCheckpointAfterDatabasePruning$|TestAuditExactValuesContract_|TestAuditSchemaEdge_(GeneratedColumnRoundTripsOrBootstrapRejectsAtomically|UnloggedRegisteredTableIsRejectedBeforeBootstrapMutation)|TestAuditPullCheckpointBoundaries$|TestAuditConcurrencyContract_|TestAuditPostgresRestart_UnloggedAcknowledgedRowMustNotDisappearWhileMetadataSurvives$|TestAuditHTTPCreatePushSession_|TestAuditProtocolContract_|TestAuditExpectedDriftResilience_|TestAuditExpectedLifecycle_)`
- Race selector:
  `^(TestAuditBootstrapAtomicity_|TestAuditFault_|TestAuditHarness_HelperProcessStartsServesRealHandlersAndStops$|TestAuditTCPProxy_|TestAuditProcessRecovery_|TestAuditHTTPRecovery_|TestAuditTransactionFault_|TestAuditConcurrency_(TwoServices|BlockedScopeAllows)|TestAuditRetryFault_|TestAuditPostgresRestart_(LoggedState|InFlight)|TestAuditProtocolGreen_|TestAuditResourceSafety_|TestAuditSSELifecycle_|TestAuditDriftResilience_|TestSyncService_CloseWaitsForInflightOperations$|TestRunBundleChangeListener_ReconnectCatchUpWakesActiveSubscribers$)`

| Exact command                                                                                                                                | Exit/result                                                                                                                                       |
|----------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./oversync -timeout=15m`                                                     | Exit 0; package 22.498s; wall 22.94s                                                                                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.25.0 go test -count=1 ./oversync -timeout=15m`                                                     | Exit 0; package 20.508s; wall 21.64s                                                                                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '<green selector>' -count=1 -timeout=30m`        | Exit 0; package 28.852s; wall 29.98s                                                                                                              |
| Same green command with `GOTOOLCHAIN=go1.25.0`                                                                                               | Exit 0; package 27.699s; wall 29.46s                                                                                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '<expected-red selector>' -count=1 -timeout=30m` | Expected exit 1; package 7.594s; wall 8.78s; failures were desired contract assertions only, with no panic, timeout, or unrelated harness failure |
| Same expected-red command with `GOTOOLCHAIN=go1.25.0`                                                                                        | Expected exit 1; package 7.068s; wall 8.13s; same defect set and no unrelated failure                                                             |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -race -count=1 ./oversync -timeout=30m`                                               | Exit 0; package 26.660s; wall 29.02s; no race report                                                                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.25.0 go test -race -count=1 ./oversync -timeout=30m`                                               | Exit 0; package 27.096s; wall 35.83s; no race report                                                                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -race -tags=oversync_audit ./oversync -run '<race selector>' -count=1 -timeout=30m`   | Exit 0; package 27.050s; wall 29.35s; no race report                                                                                              |
| Same audit-tagged race command with `GOTOOLCHAIN=go1.25.0`                                                                                   | Exit 0; package 27.596s; wall 30.40s; no race report                                                                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./... -timeout=30m`                                                          | Exit 0; `oversync` 23.731s, `oversqlite_e2e` 14.738s; wall 25.05s; all root-module packages passed                                                |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.25.0 go test -count=1 ./... -timeout=30m`                                                          | Exit 0; `oversync` 37.868s, `oversqlite_e2e` 20.180s; wall 39.32s; all root-module packages passed                                                |

An initial orchestration attempt dispatched the same two repository-wide
commands but yielded partial package output without retaining their final exit
status. It is not counted as evidence; both exact commands were rerun
individually to produce the authoritative rows above. The nested mobile module
is outside the root module's `./...` expansion and was not silently claimed.

### Fuzz Corpus Replay

Each active parser target ran with this exact command shape on both Go 1.26.5
and Go 1.25.0:

```text
env -u TEST_DATABASE_URL GOTOOLCHAIN=<toolchain> go test -tags=oversync_audit ./oversync -run '^$' -fuzz '^<target>$' -fuzztime=100x -parallel=1 -timeout=3m
```

The six `<target>` values were
`FuzzAuditProtocolSchemaName_Deterministic`,
`FuzzAuditProtocolTableName_Deterministic`,
`FuzzAuditProtocolColumnName_Deterministic`,
`FuzzAuditProtocolSchemaTableKey_Deterministic`,
`FuzzAuditProtocolNotificationChannel_Deterministic`, and
`FuzzAuditProtocolSyncKeyEncoding_Deterministic`. All twelve commands exited
0 after at least 100 executions. Package/wall times on Go 1.26.5 were
1.410s/4.67s, 0.604s/1.05s, 3.424s/3.89s, 0.440s/0.90s,
3.361s/3.80s, and 0.447s/0.89s. The Go 1.25.0 times were
0.780s/12.58s on its cold first run, then 0.601s/1.27s,
0.616s/1.06s, 0.885s/1.33s, 0.538s/1.00s, and 0.469s/0.96s.

Before the corrected sync-key target, both toolchains were invoked once with
the misspelled selector `FuzzAuditSyncKeyEncoding_Deterministic`. Each command
exited 0 with `testing: warning: no fuzz tests to fuzz`; those no-op runs are
recorded but do not count as coverage.

The canonical JSON target used the same command shape with
`FuzzAuditCanonicalJSON_Idempotent`. Both toolchains produced the required
expected exit 1 while replaying the stored seed before mutation: valid JSON
`1e700` failed `float64` decoding. Package/wall times were 0.452s/0.91s on Go
1.26.5 and 0.389s/0.83s on Go 1.25.0. Before and after the runs,
`find oversync/testdata/fuzz/FuzzAuditCanonicalJSON_Idempotent -type f -maxdepth 1 -print -exec shasum -a 256 {} \;`
reported exactly one file with SHA-256
`c349e8dbb1ab73231e2c9d775ae55311a43569a5724f11117667263b23ea9ce7`.
The minimized corpus is unchanged.

### Closeout Benchmark Smoke

| Exact command                                                                                                                                                                                                                                                                                              | Exit/result and baseline comparison                                                                                                                        |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 GOMAXPROCS=1 go test -tags=oversync_audit ./oversync -run '^$' -bench '^BenchmarkAuditCanonicalJSON$' -benchmem -benchtime=500ms -count=15 -cpu=1 -timeout=30m`                                                                                             | Exit 0; package 9.796s; wall 11.07s; final-ten mean 2.879 us, CV 0.454%, 1,872 B/43 allocs; 3.4% below the accepted Phase 6 mean and inside its 10% budget |
| Same command with `GOTOOLCHAIN=go1.25.0`                                                                                                                                                                                                                                                                   | Exit 0; package 9.469s; wall 10.62s; final-ten mean 2.825 us, CV 1.606%, 1,872 B/43 allocs; inside the budget                                              |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 GOMAXPROCS=1 OVERSYNC_AUDIT_BENCHMARK_PROFILE=smoke OVERSYNC_AUDIT_BENCHMARK_OPERATION_TIMEOUT=10m go test -tags=oversync_audit ./oversync -run '^$' -bench '^BenchmarkAuditDatabaseSnapshotCreate$' -benchtime=1x -count=15 -benchmem -cpu=1 -timeout=30m` | Exit 0; package 9.317s; wall 10.07s; 1,000-row final-ten mean 334.015ms, CV 3.722%, 2,754,080 B/52,003 allocs; 4.5% below the accepted Phase 6 mean        |
| Same command with `GOTOOLCHAIN=go1.25.0`                                                                                                                                                                                                                                                                   | Exit 0; package 9.326s; wall 10.04s; 1,000-row final-ten mean 340.258ms, CV 5.160%, 2,754,080 B/52,003 allocs; inside the budget                           |

The full benchmark profiles remain the Phase 6 capacity evidence. Phase 8
reran the smoke profile only; no one-sample top boundary was converted into a
regression baseline.

### Static Analysis and Drymint Review

| Exact command                                                          | Exit/result                                                                                                                       |
|------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `env GOTOOLCHAIN=go1.26.5 go vet ./oversync`                           | Exit 0; wall 2.04s; no diagnostics                                                                                                |
| `env GOTOOLCHAIN=go1.26.5 go vet -tags=oversync_audit ./oversync`      | Exit 0; wall 0.48s; no diagnostics                                                                                                |
| `env GOTOOLCHAIN=go1.25.0 go vet ./oversync`                           | Exit 0; wall 1.53s; no diagnostics                                                                                                |
| `env GOTOOLCHAIN=go1.25.0 go vet -tags=oversync_audit ./oversync`      | Exit 0; wall 1.55s; no diagnostics                                                                                                |
| `staticcheck -version`                                                 | Exit 0; `staticcheck 2025.1.1 (0.6.1)`                                                                                            |
| `env GOTOOLCHAIN=go1.26.5 staticcheck ./oversync`                      | Expected exit 1; 13 existing `U1000` declarations; wall 4.02s; no new audit-file diagnostic                                       |
| `env GOTOOLCHAIN=go1.26.5 staticcheck -tags=oversync_audit ./oversync` | Expected exit 1; 12 declarations from the same pre-existing set; wall 0.66s; `preparePushRows` is exercised by the tagged harness |
| Same two staticcheck commands with `GOTOOLCHAIN=go1.25.0`              | Expected exit 1; the same 13/default and 12/tagged declaration sets; wall 2.40s and 1.31s                                         |

The final explicit-path Drymint command was
`drymint review change --no-index --json --max-results 20` with one repeated
`--path` for every result of
`rg --files oversync | rg '(^oversync/audit.*_test\.go$|^oversync/integration_test_(helpers|postgres)_test\.go$)' | sort` (
29 files). It exited 0 in 1.75s and emitted one inspect-level candidate:
`BenchmarkAuditDatabasePull` versus `BenchmarkAuditDatabasePullEqualRows`
(score 0.902). Source inspection showed that the first wrapper fixes bundle
count while varying row density and the second fixes total rows while varying
bundle count. Both already delegate substantive execution to
`auditRunDatabasePullBenchmarkGroup`, so the wrappers are intentional distinct
experiment definitions and no edit is justified. A supplemental
`drymint review change --scope oversync --working-tree --json --max-results 20`
exited 0 in 0.95s and repeated the same candidate; its Git working-tree source
count was zero because the new audit files are untracked, so it is not counted
instead of the explicit-path review.

The review lacked a prewrite baseline because Phase 8 began after the audit
assets existed. That attribution limitation is recorded and does not hide an
emitted finding. Every emitted finding was inspected before closeout.

### Workspace Boundary and Documentation Checks

| Exact command                                                                                                              | Exit/result                                                                                                                                                                         |
|----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `git rev-parse HEAD`                                                                                                       | Exit 0; `1f1118f1749490daaa8f1f76a7d8516f5303a873`, the locked baseline                                                                                                             |
| `git diff --name-only -- '*.go'` plus `git ls-files --others --exclude-standard -- '*.go'`                                 | Only `oversync/*_test.go` files; no production Go file                                                                                                                              |
| `git diff --name-only` plus `git status --short --untracked-files=all`                                                     | Audit tests/helpers, evidence report/artifacts, and the fuzz seed are in scope; only `drymint.toml` and `drymint/triage.yaml` are protected unrelated changes; nothing is staged    |
| `rg --files-without-match '^//go:build oversync_audit$' oversync -g 'audit*_test.go'`                                      | Exit 1 with no output: no mismatched file; a separate `rg -l` counted all 27 expected-defect/audit files with the tag                                                               |
| `gofmt -d oversync/audit*_test.go oversync/integration_test_helpers_test.go oversync/integration_test_postgres_test.go`    | Exit 0 with no output; all audit Go/test files are formatted                                                                                                                        |
| `ps -ax -o pid=,etime=,command=                                                                                            | rg '(go test                                                                                                                                                                        |oversync.*helper|postgres:17\.10)'` and `docker ps -a --format '{{.ID}} {{.Image}} {{.Names}}' --filter ancestor=postgres:17.10` | Only the process scanner matched itself; the container query returned no row. No final test/helper/PostgreSQL 17.10 process or container was left behind. |
| `git diff --check`                                                                                                         | Exit 0; no tracked whitespace diagnostic                                                                                                                                            |
| `git diff --no-index --check -- /dev/null specs/2026-07-10-oversync-assurance-audit.md`                                    | Expected new-file exit 1; no whitespace diagnostic                                                                                                                                  |
| `git diff --no-index --check -- /dev/null engineering-docs/audit/2026-07-10-oversync-assurance.md`                         | Expected new-file exit 1; no whitespace diagnostic                                                                                                                                  |
| `shasum -a 256 drymint.toml drymint/triage.yaml`                                                                           | Exact handoff sentinels `d43d00855663b3a02342241d4bb7887046e1f3e5e1cd7a768fdbac7722d99b5f` and `1368938abe3d9b46aebb9a6ffb4ae9d01424ddc8b79e186d9eb0331a97226e87`; no audit edit    |
| `git check-ignore -v specs/2026-07-10-oversync-assurance-audit.md engineering-docs/audit/2026-07-10-oversync-assurance.md` | Only the spec is ignored. The audit-added report-directory ignore rule was removed; the report and 17 KiB of profile artifacts are visible to normal Git review and ready to track. |

All changed Go sources are tests. No `go.mod`, `go.sum`, Swagger/OpenAPI,
schema/migration, production Go, exported API, HTTP contract, runtime
dependency, production behavior, GitHub Actions, CI, or scheduled automation
file changed. The report is new and therefore directly validated until the
user stages it; its intended tracked ownership is no longer contradicted by an
ignore rule. An initial `rg -L` invocation was not accepted as proof because
`-L` means follow symbolic links in ripgrep; the explicit
`--files-without-match` command above is the authoritative tag check.

## Phase Evidence Log

| Phase                                      | Status   | Evidence summary                                                                                                                                                                                                                                                                                                                                      |
|--------------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0. Spec and report skeleton                | Complete | Source-of-truth spec and this report created; direct spec/report whitespace validation passed                                                                                                                                                                                                                                                         |
| 1. Reproducible test foundation            | Complete | Automatic PostgreSQL 17.10, per-test databases, exact override, deterministic cleanup, real-handler helper process, interruptible TCP proxy, integrity oracle, reference model, and local Go 1.25/1.26 lanes pass                                                                                                                                     |
| 2. Contract, invariant, and static audit   | Complete | Exported operations, all 14 HTTP handlers, managed PostgreSQL objects, cleanup/locking/data paths, invariants, static results, and contract drift are recorded and assigned                                                                                                                                                                           |
| 3. Correctness and schema integrity        | Complete | Normal operations, exact values, bootstrap atomicity/drift, schema edges, retention boundaries, post-prune snapshots, and the seeded model are recorded as passes or assigned findings                                                                                                                                                                |
| 4. Durability, concurrency, and faults     | Complete | Ambiguous responses, helper termination, staged restart, cancellation at transaction stages, SIGKILL during commit/pull/snapshot/prune/listener work, exact retries, two-service concurrency, small-pool behavior, SQLSTATE/lock/connection faults, integrity checks, and focused race evidence are recorded; OS-AUD-018 and OS-AUD-019 are confirmed |
| 5. Protocol robustness and resource safety | Complete | Exact actors, strict/malformed parsing, bounded pressure and cleanup, managed-object drift, status/error mapping, SSE lifecycle/reconnect, and six parser fuzz targets are recorded; OS-AUD-015 is dynamically confirmed and security remains excluded                                                                                                |
| 6. Performance and scalability             | Complete | Go 1.26.5/PostgreSQL 17.10 micro, push/capture, two pull controls, snapshot, concurrency, watch, bootstrap, cleanup, pruning, status, plan, WAL/query, RSS, and CPU/heap/block profiles completed; accepted and rejected-CV results are separated and top boundaries receive no budget                                                                |
| 7. Findings and roadmap                    | Complete | Seventeen confirmed findings have final severity/confidence and explicit evidence, reproduction, impact, affected-version, compatibility, regression, and residual-risk fields; OS-AUD-020 is separated as unresolved; compatible-first remediation is ordered with stop gates                                                                        |
| 8. Final validation and closeout           | Complete | Go 1.25.0/1.26.5 default, tagged green/expected-red, default and focused race, fuzz, smoke benchmark, repository-wide, vet, staticcheck, Drymint, boundary, protected-file, and direct document checks are recorded; no production surface changed                                                                                                    |

## Closeout Gate

Every execution-spec phase is checked, all seventeen confirmed findings and
the unresolved OS-AUD-020 hypothesis are classified, required validation is
recorded, and the remediation roadmap is decision-ready. This completes the
local assurance audit evidence, not the remediation. Stop here and request
review; production fixes require a separately approved implementation spec.
