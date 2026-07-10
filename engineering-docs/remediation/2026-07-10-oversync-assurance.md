# Oversync Assurance Remediation Ledger

Status: `C1, C2, C3, H1, the pre-H2 protocol corrective, H2, and H3 complete`

Created: 2026-07-10

Umbrella: `specs/2026-07-10-oversync-assurance-remediation.md`

Historical audit: `engineering-docs/audit/2026-07-10-oversync-assurance.md`

## Repository Baselines

| Repository | Baseline | Initial tracked state | Preserved ignored state |
| --- | --- | --- | --- |
| `/Users/pochkin/Projects/my/go-oversync` | `51e50f541b1a68b645a45e0567e2f21e90e34d0a` | clean | `specs/`, including the umbrella and C1 child spec |
| `/Users/pochkin/Projects/my/sqlitenow-kmp` | `d85f046736f96aa9aed6f3876735203b6823d66c` | clean | `specs/` |

`drymint.toml` and `drymint/triage.yaml` are protected and must remain
unchanged. No pre-existing tracked, untracked, or ignored change overlapped C1.

## C1 / OS-AUD-001

Status: `complete`

The subsection status above was reconciled on 2026-07-12 with the already-
complete C1 outcome, top-level ledger status, and umbrella checklist. The
historical reopened-review and corrective evidence below is preserved
unchanged.

### Phase 0 evidence

All commands ran on 2026-07-10 from the repository named by the command.
`TEST_DATABASE_URL` was unset for database-backed Go commands, so the audit
harness owned isolated PostgreSQL 17.10 databases.

| Command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| `GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditCanonicalJSON_PreservesExactNumbers|TestAuditPayloadExtractor_Int64RejectsFractionalNumbers|TestAuditExactValuesContract_)' -count=1 -timeout=10m -v` | 1 | 4.98s | Expected red: exact decimals/int64 were rounded, `1e700` rejected, UTF-16 order differed, adjacent integers collided, fractional int64 truncated, and PostgreSQL committed/pull payloads lost digits. |
| `GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditExactValuesCharacterization_|TestCompactEncoding_CommittedBundleHashCanonicalizesLogicalJSON$|TestCompactEncoding_RenderBundleHashUsesLowercaseHex$)' -count=1 -timeout=10m -v` | 0 | 0.75s | Existing green characterization retained. |
| `GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^FuzzAuditCanonicalJSON_Idempotent$' -count=1 -v` | 1 | 0.76s | Expected red seed replay: corpus `c349e8dbb1ab7323` fails only because valid `1e700` is routed through `float64`. Corpus SHA-256 remains `c349e8dbb1ab73231e2c9d775ae55311a43569a5724f11117667263b23ea9ce7`. |
| `GOTOOLCHAIN=go1.26.5 go test -count=1 ./oversqlite` | 0 | 3.21s | Pre-change Go client baseline green. |
| `./gradlew oversqliteComprehensive` | 0 | 65.82s | Pre-change KMP comprehensive baseline green. |
| `cd dart && flutter test packages/sqlitenow_oversqlite` | 0 | 9.67s | Pre-change Dart baseline green: 91 passed, 16 opt-in realserver tests skipped. |

Drymint `describe.scope`, bounded prewrite context, `impact.analyze`, path-set
`edit.envelope`, and prewrite baselines were captured in both repositories.
The Go path-set baseline is `rb0:5212d66865bab0b66da6d267`; the Kotlin
path-set baseline is `rb0:b3768694a9ded997d484e7f7`. Drymint rejected the
first Go baseline request because its emitted request treated the planned new
`internal` directory as an already-existing scope; existing Go paths were
captured separately and the new package will receive explicit path review.

### Phase 1 locked contract

C1 uses RFC 8785 JSON Canonicalization Scheme (JCS) for ordinary JSON values.
The JCS number domain is finite IEEE-754 binary64; the implementation does not
claim an arbitrary-precision extension. PostgreSQL `BIGINT` and exact
`NUMERIC`/`DECIMAL` business values are schema-typed JSON strings on every wire
surface. This follows RFC 8785's high-precision string guidance and permits
ordinary platform JSON parsers in every maintained runtime.

Exact int64 text uses `-?(0|[1-9][0-9]*)`, rejects negative zero, and must fit
signed 64-bit range. Exact decimal text uses
`-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?(0|[1-9][0-9]*))?`, rejects every
negative-zero spelling, and preserves the authoritative PostgreSQL committed
text rather than normalizing it in clients. General duplicate-property request
rejection remains H7; C1 conformance fixtures use unique properties.

Oversqlite configuration/runtime metadata—not SQLite declared-type names—marks
columns as `exact_int64`, `exact_decimal`, or `approximate`. Exact int64 binds
to ordinary SQLite INTEGER, exact decimal to ordinary TEXT, and approximate
finite binary64 to REAL. Invalid grammar, range, or affinity rejects before the
surrounding transaction mutates business or retry state. Ordinary TEXT is not
guessed to be decimal.

Canonical request hashes and committed bundle hashes are lowercase SHA-256 of
JCS logical-row arrays whose protocol counters are strings. The request hash
commits to the immutable staged request and is stored with the source tuple;
the bundle hash independently commits to authoritative committed rows.
`byte_count` is the exact canonical byte length.

The authoritative KMP/Dart vectors are
`oversqlite-contracts/canonical-json/jcs-typed-numerics.json`. Go executes
equivalent vectors and checks the authoritative file SHA-256 when the sibling
checkout is present. No custom lossless parser, custom number wrapper, custom
SQLite type, hash-version negotiation, fallback, alias, or state migration is
part of the released design.

### Destructive v0 cutover

1. Stop every old server and client writer.
2. Delete old staged server sessions and recreate the complete server database
   whenever synchronized business rows or historical bundles exist.
3. Delete and recreate every Go, KMP, and Dart client database containing old
   sync metadata or synchronized business data.
4. Treat old prepared/committed outboxes, canonical request hashes, remote
   bundle hashes, checkpoints, retry cursors, and outstanding offline work as
   deliberately discarded.
5. Deploy the corrected server and all participating corrected clients as one
   release set. Mixed versions and rollback to legacy writers are unsupported.
6. Only source tuples created after recreation participate in retry and
   ambiguous-response idempotency guarantees.

This procedure is destructive recreation, not repair or migration. Preserving
history, offline work, or rolling availability requires a separately approved
slice and stops C1.

### Residual risk

Historical rounded values and hashes are not reconstructable. The cutover
discards server history, staged sessions, client outboxes/checkpoints, and
possibly outstanding offline mutations. SQLite REAL remains approximate by
contract. Extremely large digit/exponent inputs may create pressure; general
admission limits remain H6/H7 and are not silently folded into C1.

### Validation evidence

All commands below ran on 2026-07-10. Database commands used an unset
`TEST_DATABASE_URL` unless an explicitly named C1-owned realserver database is
shown. No command used or reset a caller-managed database.

#### Go server and client

| Command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./...` | 0 | 24s | Final repository sweep green; `oversync`, `oversqlite`, E2E, examples, and exact cores passed. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.25.0 go test -count=1 ./...` | 0 | 23s | Final supported-toolchain sweep green. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^(TestAuditCanonicalJSON_PreservesExactNumbers|TestAuditPayloadExtractor_Int64RejectsFractionalNumbers|TestAuditExactValuesContract_)' -count=1 -timeout=10m` | 0 | 1.56s | Remediated C1 selector green, including PostgreSQL business/committed/pull/snapshot exact values. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '<remaining expected-red selector>' -count=1 -timeout=30m` | 1 | 6.20s | Expected red. Only unrelated confirmed findings failed; no C1/hash-validation interception, panic, or timeout occurred. The literal selector is the audit report selector with the C1 canonical/payload/exact groups removed. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^$' -fuzz '^FuzzAuditCanonicalJSON_Idempotent$' -fuzztime=10s` | 0 | 11.56s | Preserved 141-input baseline corpus plus 641,298 executions; valid JCS-domain inputs remained idempotent and unsupported-domain inputs rejected. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 GOMAXPROCS=1 go test -tags=oversync_audit ./oversync -run '^$' -bench '^(BenchmarkAuditCanonicalJSON|BenchmarkAuditCommittedBundleHash)$' -benchmem -benchtime=500ms -count=3 -cpu=1` | 0 | 9.20s | Canonical JSON about 3.9-4.2 us/4,176 B/90 allocs; committed hash scales linearly (about 9.1 ms and 9.1 MB at 1,000 rows). Accepted for C1; general admission/performance limits remain H6/H7. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -race -tags=oversync_audit ./oversync -run '^(TestAuditExactValuesContract_|TestAuditProcessRecovery_AmbiguousCommitResolvesToExactlyOneCommittedTuple)$' -count=1 -timeout=20m` | 0 | 3.87s | Exact PostgreSQL paths and ambiguous commit/source tuple recovery green under the race detector. |
| `GOTOOLCHAIN=go1.26.5 go vet ./...` and `GOTOOLCHAIN=go1.26.5 go vet -tags=oversync_audit ./oversync` | 0 | 0.52s / 0.27s | Green. |
| `GOTOOLCHAIN=go1.25.0 go vet ./...` and tagged audit vet | 0 | 1.2s | Green. |
| `staticcheck ./...` and `staticcheck -tags oversync_audit ./oversync` | 1 | 0.82s / 0.95s | Existing unused-helper and `lifecycle_state.go:337` diagnostics remain. The one newly detected unused C1 test helper was removed; no diagnostic points to a new exact/JCS/hash implementation. |
| `go test ./internal/jcs ./internal/protocolhash ./internal/wirevalue ./oversqlite ./oversync -run '^(TestC1|TestAuthoritative|TestValidateDecimal|TestParseInt64|TestPushSession)' -count=1` | 0 | 9.77s | Final post-edit focused exact/JCS/hash/session confirmation green. |

The first attempt to run both full Go toolchains concurrently was discarded:
the existing E2E harnesses share their default local database and interfered
with each other. Both final sweeps above were rerun sequentially and passed.

#### SQLiteNow KMP and generated Oversqlite bridge

| Command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| `./gradlew oversqliteComprehensive` | 0 | 40s | Release-level host suite green. |
| `./gradlew :library-oversqlite:jvmTest --tests '*SharedExactJsonContractFixtureTest*' --tests '*TypedNumericContractTest*' --tests '*OversqlitePayloadCodecTest*'` | 0 | 2s | Shared canonical/hash/SQLite/reset vectors and actual SQLite bindings green. |
| `./gradlew oversqlitePlatformJsNode` | 0 | 18s | JS Node runtime apply/hash/retry surface green. |
| `./gradlew oversqlitePlatformWasmBrowser` | 0 | 21s | Wasm browser runtime surface green. Webpack emitted the repository's existing optional `fs`/`path`/`crypto` resolution warnings; tests passed. |
| `./gradlew oversqlitePlatformMacosArm64` | 0 | 27s | Kotlin/Native macOS runtime surface green. |
| `./gradlew :sqlitenow-compiler:test` | 0 | 33s | Oversqlite-only generated `syncTables` override and compiler regressions green. |
| `./gradlew :sqlitenow-gradle-plugin:test` | 0 | 47s | Plugin consumer/generation lane green; run separately per repository guidance. |
| `./gradlew oversqliteRealserverJvm` | 0 | 6s | Shared KMP realserver catalog green against corrected server. |
| `./gradlew oversqliteRealserverJvmHarness` | 0 | 18s | Generated JVM/runtime harness green, including the business-rich schema. |
| `./gradlew :library-oversqlite:jvmTest --tests '*SharedExactJsonContractFixtureTest*'` | 0 | 1s | Final post-edit authoritative fixture-consumption confirmation green. |
| `./gradlew :library-oversqlite:jvmTest --tests '*BundleHashTest*' --tests '*SharedExactJsonContractFixtureTest*' --tests '*SharedRuntimeStateSchemaFixtureTest*'` | 0 | 2.79s | Post-review exact-number, shared-fixture, and destructive-reset behavior green. |
| `./gradlew :library-oversqlite:jsNodeTest` | 0 | 15s | Post-review common IEEE-754 renderer and all finite RFC 8785 Appendix B vectors green on Kotlin/JS Node. |
| `./gradlew :library-oversqlite:wasmJsBrowserTest` | 0 | 28s | Post-review common renderer green in the Wasm browser lane; only the existing optional webpack resolution warnings appeared. |
| `./gradlew :library-oversqlite:macosArm64Test` | 0 | 36s | Post-review common renderer green on Kotlin/Native macOS. |
| `./gradlew oversqliteComprehensive` | 0 | 38s | Post-review release-level host suite green after removing the stale legacy-schema test helper. |

#### Dart Oversqlite

| Command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| `cd dart && flutter test packages/sqlitenow_oversqlite --reporter compact` | 0 | 3s | 95 passed; 16 opt-in realserver tests skipped in the default lane. |
| `cd dart && flutter test packages/sqlitenow_oversqlite/test/exact_json_contract_fixture_test.dart packages/sqlitenow_oversqlite/test/payload_codec_test.dart --reporter compact` | 0 | 1s | Shared JCS/hash/mapping/reset vectors and actual SQLite INTEGER/TEXT behavior green. |
| `cd dart && dart analyze packages/sqlitenow_oversqlite` | 0 | 0.79s | No issues. |
| `cd dart && OVERSQLITE_REALSERVER_TESTS=true flutter test packages/sqlitenow_oversqlite/test/realserver_conformance_test.dart --reporter compact` | 0 | 1s | Six corrected-server lifecycle, retry, conflict, snapshot, and source-retirement cases green. |
| `cd dart && dart test -p chrome packages/sqlitenow_oversqlite/test/exact_json_contract_fixture_test.dart --reporter compact` | 1 | 1s | Not applicable: the maintained Dart Oversqlite package imports the VM `sqlite3`/`dart:ffi` runtime and has no browser build lane. Kotlin/JS and Kotlin/Wasm supply the supported web runtime proof; Dart VM uses the same authoritative file. |

#### Reset rehearsal and cleanup

`SHOW server_version` returned `17.10 (Homebrew)`. The realserver matrix used
only `oversync_c1_realserver_20260710`, created immediately before the run. The
server was started with
`DATABASE_URL=postgres://postgres:password@localhost:5432/oversync_c1_realserver_20260710?sslmode=disable`
and stopped by signal after KMP and Dart conformance. Each conformance suite
rehearsed the destructive reset endpoint and fresh client databases before
push/retry/pull/snapshot verification. The database was dropped afterward and
`lsof -nP -iTCP:8080 -sTCP:LISTEN` confirmed no listener remained.

C1 lands in the repository commit containing this ledger. No pull request is
associated with the remediation at closeout.

#### Review and closeout

Final Drymint working-tree reviews were run in both repositories (Go
`rv0:5e66e09efe9c02f30139be42`; KMP
`rv0:60196df33e10d6d187026b43`). The original
prewrite IDs could not attribute the expanded final path sets: the Go baseline
used an older source contract and the KMP baseline predated compiler/platform
paths. Current-source reviews were therefore used for the required source
inspection. Go identified `protocolhash.PushRequest` and `CommittedBundle` as a
duplicate pair; inspection retained their deliberately separate request and
authoritative-result field sets so the two security commitments cannot be
accidentally conflated. KMP identified the two Oversqlite-only generated bridge
builders; their repeated `syncTables` parameter construction was extracted to
one helper. Remaining builder similarity reflects one config factory and one
client factory and is intentional.

A subsequent code review found two C1 closeout defects. First, KMP JCS number
rendering delegated to platform `Double.toString()`, whose shortest-decimal
spelling differs at boundary values across JVM, JavaScript, Wasm, and Native.
The platform delegation was replaced with one common exact binary64-to-decimal
renderer using the IEEE-754 bits and round-to-even interval; every finite RFC
8785 Appendix B vector now passes on all four maintained KMP runtime families.
The authoritative shared fixture and the equivalent Go vectors carry checksum
`152c66cec10a5355808c809c30d684aa68ed747c77d6be6491cc6d9587722552`.
Second, an unused KMP contract-test helper still created the pre-reset outbox
schema without `canonical_json_contract`. The helper was deleted, and a
permanent negative test now proves that opening that legacy schema fails with
the documented database-recreation requirement. Drymint review
`rv0:5e40263daf03e0d73328a8cf` was clean for the complete touched KMP path set;
the low-substance add/subtract arithmetic similarity was inspected and retained
because only the shared scale alignment is similar while the checked unsigned
operations are intentionally distinct.

Final `git diff --check` passed independently in both repositories. Direct
`git diff --no-index --check -- /dev/null <ignored-spec>` returned the expected
content-difference exit 1 for each ignored spec with no whitespace diagnostic.
Protected
`drymint.toml` and `drymint/triage.yaml` have no diff. Production searches found
no custom exact-JSON package, superseded canonicalizer, custom decimal SQLite
type, number wrapper, C1 compatibility alias, fallback hash, version negotiation,
or migration branch. The umbrella remains `approved`, not complete, because C2
and later slices remain outstanding.

### C1 outcome

`OS-AUD-001` is closed within the tested C1 envelope. Server, Go, KMP, and Dart
use RFC 8785 JCS plus schema-typed exact numeric strings; request and committed
hash commitments are independently verified; exact values survive PostgreSQL
and SQLite push/replay/pull/snapshot paths; invalid mappings reject before
mutation; and post-reset ambiguous retries remain source-tuple idempotent.

## C2 / OS-AUD-018

Status: `complete`

### Baselines and expected-red evidence

The C2 implementation baseline is Go commit
`5fb59483f5bc56065049538d07b8a1da26155078`; the paired conformance-only client
baseline is SQLiteNow commit `2b551e207ddbb350c31e18fc81b8dff109cfafea`.
Both tracked worktrees were clean; ignored `specs/` content was preserved.

| Command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditSchemaEdge_UnloggedRegisteredTableIsRejectedBeforeBootstrapMutation$' -count=1 -timeout=10m -v` | 1 | 4.89s | Expected red: bootstrap accepted `relpersistence="u"` and reached the test's explicit durability failure. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditPostgresRestart_UnloggedAcknowledgedRowMustNotDisappearWhileMetadataSurvives$' -count=1 -timeout=10m -v` | 1 | 2.38s | Expected red: after SIGKILL/restart the business row changed from 1 to 0 while row state, bundle log/rows, source watermark, and hash survived; snapshot reported the missing live row. |

The user approved permanent-only registered relations and destructive
recreation of every pre-C2 server and client database. No migration, repair,
compatibility path, or old-state preservation is authorized. Drymint prewrite
baseline `rb0:4a5a8acb082b4d32495a654a` covers the planned Go source and test
paths.

### Contract and implementation

Bootstrap now queries every normalized registered schema/table pair through
`pg_namespace` and `pg_class` inside the existing bootstrap transaction. The
preflight runs immediately after the global advisory lock and before any
registered-column/index metadata lookup, existing-layout inspection, or
`sync.*` mutation. Only permanent logged relations (`relpersistence="p"`) are
accepted; unlogged (`"u"`) and temporary (`"t"`) relations produce one
`UnsupportedSchemaError` whose table-qualified diagnostics are sorted
deterministically. The same preflight runs for an already-marked layout.

No public signature, configuration, HTTP/OpenAPI shape, hash version, client
production code, or shared fixture changed. There is no opt-out, polling,
interception, migration, old-state detection, or compatibility path. Runtime
DDL remains unsupported and is caught on the next bootstrap.

The default regression covers permanent acceptance, atomic unlogged
rejection, a temporary table held by another PostgreSQL connection, mixed
permanent/unlogged registration with all offenders, stable diagnostic order,
and existing-layout revalidation. The promoted audit
regression proves rejection leaves no `sync` schema, catalog rows, or capture
triggers. The restart regression then destructively recreates its disposable
business schema as logged, commits an acknowledged row, performs a real
PostgreSQL SIGKILL/restart, and verifies the business row, row state, bundle
log/rows/hash, source watermark, pull, snapshot, and integrity oracle.

### Validation evidence

All C2 commands ran on 2026-07-11. Destructive and crash tests used an unset
`TEST_DATABASE_URL`; no caller-managed database was reset. PostgreSQL reported
`17.10 (Homebrew)` for the explicitly named real-server conformance database.

| Command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./oversync -run '^TestBootstrap_RequiresPermanentRegisteredTables$' -timeout=10m` | 0 | 2.24s package | Permanent/unlogged/mixed/repeat-bootstrap default coverage green. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit -count=1 ./oversync -run '^(TestAuditSchemaEdge_UnloggedRegisteredTableIsRejectedBeforeBootstrapMutation|TestAuditPostgresRestart_UnloggedAcknowledgedRowMustNotDisappearWhileMetadataSurvives)$' -timeout=15m` | 0 | 3.22s package | Atomic rejection and real logged-table SIGKILL recovery green. |
| The same default and tagged focused commands with `GOTOOLCHAIN=go1.25.0` and `-v` | 0 | 7.89s combined wall | Same contract green; crash recovery preserved the business row and all metadata. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./oversync -timeout=30m` | 0 | 26.27s package | Complete default server suite green. |
| The same default server command with `GOTOOLCHAIN=go1.25.0` | 0 | 24.79s package | Complete supported-toolchain server suite green. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./... -timeout=45m` | 0 | 19.75s wall | Repository-wide server, Go client, E2E, examples, and exact-core suites green. |
| The same repository-wide command with `GOTOOLCHAIN=go1.25.0` | 0 | 49.15s wall | Full supported-toolchain sweep green. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -race -count=1 ./oversync -run '^TestBootstrap_RequiresPermanentRegisteredTables$' -timeout=15m` followed by the tagged focused C2 selector with `-race` | 0 | 9.24s combined wall | Bootstrap and crash/restart C2 paths green under the race detector. |
| Default and `oversync_audit` `go vet ./...` on Go 1.26.5 and Go 1.25.0 | 0 | 1.41-1.55s each | All four vet lanes green. |
| Default and `oversync_audit` `staticcheck ./...` on Go 1.26.5 and Go 1.25.0 | 1 | 2.46-2.97s each | Pre-existing unused-helper backlog plus `oversqlite/lifecycle_state.go:337` SA4006/SA4017 remains identical across lanes; no diagnostic points to a C2 file. |
| Remaining confirmed-defect selector, excluding remediated C1 and C2 tests, on Go 1.26.5 and Go 1.25.0 | 1 | 5.74s / 5.76s | Expected red on unrelated later findings only; no panic, timeout, C1 regression, or C2 regression. |
| `./gradlew oversqliteRealserverJvm oversqliteRealserverJvmHarness` | 0 | 24s | Fresh KMP shared JVM catalog and generated runtime harness green. |
| `cd dart && OVERSQLITE_REALSERVER_TESTS=true flutter test packages/sqlitenow_oversqlite/test/realserver_conformance_test.dart --reporter compact` | 0 | 1s | Six fresh-database Dart lifecycle, retry, restore, snapshot, conflict, and source-retirement cases green. |

Go client fresh-database conformance is included in both repository-wide
`oversqlite` and `oversqlite_e2e` passes. Fuzzing and benchmarks are not
applicable because C2 changes neither parsing nor a performance-sensitive hot
path.

### Reset rehearsal, review, and cleanup

The client matrix used only
`oversync_c2_realserver_20260711`, created immediately before the run. The
corrected server used permanent logged example tables and each conformance
suite recreated its client database and reset its disposable server schema.
The server was stopped by signal, the named database was dropped, its absence
was verified through `pg_database`, and port 8080 had no listener. Docker
reported no remaining testcontainers-managed containers or volumes.

Drymint baseline-backed explicit-path review
`rv0:f1aa0cfa7356c0a7fc808c33` attributed all ten introduced/changed behaviors
to C2. Its sole candidate was low-value structural similarity between the
unlogged-table audit regression and the existing view-registration rejection
test. Source inspection retained the separate regressions: they share only the
standard invalid-schema bootstrap shell, while proving different relation
contracts and diagnostics. No new dependency edge or production duplication
requires cleanup.

A subsequent code review found that `Bootstrap` performed
`information_schema.columns` inspection before entering the persistence
preflight transaction. Because PostgreSQL omits another session's temporary
tables from `information_schema` even though their `pg_class` rows are visible,
that path rejected early with an untyped metadata error rather than the locked
`UnsupportedSchemaError` and `relpersistence="t"` diagnostic. Registered-key
column and unique-index inspection now uses the same bootstrap transaction
after the persistence preflight. A permanent regression holds the temp-table
connection while Bootstrap uses another pool connection and proves typed,
atomic rejection. Explicit-path Drymint review
`rv0:0cb0d763471e21163e0cddbc` produced only low-substance test-shell
similarity to an unrelated HTTP capabilities test; source inspection found no
reuse or cleanup action.

| Post-review command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./oversync -run '^TestBootstrap_RequiresPermanentRegisteredTables$' -timeout=10m -v` | 0 | 3.12s | Cross-session temporary, permanent, unlogged, mixed, and repeat-bootstrap cases green. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./oversync -timeout=30m` | 0 | 18.93s package | Full default server suite green after transactional metadata reordering. |
| The same default server command with `GOTOOLCHAIN=go1.25.0` | 0 | 18.92s package | Supported older toolchain green. |
| Focused C2 audit selector followed by the default C2 selector under `-race` on Go 1.26.5 | 0 | 9.56s combined wall | Real restart, atomic unlogged rejection, and cross-session temporary rejection green. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -count=1 ./... -timeout=45m` followed by `GOTOOLCHAIN=go1.26.5 go vet ./oversync` | 0 | 20.12s combined wall | Repository-wide server/client/E2E compilation and tests plus server vet green. |

The supported release procedure is destructive: stop every server and client,
recreate PostgreSQL with permanent logged business tables, recreate every Go,
KMP, and Dart client database, and deploy corrected components together. It is
not an `ALTER TABLE` repair or rolling migration. Prior business rows, sync
history, staged sessions, checkpoints, outboxes, and offline work are
deliberately discarded. Previously lost rows cannot be recovered.

`OS-AUD-018` is closed within this permanent-table and destructive-reset
contract. At C2 closeout the umbrella remained `approved`, not complete,
because C3 and later slices were outstanding.

## C3 / OS-AUD-002

Status: `complete`

### Contract and implementation

Fresh schema creation now installs
`sync.reject_registered_table_truncate()`. Bootstrap deterministically installs
`oversync_registered_truncate_guard` as an unconditional statement-level
`BEFORE TRUNCATE` trigger on every registered root and every partition that
exists at Bootstrap time. It passes the registered root identity to the guard,
which raises SQLSTATE `55000` with stable message, detail, and destructive-reset
hint fields. The existing advisory lock and transaction make replacement and
failure atomic; owner and row-capture triggers retain their prior behavior.

Permanent regressions cover direct, explicit-transaction, multi-table,
registered-origin and unregistered-origin `CASCADE`, root and direct partition,
`WithinSyncBundle` with zero prior events, and `WithinSyncBundle` after captured
DML. They prove rejection preserves business rows, row state, bundle log/rows
and hashes, source watermark, pull, snapshot, and integrity-oracle state.
Repeated and parallel Bootstrap converge, and a synthetic trigger-installation
failure rolls back the complete managed-trigger replacement.

The live `/test/reset` endpoint, mobile-flow `TRUNCATE` cleanup, and maintained
schema-drop test reset flows were removed. Server and E2E tests create and drop
isolated databases. KMP, Dart, and Swift real-server test support no longer
calls a reset endpoint; outer orchestration starts each required lane with a
fresh PostgreSQL database. No client production, wire, shared fixture, local
schema, generated output, Swagger, or Drymint policy changed.

### Validation evidence

All commands ran on 2026-07-11 with `TEST_DATABASE_URL` unset for destructive
Go database tests. PostgreSQL reported `17.10 (Homebrew)`.

| Command | Exit | Wall time | Disposition |
| --- | ---: | ---: | --- |
| Pre-implementation `env -u TEST_DATABASE_URL GOTOOLCHAIN=go1.26.5 go test -tags=oversync_audit ./oversync -run '^TestAuditRegisteredTable_TruncateFailsClosed$' -count=1 -timeout=10m -v` | 1 | 4.18s | Expected red: business rows became 0 while live row state remained 1 and no matching bundle/source advance existed. |
| Focused five-test C3 default selector on Go 1.26.5 / 1.25.0 | 0 / 0 | 2.82s / 3.31s | Direct, cascade, multi-table, partition, idempotency, and installer rollback green. |
| C3 audit/atomicity/drift/partition selector on Go 1.26.5 / 1.25.0 | 0 / 0 | 1.99s / 3.13s | Promoted audit reproducer and Bootstrap lifecycle coverage green. |
| Explicit transaction regression rerun on Go 1.26.5 / 1.25.0 | 0 / 0 | 2.51s / 2.78s | Failed `TRUNCATE` leaves the explicit transaction rollbackable and preserves all recorded state. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=<toolchain> go test -count=1 ./oversync -timeout=30m` | 0 / 0 | 19.95s / 20.37s | Complete server suite green on Go 1.26.5 and 1.25.0. |
| `env -u TEST_DATABASE_URL GOTOOLCHAIN=<toolchain> go test -count=1 ./... -timeout=45m` | 0 / 0 | 23.26s / 24.79s | Repository-wide server, Go client, E2E, examples, and exact-core suites green. |
| Focused default and promoted audit selectors with `-race` on Go 1.26.5 | 0 / 0 | 5.41s / 4.71s | No race report. |
| Default and tagged `go vet` on Go 1.26.5 and 1.25.0 | 0 | 0.24-0.65s | All four lanes green. |
| Default and tagged `staticcheck` on Go 1.26.5 and 1.25.0 | 1 | 1.19-1.52s | Tracked pre-existing U1000 plus SA4006/SA4017 backlog reproduced; no new C3 guard diagnostic. |
| Remaining expected-red audit selector with C1/C2/C3 removed, Go 1.26.5 / 1.25.0 | 1 / 1 | 5.43s / 5.54s | Only later findings failed; no C1, C2, or C3 regression, panic, or timeout. |
| Fresh database `./gradlew oversqliteRealserverJvm` | 0 | 4s | KMP shared JVM catalog green. |
| Fresh database `./gradlew oversqliteRealserverJvmHarness` | 0 | 13s | Generated runtime harness green. |
| Fresh database Dart real-server conformance command | 0 | 1s | All six lifecycle, retry, restore, snapshot, conflict, and source-retirement cases green. |
| Swift sync fixture `swift test` | 0 | 2.06s build | Six local tests passed; the opt-in real-server smoke compiled and skipped without its environment flag. |

Drymint baseline-backed reviews `rv0:b8da2e0332ab9d0b6532f9f6` (Go) and
`rv0:67fb47942b51b1e6ed8d2f16` (Kotlin) were inspected. The Go duplicate was
the modified mobile-flow `main` compared with itself, while the reported E2E
dependency was a pre-existing resolver interface expansion. The Kotlin match
shared only a low-substance test shell with an unrelated automatic-download
test. Neither requires reuse, architecture, policy, or production cleanup.
Dart, Swift, Markdown, and ignored specs were reviewed raw because they are not
covered by the configured Drymint language index.

### Reset, cleanup, and residual risk

The three client lanes used separately created databases
`oversync_c3_kmp_jvm_20260711`, `oversync_c3_kmp_harness_20260711`, and
`oversync_c3_dart_20260711`. Each server was stopped before its database was
dropped. All named databases were removed and port 8080 had no listener.

Administrative reset is only: stop all server and client processes, delete and
recreate PostgreSQL, recreate permanent registered tables, recreate every
client database, and deploy corrected server and clients together. All prior
business rows, bundles, checkpoints, outboxes, and offline work are discarded.
Superusers can still bypass triggers by disabling them or changing replication
role, and DDL can attach a partition after Bootstrap without its own guard;
those are privileged administrative risks, not an expansion into general DDL
policing or managed-layout drift.

`OS-AUD-002` is closed within this fail-closed and destructive-reset contract.
The umbrella remains `approved`, not complete, because later remediation slices
remain outstanding. C4 and H1 were not started.

### Critical-wave post-closeout validation

The umbrella Critical-wave gate ran only after C3 review and closeout. The
combined C1 exact-value, C2 durability, and C3 truncate audit selector passed
on Go 1.26.5 and 1.25.0 in 4.73s and 4.83s wall time. The earlier sequential
repository-wide sweeps on both toolchains, race, vet, staticcheck disposition,
remaining expected-red selector, and fresh KMP/Dart real-server lanes remained
the release evidence for the wave.

The preserved `FuzzAuditCanonicalJSON_Idempotent` corpus passed 57,822
executions in 14.58s wall time. KMP `oversqliteComprehensive` passed in 38s;
the focused shared exact JSON, typed numeric, and payload JVM fixtures passed
in 10.83s. Dart's full package suite passed 95 tests with 16 opt-in real-server
tests skipped in 3.36s, and `dart analyze packages/sqlitenow_oversqlite`
reported no issues in 0.68s. The opt-in KMP and Dart real-server lanes had
already passed against separately recreated corrected-server databases during
C3 closeout.

Residual compatibility and migration risk is unchanged: the Critical wave is
a coordinated stopped-process destructive cutover. PostgreSQL and every client
database must be recreated, all old rows and sync/offline state are discarded,
and mixed corrected/legacy operation is unsupported. The Critical wave is
validated; the umbrella remains `approved` because later waves are untouched.

## H1 / OS-AUD-003

Status: `complete`

H1 defines `retained_bundle_floor` as the highest discarded bundle. Pull from
the floor is valid and returns only bundles strictly above it; a lower
checkpoint returns `409 history_pruned`. A checkpoint or positive target above
current committed history returns `409 checkpoint_ahead`, target zero captures
the current ceiling, and a positive target below the checkpoint returns
`400 invalid_request`. Committed push replay intentionally remains stricter and
returns history-pruned at or below the floor.

Go, KMP, and Dart now persist the existing rebuild gate before authoritative
checkpoint recovery and automatically resume it from normal sync or pull after
interruption. Recovery reconciles pending offline work first; unresolved work
returns a typed actionable blocker while preserving the app row, durable
outbox, checkpoint, and gate. The checkpoint and gate change only in the final
atomic snapshot apply. No client schema migration, destructive reset,
successful JSON-shape change, endpoint change, or generated-output edit was
required.

The pre-edit H1 selector failed in 5.633s on the documented zero-after-prune,
at-floor, and future-target boundaries. After implementation the same selector
passed, both Go 1.26.5 and 1.25.0 package and repository-wide sweeps passed,
focused race passed, and vet passed in default and audit modes. Staticcheck
2025.1.1 reproduced only the tracked U1000 and SA4006/SA4017 backlog. The full
audit package remains expected-red for later findings; its H1 tests are green.

KMP `oversqliteComprehensive` passed, as did JVM, iOS simulator, macOS ARM64,
JS Node, and Wasm platform and real-server host lanes. Dart passed 97 package
tests with 16 opt-in live tests skipped, analysis, and all six focused
real-server conformance cases. Shared wire, pull behavior, lifecycle state, and
live PostgreSQL 17.10 cases cover pruning, poisoned future checkpoints,
automatic restart continuation, atomic replacement, and pending-work
preservation.

Two aggregate release wrappers retain explicitly unrelated failures: Android's
existing mock push response omits `canonical_request_hash`, and Dart's rich
schema fixture sends exact int64 as a JSON number instead of the required
string. The H1 live cases completed independently and passed; neither failure
touches checkpoint/floor/recovery behavior. Drymint current-source reviews were
clean after the prewrite baseline became unusable when the permanent E2E path
expanded the source set.

Live conformance used a disposable PostgreSQL 17.10 Docker database and a
corrected server mapped to port 18080 because an unrelated process owned 8080.
The server/database containers, network, and temporary binary were removed.
H2 and later slices were not started. The H1-wave validation remains unchecked
until H1 through H5 are complete.

### Reopened review disposition

Independent review found that the first closeout did not protect four client
edge cases: KMP/Dart committed-remote replay pruning during automatic
checkpoint recovery, KMP transient reconcile-push retry classification,
post-push durable blocker fields in Go/KMP/Dart, and Dart
`syncThenDetach()` recovery orchestration. At that point H1 was reopened until
those defects had expected-red-then-green permanent regressions and focused
cross-client validation. The earlier validation remained historical evidence,
not acceptance evidence by itself. H2 and the H1-H5 wave validation remained
unstarted.

### Corrective implementation and validation

The reopened pass corrected all four review findings. Go, KMP, and Dart now
reload durable blocker fields after a failed push. KMP and Dart automatically
preserve committed-remote outbox state through snapshot recovery when replay
is pruned. KMP transient failures reach the configured retry loop and typed
authentication errors remain terminal; its remote API now constructs the
existing upload/download HTTP exception types. Dart preserves terminal and
transient HTTP/protocol errors for caller scheduling and routes
`syncThenDetach()` through checkpoint recovery before ordinary push.

Expected-red regressions reproduced each defect before the production changes.
Final validation passed the promoted PostgreSQL-backed H1 selector, focused Go
recovery tests and race, Go vet, full Go 1.26.5 and 1.25.0 repository sweeps,
KMP focused recovery fixtures, `oversqliteComprehensive`, JS Node tests,
macOS/iOS simulator common-source compilation, all 100 default Dart package
tests with 16 opt-in live tests skipped, and Dart analysis. Drymint review had
no actionable production finding; low-substance similarity was retained
between distinct recovery scenarios. Both repository whitespace checks and
ignored-spec checks were clean.

The corrective pass did not change the server contract, so it did not repeat
the original full real-server matrix. The promoted test-owned PostgreSQL
selector and original live evidence remain applicable, while deterministic
KMP/Dart transport fixtures cover the committed-replay pruning race directly.
H2 and later slices were not started, and the H1-H5 wave validation remains
unchecked.

### Final nethttp heavy real-server validation

At the user's request H1 was reopened once more and the complete heavy matrix
was exercised against the actual `examples/nethttp_server` binary and an
isolated PostgreSQL 17.10 Docker database. The server status identified
`app_name=nethttp-server-example`. After Docker Desktop restarted and an
unrelated process claimed port 8080, the test-owned stack was restored on port
58080 and affected web lanes were forced to rerun rather than relying on
up-to-date output.

Go nethttp server tests and `oversqlite_e2e` passed. KMP JVM catalog/heavy and
JVM harness heavy passed. KMP macOS ARM64, iOS simulator ARM64, JS Node, and
Wasm browser heavy suites passed with no skipped or failed real-server tests.
Every KMP Android live real-server suite passed, including shared-connection,
multi-chunk, long-horizon, rich-schema, stale-follower prune recovery, and
lifecycle coverage; the aggregate alone remains red on the previously recorded
mock push response missing `canonical_request_hash`.

Dart VM conformance and watch passed all nine cases. Its heavy H1 cases passed
multi-chunk/interleaving, stale-follower prune recovery, conflict convergence,
and concurrent-read catch-up. Flutter Android passed its H1 lifecycle,
conflict, watch, multi-chunk, stale-follower prune recovery, and
concurrent-read cases. Aggregate Dart/Flutter failures remain confined to
non-H1 protocol fixtures: exact-int64 is encoded as a JSON number where the
server requires a string, and retired-source probes manually omit required
`canonical_request_hash`, causing 400 request validation before their expected
409 source-retired assertion. The aggregate's early exits were bypassed by
running every remaining live test file individually.

The server/database containers, Docker network, and temporary binary were
removed, and no listener remained on port 58080. H2 and later slices were not
started. The H1-H5 wave validation remains unchecked.

## Pre-H2 Dart numerics and canonical-fixture corrective

Status: `complete`

The final H1 heavy matrix exposed three non-H1 aggregate failures: generated
Dart clients cannot pass the documented exact-numeric `syncTables` override,
one KMP Android mock omits required canonical request hashes, and Dart
retired-source probes omit the required request hash. The user approved a
narrow corrective spec and implementation before H2. H1 remains complete; H2
and the H1-H5 wave validation remain unstarted.

Generated Dart configuration helpers now accept the documented `syncTables`
override, allowing rich-schema real-server clients to preserve exact-int64 and
approximate numeric metadata. Checked-in outputs were regenerated and a repeat
generation was byte-stable. The Android blob mock now returns and verifies the
canonical request hash and matching bundle hash. Dart VM and Flutter Android
retired-source probes now provide a valid request hash and reach the intended
409 rule.

The compiler expected-red regression and complete compiler test task passed,
as did focused Dart metadata/payload tests, the focused Android device test,
`oversqliteComprehensive`, the full Dart package suite, and Dart analysis. Both
complete heavy wrappers passed against the actual nethttp sample and isolated
PostgreSQL 17.10: KMP `oversqliteRealserverAllHeavy` and Dart
`oversqlite_realserver_all_heavy.sh` with Flutter Android enabled. Drymint
current-source review found no actionable issue, and whitespace and generated
output checks were clean.

The live stack used host port 58080. Port 8080 was not bound, probed, stopped,
or otherwise disturbed. Disposable infrastructure was removed. H1 remains
complete; H2 and later slices were not started, and the H1-H5 wave validation
remains unchecked.

## H2 / OS-AUD-005 Atomic populated-table adoption

Status: `complete`

Approved child specification:
`specs/2026-07-11-oversync-remediation-h2-populated-table-adoption.md`

Independent review reopened H2 on 2026-07-11. The initial coherent-state
validator accepted gaps inside retained bundle history, trusted stored bundle
hashes and byte counts without recomputation, and did not compare a live
business payload with its current retained bundle row. The corrective pass now
requires an exact retained sequence window, recomputes every retained bundle's
row count/hash/byte count, and compares every live row newer than the retained
floor with exactly one canonically equivalent non-delete row in its current
bundle. Versions at or below the floor remain compatible with legitimate
history pruning.

Permanent regressions cover missing middle history, retained-row payload/hash
corruption, stored byte-count corruption, trigger-bypassed live payload drift,
clean retry, and coherent
fully-pruned current versions. Focused, race, both-toolchain server and full
repository suites, tagged/untagged vet, and the established staticcheck
baseline passed. KMP and Dart shared/full checks passed, and the repeated
PostgreSQL 17.10 actual-nethttp run passed the maintained Go client, KMP
all-surface wrapper, and Dart conformance/watch/rich-schema wrapper through
host port 58080. Drymint's only explicit-path match was unrelated
low-substance PostgreSQL query boilerplate; no policy/configuration changed.
H3 and the H1-H5 wave remain unstarted.

A second review reopened and reclosed H2 on 2026-07-11. Bootstrap now
participates in the service in-flight drain, so `Close` cannot return while a
Bootstrap attempt can still commit. Coherent-state validation requires every
unpruned tombstone to match exactly one retained delete; pruned tombstones
remain compatible. Adoption discovers scopes before loading rows, holds
business payloads for one scope at a time, streams retained history one bundle
at a time, and retains only current-row evidence instead of duplicating all
history payloads.

Expected-red regressions reproduced the shutdown race and retained-insert
tombstone acceptance before the fix. The new and existing adoption matrix,
both-toolchain server and repository suites, focused race, tagged H2/C2/C3/H1
interactions, tagged/untagged vet, and the established staticcheck baseline
passed. Drymint baseline `rb0:b7f6b9c36f0978fde37bfb6c` found only
low-substance similarity between distinct corruption tests; its explicit-path
review was clean, and no policy/configuration changed. An isolated actual
nethttp/PostgreSQL 17.10 run on host port 58080 passed the maintained Go
client, KMP JVM/harness and all-surface wrapper, and Dart
conformance/watch/rich-schema wrapper. Test-owned infrastructure was removed,
no listener remained on 58080, and host port 8080 was never used or probed.
No client production, schema, fixture, wire, or generated-output change was
required. H3 and the H1-H5 wave remain unstarted.

Bootstrap now holds the global bootstrap advisory lock and deterministic
registered-relation locks while one transaction installs the managed layout
and capture triggers, observes authoritative rows, creates or validates each
scope's durable sync representation, and proves final snapshot equivalence.
Pristine and initialized-empty populated scopes receive exactly one ordinary
committed baseline bundle at sequence and row version 1. Already coherent
scopes remain unchanged. Partial or inconsistent sync metadata is rejected
atomically, leaves business rows untouched, and keeps the service unready for
a clean retry.

The permanent matrix covers readiness, empty/populated/coherent/partial state,
multiple tables and owners, hash and checkpoint reconstruction, concurrent
writes and bootstrap instances, cancellation, rollback/retry, actual sample
startup, and maintained Go-client consumption. Shared KMP/Dart handshake and
pull/snapshot fixtures encode the same adopted baseline; no Go, KMP, or Dart
production client change was required.

Focused and full Go tests passed with Go 1.26.5 and 1.25.0, including the H2
audit reproducer, C2/C3 interaction cases, race, and tagged/untagged vet.
Staticcheck reported only the existing unused-declaration baseline and no H2
finding. KMP focused fixtures and `oversqliteComprehensive`, Dart focused/full
tests, and Dart analysis passed. The actual Linux ARM64 nethttp sample binary
adopted a pre-seeded row against PostgreSQL 17.10, after which the maintained
Go client, KMP all-surface wrapper, and Dart real-server wrapper passed through
host port 58080. Disposable infrastructure was removed; no listener remained
on 58080 or 55432, and host port 8080 was never used or probed.

Drymint baseline-backed and explicit-path reviews were completed after an
index refresh. The only results were low-substance delete-handler symmetry and
a post-edit `SetupServer` self-match; source inspection found no actionable
duplication and no policy/configuration changed. H3 and later slices were not
started. The H1-H5 wave validation remains unchecked.

## H3 / OS-AUD-006 Fail-closed nullable identity validation

Status: `complete`

Approved child specification:
`specs/2026-07-11-oversync-remediation-h3-nullable-identity.md`

Implementation began on 2026-07-11 from `go-oversync`
`abff401ae80faf6ea17e396ec8df7b35d36cb23f` and `sqlitenow-kmp`
`577f083eaaea4ebb373014e566da0ccdfa12c61d`. The Go repository had no tracked
changes and intentionally ignored `specs/`. SQLiteNow retained exactly the
three pre-existing unstaged H1/H2 shared-contract changes in
`oversqlite-contracts/README.md`,
`oversqlite-contracts/protocol-handshake/connect.json`, and
`oversqlite-contracts/pull-snapshot/basic.json`, plus ignored `specs/`; the
index was empty in both repositories. H3 is limited to nullable registered
identity admission, trigger NULL defense, and Go/KMP/Dart local visible-key
validation. H4, H5, and the combined H1-H5 wave validation remain unstarted.

H3 now fails closed on nullable PostgreSQL owner/key declarations for logical
registered roots and recursive physical targets. A non-mutating pool preflight
is followed by an authoritative catalog reload after the global bootstrap lock
and sorted relation locks, before any sync-layout, trigger, or adoption
mutation. Typed diagnostics aggregate every offender and require
application-owned NULL repair plus explicit `NOT NULL`. Managed owner/capture
triggers also reject NULL old/new owner and key values before comparison or
encoding while retaining valid INSERT owner autofill.

Go, KMP, and Dart local runtimes require an explicit non-null TEXT/BLOB visible
key before creating or changing `_sync_*` state. Shared fixtures prove fresh
zero residue and preservation of existing application, managed, dirty, source,
and checkpoint state after nullable drift. The exact audit reproducer went red
for the original acceptance and green after the fix. Both Go toolchains passed
the full H3 selector, full repository suites, focused race, tagged bootstrap
concurrency/cancellation, tagged/untagged vet, and the established staticcheck
baseline with no H3 diagnostic. KMP focused/comprehensive and every maintained
real-server surface passed; Dart focused/full/analyze and its conformance,
watch, and rich-schema wrapper passed.

An actual Linux ARM64 Go 1.26.5 nethttp binary and `postgres:17.10` proved that
a nullable populated declaration fails before the host listener and leaves the
row plus absent `sync` schema unchanged. After test-owned repair, the maintained
Go, KMP, and Dart clients passed through host port 58080. Disposable
infrastructure was removed and no listener remained. Host port 8080 was never
used or probed. Wire, checkpoint, pull/snapshot, PostgreSQL layout, SQLite
runtime-state, and generated-output versions remain unchanged.

Drymint explicit reviews `rv0:278b7e3d24991259d5e8c27b` and
`rv0:da43569f87eaf4caae2ddf95` were clean; the Kotlin baseline review's only
match was low-value overlap between distinct test contracts. No policy or
configuration changed. During validation, `sqlitenow-kmp` was concurrently
amended to `078dc68531d9f82d34aa520c09861c1a9e1097cd`, absorbing exactly the
three pre-existing H1/H2 diffs; H3 stayed unstaged and was revalidated on top.
H4, H5, and the H1-H5 wave remain unstarted.

Corrective review found that an already-marked layout returned before H3's
owner/capture function definitions ran. Bootstrap now replaces those two
H3-owned functions inside the existing transaction on marked layouts, with a
regression proving both NULL guards are restored. The complete H3 server
selector passed on Go 1.26.5 and 1.25.0, the tagged marked-layout drift
characterization passed, and the full `oversync` package passed on Go 1.26.5.
Drymint baseline review `rv0:4d5677f787f79aaadbe3e3aa` found only
low-substance overlap between unrelated tests after source inspection; explicit
audit-test review `rv0:eaee603d9cf773ae7768ad5d` was clean. No policy or
configuration changed, and H4 remains unstarted.
