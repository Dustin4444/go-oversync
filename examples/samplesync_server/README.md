Samplesync Server (PostgreSQL)

This example runs a minimal bundle-based go-oversync server for the `samplesync-kmp` app.

The server uses business tables as authoritative state and exposes the supported lifecycle and
bundle-based sync endpoints.

Endpoints

- `POST /dummy-signin`
- `POST /sync/connect`
- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{push_id}/chunks`
- `POST /sync/push-sessions/{push_id}/commit`
- `DELETE /sync/push-sessions/{push_id}`
- `GET /sync/committed-bundles/{bundle_seq}/rows`
- `GET /sync/pull`
- `GET /sync/watch`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/capabilities`
- `GET /syncx/health`
- `GET /syncx/status`

Database

- Schema: `business`
- Tables:
  - `business.person`
  - `business.person_address`
  - `business.comment`

Registered tables use scope-bound PostgreSQL identity with `_sync_scope_id TEXT NOT NULL`.
Each registered table keeps one visible UUID sync key column, scope-plus-key uniqueness, and
scope-inclusive foreign keys. The synced table set is FK-closed.

Run locally

1. Start PostgreSQL:
   `docker run --rm -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=samplesync -p 5432:5432 postgres:16`
2. Set environment variables and start the server:
   `DATABASE_URL="postgres://postgres:postgres@localhost:5432/samplesync?sslmode=disable" JWT_SECRET="dev-secret" go run ./examples/samplesync_server`

Concurrency

- Snapshot admission defaults to eight active builds and four active chunk requests.
- Override the limits with positive integers in
  `OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS` and
  `OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS`.
- Requests above the active snapshot limit receive a retryable HTTP 429 response instead of
  entering an unbounded server queue. Supported clients honor `Retry-After` and retry within
  their configured capacity-wait budget.
- The PostgreSQL pool allows 20 connections in this local sample. Snapshot concurrency limits
  bound the memory-heavy work; they do not limit connected users or devices.

Client settings

- Base URL: `http://localhost:8080` (desktop/iOS) or `http://10.0.2.2:8080` (Android emulator)
- Schema: `business`
- Send `Oversync-Source-ID: <current-source-id>` on authenticated `/sync/*` requests
- `/sync/watch` is available as an optional wake-up stream; clients still pull data through
  `/sync/pull`
- Call `POST /sync/connect` after local `Open()` to resolve
  `remote_authoritative`, `initialize_local`, `initialize_empty`, or `retry_later`
- Expect lifecycle-related sync failures:
  - `scope_uninitialized`
  - `scope_initializing`
  - `initialization_stale`
  - `initialization_expired`
