package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestManagedLayoutFacts_DeterministicOrderingAndFingerprint(t *testing.T) {
	facts := []managedLayoutFact{
		{Kind: "table", Identity: "sync.z", Attribute: "type", Value: "ordinary"},
		{Kind: "table", Identity: "sync.a", Attribute: "type", Value: "ordinary"},
	}
	reversed := []managedLayoutFact{facts[1], facts[0]}
	left, err := managedLayoutFingerprint(facts)
	require.NoError(t, err)
	right, err := managedLayoutFingerprint(reversed)
	require.NoError(t, err)
	require.Equal(t, left, right)
}

func TestManagedLayoutFacts_LengthDelimitedFingerprint(t *testing.T) {
	left, err := managedLayoutFingerprint([]managedLayoutFact{
		{Kind: "ab", Identity: "c", Attribute: "d", Value: "e"},
	})
	require.NoError(t, err)
	right, err := managedLayoutFingerprint([]managedLayoutFact{
		{Kind: "a", Identity: "bc", Attribute: "d", Value: "e"},
	})
	require.NoError(t, err)
	require.NotEqual(t, left, right)
}

func TestManagedLayoutFacts_RejectDuplicateKeys(t *testing.T) {
	_, err := managedLayoutFingerprint([]managedLayoutFact{
		{Kind: "table", Identity: "sync.meta", Attribute: "kind", Value: "r"},
		{Kind: "table", Identity: "sync.meta", Attribute: "kind", Value: "r"},
	})
	require.ErrorContains(t, err, "duplicate managed layout fact")
}

func TestManagedLayoutFacts_BoundedSortedDifferences(t *testing.T) {
	expected := []managedLayoutFact{
		{Kind: "index", Identity: "sync.b", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.a", Attribute: "valid", Value: "true"},
		{Kind: "table", Identity: "sync.c", Attribute: "kind", Value: "r"},
	}
	actual := []managedLayoutFact{
		{Kind: "index", Identity: "sync.a", Attribute: "valid", Value: "false"},
		{Kind: "function", Identity: "sync.extra()", Attribute: "language", Value: "sql"},
	}
	differences, total, err := compareManagedLayoutFacts(expected, actual, 2)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Equal(t, []managedLayoutDifference{
		{Category: "unexpected", Kind: "function", Identity: "sync.extra()", Attribute: "language", Actual: "sql"},
		{Category: "changed", Kind: "index", Identity: "sync.a", Attribute: "valid", Expected: "true", Actual: "false"},
	}, differences)
}

type managedLayoutTestFixture struct {
	ctx              context.Context
	harness          *schemaBootstrapFailureHarness
	registeredTables []RegisteredTable
	service          *SyncService
}

type managedLayoutCountingQuerier struct {
	syncCatalogQuerier
	queryCount int
}

func (q *managedLayoutCountingQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.queryCount++
	return q.syncCatalogQuerier.Query(ctx, sql, args...)
}

func newManagedLayoutTestFixture(t *testing.T, prefix string) *managedLayoutTestFixture {
	t.Helper()
	h := newSchemaBootstrapFailureHarness(t, prefix)
	h.execf(t, `
		CREATE TABLE %s.records (
			id UUID NOT NULL,
			_sync_scope_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, id)
		)
	`, h.schemaIdent)
	rowID := uuid.New()
	h.execf(t, `INSERT INTO %s.records (id, _sync_scope_id, payload) VALUES ('%s', 'managed-owner', 'original')`, h.schemaIdent, rowID)
	registeredTables := []RegisteredTable{h.registeredTable("records", "id")}
	service, err := NewRuntimeService(h.pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "managed-layout-initial",
		RegisteredTables:          registeredTables,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.NoError(t, service.Bootstrap(h.ctx))
	return &managedLayoutTestFixture{
		ctx:              h.ctx,
		harness:          h,
		registeredTables: registeredTables,
		service:          service,
	}
}

func (f *managedLayoutTestFixture) newService(t *testing.T, appName string) *SyncService {
	t.Helper()
	service, err := NewRuntimeService(f.harness.pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   appName,
		RegisteredTables:          f.registeredTables,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}

func (f *managedLayoutTestFixture) authoritativeCounts(t *testing.T) []int64 {
	t.Helper()
	var businessRows, users, rows, bundles, bundleRows, sources int64
	require.NoError(t, f.harness.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.records`, f.harness.schemaIdent)).Scan(&businessRows))
	require.NoError(t, f.harness.pool.QueryRow(f.ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.user_state),
			(SELECT COUNT(*) FROM sync.row_state),
			(SELECT COUNT(*) FROM sync.bundle_log),
			(SELECT COUNT(*) FROM sync.bundle_rows),
			(SELECT COUNT(*) FROM sync.source_state)
	`).Scan(&users, &rows, &bundles, &bundleRows, &sources))
	return []int64{businessRows, users, rows, bundles, bundleRows, sources}
}

func TestBootstrap_ValidatesManagedLayoutManifest(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_valid_")
	var functionOID, captureTriggerOID uint32
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT 'sync.capture_registered_row_change()'::regprocedure::oid`).Scan(&functionOID))
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `
		SELECT trigger.oid
		FROM pg_trigger AS trigger
		JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1 AND relation.relname = 'records' AND trigger.tgname = $2
	`, fixture.harness.schemaName, registeredTableCaptureTriggerName).Scan(&captureTriggerOID))

	applicationTriggerName := "application_owned_trigger"
	fixture.harness.execf(t, `
		CREATE FUNCTION %s.application_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
		CREATE TRIGGER %s BEFORE UPDATE ON %s.records FOR EACH ROW EXECUTE FUNCTION %s.application_trigger()
	`, fixture.harness.schemaIdent, applicationTriggerName, fixture.harness.schemaIdent, fixture.harness.schemaIdent)
	require.NoError(t, fixture.service.Bootstrap(fixture.ctx))
	require.NoError(t, fixture.service.validateManagedLayout(fixture.ctx, fixture.harness.pool))

	var functionOIDAfter, captureTriggerOIDAfter uint32
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT 'sync.capture_registered_row_change()'::regprocedure::oid`).Scan(&functionOIDAfter))
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT oid FROM pg_trigger WHERE tgrelid = to_regclass($1) AND tgname = $2`, fixture.harness.schemaName+".records", registeredTableCaptureTriggerName).Scan(&captureTriggerOIDAfter))
	require.Equal(t, functionOID, functionOIDAfter)
	require.Equal(t, captureTriggerOID, captureTriggerOIDAfter)
}

func TestBootstrap_RejectsManagedLayoutDriftAtomically(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "missing table", sql: `DROP TABLE sync.snapshot_session_rows`},
		{name: "extra relation", sql: `CREATE TABLE sync.operator_table (id integer)`},
		{name: "extra partitioned relation", sql: `CREATE TABLE sync.operator_partitioned (id integer) PARTITION BY RANGE (id)`},
		{name: "extra view", sql: `CREATE VIEW sync.operator_view AS SELECT 1 AS value`},
		{name: "extra sequence", sql: `CREATE SEQUENCE sync.operator_sequence`},
		{name: "extra function", sql: `CREATE FUNCTION sync.operator_function() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$`},
		{name: "extra column", sql: `ALTER TABLE sync.snapshot_sessions ADD COLUMN operator_column text`},
		{name: "extra generated column", sql: `ALTER TABLE sync.snapshot_sessions ADD COLUMN operator_generated bigint GENERATED ALWAYS AS (row_count + 1) STORED`},
		{name: "column type", sql: `ALTER TABLE sync.snapshot_sessions ALTER COLUMN row_count TYPE integer`},
		{name: "column nullability", sql: `ALTER TABLE sync.snapshot_sessions ALTER COLUMN row_count DROP NOT NULL`},
		{name: "column default", sql: `ALTER TABLE sync.snapshot_sessions ALTER COLUMN row_count SET DEFAULT 9`},
		{name: "column identity", sql: `ALTER TABLE sync.user_state ALTER COLUMN user_pk DROP IDENTITY`},
		{name: "column collation", sql: `ALTER TABLE sync.bundle_log ALTER COLUMN canonical_request_hash TYPE text COLLATE "C"`},
		{name: "table persistence", sql: `ALTER TABLE sync.snapshot_session_rows SET UNLOGGED`},
		{name: "table row security", sql: `ALTER TABLE sync.snapshot_sessions ENABLE ROW LEVEL SECURITY`},
		{name: "table replica identity", sql: `ALTER TABLE sync.snapshot_sessions REPLICA IDENTITY FULL`},
		{name: "missing constraint", sql: `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_op_code_chk`},
		{name: "missing constraint backed index", sql: `ALTER TABLE sync.bundle_log DROP CONSTRAINT bundle_log_source_tuple_key`},
		{name: "constraint validation state", sql: `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_op_code_chk; ALTER TABLE sync.bundle_rows ADD CONSTRAINT bundle_rows_op_code_chk CHECK (op_code IN (1, 2, 3)) NOT VALID`},
		{name: "foreign key action", sql: `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_bundle_fk; ALTER TABLE sync.bundle_rows ADD CONSTRAINT bundle_rows_bundle_fk FOREIGN KEY (user_pk, bundle_seq) REFERENCES sync.bundle_log(user_pk, bundle_seq)`},
		{name: "extra constraint", sql: `ALTER TABLE sync.snapshot_sessions ADD CONSTRAINT operator_check CHECK (row_count >= 0) NOT VALID`},
		{name: "missing explicit index", sql: `DROP INDEX sync.rs_user_live_snapshot_idx`},
		{name: "altered explicit index", sql: `DROP INDEX sync.rs_user_live_snapshot_idx; CREATE INDEX rs_user_live_snapshot_idx ON sync.row_state(user_pk, bundle_seq)`},
		{name: "sequence configuration", sql: `ALTER SEQUENCE sync.accepted_push_replay_seq INCREMENT BY 2`},
		{name: "sequence ownership", sql: `ALTER SEQUENCE sync.accepted_push_replay_seq OWNED BY sync.snapshot_sessions.row_count`},
		{name: "function body", sql: `CREATE OR REPLACE FUNCTION sync.reject_registered_table_truncate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`},
		{name: "function attributes", sql: `ALTER FUNCTION sync.capture_registered_row_change() IMMUTABLE`},
		{name: "extra rule", sql: `CREATE RULE operator_rule AS ON UPDATE TO sync.snapshot_sessions DO ALSO SELECT 1`},
		{name: "extra sync trigger", sql: `CREATE FUNCTION sync.operator_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; CREATE TRIGGER operator_trigger BEFORE UPDATE ON sync.snapshot_sessions FOR EACH ROW EXECUTE FUNCTION sync.operator_trigger()`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagedLayoutTestFixture(t, "managed_drift_")
			before := fixture.authoritativeCounts(t)
			_, err := fixture.harness.pool.Exec(fixture.ctx, test.sql)
			require.NoError(t, err)
			second := fixture.newService(t, "managed-layout-drift-"+strings.ReplaceAll(test.name, " ", "-"))
			err = second.Bootstrap(fixture.ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr)
			require.Contains(t, err.Error(), "managed sync layout mismatch")
			require.Contains(t, err.Error(), syncSchemaLayoutName)
			require.Contains(t, err.Error(), "expected fingerprint")
			require.Contains(t, err.Error(), "actual fingerprint")
			require.Equal(t, before, fixture.authoritativeCounts(t))
			_, err = second.Connect(fixture.ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, &ConnectRequest{})
			require.ErrorIs(t, err, errServiceNotReady)
		})
	}
}

func TestBootstrap_ManagedLayoutDriftReadinessAndRetry(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_retry_")
	service := fixture.newService(t, "managed-layout-retry")
	_, err := fixture.harness.pool.Exec(fixture.ctx, `DROP INDEX sync.rs_user_live_snapshot_idx`)
	require.NoError(t, err)
	err = service.Bootstrap(fixture.ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	_, err = service.Connect(fixture.ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)
	_, err = fixture.harness.pool.Exec(fixture.ctx, `CREATE INDEX rs_user_live_snapshot_idx ON sync.row_state(user_pk, table_id, key_bytes) WHERE deleted = FALSE`)
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(fixture.ctx))
	_, err = service.Connect(fixture.ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, &ConnectRequest{})
	require.NoError(t, err)
	require.NoError(t, fixture.newService(t, "managed-layout-new-instance").Bootstrap(fixture.ctx))
}

func TestBootstrap_ManagedLayoutValidationConcurrency(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_concurrent_")
	_, err := fixture.harness.pool.Exec(fixture.ctx, `ALTER SEQUENCE sync.history_pruned_error_seq CACHE 2`)
	require.NoError(t, err)
	services := []*SyncService{
		fixture.newService(t, "managed-layout-concurrent-a"),
		fixture.newService(t, "managed-layout-concurrent-b"),
	}
	errors := make([]error, len(services))
	var wait sync.WaitGroup
	for i, service := range services {
		wait.Add(1)
		go func(index int, service *SyncService) {
			defer wait.Done()
			errors[index] = service.Bootstrap(fixture.ctx)
		}(i, service)
	}
	wait.Wait()
	for _, err := range errors {
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr)
	}
	_, err = fixture.harness.pool.Exec(fixture.ctx, `ALTER SEQUENCE sync.history_pruned_error_seq CACHE 1`)
	require.NoError(t, err)
	for _, service := range services {
		require.NoError(t, service.Bootstrap(fixture.ctx))
	}
}

func TestBootstrap_FreshLayoutSelfValidates(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_fresh_")
	require.NoError(t, fixture.service.validateManagedLayout(fixture.ctx, fixture.harness.pool))
	var markerCount, catalogCount int
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM sync.meta`).Scan(&markerCount))
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM sync.table_catalog`).Scan(&catalogCount))
	require.Equal(t, 1, markerCount)
	require.Equal(t, 1, catalogCount)
}

func TestManagedLayoutValidation_UsesFixedCatalogQueryCount(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_queries_")
	full := &managedLayoutCountingQuerier{syncCatalogQuerier: fixture.harness.pool}
	require.NoError(t, fixture.service.validateManagedLayout(fixture.ctx, full))
	require.Equal(t, 9, full.queryCount, "one target query plus eight object-class queries")
	volatile := &managedLayoutCountingQuerier{syncCatalogQuerier: fixture.harness.pool}
	require.NoError(t, fixture.service.validateManagedLayoutVolatileFacts(fixture.ctx, volatile))
	require.Equal(t, 3, volatile.queryCount, "one target query plus function and trigger queries")
}

func TestRegisteredTableTriggers_ValidateWithoutRepair(t *testing.T) {
	tests := []struct {
		name     string
		mutation func(tableIdent string) string
	}{
		{name: "disabled", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER %s`, tableIdent, registeredTableCaptureTriggerName)
		}},
		{name: "missing", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`DROP TRIGGER %s ON %s`, registeredTableCaptureTriggerName, tableIdent)
		}},
		{name: "wrong function", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`DROP TRIGGER %s ON %s; CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.enforce_registered_row_owner()`, registeredTableCaptureTriggerName, tableIdent, registeredTableCaptureTriggerName, tableIdent)
		}},
		{name: "wrong arguments", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`DROP TRIGGER %s ON %s; CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.capture_registered_row_change('id', '1', '999')`, registeredTableCaptureTriggerName, tableIdent, registeredTableCaptureTriggerName, tableIdent)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagedLayoutTestFixture(t, "managed_trigger_")
			tableIdent := pgx.Identifier{fixture.harness.schemaName, "records"}.Sanitize()
			_, err := fixture.harness.pool.Exec(fixture.ctx, test.mutation(tableIdent))
			require.NoError(t, err)
			var beforeCount int
			var beforeDefinition string
			err = fixture.harness.pool.QueryRow(fixture.ctx, `
				SELECT COUNT(*), COALESCE(max(pg_get_triggerdef(oid)), '')
				FROM pg_trigger
				WHERE tgrelid = to_regclass($1) AND tgname = $2
			`, fixture.harness.schemaName+".records", registeredTableCaptureTriggerName).Scan(&beforeCount, &beforeDefinition)
			require.NoError(t, err)
			second := fixture.newService(t, "managed-layout-trigger-"+strings.ReplaceAll(test.name, " ", "-"))
			err = second.Bootstrap(fixture.ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr)
			var afterCount int
			var afterDefinition string
			require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `
				SELECT COUNT(*), COALESCE(max(pg_get_triggerdef(oid)), '')
				FROM pg_trigger
				WHERE tgrelid = to_regclass($1) AND tgname = $2
			`, fixture.harness.schemaName+".records", registeredTableCaptureTriggerName).Scan(&afterCount, &afterDefinition))
			require.Equal(t, beforeCount, afterCount)
			require.Equal(t, beforeDefinition, afterDefinition)
		})
	}
}
