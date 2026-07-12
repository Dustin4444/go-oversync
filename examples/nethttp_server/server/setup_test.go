package server

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestSetupServer_AdoptsPopulatedRowsBeforeBootstrap(t *testing.T) {
	userID := "nethttp-adopt-" + uuid.NewString()
	rowID := uuid.New()
	ts, err := NewTestServer(&ServerConfig{
		Logger: slog.New(slog.DiscardHandler),
		BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			tableIdent := pgx.Identifier{schema, "users"}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (_sync_scope_id, id, name, email)
				VALUES ($1, $2, 'Existing', 'existing@example.com')
			`, tableIdent), userID, rowID)
			return err
		},
	})
	require.NoError(t, err)
	t.Cleanup(ts.Close)

	actor := oversync.Actor{UserID: userID, SourceID: "nethttp-reader"}
	connect, err := ts.SyncService.Connect(context.Background(), actor, &oversync.ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)
	pull, err := ts.SyncService.ProcessPull(context.Background(), actor, 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), pull.StableBundleSeq)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	require.Equal(t, rowID.String(), pull.Bundles[0].Rows[0].Key["id"])
}

func TestRegisteredTablesForBusinessSchema_UsesBusinessRichV0Contract(t *testing.T) {
	tables := RegisteredTablesForBusinessSchema("custom_business")

	require.Equal(t, []oversync.RegisteredTable{
		{Schema: "custom_business", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "posts", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "categories", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "teams", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "team_members", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "file_reviews", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "typed_rows", SyncKeyColumns: []string{"id"}},
	}, tables)
	for _, table := range tables {
		require.NotContains(t, table.Table, "blob_")
	}
}

func TestRegisteredTablesForBusinessSchema_DefaultsToBusinessSchema(t *testing.T) {
	tables := RegisteredTablesForBusinessSchema("")

	require.NotEmpty(t, tables)
	for _, table := range tables {
		require.Equal(t, "business", table.Schema)
	}
}

func TestConfiguredPoolSizeFromEnv_Defaults(t *testing.T) {
	t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "")
	t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "")

	maxConns, minConns, err := configuredPoolSizeFromEnv()
	require.NoError(t, err)
	require.Equal(t, int32(50), maxConns)
	require.Equal(t, int32(5), minConns)
}

func TestConfiguredPoolSizeFromEnv_Overrides(t *testing.T) {
	t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "120")
	t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "12")

	maxConns, minConns, err := configuredPoolSizeFromEnv()
	require.NoError(t, err)
	require.Equal(t, int32(120), maxConns)
	require.Equal(t, int32(12), minConns)
}

func TestConfiguredPoolSizeFromEnv_RejectsInvalidValues(t *testing.T) {
	t.Run("non-positive max", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "0")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})

	t.Run("negative min", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "-1")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})

	t.Run("min exceeds max", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "10")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "11")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})
}

func TestConfiguredSnapshotConcurrencyFromEnv(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "8")
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", "12")

	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(0, 0)
	require.NoError(t, err)
	require.Equal(t, 8, builds)
	require.Equal(t, 12, chunks)

	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "0")
	_, _, err = configuredSnapshotConcurrencyFromEnv(0, 0)
	require.ErrorContains(t, err, "positive integer")
}
