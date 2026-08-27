package oversqlite_e2e

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type typedRowFixture struct {
	ID                  string
	Name                string
	Note                *string
	CountValue          *int64
	SmallCount          *int64
	MediumCount         *int64
	ExactAmountLiteral  *string
	ExactAmountExpected *string
	EnabledFlag         int64
	RatingLiteral       *string
	RatingExpectedText  *string
	Float4Literal       *string
	Float4ExpectedText  *string
	DataHex             *string
	CreatedAt           *string
}

type businessRichManifest struct {
	NumericScenarios []struct {
		Name      string            `json:"name"`
		Local     map[string]string `json:"local"`
		Committed map[string]string `json:"committed"`
	} `json:"numericScenarios"`
}

func TestEndToEnd_UniformNumericScenariosUseSharedBusinessRichManifest(t *testing.T) {
	manifest := loadBusinessRichManifest(t)
	ctx := context.Background()
	schema := "e2e_uniform_numeric_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)
	userID := "e2e-uniform-numeric-" + uuid.NewString()

	seedClient, seedDB := newSQLiteClient(t, server, userID, "numeric-seed", oversqlite.DefaultConfig(schema, syncTables("typed_rows")), typedRowsDDL)
	pullClient, pullDB := newSQLiteClient(t, server, userID, "numeric-pull", oversqlite.DefaultConfig(schema, syncTables("typed_rows")), typedRowsDDL)
	hydrateClient, hydrateDB := newSQLiteClient(t, server, userID, "numeric-hydrate", oversqlite.DefaultConfig(schema, syncTables("typed_rows")), typedRowsDDL)

	rows := make([]typedRowFixture, 0, len(manifest.NumericScenarios))
	for _, scenario := range manifest.NumericScenarios {
		row := typedRowFixture{
			ID:                  uuid.NewString(),
			Name:                scenario.Name,
			CountValue:          optionalInt64(t, scenario.Local["count_value"]),
			SmallCount:          optionalInt64(t, scenario.Local["small_count"]),
			MediumCount:         optionalInt64(t, scenario.Local["medium_count"]),
			ExactAmountLiteral:  optionalString(scenario.Local["exact_amount"]),
			ExactAmountExpected: optionalString(scenario.Committed["exact_amount"]),
			EnabledFlag:         int64Value(t, scenario.Local["enabled_flag"], 1),
			RatingLiteral:       optionalString(scenario.Local["rating"]),
			RatingExpectedText:  optionalString(scenario.Committed["rating"]),
			Float4Literal:       optionalString(scenario.Local["float4_value"]),
			Float4ExpectedText:  optionalString(scenario.Committed["float4_value"]),
		}
		if value := scenario.Committed["enabled_flag"]; value != "" {
			if value == "false" {
				row.EnabledFlag = 0
			} else {
				row.EnabledFlag = 1
			}
		}
		insertTypedRow(t, seedDB, row)
		t.Logf("numeric scenario %s row_id=%s", scenario.Name, row.ID)
		rows = append(rows, row)
	}

	mustPushPendingE2E(t, seedClient, ctx)
	directCount := int64(9007199254740993)
	directSmall := int64(-32768)
	directMedium := int64(2147483647)
	directAmount := "42.5000000000"
	directRating := "5e-324"
	directFloat4 := "1.2345678806304932"
	directRow := typedRowFixture{
		ID: uuid.NewString(), Name: "direct-postgresql-write", CountValue: &directCount,
		SmallCount: &directSmall, MediumCount: &directMedium,
		ExactAmountExpected: &directAmount, EnabledFlag: 0,
		RatingExpectedText: &directRating, Float4ExpectedText: &directFloat4,
	}
	actor := oversync.Actor{UserID: userID, SourceID: "direct-postgresql"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, actor, oversync.BundleSource{SourceID: actor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.typed_rows
			(id, name, count_value, small_count, medium_count, exact_amount, enabled_flag, rating, float4_value)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, pgx.Identifier{server.BusinessSchema}.Sanitize()), directRow.ID, directRow.Name,
			directCount, directSmall, directMedium, directAmount, false, 5e-324, float32(1.2345678901234567))
		return err
	}))
	mustPullToStableE2E(t, seedClient, ctx)
	rows = append(rows, directRow)
	mustPullToStableE2E(t, pullClient, ctx)
	mustRebuildE2E(t, hydrateClient, ctx)
	for _, row := range rows {
		assertTypedRowState(t, seedDB, row)
		assertTypedRowState(t, pullDB, row)
		assertTypedRowState(t, hydrateDB, row)
	}
	if count := dirtyRowCount(t, seedDB); count != 0 {
		rows, err := seedDB.Query(`SELECT key_json, payload FROM _sync_dirty_rows ORDER BY dirty_ordinal`)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var key, payload string
			require.NoError(t, rows.Scan(&key, &payload))
			t.Logf("remaining dirty key=%s payload=%s", key, payload)
		}
		require.Equal(t, 0, count)
	}
}

func loadBusinessRichManifest(t *testing.T) businessRichManifest {
	t.Helper()
	root := os.Getenv("SQLITENOW_KMP_REPO")
	if root == "" {
		cwd, err := os.Getwd()
		require.NoError(t, err)
		for current := cwd; ; current = filepath.Dir(current) {
			candidate := filepath.Join(current, "sqlitenow-kmp")
			if _, statErr := os.Stat(filepath.Join(candidate, "oversqlite-contracts", "rich-schema", "business-rich-v0.json")); statErr == nil {
				root = candidate
				break
			}
			if filepath.Dir(current) == current {
				break
			}
		}
	}
	require.NotEmpty(t, root, "set SQLITENOW_KMP_REPO when repositories are not siblings")
	raw, err := os.ReadFile(filepath.Join(root, "oversqlite-contracts", "rich-schema", "business-rich-v0.json"))
	require.NoError(t, err)
	var manifest businessRichManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.Len(t, manifest.NumericScenarios, 10)
	return manifest
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func optionalInt64(t *testing.T, value string) *int64 {
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	require.NoError(t, err)
	return &parsed
}
func int64Value(t *testing.T, value string, fallback int64) int64 {
	parsed := optionalInt64(t, value)
	if parsed == nil {
		return fallback
	}
	return *parsed
}
func dirtyRowCount(t *testing.T, db *sql.DB) int {
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count))
	return count
}

func TestEndToEnd_TypedRowsPushPullHydrateAndImmediatePullStayConsistent(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_typed_rows_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-typed-user-" + uuid.NewString()
	config := oversqlite.DefaultConfig(schema, syncTables("typed_rows"))

	seedClient, seedDB := newSQLiteClient(t, server, userID, "device-seed", config, typedRowsDDL)
	activeClient, activeDB := newSQLiteClient(t, server, userID, "device-active", oversqlite.DefaultConfig(schema, syncTables("typed_rows")), typedRowsDDL)
	hydrateClient, hydrateDB := newSQLiteClient(t, server, userID, "device-hydrate", oversqlite.DefaultConfig(schema, syncTables("typed_rows")), typedRowsDDL)

	seedRatingLiteral := "1.25"
	seedRatingExpected := "1.25"
	seedDataHex := "00112233445566778899aabbccddeeff"
	seedCreatedAt := "2026-03-24T18:42:11Z"
	seedCountValue := int64(42)
	seedRow := typedRowFixture{
		ID:                 uuid.NewString(),
		Name:               "Seed Typed Row",
		CountValue:         &seedCountValue,
		EnabledFlag:        1,
		RatingLiteral:      &seedRatingLiteral,
		RatingExpectedText: &seedRatingExpected,
		DataHex:            &seedDataHex,
		CreatedAt:          &seedCreatedAt,
	}
	insertTypedRow(t, seedDB, seedRow)
	mustPushPendingE2E(t, seedClient, ctx)

	activeNote := "second-device"
	activeRatingLiteral := "6.57111473696007"
	activeRatingExpected := "6.57111473696007"
	activeRow := typedRowFixture{
		ID:                 uuid.NewString(),
		Name:               "Active Typed Row",
		Note:               &activeNote,
		EnabledFlag:        0,
		RatingLiteral:      &activeRatingLiteral,
		RatingExpectedText: &activeRatingExpected,
	}
	insertTypedRow(t, activeDB, activeRow)

	peerNote := "peer-from-server"
	peerRatingLiteral := "2.5"
	peerRatingExpected := "2.5"
	peerCreatedAt := "2026-03-24T10:42:11-08:00"
	peerRow := typedRowFixture{
		ID:                 uuid.NewString(),
		Name:               "Peer Typed Row",
		Note:               &peerNote,
		EnabledFlag:        1,
		RatingLiteral:      &peerRatingLiteral,
		RatingExpectedText: &peerRatingExpected,
		CreatedAt:          &peerCreatedAt,
	}
	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 1,
	}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.typed_rows (id, name, note, count_value, enabled_flag, rating, data, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, pgx.Identifier{server.BusinessSchema}.Sanitize()),
			peerRow.ID,
			peerRow.Name,
			peerNote,
			nil,
			peerRow.EnabledFlag == 1,
			2.5,
			nil,
			peerCreatedAt,
		)
		return err
	}))

	mustPushPendingE2E(t, activeClient, ctx)
	mustPullToStableE2E(t, activeClient, ctx)
	mustRebuildE2E(t, hydrateClient, ctx)

	assertTableCount(t, activeDB, "typed_rows", 3)
	assertTableCount(t, hydrateDB, "typed_rows", 3)
	assertTableCount(t, seedDB, "_sync_dirty_rows", 0)
	assertTableCount(t, activeDB, "_sync_dirty_rows", 0)
	assertTableCount(t, hydrateDB, "_sync_dirty_rows", 0)

	assertTypedRowState(t, activeDB, seedRow)
	assertTypedRowState(t, activeDB, activeRow)
	assertTypedRowState(t, activeDB, peerRow)
	assertTypedRowState(t, hydrateDB, seedRow)
	assertTypedRowState(t, hydrateDB, activeRow)
	assertTypedRowState(t, hydrateDB, peerRow)
}

func insertTypedRow(t *testing.T, db *sql.DB, row typedRowFixture) {
	t.Helper()

	var dataBytes []byte
	if row.DataHex != nil {
		var err error
		dataBytes, err = hex.DecodeString(*row.DataHex)
		require.NoError(t, err)
	}

	_, err := db.Exec(`
		INSERT INTO typed_rows(id, name, note, count_value, small_count, medium_count, exact_amount, enabled_flag, rating, float4_value, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		row.ID,
		row.Name,
		nullStringPointer(row.Note),
		nullInt64Pointer(row.CountValue),
		nullInt64Pointer(row.SmallCount),
		nullInt64Pointer(row.MediumCount),
		nullStringPointer(row.ExactAmountLiteral),
		row.EnabledFlag,
		nullFloatLiteral(t, row.RatingLiteral),
		nullFloatLiteral(t, row.Float4Literal),
		nullBytesPointer(dataBytes, row.DataHex != nil),
		nullStringPointer(row.CreatedAt),
	)
	require.NoError(t, err)
}

func assertTypedRowState(t *testing.T, db *sql.DB, row typedRowFixture) {
	t.Helper()

	var (
		name        string
		note        sql.NullString
		countValue  sql.NullInt64
		smallCount  sql.NullInt64
		mediumCount sql.NullInt64
		exactAmount sql.NullString
		enabledFlag int64
		ratingValue sql.NullFloat64
		float4Value sql.NullFloat64
		dataLen     sql.NullInt64
		dataHex     sql.NullString
		createdAt   sql.NullString
	)

	require.NoError(t, db.QueryRow(`
		SELECT
			name,
			note,
			count_value,
			small_count,
			medium_count,
			exact_amount,
			enabled_flag,
			rating,
			float4_value,
			CASE WHEN data IS NULL THEN NULL ELSE length(data) END,
			CASE WHEN data IS NULL THEN NULL ELSE hex(data) END,
			created_at
		FROM typed_rows
		WHERE id = ?
	`, row.ID).Scan(&name, &note, &countValue, &smallCount, &mediumCount, &exactAmount, &enabledFlag, &ratingValue, &float4Value, &dataLen, &dataHex, &createdAt))

	require.Equal(t, row.Name, name)
	assertNullStringEquals(t, note, row.Note)
	assertNullInt64Equals(t, countValue, row.CountValue)
	assertNullInt64Equals(t, smallCount, row.SmallCount)
	assertNullInt64Equals(t, mediumCount, row.MediumCount)
	assertNullStringEquals(t, exactAmount, row.ExactAmountExpected)
	require.Equal(t, row.EnabledFlag, enabledFlag)
	assertNullFloatEquals(t, ratingValue, row.RatingExpectedText)
	assertNullFloatEquals(t, float4Value, row.Float4ExpectedText)

	if row.DataHex == nil {
		require.False(t, dataLen.Valid)
		require.False(t, dataHex.Valid)
	} else {
		require.True(t, dataLen.Valid)
		require.True(t, dataHex.Valid)
		require.Equal(t, int64(len(*row.DataHex)/2), dataLen.Int64)
		require.Equal(t, strings.ToUpper(*row.DataHex), dataHex.String)
	}

	if row.CreatedAt == nil {
		require.False(t, createdAt.Valid)
		return
	}

	require.True(t, createdAt.Valid)
	require.Equal(t, mustParseRFC3339Instant(t, *row.CreatedAt), mustParseRFC3339Instant(t, createdAt.String))
}

func assertTableCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count))
	require.Equal(t, want, count)
}

func assertNullStringEquals(t *testing.T, got sql.NullString, want *string) {
	t.Helper()
	if want == nil {
		require.False(t, got.Valid)
		return
	}
	require.True(t, got.Valid)
	require.Equal(t, *want, got.String)
}

func assertNullInt64Equals(t *testing.T, got sql.NullInt64, want *int64) {
	t.Helper()
	if want == nil {
		require.False(t, got.Valid)
		return
	}
	require.True(t, got.Valid)
	require.Equal(t, *want, got.Int64)
}

func assertNullFloatEquals(t *testing.T, got sql.NullFloat64, want *string) {
	t.Helper()
	if want == nil {
		require.False(t, got.Valid)
		return
	}
	require.True(t, got.Valid)
	expected, err := strconv.ParseFloat(*want, 64)
	require.NoError(t, err)
	require.Equal(t, expected, got.Float64)
}

func mustParseRFC3339Instant(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err)
	return parsed.UTC()
}

func nullStringPointer(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullInt64Pointer(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullFloatLiteral(t *testing.T, value *string) any {
	t.Helper()
	if value == nil {
		return nil
	}
	parsed, err := strconv.ParseFloat(*value, 64)
	require.NoError(t, err)
	return parsed
}

func nullBytesPointer(value []byte, valid bool) any {
	if !valid {
		return nil
	}
	return value
}
