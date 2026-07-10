package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestC1JCSCanonicalJSONAndTypedStringRequestHashes(t *testing.T) {
	exact, err := canonicalizeJSON(`{"n":"9007199254740993","d":"1234567890.123456789"}`)
	require.NoError(t, err)
	require.Equal(t, `{"d":"1234567890.123456789","n":"9007199254740993"}`, string(exact))

	rows := func(number string) []oversync.PushRequestRow {
		return []oversync.PushRequestRow{{
			Schema:         "public",
			Table:          "exact_numbers",
			Key:            oversync.SyncKey{"id": "n-1"},
			Op:             oversync.OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(`{"id":"n-1","value":"` + number + `"}`),
		}}
	}
	left, err := computeCanonicalPushRequestHash(rows("9007199254740992"))
	require.NoError(t, err)
	right, err := computeCanonicalPushRequestHash(rows("9007199254740993"))
	require.NoError(t, err)
	require.NotEqual(t, left, right, "adjacent exact request values require distinct hashes")
}

func TestC1UploadMutationAndReplayPreserveTypedExactValues(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "exact_rows", SyncKeyColumnName: "id", NumericColumns: map[string]NumericColumnKind{"value": NumericColumnExactDecimal}}}, `
		CREATE TABLE exact_rows (
			id TEXT PRIMARY KEY NOT NULL,
			value TEXT NOT NULL,
			data BLOB
		)
	`)

	wire, err := client.processPayloadForUpload(
		"exact_rows",
		`{"id":"n-1","value":"1234567890.123456789","data":"0011ff"}`,
	)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"n-1","value":"1234567890.123456789","data":"ABH/"}`, string(wire))

	equal, err := replayEquivalentJSON(`{"value":"9007199254740992"}`, `{"value":"9007199254740993"}`)
	require.NoError(t, err)
	require.False(t, equal, "remote replay comparison must distinguish adjacent exact integers")
}

func TestC1SQLiteIntegerAndDecimalMapping(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{
		TableName: "exact_rows", SyncKeyColumnName: "id",
		NumericColumns: map[string]NumericColumnKind{
			"min_value": NumericColumnExactInt64,
			"max_value": NumericColumnExactInt64,
			"amount":    NumericColumnExactDecimal,
		},
	}}, `
		CREATE TABLE exact_rows (
			id TEXT PRIMARY KEY NOT NULL,
			min_value INTEGER NOT NULL,
			max_value INTEGER NOT NULL,
			amount TEXT NOT NULL
		)
	`)
	tx := mustBeginTx(t, db)
	defer tx.Rollback()
	payload := map[string]interface{}{
		"id":        "n-1",
		"min_value": "-9223372036854775808",
		"max_value": "9223372036854775807",
		"amount":    "1234567890.123456789",
	}
	require.NoError(t, client.upsertRowInTx(ctx, tx, "exact_rows", payload))
	var minValue, maxValue, amount, minType, maxType, amountType string
	require.NoError(t, tx.QueryRowContext(ctx, `
		SELECT CAST(min_value AS TEXT), CAST(max_value AS TEXT), amount,
		       typeof(min_value), typeof(max_value), typeof(amount)
		FROM exact_rows WHERE id = 'n-1'
	`).Scan(&minValue, &maxValue, &amount, &minType, &maxType, &amountType))
	require.Equal(t, "-9223372036854775808", minValue)
	require.Equal(t, "9223372036854775807", maxValue)
	require.Equal(t, "1234567890.123456789", amount)
	require.Equal(t, "integer", minType)
	require.Equal(t, "integer", maxType)
	require.Equal(t, "text", amountType)

	require.Error(t, client.upsertRowInTx(ctx, tx, "exact_rows", map[string]interface{}{
		"id": "n-2", "min_value": "1.5", "max_value": "1", "amount": "1",
	}))
	var count int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM exact_rows WHERE id = 'n-2'`).Scan(&count))
	require.Zero(t, count, "unsupported mapping must reject before mutation")
}

func TestC1FreshDurableStateUsesOnlyRevisedContract(t *testing.T) {
	_, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL)
	`)
	rows, err := db.Query(`PRAGMA table_info(_sync_outbox_bundle)`)
	require.NoError(t, err)
	defer rows.Close()
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, declaredType string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &declaredType, &notNull, &defaultValue, &pk))
		found = found || name == "canonical_json_contract"
	}
	require.NoError(t, rows.Err())
	require.True(t, found, "fresh corrected durable state must carry the JCS typed-numeric-string contract marker")
}
