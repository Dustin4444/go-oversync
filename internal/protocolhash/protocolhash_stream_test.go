package protocolhash

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func committedBundleLegacyReference(rows []BundleRow) (string, int64, error) {
	logicalRows := make([]map[string]any, 0, len(rows))
	for ordinal, row := range rows {
		payload, err := payloadValue(row.Op, row.Payload)
		if err != nil {
			return "", 0, fmt.Errorf("decode committed payload for %s.%s row %d: %w", row.Schema, row.Table, ordinal, err)
		}
		logicalRows = append(logicalRows, map[string]any{
			"row_ordinal": strconv.Itoa(ordinal),
			"schema":      row.Schema,
			"table":       row.Table,
			"key":         row.Key,
			"op":          row.Op,
			"row_version": strconv.FormatInt(row.RowVersion, 10),
			"payload":     payload,
		})
	}
	return hash(logicalRows)
}

func TestCommittedBundleHasher_MatchesAggregateReferenceVectors(t *testing.T) {
	tests := []struct {
		name      string
		rows      []BundleRow
		wantHash  string
		wantBytes int64
	}{
		{
			name:      "empty",
			wantHash:  "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
			wantBytes: 2,
		},
		{
			name: "nested live row",
			rows: []BundleRow{{
				Schema: "audit", Table: "records", Key: map[string]any{"id": "row-a"},
				Op: "INSERT", RowVersion: 7,
				Payload: []byte(`{"z":{"b":2,"a":1},"a":[true,null,"text"]}`),
			}},
			wantHash:  "f8a5d70fda41203ba81ca8ed5932e641d45b269cf513fa141087b0b3554d1a9d",
			wantBytes: 162,
		},
		{
			name: "multiple rows preserve authoritative order",
			rows: []BundleRow{
				{
					Schema: "audit", Table: "records", Key: map[string]any{"id": "row-a"},
					Op: "INSERT", RowVersion: 7,
					Payload: []byte(`{"z":{"b":2,"a":1},"a":[true,null,"text"]}`),
				},
				{
					Schema: "audit", Table: "records", Key: map[string]any{"id": "row-b"},
					Op: "UPDATE", RowVersion: 8,
					Payload: []byte(`{"value":42,"label":"second"}`),
				},
			},
			wantHash:  "6ba5f4fdb66a498ddce09f67d55c87619b6b35a243e1eae9a1344186f000bad2",
			wantBytes: 310,
		},
		{
			name: "tombstone ignores payload",
			rows: []BundleRow{{
				Schema: "audit", Table: "records", Key: map[string]any{"id": "gone"},
				Op: "DELETE", RowVersion: 9, Payload: []byte(`{"ignored":"payload"}`),
			}},
			wantHash:  "427fa0337df035b97d95b0fb6db2bb6329956fd11889b562b2efbf71958a40a5",
			wantBytes: 123,
		},
		{
			name: "unicode uuid numeric and duplicate",
			rows: []BundleRow{
				{
					Schema: "監査", Table: "récords",
					Key: map[string]any{"id": "ffffffff-ffff-ffff-ffff-ffffffffffff"},
					Op:  "INSERT", RowVersion: 10,
					Payload: []byte(`{"東京":"値","integer":"9007199254740993","nested":{"β":2,"α":1}}`),
				},
				{
					Schema: "監査", Table: "récords",
					Key: map[string]any{"id": "ffffffff-ffff-ffff-ffff-ffffffffffff"},
					Op:  "UPDATE", RowVersion: 11,
					Payload: []byte(`{"integer":"9007199254740993"}`),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			aggregateHash, aggregateBytes, aggregateErr := CommittedBundle(test.rows)
			hasher := NewCommittedBundleHasher()
			for _, row := range test.rows {
				require.NoError(t, hasher.Add(row))
			}
			streamHash, streamBytes, streamErr := hasher.Sum()

			require.Equal(t, aggregateErr, streamErr)
			require.Equal(t, aggregateHash, streamHash)
			require.Equal(t, aggregateBytes, streamBytes)
			if test.wantHash != "" {
				require.Equal(t, test.wantHash, streamHash)
				require.Equal(t, test.wantBytes, streamBytes)
			}
		})
	}
}

func TestCommittedBundleHasher_MatchesAggregateErrors(t *testing.T) {
	rows := []BundleRow{
		{Schema: "audit", Table: "records", Key: map[string]any{"id": "ok"}, Op: "INSERT", RowVersion: 1, Payload: []byte(`{"ok":true}`)},
		{Schema: "audit", Table: "records", Key: map[string]any{"id": "bad"}, Op: "UPDATE", RowVersion: 2, Payload: []byte(`{"broken":`)},
	}

	_, _, aggregateErr := CommittedBundle(rows)
	require.Error(t, aggregateErr)
	hasher := NewCommittedBundleHasher()
	require.NoError(t, hasher.Add(rows[0]))
	streamErr := hasher.Add(rows[1])
	require.EqualError(t, streamErr, aggregateErr.Error())
}

func TestCommittedBundleHasher_AuthoritativeOrderChangesHash(t *testing.T) {
	rows := []BundleRow{
		{Schema: "audit", Table: "records", Key: map[string]any{"id": "a"}, Op: "INSERT", RowVersion: 1, Payload: []byte(`{"value":1}`)},
		{Schema: "audit", Table: "records", Key: map[string]any{"id": "b"}, Op: "INSERT", RowVersion: 1, Payload: []byte(`{"value":2}`)},
	}
	hashRows := func(rows []BundleRow) string {
		t.Helper()
		hasher := NewCommittedBundleHasher()
		for _, row := range rows {
			require.NoError(t, hasher.Add(row))
		}
		hash, _, err := hasher.Sum()
		require.NoError(t, err)
		return hash
	}
	require.NotEqual(t, hashRows(rows), hashRows([]BundleRow{rows[1], rows[0]}))
}

func TestCommittedBundleHasher_MatchesLegacyCanonicalBytesForEscapingAndUnicode(t *testing.T) {
	rows := []BundleRow{
		{
			Schema: "audit<&\u2028", Table: "records\u2029𐀀", Key: map[string]any{
				"id": "quote\" slash\\ controls\b\t\n\f\r\u0001 <>& 東京",
			},
			Op: "INSERT", RowVersion: 9223372036854775807,
			Payload: []byte(`{"𐀀":"supplementary","":"bmp","html":"<>&","line":"\u2028\u2029"}`),
		},
	}
	wantHash, wantBytes, err := committedBundleLegacyReference(rows)
	require.NoError(t, err)

	hasher := NewCommittedBundleHasher()
	require.NoError(t, hasher.Add(rows[0]))
	gotHash, gotBytes, err := hasher.Sum()
	require.NoError(t, err)
	require.Equal(t, wantHash, gotHash)
	require.Equal(t, wantBytes, gotBytes)
}
