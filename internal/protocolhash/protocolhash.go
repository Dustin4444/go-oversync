// Package protocolhash defines the language-neutral logical inputs for C1
// request and committed-bundle hashes.
package protocolhash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

type PushRow struct {
	Schema         string
	Table          string
	Key            map[string]any
	Op             string
	BaseRowVersion int64
	Payload        []byte
}

type BundleRow struct {
	Schema     string
	Table      string
	Key        map[string]any
	Op         string
	RowVersion int64
	Payload    []byte
}

func PushRequest(rows []PushRow) (string, int64, error) {
	logicalRows := make([]map[string]any, 0, len(rows))
	for ordinal, row := range rows {
		payload, err := payloadValue(row.Op, row.Payload)
		if err != nil {
			return "", 0, fmt.Errorf("decode request payload for %s.%s row %d: %w", row.Schema, row.Table, ordinal, err)
		}
		logicalRows = append(logicalRows, map[string]any{
			"row_ordinal":      strconv.Itoa(ordinal),
			"schema":           row.Schema,
			"table":            row.Table,
			"key":              row.Key,
			"op":               row.Op,
			"base_row_version": strconv.FormatInt(row.BaseRowVersion, 10),
			"payload":          payload,
		})
	}
	return hash(logicalRows)
}

func CommittedBundle(rows []BundleRow) (string, int64, error) {
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

func IsCanonicalHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func payloadValue(op string, payload []byte) (any, error) {
	if op == "DELETE" || len(payload) == 0 {
		return nil, nil
	}
	return jcs.Decode(payload)
}

func hash(value any) (string, int64, error) {
	canonical, err := jcs.Marshal(value)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), int64(len(canonical)), nil
}
