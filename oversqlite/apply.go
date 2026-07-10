// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/wirevalue"
)

func (c *Client) upsertRowInTx(ctx context.Context, tx *sql.Tx, table string, payload map[string]interface{}) error {
	return c.upsertRowInTxUsing(ctx, tx, tx, table, payload)
}

func (c *Client) upsertRowInTxUsing(ctx context.Context, execer execContexter, tx *sql.Tx, table string, payload map[string]interface{}) error {
	tableInfo, err := c.getTableInfoTx(tx, strings.ToLower(table))
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	pkColumn, err := c.primaryKeyColumnForTable(table)
	if err != nil {
		return err
	}

	columnByLower := make(map[string]ColumnInfo, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columnByLower[strings.ToLower(col.Name)] = col
	}
	if len(payload) != len(columnByLower) {
		return fmt.Errorf("payload for %s must contain every table column", table)
	}

	columns := make([]string, 0, len(tableInfo.Columns))
	values := make([]interface{}, 0, len(tableInfo.Columns))
	updateAssignments := make([]string, 0, len(tableInfo.Columns))

	for _, col := range tableInfo.Columns {
		key := strings.ToLower(col.Name)
		val, ok := payload[key]
		if !ok {
			return fmt.Errorf("payload for %s is missing column %s", table, col.Name)
		}

		if col.IsBlob() && val != nil {
			s, ok := val.(string)
			if !ok {
				return fmt.Errorf("payload for %s column %s must be string-encoded blob data", table, col.Name)
			}
			var (
				decoded []byte
				err     error
			)
			if strings.EqualFold(col.Name, pkColumn) || tableInfo.IsBlobReferenceColumn(col.Name) {
				decoded, err = decodeCanonicalWireUUIDBytes(s)
				if err != nil {
					return fmt.Errorf("failed to decode canonical wire UUID payload field %s: %w", col.Name, err)
				}
			} else {
				decoded, err = decodeCanonicalWireBlobBytes(s)
				if err != nil {
					return fmt.Errorf("failed to decode canonical wire blob payload field %s: %w", col.Name, err)
				}
			}
			val = decoded
		} else if val != nil {
			val, err = c.sqliteValueForWireJSON(table, col, val)
			if err != nil {
				return fmt.Errorf("payload for %s column %s: %w", table, col.Name, err)
			}
		}

		columns = append(columns, quoteIdent(col.Name))
		values = append(values, val)
		if !strings.EqualFold(col.Name, pkColumn) {
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s = excluded.%s", quoteIdent(col.Name), quoteIdent(col.Name)))
		}
	}

	if _, ok := payload[strings.ToLower(pkColumn)]; !ok {
		return fmt.Errorf("payload for %s is missing primary key column %s", table, pkColumn)
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if len(updateAssignments) == 0 {
		query += fmt.Sprintf(" ON CONFLICT(%s) DO NOTHING", quoteIdent(pkColumn))
	} else {
		query += fmt.Sprintf(" ON CONFLICT(%s) DO UPDATE SET %s", quoteIdent(pkColumn), strings.Join(updateAssignments, ", "))
	}

	if _, err := execer.ExecContext(ctx, query, values...); err != nil {
		return fmt.Errorf("failed to execute upsert: %w", err)
	}
	return nil
}

func (c *Client) sqliteValueForWireJSON(table string, col ColumnInfo, value any) (any, error) {
	number, isNumber := value.(json.Number)
	if kind, configured := c.numericKindForColumn(table, col.Name); configured {
		switch kind {
		case wirevalue.NumericKindExactInt64:
			raw, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("exact-int64 requires a JSON string")
			}
			parsed, err := wirevalue.ParseInt64(raw)
			if err != nil {
				return nil, fmt.Errorf("invalid exact int64: %w", err)
			}
			return parsed, nil
		case wirevalue.NumericKindExactDecimal:
			raw, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("exact-decimal requires a JSON string")
			}
			if err := wirevalue.ValidateDecimal(raw); err != nil {
				return nil, fmt.Errorf("invalid exact decimal: %w", err)
			}
			return raw, nil
		case wirevalue.NumericKindApproximate:
			if !isNumber {
				return nil, fmt.Errorf("approximate numeric column requires a JSON number")
			}
			parsed, err := strconv.ParseFloat(number.String(), 64)
			if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
				return nil, fmt.Errorf("approximate numeric column requires a finite binary64 value")
			}
			return parsed, nil
		}
	}
	if col.IsInteger() {
		switch typed := value.(type) {
		case json.Number:
			parsed, err := strconv.ParseInt(typed.String(), 10, 64)
			if err != nil || parsed > 9007199254740991 || parsed < -9007199254740991 {
				return nil, fmt.Errorf("INTEGER outside the JCS exact range requires exact_int64 metadata and a JSON string")
			}
			return parsed, nil
		case int64:
			return typed, nil
		case int:
			return int64(typed), nil
		default:
			return nil, fmt.Errorf("INTEGER requires an exact signed 64-bit integer")
		}
	}
	if col.IsReal() {
		switch typed := value.(type) {
		case json.Number:
			parsed, err := strconv.ParseFloat(typed.String(), 64)
			if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
				return nil, fmt.Errorf("REAL requires a finite representable value")
			}
			return parsed, nil
		case float64:
			if math.IsInf(typed, 0) || math.IsNaN(typed) {
				return nil, fmt.Errorf("REAL requires a finite representable value")
			}
			return typed, nil
		default:
			return nil, fmt.Errorf("REAL requires a finite numeric value")
		}
	}
	if isNumber {
		return nil, fmt.Errorf("JSON number requires a compatible INTEGER or REAL destination")
	}
	return value, nil
}

func (c *Client) updateRowMeta(ctx context.Context, pk, tableName string, serverVersion int64, deleted bool) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin row meta transaction: %w", err)
	}
	defer tx.Rollback()
	if err := c.updateRowMetaInTx(ctx, tx, pk, tableName, serverVersion, deleted); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit row meta transaction: %w", err)
	}
	return nil
}

func (c *Client) updateRowMetaInTx(ctx context.Context, tx *sql.Tx, pk, tableName string, serverVersion int64, deleted bool) error {
	return c.updateRowMetaInTxUsing(ctx, tx, tx, pk, tableName, serverVersion, deleted)
}

func (c *Client) updateRowMetaInTxUsing(ctx context.Context, execer execContexter, tx *sql.Tx, pk, tableName string, serverVersion int64, deleted bool) error {
	keyJSON, err := c.syncKeyJSONForPKInTx(tx, tableName, pk)
	if err != nil {
		return err
	}
	if err := c.updateStructuredRowStateInTxUsing(ctx, execer, c.config.Schema, tableName, keyJSON, serverVersion, deleted); err != nil {
		return err
	}
	return nil
}

func (c *Client) syncKeyJSONForPKInTx(tx *sql.Tx, tableName, pk string) (string, error) {
	keyColumns, err := c.syncKeyColumnsForTable(tableName)
	if err != nil {
		return "", err
	}
	if len(keyColumns) != 1 {
		return "", fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
	}
	keyJSONBytes, err := json.Marshal(map[string]any{strings.ToLower(keyColumns[0]): pk})
	if err != nil {
		return "", fmt.Errorf("failed to encode sync key for %s.%s: %w", tableName, pk, err)
	}
	return string(keyJSONBytes), nil
}

func (c *Client) updateStructuredRowStateInTx(ctx context.Context, tx *sql.Tx, schemaName, tableName, keyJSON string, rowVersion int64, deleted bool) error {
	return c.updateStructuredRowStateInTxUsing(ctx, tx, schemaName, tableName, keyJSON, rowVersion, deleted)
}

func (c *Client) updateStructuredRowStateInTxUsing(ctx context.Context, execer execContexter, schemaName, tableName, keyJSON string, rowVersion int64, deleted bool) error {
	deletedInt := 0
	if deleted {
		deletedInt = 1
	}
	if _, err := execer.ExecContext(ctx, `
		INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted, updated_at)
		VALUES(?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
			row_version = excluded.row_version,
			deleted = excluded.deleted,
			updated_at = excluded.updated_at
	`, schemaName, tableName, keyJSON, rowVersion, deletedInt); err != nil {
		return fmt.Errorf("failed to update structured row state for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	return nil
}

// GetTableRowCount returns the number of rows in a business table
func (c *Client) GetTableRowCount(tableName string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(tableName))
	err := c.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in %s: %w", tableName, err)
	}
	return count, nil
}

// GetTableRows returns all rows from a business table (for debugging/inspection)
func (c *Client) GetTableRows(tableName string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", quoteIdent(tableName))
	rows, err := c.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}
