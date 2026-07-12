package oversync

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/wirevalue"
)

func (s *SyncService) normalizePushPayloadFields(schemaName, tableName string, payloadObject map[string]any) error {
	columnTypes := s.columnTypesForTable(schemaName, tableName)
	if len(columnTypes) == 0 {
		return nil
	}

	for columnName, rawValue := range payloadObject {
		columnType := columnTypes[strings.ToLower(columnName)]
		if rawValue == nil {
			continue
		}
		switch columnType {
		case "bytea":
			encodedValue, ok := rawValue.(string)
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload binary field %s.%s.%s must be a base64 string", schemaName, tableName, columnName)}
			}
			decodedValue, err := base64.StdEncoding.DecodeString(encodedValue)
			if err != nil {
				return &PushValidationError{Message: fmt.Sprintf("payload binary field %s.%s.%s must be valid base64", schemaName, tableName, columnName)}
			}
			payloadObject[columnName] = "\\x" + hex.EncodeToString(decodedValue)
		case "int2", "int4", "int8":
			raw, ok := rawValue.(string)
			if !ok {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical signed integer JSON string", "legacy_json_number")
			}
			value, err := wirevalue.ParseInt64(raw)
			if err != nil {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical signed integer JSON string", "invalid_integer")
			}
			if !integerFitsPostgresType(value, columnType) {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical signed integer JSON string", "out_of_range")
			}
		case "numeric", "decimal":
			raw, ok := rawValue.(string)
			if !ok {
				return numericPushValidationError(schemaName, tableName, columnName, "finite decimal JSON string", "legacy_json_number")
			}
			if err := wirevalue.ValidateDecimal(raw); err != nil {
				return numericPushValidationError(schemaName, tableName, columnName, "finite decimal JSON string", "invalid_decimal")
			}
		case "float4", "float8":
			raw, ok := rawValue.(string)
			if !ok {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical finite binary64 JSON string", "legacy_json_number")
			}
			if _, err := wirevalue.ParseFloat64(raw); err != nil {
				return numericPushValidationError(schemaName, tableName, columnName, "canonical finite binary64 JSON string", "invalid_float")
			}
		case "bool":
			switch raw := rawValue.(type) {
			case bool:
				// Direct protocol clients may send a JSON Boolean.
			case string:
				switch raw {
				case "0":
					payloadObject[columnName] = false
				case "1":
					payloadObject[columnName] = true
				default:
					return numericPushValidationError(schemaName, tableName, columnName, `JSON Boolean or SQLite ingress string "0"/"1"`, "invalid_boolean_bridge")
				}
			default:
				return numericPushValidationError(schemaName, tableName, columnName, `JSON Boolean or SQLite ingress string "0"/"1"`, "invalid_boolean_bridge")
			}
		}
	}

	return nil
}

func (s *SyncService) canonicalizeWirePayload(schemaName, tableName string, payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return payload, nil
	}

	columnTypes := s.columnTypesForTable(schemaName, tableName)
	if len(columnTypes) == 0 {
		return append(json.RawMessage(nil), payload...), nil
	}

	payloadObject, err := jcs.DecodeObject(payload)
	if err != nil {
		return nil, fmt.Errorf("decode payload for %s.%s: %w", schemaName, tableName, err)
	}

	changed := false
	if stripHiddenOwnerColumn(payloadObject) {
		changed = true
	}
	for columnName, rawValue := range payloadObject {
		if rawValue == nil {
			continue
		}
		columnType := columnTypes[strings.ToLower(columnName)]
		switch columnType {
		case "bytea":
			canonicalValue, ok, err := canonicalizeWireBinaryValue(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize binary payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if ok && canonicalValue != rawValue {
				payloadObject[columnName] = canonicalValue
				changed = true
			}
		case "int2", "int4", "int8":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize integer payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			value, err := wirevalue.ParseInt64(raw)
			if err != nil {
				return nil, fmt.Errorf("canonicalize integer payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if !integerFitsPostgresType(value, columnType) {
				return nil, fmt.Errorf("canonicalize integer payload for %s.%s.%s: value is outside PostgreSQL %s range", schemaName, tableName, columnName, columnType)
			}
			payloadObject[columnName] = raw
			changed = true
		case "numeric", "decimal":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize exact decimal payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if err := wirevalue.ValidateDecimal(raw); err != nil {
				return nil, fmt.Errorf("canonicalize exact decimal payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			payloadObject[columnName] = raw
			changed = true
		case "float4", "float8":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize floating payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			value, err := parseDatabaseFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("canonicalize floating payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if columnType == "float4" {
				value = float64(float32(value))
			}
			canonical, err := wirevalue.RenderFloat64(value)
			if err != nil {
				return nil, fmt.Errorf("canonicalize floating payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			payloadObject[columnName] = canonical
			changed = true
		case "bool":
			if _, ok := rawValue.(bool); !ok {
				return nil, fmt.Errorf("canonicalize Boolean payload for %s.%s.%s: expected database Boolean, got %T", schemaName, tableName, columnName, rawValue)
			}
		}
	}

	if !changed {
		return append(json.RawMessage(nil), payload...), nil
	}

	canonicalPayload, err := jcs.Marshal(payloadObject)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical payload for %s.%s: %w", schemaName, tableName, err)
	}
	canonicalPayload, err = canonicalJSON(canonicalPayload)
	if err != nil {
		return nil, fmt.Errorf("canonicalize canonical payload for %s.%s: %w", schemaName, tableName, err)
	}
	return canonicalPayload, nil
}

func numericPushValidationError(schemaName, tableName, columnName, expected, category string) error {
	return &PushValidationError{Message: fmt.Sprintf(
		"payload field %s.%s.%s must use %s (category=%s)",
		schemaName,
		tableName,
		columnName,
		expected,
		category,
	)}
}

func integerFitsPostgresType(value int64, columnType string) bool {
	switch columnType {
	case "int2":
		return value >= -32768 && value <= 32767
	case "int4":
		return value >= -2147483648 && value <= 2147483647
	case "int8":
		return true
	default:
		return false
	}
}

func parseDatabaseFloat(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, fmt.Errorf("database value must be finite")
	}
	return value, nil
}

func exactNumberText(value any) (string, error) {
	switch typed := value.(type) {
	case json.Number:
		return typed.String(), nil
	case string:
		return typed, nil
	default:
		return "", fmt.Errorf("expected exact numeric JSON string or database number, got %T", value)
	}
}

func (s *SyncService) columnTypesForTable(schemaName, tableName string) map[string]string {
	if s == nil {
		return nil
	}
	return s.columnTypesByTable[Key(schemaName, tableName)]
}

func canonicalizeWireBinaryValue(value any) (string, bool, error) {
	if value == nil {
		return "", false, nil
	}

	rawValue, ok := value.(string)
	if !ok {
		return "", false, fmt.Errorf("expected JSON string, got %T", value)
	}

	decoded, err := decodeStoredBinaryValue(rawValue)
	if err != nil {
		return "", false, err
	}
	return base64.StdEncoding.EncodeToString(decoded), true, nil
}

func decodeStoredBinaryValue(rawValue string) ([]byte, error) {
	if strings.HasPrefix(rawValue, "\\x") || strings.HasPrefix(rawValue, "\\X") {
		decoded, err := hex.DecodeString(rawValue[2:])
		if err != nil {
			return nil, fmt.Errorf("decode postgres bytea hex: %w", err)
		}
		return decoded, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(rawValue)
	if err != nil {
		return nil, fmt.Errorf("decode base64: %w", err)
	}
	return decoded, nil
}
