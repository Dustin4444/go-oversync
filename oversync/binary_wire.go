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
		case "int8":
			raw, ok := rawValue.(string)
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload exact-int64 field %s.%s.%s must be a JSON string", schemaName, tableName, columnName)}
			}
			if _, err := wirevalue.ParseInt64(raw); err != nil {
				return &PushValidationError{Message: fmt.Sprintf("payload exact-int64 field %s.%s.%s is invalid: %v", schemaName, tableName, columnName, err)}
			}
		case "numeric":
			raw, ok := rawValue.(string)
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload exact-decimal field %s.%s.%s must be a JSON string", schemaName, tableName, columnName)}
			}
			if err := wirevalue.ValidateDecimal(raw); err != nil {
				return &PushValidationError{Message: fmt.Sprintf("payload exact-decimal field %s.%s.%s is invalid: %v", schemaName, tableName, columnName, err)}
			}
		case "float4", "float8":
			number, ok := rawValue.(json.Number)
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload approximate field %s.%s.%s must be a JSON number", schemaName, tableName, columnName)}
			}
			parsed, err := strconv.ParseFloat(number.String(), 64)
			if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
				return &PushValidationError{Message: fmt.Sprintf("payload approximate field %s.%s.%s must be a finite binary64 value", schemaName, tableName, columnName)}
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
		case "int8":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize exact int64 payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if _, err := wirevalue.ParseInt64(raw); err != nil {
				return nil, fmt.Errorf("canonicalize exact int64 payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			payloadObject[columnName] = raw
			changed = true
		case "numeric":
			raw, err := exactNumberText(rawValue)
			if err != nil {
				return nil, fmt.Errorf("canonicalize exact decimal payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			if err := wirevalue.ValidateDecimal(raw); err != nil {
				return nil, fmt.Errorf("canonicalize exact decimal payload for %s.%s.%s: %w", schemaName, tableName, columnName, err)
			}
			payloadObject[columnName] = raw
			changed = true
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
