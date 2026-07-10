//go:build oversync_audit

package oversync

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

const auditProtocolFuzzInputLimit = 4 << 10

// JSON fuzz disposition: FuzzAuditCanonicalJSON_Idempotent in
// audit_contract_test.go remains the single JSON/canonicalization fuzz owner.
// It already owns the minimized corpus under testdata/fuzz, so this file does
// not create a duplicate target with divergent seeds or evidence.

func addAuditProtocolIdentifierSeeds(f *testing.F) {
	f.Helper()
	for _, seed := range []string{
		"",
		"audit",
		"audit_name_1",
		"Audit",
		"audit-name",
		"audit name",
		"audit\x00name",
		"é",
		strings.Repeat("a", 64),
	} {
		f.Add(seed)
	}
}

func auditProtocolFuzzIdentifierDeterminism(t *testing.T, input string, validate func(string) bool) {
	t.Helper()
	if len(input) > auditProtocolFuzzInputLimit {
		t.Skip()
	}
	first := validate(input)
	second := validate(input)
	if first != second {
		t.Fatalf("identifier validation is non-deterministic for %q", input)
	}
}

func FuzzAuditProtocolSchemaName_Deterministic(f *testing.F) {
	addAuditProtocolIdentifierSeeds(f)
	f.Fuzz(func(t *testing.T, input string) {
		auditProtocolFuzzIdentifierDeterminism(t, input, isValidSchemaName)
	})
}

func FuzzAuditProtocolTableName_Deterministic(f *testing.F) {
	addAuditProtocolIdentifierSeeds(f)
	f.Fuzz(func(t *testing.T, input string) {
		auditProtocolFuzzIdentifierDeterminism(t, input, isValidTableName)
	})
}

func FuzzAuditProtocolColumnName_Deterministic(f *testing.F) {
	addAuditProtocolIdentifierSeeds(f)
	f.Fuzz(func(t *testing.T, input string) {
		auditProtocolFuzzIdentifierDeterminism(t, input, isValidColumnName)
	})
}

func FuzzAuditProtocolSchemaTableKey_Deterministic(f *testing.F) {
	for _, seed := range []string{
		"events",
		"public.events",
		" AUDIT.EVENTS ",
		"audit.events.extra",
		".events",
		"audit.",
		"audit.event-name",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > auditProtocolFuzzInputLimit {
			t.Skip()
		}
		first, firstOK := normalizeSchemaTableKey(input)
		second, secondOK := normalizeSchemaTableKey(input)
		if first != second || firstOK != secondOK {
			t.Fatalf("schema.table parsing is non-deterministic for %q", input)
		}
		if !firstOK {
			return
		}

		parts := strings.Split(first, ".")
		if len(parts) != 2 || !isValidSchemaName(parts[0]) || !isValidTableName(parts[1]) {
			t.Fatalf("accepted schema.table key is not valid: input=%q normalized=%q", input, first)
		}
		normalizedAgain, ok := normalizeSchemaTableKey(first)
		if !ok || normalizedAgain != first {
			t.Fatalf("schema.table normalization is not idempotent: first=%q second=%q", first, normalizedAgain)
		}
	})
}

func auditProtocolFuzzErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func FuzzAuditProtocolNotificationChannel_Deterministic(f *testing.F) {
	for _, seed := range []string{
		"oversync_bundle_change_v1",
		"",
		"channel with spaces",
		"channel\x00name",
		strings.Repeat("a", 63),
		strings.Repeat("a", 64),
		"通知",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > auditProtocolFuzzInputLimit {
			t.Skip()
		}
		first, firstErr := quotePostgresNotificationChannel(input)
		second, secondErr := quotePostgresNotificationChannel(input)
		if first != second || auditProtocolFuzzErrorText(firstErr) != auditProtocolFuzzErrorText(secondErr) {
			t.Fatalf("notification-channel quoting is non-deterministic for %q", input)
		}
		if firstErr == nil && first == "" {
			t.Fatalf("accepted notification channel produced an empty quoted identifier for %q", input)
		}
	})
}

func FuzzAuditProtocolSyncKeyEncoding_Deterministic(f *testing.F) {
	f.Add(syncKeyTypeUUID, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	f.Add(syncKeyTypeUUID, "6BA7B810-9DAD-11D1-80B4-00C04FD430C8")
	f.Add(syncKeyTypeUUID, "not-a-uuid")
	f.Add(syncKeyTypeText, " exact Text key ")
	f.Add(syncKeyTypeText, "")
	f.Add(syncKeyTypeText, "key\x00value")
	f.Add("unsupported", "value")

	f.Fuzz(func(t *testing.T, keyType string, input string) {
		if len(keyType)+len(input) > auditProtocolFuzzInputLimit {
			t.Skip()
		}

		firstBytes, firstValue, firstErr := encodeKeyBytes(keyType, input)
		secondBytes, secondValue, secondErr := encodeKeyBytes(keyType, input)
		if !bytes.Equal(firstBytes, secondBytes) ||
			!reflect.DeepEqual(firstValue, secondValue) ||
			auditProtocolFuzzErrorText(firstErr) != auditProtocolFuzzErrorText(secondErr) {
			t.Fatalf("sync-key encoding is non-deterministic: type=%q input=%q", keyType, input)
		}
		if firstErr != nil {
			return
		}

		firstText, firstDecodedValue, firstDecodeErr := decodeKeyBytes(keyType, firstBytes)
		secondText, secondDecodedValue, secondDecodeErr := decodeKeyBytes(keyType, firstBytes)
		if firstText != secondText ||
			!reflect.DeepEqual(firstDecodedValue, secondDecodedValue) ||
			auditProtocolFuzzErrorText(firstDecodeErr) != auditProtocolFuzzErrorText(secondDecodeErr) {
			t.Fatalf("sync-key decoding is non-deterministic: type=%q input=%q", keyType, input)
		}
		if firstDecodeErr != nil {
			t.Fatalf("encoded sync key cannot be decoded: type=%q input=%q err=%v", keyType, input, firstDecodeErr)
		}
		if keyType == syncKeyTypeText && firstText != input {
			t.Fatalf("text sync key changed during round trip: input=%q output=%q", input, firstText)
		}

		roundTripBytes, _, roundTripErr := encodeKeyBytes(keyType, firstText)
		if roundTripErr != nil || !bytes.Equal(firstBytes, roundTripBytes) {
			t.Fatalf("sync-key byte round trip changed: type=%q input=%q err=%v", keyType, input, roundTripErr)
		}
	})
}
