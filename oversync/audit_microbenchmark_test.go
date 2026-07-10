//go:build oversync_audit

package oversync

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
)

const auditMicroUUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

// BenchmarkAuditCanonicalJSON and BenchmarkAuditCommittedBundleHash remain the
// owners for canonical JSON and full logical bundle hashing. These benchmarks
// cover the adjacent pure operations that did not previously have owners.

func BenchmarkAuditMicroKeyEncoding(b *testing.B) {
	textKey := "audit-text-key-0123456789abcdef"
	uuidBytes, _, err := encodeKeyBytes(syncKeyTypeUUID, auditMicroUUID)
	if err != nil {
		b.Fatal(err)
	}
	textBytes, _, err := encodeKeyBytes(syncKeyTypeText, textKey)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("uuid_encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(auditMicroUUID)))
		b.ResetTimer()
		for b.Loop() {
			encoded, _, err := encodeKeyBytes(syncKeyTypeUUID, auditMicroUUID)
			if err != nil || len(encoded) != 16 {
				b.Fatalf("encode UUID key: len=%d err=%v", len(encoded), err)
			}
		}
	})

	b.Run("uuid_decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(uuidBytes)))
		b.ResetTimer()
		for b.Loop() {
			decoded, _, err := decodeKeyBytes(syncKeyTypeUUID, uuidBytes)
			if err != nil || decoded != auditMicroUUID {
				b.Fatalf("decode UUID key: value=%q err=%v", decoded, err)
			}
		}
	})

	b.Run("text_encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(textKey)))
		b.ResetTimer()
		for b.Loop() {
			encoded, _, err := encodeKeyBytes(syncKeyTypeText, textKey)
			if err != nil || len(encoded) != len(textKey) {
				b.Fatalf("encode text key: len=%d err=%v", len(encoded), err)
			}
		}
	})

	b.Run("text_decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(textBytes)))
		b.ResetTimer()
		for b.Loop() {
			decoded, _, err := decodeKeyBytes(syncKeyTypeText, textBytes)
			if err != nil || decoded != textKey {
				b.Fatalf("decode text key: value=%q err=%v", decoded, err)
			}
		}
	})
}

func auditMicroBinaryBytes(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte((i*31 + 17) % 251)
	}
	return value
}

func BenchmarkAuditMicroBinaryConversion(b *testing.B) {
	raw := auditMicroBinaryBytes(1024)
	encoded := base64.StdEncoding.EncodeToString(raw)
	stored := `\x` + hex.EncodeToString(raw)
	service := &SyncService{
		columnTypesByTable: map[string]map[string]string{
			"audit.files": {"data": "bytea"},
		},
	}

	b.Run("push_base64_to_stored", func(b *testing.B) {
		payload := map[string]any{"data": encoded}
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		b.ResetTimer()
		for b.Loop() {
			payload["data"] = encoded
			if err := service.normalizePushPayloadFields("audit", "files", payload); err != nil {
				b.Fatal(err)
			}
			converted, ok := payload["data"].(string)
			if !ok || len(converted) != len(stored) {
				b.Fatalf("unexpected stored binary value: type=%T len=%d", payload["data"], len(converted))
			}
		}
	})

	b.Run("stored_hex_to_wire", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		b.ResetTimer()
		for b.Loop() {
			converted, changed, err := canonicalizeWireBinaryValue(stored)
			if err != nil || !changed || converted != encoded {
				b.Fatalf("unexpected wire binary value: changed=%t len=%d err=%v", changed, len(converted), err)
			}
		}
	})
}

func auditMicroPayloadService() *SyncService {
	const tableKey = "audit.files"
	return &SyncService{
		registeredTables: map[string]bool{tableKey: true},
		registeredTableInfo: map[string]registeredTableRuntimeInfo{
			tableKey: {
				schemaName:    "audit",
				tableName:     "files",
				tableID:       1,
				syncKeyColumn: "id",
				syncKeyType:   syncKeyTypeUUID,
			},
		},
		columnTypesByTable: map[string]map[string]string{
			tableKey: {
				"id":   "uuid",
				"name": "text",
				"data": "bytea",
			},
		},
	}
}

func auditMicroPayload() json.RawMessage {
	encoded := base64.StdEncoding.EncodeToString(auditMicroBinaryBytes(256))
	return json.RawMessage(fmt.Sprintf(
		`{"id":%q,"name":"audit payload","data":%q,"nested":{"enabled":true,"ordinal":7}}`,
		auditMicroUUID,
		encoded,
	))
}

func BenchmarkAuditMicroPayloadPreparation(b *testing.B) {
	payload := auditMicroPayload()
	service := auditMicroPayloadService()
	row := PushRequestRow{
		Schema:         "audit",
		Table:          "files",
		Key:            SyncKey{"id": auditMicroUUID},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        payload,
	}

	b.Run("prepare_push_row", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for b.Loop() {
			prepared, err := service.preparePushRows([]PushRequestRow{row})
			if err != nil || len(prepared) != 1 || len(prepared[0].payload) == 0 {
				b.Fatalf("prepare push row: rows=%d err=%v", len(prepared), err)
			}
		}
	})

	b.Run("inject_owner", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for b.Loop() {
			ownerPayload, err := injectOwnerUserIDPayload(payload, "audit-owner")
			if err != nil || !bytes.Contains(ownerPayload, []byte(`"_sync_scope_id":"audit-owner"`)) {
				b.Fatalf("inject owner: len=%d err=%v", len(ownerPayload), err)
			}
		}
	})
}

func auditMicroCapturedEvents(eventCount int) []capturedBundleEvent {
	events := make([]capturedBundleEvent, eventCount)
	for i := range events {
		logicalRow := i / 2
		keyBytes := make([]byte, 16)
		binary.BigEndian.PutUint64(keyBytes[:8], uint64(logicalRow+1))
		binary.BigEndian.PutUint64(keyBytes[8:], uint64(logicalRow+101))
		opCode := opCodeInsert
		if i%2 == 1 {
			opCode = opCodeUpdate
		}
		events[i] = capturedBundleEvent{
			ordinal:  int64(i + 1),
			userPK:   7,
			tableID:  int32(logicalRow%4 + 1),
			opCode:   opCode,
			keyBytes: keyBytes,
			payload:  []byte(fmt.Sprintf(`{"id":%d,"value":"audit-%04d"}`, logicalRow, i)),
		}
	}
	return events
}

func BenchmarkAuditMicroCapturedEventNormalization(b *testing.B) {
	for _, eventCount := range []int{1, 100, 1000} {
		events := auditMicroCapturedEvents(eventCount)
		inputBytes := 0
		for _, event := range events {
			inputBytes += len(event.keyBytes) + len(event.payload)
		}

		b.Run(fmt.Sprintf("events_%d", eventCount), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(inputBytes))
			b.ResetTimer()
			for b.Loop() {
				normalized := normalizeCapturedBundleEvents(events)
				if len(normalized) != (eventCount+1)/2 {
					b.Fatalf("normalized rows=%d want=%d", len(normalized), (eventCount+1)/2)
				}
			}
			b.ReportMetric(float64(eventCount), "events/op")
		})
	}
}

func BenchmarkAuditMicroBundleHashRendering(b *testing.B) {
	digest := make([]byte, 32)
	for i := range digest {
		digest[i] = byte(i*7 + 3)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(digest)))
	b.ResetTimer()
	for b.Loop() {
		rendered := renderBundleHash(digest)
		if len(rendered) != 64 {
			b.Fatalf("rendered hash length=%d", len(rendered))
		}
	}
}
