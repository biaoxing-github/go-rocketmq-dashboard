package goadmin

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEncodeCommandUsesJSONRemotingFrame(t *testing.T) {
	frame, err := encodeCommand(remotingCommand{
		Code:     requestCodeGetAllTopicListFromNameServer,
		Language: "JAVA",
		Version:  0,
		Opaque:   7,
		Flag:     0,
	})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	if len(frame) < 12 {
		t.Fatalf("frame too short: %d", len(frame))
	}
	length := int(binary.BigEndian.Uint32(frame[0:4]))
	if length != len(frame)-4 {
		t.Fatalf("length mismatch: header length says %d, frame payload is %d", length, len(frame)-4)
	}
	headerMark := binary.BigEndian.Uint32(frame[4:8])
	if protocol := byte(headerMark >> 24); protocol != serializeTypeJSON {
		t.Fatalf("expected JSON protocol marker, got %d", protocol)
	}
	headerLength := int(headerMark & 0x00ffffff)
	if headerLength <= 0 || 8+headerLength > len(frame) {
		t.Fatalf("bad header length %d for frame %d", headerLength, len(frame))
	}
	var header remotingCommand
	if err := json.Unmarshal(frame[8:8+headerLength], &header); err != nil {
		t.Fatalf("decode header json: %v", err)
	}
	if header.Code != requestCodeGetAllTopicListFromNameServer || header.Opaque != 7 || header.Language != "JAVA" {
		t.Fatalf("unexpected header: %#v", header)
	}
}

func writeTopicListFileForTest(t *testing.T, content string) string {
	t.Helper()
	fileName := t.TempDir() + "/topics.json"
	if err := os.WriteFile(fileName, []byte(content), 0o600); err != nil {
		t.Fatalf("write topic list file: %v", err)
	}
	return fileName
}

func writeSubGroupListFileForTest(t *testing.T, content string) string {
	t.Helper()
	fileName := t.TempDir() + "/groups.json"
	if err := os.WriteFile(fileName, []byte(content), 0o600); err != nil {
		t.Fatalf("write subscription group list file: %v", err)
	}
	return fileName
}

func writeExportMetadataFixtureForTest(t *testing.T, configType string, rows map[string]string) string {
	t.Helper()
	parentPath := t.TempDir()
	dbPath := joinRocketMQPath(parentPath, configType)
	writeRocksDBFixtureForTest(t, dbPath, configType)
	return parentPath
}

func writeRocksDBConfigToJsonFixtureForTest(t *testing.T, configType string, rows map[string]string) string {
	t.Helper()
	parentPath := t.TempDir()
	dbPath := joinRocketMQPath(parentPath, rocksDBConfigLocalDir(configType))
	writeRocksDBFixtureForTest(t, dbPath, configType)
	return parentPath
}

func writeRocksDBFixtureForTest(t *testing.T, dbPath string, configType string) {
	t.Helper()
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("create rocksdb fixture dir: %v", err)
	}
	fixtures, ok := exportMetadataRocksDBFixture[configType]
	if !ok {
		t.Fatalf("unknown rocksdb fixture type %s", configType)
	}
	for fileName, encoded := range fixtures {
		payload, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("decode rocksdb fixture %s/%s: %v", configType, fileName, err)
		}
		if err := os.WriteFile(filepath.Join(dbPath, fileName), payload, 0o644); err != nil {
			t.Fatalf("write rocksdb fixture %s/%s: %v", configType, fileName, err)
		}
	}
}

// exportMetadataRocksDBFixture 保存官方 ConfigRocksDBStorage 生成的最小 RocksDB 样本。
var exportMetadataRocksDBFixture = map[string]map[string]string{
	"topics": {
		"CURRENT":         "TUFOSUZFU1QtMDAwMDE3Cg==",
		"MANIFEST-000017": "3mr/agQAAYhAAQxW+bj4HAABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcm4pPl1oAAECDAoMBAFnAA3mCQ5Ub3BpY0EBAQAAAAAAAA5Ub3BpY0EBAQAAAAAAAAEBBQXapMjRBgYF2qTI0QYNAQEHAAgHVW5rbm93bgMIDAAAAAAAAAAMEDus3bR0nlpvz4Tt6uL3c40PAo0JAe97hOkvAAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yyAEByQENa3ZEYXRhVmVyc2lvblIJVXEHAAECBAQByAEBGsN+JCsAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3LIAQLJAQlmb3JiaWRkZW6zI73LBwABAgQEAcgBAv/9qOEJAAEJAAMRywECBAHPn3ryCAABAg0JAAMRBAGXTdfzCAABCQADEQoNBAF7w+ZFCwABAg0JAAMRBAHIAQFTgsJ1CwABAg0JAAMSBAHIAQKUv80PYAABAhQJAAMWBAJnABXmCQ5Ub3BpY0IBAgAAAAAAAA5Ub3BpY0IBAgAAAAAAAAICBQXapMjRBgYF2qTI0QYNAQIHAAgHVW5rbm93bgwQOqzdtHSeWm/XhO3q4vdzjQ8CjQkBb7rZqggAAQkAAxYKFAQC",
		"000013.sst":      "AA47VG9waWNBAQEAAAAAAAB7InRvcGljTmFtZSI6IlRvcGljQSIsInJlYWRRdWV1ZU51bXMiOjQsIndyaXRlUXVldWVOdW1zIjo0fQAAAAABAAAAAP/2OdAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAEAABAAAAAAAIgAAABABAAAAAAAAAAAAAAAAAAAAAIAEA/wAJAAAAmgIv8AAGVG9waWNBAFQAAAAAAQAAAAAaKgVVACQEcm9ja3NkYi5ibG9jay5iYXNlZC50YWJsZS5pbmRleC50eXBlAAAAABoQAXByZWZpeC5maWx0ZXJpbmcwGhMBd2hvbGUua2V5LmZpbHRlcmluZzEIEAFjb2x1bW4uZmFtaWx5LmlkABYEB25hbWVkZWZhdWx0CggabXBhcmF0b3JsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgwHDXJlc3Npb25Ob0NvbXByZXNzaW9uEwiRAV9vcHRpb25zd2luZG93X2JpdHM9LTE0OyBsZXZlbD0zMjc2Nzsgc3RyYXRlZ3k9MDsgbWF4X2RpY3RfYnl0ZXM9MDsgenN0ZF9tYXhfdHJhaW5fYnl0ZXM9MDsgZW5hYmxlZD0wOyBtYXhfZGljdF9idWZmZXJfYnl0ZXM9MDsgdXNlX3pzdGRfZGljdF90cmFpbmVyPTE7IAkTJHJlYXRpbmcuZGIuaWRlbnRpdHk1MDM5MGI0MC04Y2VjLTQzN2YtOTM2ZS0yZTY4NzhiNzE1NzcRDQxob3N0LmlkZW50aXR5YWE5MzNhZTlhZDkwERAUc2Vzc2lvbi5pZGVudGl0eTBIQklVS0tUUFhBMFZITTJLRE9CDgcFb24udGltZdqkyNEGCAkBZGF0YS5zaXplWQkLAWVsZXRlZC5rZXlzAAgSBWZpbGUuY3JlYXRpb24udGltZdqkyNEGCwoLdGVyLnBvbGljeWJsb29tZmlsdGVyDwQBc2l6ZUUKDgF4ZWQua2V5Lmxlbmd0aAAJDQFvcm1hdC52ZXJzaW9uAAgVAWluZGV4LmtleS5pcy51c2VyLmtleQEOBAFzaXplFw4WAXZhbHVlLmlzLmRlbHRhLmVuY29kZWQBCA4BbWVyZ2Uub3BlcmFuZHMAEwMUdG9yU3RyaW5nQXBwZW5kT3BlcmF0b3IIDwFudW0uZGF0YS5ibG9ja3MBDAcBZW50cmllcwEMDgFmaWx0ZXJfZW50cmllcwEMDwFyYW5nZS1kZWxldGlvbnMACA8Fb2xkZXN0LmtleS50aW1l2qTI0QYJEwFyaWdpbmFsLmZpbGUubnVtYmVyDQgVB3ByZWZpeC5leHRyYWN0b3IubmFtZW51bGxwdHIKEQJvcGVydHkuY29sbGVjdG9yc1tdCAwBcmF3LmtleS5zaXplDgwKAXZhbHVlLnNpemU7CBEBdGFpbC5zdGFydC5vZmZzZXRZAAAAAAEAAAAAAi2tOAAlAmZ1bGxmaWx0ZXIucm9ja3NkYi5CdWlsdGluQmxvb21GaWx0ZXJZRQASBHJvY2tzZGIucHJvcGVydGllc7oBngcAAAAAKgAAAAIAAAAAsgOcEQHdCE+jARIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQAAAPfP9IW3QeKI",
		"000021.sst":      "AA47VG9waWNCAQIAAAAAAAB7InRvcGljTmFtZSI6IlRvcGljQiIsInJlYWRRdWV1ZU51bXMiOjgsIndyaXRlUXVldWVOdW1zIjo4fQAAAAABAAAAAMci4p4ABAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAQAAAACAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAgAAgAMAAA/wAJAAAAhstv3wAGVG9waWNCAFQAAAAAAQAAAAA+g6XsACQEcm9ja3NkYi5ibG9jay5iYXNlZC50YWJsZS5pbmRleC50eXBlAAAAABoQAXByZWZpeC5maWx0ZXJpbmcwGhMBd2hvbGUua2V5LmZpbHRlcmluZzEIEAFjb2x1bW4uZmFtaWx5LmlkABYEB25hbWVkZWZhdWx0CggabXBhcmF0b3JsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgwHDXJlc3Npb25Ob0NvbXByZXNzaW9uEwiRAV9vcHRpb25zd2luZG93X2JpdHM9LTE0OyBsZXZlbD0zMjc2Nzsgc3RyYXRlZ3k9MDsgbWF4X2RpY3RfYnl0ZXM9MDsgenN0ZF9tYXhfdHJhaW5fYnl0ZXM9MDsgZW5hYmxlZD0wOyBtYXhfZGljdF9idWZmZXJfYnl0ZXM9MDsgdXNlX3pzdGRfZGljdF90cmFpbmVyPTE7IAkTJHJlYXRpbmcuZGIuaWRlbnRpdHk1MDM5MGI0MC04Y2VjLTQzN2YtOTM2ZS0yZTY4NzhiNzE1NzcRDQxob3N0LmlkZW50aXR5YWE5MzNhZTlhZDkwERAUc2Vzc2lvbi5pZGVudGl0eTBIQklVS0tUUFhBMFZITTJLRE9BDgcFb24udGltZdqkyNEGCAkBZGF0YS5zaXplWQkLAWVsZXRlZC5rZXlzAAgSBWZpbGUuY3JlYXRpb24udGltZdqkyNEGCwoLdGVyLnBvbGljeWJsb29tZmlsdGVyDwQBc2l6ZUUKDgF4ZWQua2V5Lmxlbmd0aAAJDQFvcm1hdC52ZXJzaW9uAAgVAWluZGV4LmtleS5pcy51c2VyLmtleQEOBAFzaXplFw4WAXZhbHVlLmlzLmRlbHRhLmVuY29kZWQBCA4BbWVyZ2Uub3BlcmFuZHMAEwMUdG9yU3RyaW5nQXBwZW5kT3BlcmF0b3IIDwFudW0uZGF0YS5ibG9ja3MBDAcBZW50cmllcwEMDgFmaWx0ZXJfZW50cmllcwEMDwFyYW5nZS1kZWxldGlvbnMACA8Fb2xkZXN0LmtleS50aW1l2qTI0QYJEwFyaWdpbmFsLmZpbGUubnVtYmVyFQgVB3ByZWZpeC5leHRyYWN0b3IubmFtZW51bGxwdHIKEQJvcGVydHkuY29sbGVjdG9yc1tdCAwBcmF3LmtleS5zaXplDgwKAXZhbHVlLnNpemU7CBEBdGFpbC5zdGFydC5vZmZzZXRZAAAAAAEAAAAAoKkE+wAlAmZ1bGxmaWx0ZXIucm9ja3NkYi5CdWlsdGluQmxvb21GaWx0ZXJZRQASBHJvY2tzZGIucHJvcGVydGllc7oBngcAAAAAKgAAAAIAAAAAsgOcEQHdCE+jARIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQAAAPfP9IW3QeKI",
	},
	"subscriptionGroups": {
		"CURRENT":         "TUFOSUZFU1QtMDAwMDE3Cg==",
		"MANIFEST-000017": "3mr/agQAAYhAAQxW+bj4HAABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcuKUGPBoAAECDAoMBAFnAA3WCQ5Hcm91cEEBAQAAAAAAAA5Hcm91cEEBAQAAAAAAAAEBBQXapMjRBgYF2qTI0QYNAQEHAAgHVW5rbm93bgMIDAAAAAAAAAAMEDms3bR0nlpvnby7UsTO4SwPAo0JAe97hOkvAAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yyAEByQENa3ZEYXRhVmVyc2lvblIJVXEHAAECBAQByAEBGsN+JCsAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3LIAQLJAQlmb3JiaWRkZW6zI73LBwABAgQEAcgBAv/9qOEJAAEJAAMRywECBAHPn3ryCAABAg0JAAMRBAGXTdfzCAABCQADEQoNBAF7w+ZFCwABAg0JAAMRBAHIAQFTgsJ1CwABAg0JAAMSBAHIAQLFInDCYAABAhQJAAMWBAJnABXXCQ5Hcm91cEIBAgAAAAAAAA5Hcm91cEIBAgAAAAAAAAICBQXapMjRBgYF2qTI0QYNAQIHAAgHVW5rbm93bgwQOKzdtHSeWm+FvLtSxM7hLA8CjQkBb7rZqggAAQkAAxYKFAQC",
		"000013.sst":      "AA4rR3JvdXBBAQEAAAAAAAB7Imdyb3VwTmFtZSI6Ikdyb3VwQSIsImNvbnN1bWVFbmFibGUiOnRydWV9AAAAAAEAAAAAunGwQhAAAAIAAAAAAAAAAAAAAAAAAAAAgAAAAAEAAAAAAAAAAAiAAAAIAAAAAAAAAAAAAABAAAEAAAAAAAAAAAAAAAD/AAkAAACl7TWoAAZHcm91cEEARAAAAAABAAAAAEtH3p0AJARyb2Nrc2RiLmJsb2NrLmJhc2VkLnRhYmxlLmluZGV4LnR5cGUAAAAAGhABcHJlZml4LmZpbHRlcmluZzAaEwF3aG9sZS5rZXkuZmlsdGVyaW5nMQgQAWNvbHVtbi5mYW1pbHkuaWQAFgQHbmFtZWRlZmF1bHQKCBptcGFyYXRvcmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yDAcNcmVzc2lvbk5vQ29tcHJlc3Npb24TCJEBX29wdGlvbnN3aW5kb3dfYml0cz0tMTQ7IGxldmVsPTMyNzY3OyBzdHJhdGVneT0wOyBtYXhfZGljdF9ieXRlcz0wOyB6c3RkX21heF90cmFpbl9ieXRlcz0wOyBlbmFibGVkPTA7IG1heF9kaWN0X2J1ZmZlcl9ieXRlcz0wOyB1c2VfenN0ZF9kaWN0X3RyYWluZXI9MTsgCRMkcmVhdGluZy5kYi5pZGVudGl0eTc5OTUwOWFhLWIxY2YtNDFjZi04Y2Y0LTAyMTIxYWM1NWFiMhENDGhvc3QuaWRlbnRpdHlhYTkzM2FlOWFkOTAREBRzZXNzaW9uLmlkZW50aXR5MEhCSVVLS1RQWEEwVkhNMktETzkOBwVvbi50aW1l2qTI0QYICQFkYXRhLnNpemVJCQsBZWxldGVkLmtleXMACBIFZmlsZS5jcmVhdGlvbi50aW1l2qTI0QYLCgt0ZXIucG9saWN5Ymxvb21maWx0ZXIPBAFzaXplRQoOAXhlZC5rZXkubGVuZ3RoAAkNAW9ybWF0LnZlcnNpb24ACBUBaW5kZXgua2V5LmlzLnVzZXIua2V5AQ4EAXNpemUXDhYBdmFsdWUuaXMuZGVsdGEuZW5jb2RlZAEIDgFtZXJnZS5vcGVyYW5kcwATAxR0b3JTdHJpbmdBcHBlbmRPcGVyYXRvcggPAW51bS5kYXRhLmJsb2NrcwEMBwFlbnRyaWVzAQwOAWZpbHRlcl9lbnRyaWVzAQwPAXJhbmdlLWRlbGV0aW9ucwAIDwVvbGRlc3Qua2V5LnRpbWXapMjRBgkTAXJpZ2luYWwuZmlsZS5udW1iZXINCBUHcHJlZml4LmV4dHJhY3Rvci5uYW1lbnVsbHB0cgoRAm9wZXJ0eS5jb2xsZWN0b3JzW10IDAFyYXcua2V5LnNpemUODAoBdmFsdWUuc2l6ZSsIEQF0YWlsLnN0YXJ0Lm9mZnNldEkAAAAAAQAAAABtaaW/ACUCZnVsbGZpbHRlci5yb2Nrc2RiLkJ1aWx0aW5CbG9vbUZpbHRlcklFABIEcm9ja3NkYi5wcm9wZXJ0aWVzqgGeBwAAAAAqAAAAAgAAAABc7QvRAc0IT5MBEgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFAAAA98/0hbdB4og=",
		"000021.sst":      "AA4sR3JvdXBCAQIAAAAAAAB7Imdyb3VwTmFtZSI6Ikdyb3VwQiIsImNvbnN1bWVFbmFibGUiOmZhbHNlfQAAAAABAAAAAP8VIRQAAAAAAAAAAAAAAIAAAAAAAAAAAAABIAAAAAAAAAAAAAACAAAAAAAAAAAABAUAAAAABAAAAAAAAIAAAAAAAAAA/wAJAAAAvoob9QAGR3JvdXBCAEUAAAAAAQAAAADTWDOXACQEcm9ja3NkYi5ibG9jay5iYXNlZC50YWJsZS5pbmRleC50eXBlAAAAABoQAXByZWZpeC5maWx0ZXJpbmcwGhMBd2hvbGUua2V5LmZpbHRlcmluZzEIEAFjb2x1bW4uZmFtaWx5LmlkABYEB25hbWVkZWZhdWx0CggabXBhcmF0b3JsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgwHDXJlc3Npb25Ob0NvbXByZXNzaW9uEwiRAV9vcHRpb25zd2luZG93X2JpdHM9LTE0OyBsZXZlbD0zMjc2Nzsgc3RyYXRlZ3k9MDsgbWF4X2RpY3RfYnl0ZXM9MDsgenN0ZF9tYXhfdHJhaW5fYnl0ZXM9MDsgZW5hYmxlZD0wOyBtYXhfZGljdF9idWZmZXJfYnl0ZXM9MDsgdXNlX3pzdGRfZGljdF90cmFpbmVyPTE7IAkTJHJlYXRpbmcuZGIuaWRlbnRpdHk3OTk1MDlhYS1iMWNmLTQxY2YtOGNmNC0wMjEyMWFjNTVhYjIRDQxob3N0LmlkZW50aXR5YWE5MzNhZTlhZDkwERAUc2Vzc2lvbi5pZGVudGl0eTBIQklVS0tUUFhBMFZITTJLRE84DgcFb24udGltZdqkyNEGCAkBZGF0YS5zaXplSgkLAWVsZXRlZC5rZXlzAAgSBWZpbGUuY3JlYXRpb24udGltZdqkyNEGCwoLdGVyLnBvbGljeWJsb29tZmlsdGVyDwQBc2l6ZUUKDgF4ZWQua2V5Lmxlbmd0aAAJDQFvcm1hdC52ZXJzaW9uAAgVAWluZGV4LmtleS5pcy51c2VyLmtleQEOBAFzaXplFw4WAXZhbHVlLmlzLmRlbHRhLmVuY29kZWQBCA4BbWVyZ2Uub3BlcmFuZHMAEwMUdG9yU3RyaW5nQXBwZW5kT3BlcmF0b3IIDwFudW0uZGF0YS5ibG9ja3MBDAcBZW50cmllcwEMDgFmaWx0ZXJfZW50cmllcwEMDwFyYW5nZS1kZWxldGlvbnMACA8Fb2xkZXN0LmtleS50aW1l2qTI0QYJEwFyaWdpbmFsLmZpbGUubnVtYmVyFQgVB3ByZWZpeC5leHRyYWN0b3IubmFtZW51bGxwdHIKEQJvcGVydHkuY29sbGVjdG9yc1tdCAwBcmF3LmtleS5zaXplDgwKAXZhbHVlLnNpemUsCBEBdGFpbC5zdGFydC5vZmZzZXRKAAAAAAEAAAAAALTxuwAlAmZ1bGxmaWx0ZXIucm9ja3NkYi5CdWlsdGluQmxvb21GaWx0ZXJKRQASBHJvY2tzZGIucHJvcGVydGllc6sBngcAAAAAKgAAAAIAAAAA7WSypwHOCE+UARIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQAAAPfP9IW3QeKI",
	},
	"consumerOffsets": {
		"CURRENT":         "TUFOSUZFU1QtMDAwMDA1Cg==",
		"MANIFEST-000005": "BSsoQwAAAVb5uPgcAAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9y9AMMWgQAAQIABAD8LCb5BgABCQADBgQA8oVlcTUAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBAMGBADIAQHJAQ1rdkRhdGFWZXJzaW9uQ4mtbjEAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBAMIBADIAQLJAQlmb3JiaWRkZW48ydr8igABAgwJAAMOBAJnAA2tCiNHb2FkbWluVG9waWNBQEdvYWRtaW5Hcm91cEEBAQAAAAAAACNHb2FkbWluVG9waWNCQEdvYWRtaW5Hcm91cEIBAgAAAAAAAAECBQXx+cjRBgYF8fnI0QYNAQEHAAgHVW5rbm93bgwQ+Zj5IisHrTu7noayhfCuMg8CpgkB07XP/AgAAQkAAw4KDAQC",
		"000013.sst":      "ACMhR29hZG1pblRvcGljQUBHb2FkbWluR3JvdXBBAQEAAAAAAAB7Im9mZnNldFRhYmxlIjp7IjAiOjEyMywiMSI6NDU2fX0MFxlCQEdvYWRtaW5Hcm91cEIBAgAAAAAAAHsib2Zmc2V0VGFibGUiOnsiMCI6Nzg5fX0AAAAAAQAAAACR3MHHAAAAAAIIACAAAAAAAAENAAAAAQAAAIAQAAICAAAAAAAAAAAAAABAAAAAAAAAEAAAAAAAEEAAAAAAAAAgAgAAAP8ACQAAAAjg/uQAG0dvYWRtaW5Ub3BpY0JAR29hZG1pbkdyb3VwQgCCAQAAAAABAAAAAI1XQgcAJARyb2Nrc2RiLmJsb2NrLmJhc2VkLnRhYmxlLmluZGV4LnR5cGUAAAAAGhABcHJlZml4LmZpbHRlcmluZzAaEwF3aG9sZS5rZXkuZmlsdGVyaW5nMQgQAWNvbHVtbi5mYW1pbHkuaWQAFgQHbmFtZWRlZmF1bHQKCBptcGFyYXRvcmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yDAcNcmVzc2lvbk5vQ29tcHJlc3Npb24TCJEBX29wdGlvbnN3aW5kb3dfYml0cz0tMTQ7IGxldmVsPTMyNzY3OyBzdHJhdGVneT0wOyBtYXhfZGljdF9ieXRlcz0wOyB6c3RkX21heF90cmFpbl9ieXRlcz0wOyBlbmFibGVkPTA7IG1heF9kaWN0X2J1ZmZlcl9ieXRlcz0wOyB1c2VfenN0ZF9kaWN0X3RyYWluZXI9MTsgCRMkcmVhdGluZy5kYi5pZGVudGl0eWViNjVkNGUyLWEzMGUtNDU5NC04MWUxLTE4ZWE1ZjQ4ODIyMRENDGhvc3QuaWRlbnRpdHlhYTkzM2FlOWFkOTAREBRzZXNzaW9uLmlkZW50aXR5NEpJN1pNMzBXTzRJTUVLUjNFVVgOBwVvbi50aW1l8fnI0QYICQJkYXRhLnNpemWHAQkLAWVsZXRlZC5rZXlzAAgSBWZpbGUuY3JlYXRpb24udGltZfH5yNEGCwoLdGVyLnBvbGljeWJsb29tZmlsdGVyDwQBc2l6ZUUKDgF4ZWQua2V5Lmxlbmd0aAAJDQFvcm1hdC52ZXJzaW9uAAgVAWluZGV4LmtleS5pcy51c2VyLmtleQEOBAFzaXplLQ4WAXZhbHVlLmlzLmRlbHRhLmVuY29kZWQBCA4BbWVyZ2Uub3BlcmFuZHMAEwMUdG9yU3RyaW5nQXBwZW5kT3BlcmF0b3IIDwFudW0uZGF0YS5ibG9ja3MBDAcBZW50cmllcwIMDgFmaWx0ZXJfZW50cmllcwIMDwFyYW5nZS1kZWxldGlvbnMACA8Fb2xkZXN0LmtleS50aW1l8fnI0QYJEwFyaWdpbmFsLmZpbGUubnVtYmVyDQgVB3ByZWZpeC5leHRyYWN0b3IubmFtZW51bGxwdHIKEQJvcGVydHkuY29sbGVjdG9yc1tdCAwBcmF3LmtleS5zaXplRgwKAXZhbHVlLnNpemU6CBECdGFpbC5zdGFydC5vZmZzZXSHAQAAAAABAAAAAB/wf6AAJQNmdWxsZmlsdGVyLnJvY2tzZGIuQnVpbHRpbkJsb29tRmlsdGVyhwFFABIEcm9ja3NkYi5wcm9wZXJ0aWVz/gGgBwAAAAArAAAAAgAAAAAfCK+BAaMJUNEBKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFAAAA98/0hbdB4og=",
	},
}

func TestDecodeCommandSeparatesHeaderAndBody(t *testing.T) {
	body := []byte(`{"topicList":["TopicB","TopicA"],"brokerAddr":null}`)
	frame := remotingFrameForTest(t, remotingCommand{
		Code:     responseCodeSuccess,
		Language: "JAVA",
		Version:  0,
		Opaque:   7,
		Flag:     1,
	}, body)

	command, err := decodeCommand(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode command: %v", err)
	}
	if command.Code != responseCodeSuccess || command.Opaque != 7 {
		t.Fatalf("unexpected command header: %#v", command)
	}
	if string(command.Body) != string(body) {
		t.Fatalf("body mismatch\nexpected=%s\nactual=%s", body, command.Body)
	}
}

func TestDecodeCommandSupportsRocketMQRemotingHeader(t *testing.T) {
	body := []byte(`{"properties":{"consumerGroup":"GoadminConsumerStatusM575"}}`)
	frame := rocketMQRemotingFrameForTest(t, remotingCommand{
		Code:     responseCodeSuccess,
		Language: "JAVA",
		Version:  477,
		Opaque:   17,
		Flag:     1,
		Remark:   "OK",
		ExtFields: map[string]string{
			"consumerGroup": "GoadminConsumerStatusM575",
			"clientId":      "172.24.0.3@1781658757829",
		},
	}, body)

	command, err := decodeCommand(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode rocketmq command: %v", err)
	}
	if command.Code != responseCodeSuccess || command.Language != "JAVA" || command.Version != 477 || command.Opaque != 17 || command.Flag != 1 || command.Remark != "OK" {
		t.Fatalf("unexpected command header: %#v", command)
	}
	if command.ExtFields["consumerGroup"] != "GoadminConsumerStatusM575" || command.ExtFields["clientId"] != "172.24.0.3@1781658757829" {
		t.Fatalf("unexpected ext fields: %#v", command.ExtFields)
	}
	if string(command.Body) != string(body) {
		t.Fatalf("body mismatch\nexpected=%s\nactual=%s", body, command.Body)
	}
}

func TestDecodeTopicListBodyMatchesOfficialHashSetOrder(t *testing.T) {
	body := []byte(`{"topicList":["OFFSET_MOVED_EVENT","DefaultCluster","%RETRY%TOOLS_CONSUMER","55924048bd08"]}`)

	topics, err := decodeTopicListBody(body)
	if err != nil {
		t.Fatalf("decode topicList body: %v", err)
	}

	expected := []string{"OFFSET_MOVED_EVENT", "55924048bd08", "%RETRY%TOOLS_CONSUMER", "DefaultCluster"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("topicList order mismatch\nexpected=%v\nactual=%v", expected, topics)
	}
}

func TestRunNativeTopicListFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		topicList: func(ctx context.Context, nameServer string) ([]string, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			return []string{"TopicB", "TopicA"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"topicList", "-n", "127.0.0.1:9876"}, client)
	if err != nil {
		t.Fatalf("run native topicList: %v", err)
	}
	if !supported {
		t.Fatalf("expected topicList to be supported")
	}
	if output != "TopicB\nTopicA\n" {
		t.Fatalf("unexpected output %q", output)
	}
}

func TestRunNativeTopicListFormatsOfficialClusterModel(t *testing.T) {
	client := nativeClientFunc{
		topicListCluster: func(ctx context.Context, nameServer string) ([]topicClusterRow, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			return []topicClusterRow{
				{ClusterName: "DefaultCluster", Topic: "TopicA", ConsumerGroup: "GroupA"},
				{ClusterName: "DefaultCluster", Topic: "TopicB", ConsumerGroup: ""},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"topicList", "-n", "127.0.0.1:9876", "-c"}, client)
	if err != nil {
		t.Fatalf("run native topicList -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected topicList -c to be supported")
	}
	expected := fmt.Sprintf("%-20s  %-48s  %-48s\n", "#Cluster Name", "#Topic", "#Consumer Group") +
		fmt.Sprintf("%-20s  %-64s  %-64s\n", "DefaultCluster", "TopicA", "GroupA") +
		fmt.Sprintf("%-20s  %-64s  %-64s\n", "DefaultCluster", "TopicB", "")
	if output != expected {
		t.Fatalf("topicList -c output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeAllocateMQFormatsOfficialJSON(t *testing.T) {
	client := nativeClientFunc{
		allocateMQ: func(ctx context.Context, nameServer string, topic string, ipList string) ([]allocateMQAssignment, error) {
			if nameServer != "127.0.0.1:9876" || topic != "TopicTest" || ipList != "172.24.0.2,172.24.0.3" {
				t.Fatalf("unexpected allocateMQ args namesrv=%s topic=%s ipList=%s", nameServer, topic, ipList)
			}
			return []allocateMQAssignment{
				{IP: "172.24.0.2", Queues: []messageQueueIdentity{
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1},
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 2},
				}},
				{IP: "172.24.0.3", Queues: []messageQueueIdentity{
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0},
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 3},
				}},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"allocateMQ", "-n", "127.0.0.1:9876", "-t", "TopicTest", "-i", "172.24.0.2,172.24.0.3"}, client)
	if err != nil {
		t.Fatalf("run native allocateMQ: %v", err)
	}
	if !supported {
		t.Fatalf("expected allocateMQ to be supported")
	}
	expected := `{"result":{"172.24.0.2":[{"brokerName":"broker-a","queueId":1,"topic":"TopicTest"},{"brokerName":"broker-a","queueId":2,"topic":"TopicTest"}],"172.24.0.3":[{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"},{"brokerName":"broker-a","queueId":3,"topic":"TopicTest"}]}}` + "\n"
	if output != expected {
		t.Fatalf("allocateMQ output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativePrintMsgByQueuePrintsOfficialMessageExt(t *testing.T) {
	client := nativeClientFunc{
		printMessagesByQueue: func(ctx context.Context, nameServer string, options printMsgByQueueOptions) (*printMsgByQueueResult, error) {
			expected := printMsgByQueueOptions{
				Topic:             "TopicTest",
				BrokerName:        "broker-a",
				QueueID:           0,
				HasBeginTimestamp: true,
				BeginTimestamp:    10,
				HasEndTimestamp:   true,
				EndTimestamp:      20,
				PrintMessage:      true,
				PrintBody:         false,
				CharsetName:       "UTF-8",
				SubExpression:     "TagA || TagB",
				CalculateByTag:    true,
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected printMsgByQueue args namesrv=%s options=%#v", nameServer, options)
			}
			return &printMsgByQueueResult{Messages: []messageDetail{printMsgByQueueDetailForTest()}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"printMsgByQueue",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-a", "broker-a",
		"-i", "0",
		"-b", "10",
		"-e", "20",
		"-p", "true",
		"-d", "false",
		"-c", "UTF-8",
		"-s", "TagA || TagB",
		"-f", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native printMsgByQueue: %v", err)
	}
	if !supported {
		t.Fatalf("expected printMsgByQueue to be supported")
	}
	expected := "MSGID: UNIQ-1 MessageExt [brokerName=broker-a, queueId=0, storeSize=128, queueOffset=7, sysFlag=0, bornTimestamp=1780891116911, bornHost=/172.24.0.4:48298, storeTimestamp=1780891116954, storeHost=/172.24.0.3:10911, msgId=AC18000300002A9F00000000000003E8, commitLogOffset=1000, bodyCRC=131628133, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MSG_REGION=DefaultRegion, UNIQ_KEY=UNIQ-1, CLUSTER=DefaultCluster, MIN_OFFSET=0, TAGS=TagA, KEYS=OrderKey, WAIT=true, TRACE_ON=true, MAX_OFFSET=8}, body=[104, 101, 108, 108, 111], transactionId='null'}] BODY: NOT PRINT BODY\n" +
		"Tag: TagA                           Count: 1\n"
	if output != expected {
		t.Fatalf("printMsgByQueue output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeDumpCompactionLogPrintsMissingFileName(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{"dumpCompactionLog"}, nil)
	if err != nil {
		t.Fatalf("dumpCompactionLog without -f: %v", err)
	}
	if !supported {
		t.Fatalf("expected dumpCompactionLog without -f to be supported")
	}
	if output != "miss dump log file name\n" {
		t.Fatalf("unexpected missing file output %q", output)
	}
}

func TestRunNativeDumpCompactionLogFormatsOfficialMessageExt(t *testing.T) {
	filePath := t.TempDir() + "/compaction.log"
	record := queryMessageRecordForTest(t, queryMessageRecordFixture{
		Topic:           "TopicTest",
		Keys:            "OrderKey",
		UniqKey:         "UNIQ-1",
		QueueID:         0,
		QueueOffset:     7,
		CommitLogOffset: 1000,
		Body:            []byte("hello"),
		BodyCRC:         131628133,
		BornTimestamp:   1780891116911,
		StoreTimestamp:  1780891116954,
		BornHostIP:      []byte{172, 24, 0, 4},
		BornHostPort:    48298,
		StoreHostIP:     []byte{172, 24, 0, 3},
		StoreHostPort:   10911,
	})
	if err := os.WriteFile(filePath, record, 0o644); err != nil {
		t.Fatalf("write compaction log fixture: %v", err)
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"dumpCompactionLog", "-f", filePath}, nil)
	if err != nil {
		t.Fatalf("dumpCompactionLog fixture: %v", err)
	}
	if !supported {
		t.Fatalf("expected dumpCompactionLog to be supported")
	}
	expected := "MessageExt [brokerName=null, queueId=0, storeSize=135, queueOffset=7, sysFlag=0, bornTimestamp=1780891116911, bornHost=/172.24.0.4:48298, storeTimestamp=1780891116954, storeHost=/172.24.0.3:10911, msgId=AC18000300002A9F00000000000003E8, commitLogOffset=1000, bodyCRC=131628133, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={UNIQ_KEY=UNIQ-1, KEYS=OrderKey}, body=null, transactionId='null'}]\n"
	if output != expected {
		t.Fatalf("dumpCompactionLog output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativePrintMsgPrintsOfficialQueues(t *testing.T) {
	client := nativeClientFunc{
		printMessages: func(ctx context.Context, nameServer string, options printMsgOptions) (*printMsgResult, error) {
			expected := printMsgOptions{
				Topic:             "TopicTest",
				LMQParentTopic:    "ParentTopic",
				HasBeginTimestamp: true,
				BeginTimestamp:    10,
				HasEndTimestamp:   true,
				EndTimestamp:      20,
				HasPrintBody:      true,
				PrintBody:         false,
				CharsetName:       "UTF-8",
				SubExpression:     "TagA || TagB",
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected printMsg args namesrv=%s options=%#v", nameServer, options)
			}
			return &printMsgResult{Queues: []printMsgQueueResult{{
				Queue:     messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0},
				MinOffset: 0,
				MaxOffset: 1,
				Messages:  []messageDetail{printMsgByQueueDetailForTest()},
			}}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"printMsg",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-l", "ParentTopic",
		"-b", "10",
		"-e", "20",
		"-d", "false",
		"-c", "UTF-8",
		"-s", "TagA || TagB",
	}, client)
	if err != nil {
		t.Fatalf("run native printMsg: %v", err)
	}
	if !supported {
		t.Fatalf("expected printMsg to be supported")
	}
	expected := "minOffset=0, maxOffset=1, MessageQueue [topic=TopicTest, brokerName=broker-a, queueId=0]\n" +
		"MSGID: UNIQ-1 MessageExt [brokerName=broker-a, queueId=0, storeSize=128, queueOffset=7, sysFlag=0, bornTimestamp=1780891116911, bornHost=/172.24.0.4:48298, storeTimestamp=1780891116954, storeHost=/172.24.0.3:10911, msgId=AC18000300002A9F00000000000003E8, commitLogOffset=1000, bodyCRC=131628133, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MSG_REGION=DefaultRegion, UNIQ_KEY=UNIQ-1, CLUSTER=DefaultCluster, MIN_OFFSET=0, TAGS=TagA, KEYS=OrderKey, WAIT=true, TRACE_ON=true, MAX_OFFSET=8}, body=[104, 101, 108, 108, 111], transactionId='null'}] BODY: NOT PRINT BODY\n" +
		"--------------------------------------------------------\n"
	if output != expected {
		t.Fatalf("printMsg output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestFormatPrintMsgDefaultsToPrintingBody(t *testing.T) {
	output, err := formatPrintMsg(&printMsgResult{Queues: []printMsgQueueResult{{
		Queue:     messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0},
		MinOffset: 0,
		MaxOffset: 1,
		Messages:  []messageDetail{printMsgByQueueDetailForTest()},
	}}}, printMsgOptions{CharsetName: "UTF-8"})
	if err != nil {
		t.Fatalf("format printMsg: %v", err)
	}
	if !strings.Contains(output, "BODY: hello\n") {
		t.Fatalf("expected printMsg default body output, got:\n%s", output)
	}
	if !strings.HasSuffix(output, "--------------------------------------------------------\n") {
		t.Fatalf("expected queue separator, got:\n%s", output)
	}
}

func TestRunNativeConsumeMessageByOffsetPrintsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		consumeMessages: func(ctx context.Context, nameServer string, options consumeMessageOptions) (*consumeMessageResult, error) {
			expected := consumeMessageOptions{
				Topic:             "TopicTest",
				BrokerName:        "broker-a",
				QueueID:           0,
				HasQueueID:        true,
				Offset:            7,
				HasOffset:         true,
				ConsumerGroup:     "GroupA",
				MessageCount:      1,
				HasBeginTimestamp: true,
				BeginTimestamp:    10,
				HasEndTimestamp:   true,
				EndTimestamp:      20,
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected consumeMessage args namesrv=%s options=%#v", nameServer, options)
			}
			return &consumeMessageResult{Notices: []string{"The oldler 1 message will be provided"}, Messages: []messageDetail{printMsgByQueueDetailForTest()}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"consumeMessage",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-b", "broker-a",
		"-i", "0",
		"-o", "7",
		"-c", "1",
		"-g", "GroupA",
		"-s", "10",
		"-e", "20",
	}, client)
	if err != nil {
		t.Fatalf("run native consumeMessage: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumeMessage to be supported")
	}
	expected := "The oldler 1 message will be provided\n" +
		"Consume ok\n" +
		"MSGID: UNIQ-1 MessageExt [brokerName=broker-a, queueId=0, storeSize=128, queueOffset=7, sysFlag=0, bornTimestamp=1780891116911, bornHost=/172.24.0.4:48298, storeTimestamp=1780891116954, storeHost=/172.24.0.3:10911, msgId=AC18000300002A9F00000000000003E8, commitLogOffset=1000, bodyCRC=131628133, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MSG_REGION=DefaultRegion, UNIQ_KEY=UNIQ-1, CLUSTER=DefaultCluster, MIN_OFFSET=0, TAGS=TagA, KEYS=OrderKey, WAIT=true, TRACE_ON=true, MAX_OFFSET=8}, body=[104, 101, 108, 108, 111], transactionId='null'}] BODY: hello\n"
	if output != expected {
		t.Fatalf("consumeMessage output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestFormatConsumeMessagePrintsOfficialMessages(t *testing.T) {
	detail := printMsgByQueueDetailForTest()
	output, err := formatConsumeMessage(&consumeMessageResult{Messages: []messageDetail{detail}})
	if err != nil {
		t.Fatalf("format consumeMessage: %v", err)
	}
	expected := "Consume ok\n" +
		"MSGID: UNIQ-1 MessageExt [brokerName=broker-a, queueId=0, storeSize=128, queueOffset=7, sysFlag=0, bornTimestamp=1780891116911, bornHost=/172.24.0.4:48298, storeTimestamp=1780891116954, storeHost=/172.24.0.3:10911, msgId=AC18000300002A9F00000000000003E8, commitLogOffset=1000, bodyCRC=131628133, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MSG_REGION=DefaultRegion, UNIQ_KEY=UNIQ-1, CLUSTER=DefaultCluster, MIN_OFFSET=0, TAGS=TagA, KEYS=OrderKey, WAIT=true, TRACE_ON=true, MAX_OFFSET=8}, body=[104, 101, 108, 108, 111], transactionId='null'}] BODY: hello\n"
	if output != expected {
		t.Fatalf("consumeMessage format mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestFormatPrintMsgByQueuePrintsBodyAndTagCounts(t *testing.T) {
	detail := printMsgByQueueDetailForTest()
	output, err := formatPrintMsgByQueue(&printMsgByQueueResult{Messages: []messageDetail{detail}}, printMsgByQueueOptions{
		PrintMessage:   true,
		PrintBody:      true,
		CharsetName:    "UTF-8",
		CalculateByTag: true,
	})
	if err != nil {
		t.Fatalf("format printMsgByQueue: %v", err)
	}
	if !strings.Contains(output, "BODY: hello\n") {
		t.Fatalf("expected printed UTF-8 body, output:\n%s", output)
	}
	if !strings.Contains(output, "Tag: TagA                           Count: 1\n") {
		t.Fatalf("expected tag count, output:\n%s", output)
	}
}

func TestCalculatePrintMsgByQueueTagCountsPreservesOfficialTagText(t *testing.T) {
	counts := calculatePrintMsgByQueueTagCounts([]messageDetail{
		{Tags: " TagA "},
		{Tags: "TagB"},
		{Tags: " TagA "},
		{Tags: "   "},
	})
	expected := []printMsgByQueueTagCount{
		{Tag: " TagA ", Count: 2},
		{Tag: "TagB", Count: 1},
	}
	if !reflect.DeepEqual(counts, expected) {
		t.Fatalf("tag counts mismatch\nexpected=%#v\nactual=%#v", expected, counts)
	}
}

func TestAllocateMQUsesJavaHashSetMessageQueueOrder(t *testing.T) {
	route := []byte(`{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"55924048bd08","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"55924048bd08","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}]}`)
	assignments, err := allocateMQAssignmentsFromRoute("GoadminQueryKeyTest", "172.24.0.2,172.24.0.3", route)
	if err != nil {
		t.Fatalf("allocate from route: %v", err)
	}
	output := formatAllocateMQ(assignments)
	expected := `{"result":{"172.24.0.2":[{"brokerName":"55924048bd08","queueId":1,"topic":"GoadminQueryKeyTest"},{"brokerName":"55924048bd08","queueId":2,"topic":"GoadminQueryKeyTest"}],"172.24.0.3":[{"brokerName":"55924048bd08","queueId":0,"topic":"GoadminQueryKeyTest"},{"brokerName":"55924048bd08","queueId":3,"topic":"GoadminQueryKeyTest"}]}}` + "\n"
	if output != expected {
		t.Fatalf("allocateMQ Java HashSet order mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeClusterListFormatsOfficialBaseInfo(t *testing.T) {
	client := nativeClientFunc{
		clusterList: func(ctx context.Context, nameServer string, clusterName string) ([]clusterListRow, error) {
			if nameServer != "127.0.0.1:9876" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected clusterList args namesrv=%s cluster=%s", nameServer, clusterName)
			}
			return []clusterListRow{{
				ClusterName:                   "DefaultCluster",
				BrokerName:                    "broker-a",
				BrokerID:                      "0",
				Addr:                          "127.0.0.1:10911",
				Version:                       "V5_3_2",
				InTPS:                         1.25,
				SendThreadPoolQueueSize:       "2",
				SendThreadPoolQueueHeadWaitMS: "3",
				OutTPS:                        4.5,
				PullThreadPoolQueueSize:       "6",
				PullThreadPoolQueueHeadWaitMS: "7",
				AckThreadPoolQueueSize:        "N",
				AckThreadPoolQueueHeadWaitMS:  "N",
				TimerReadBehind:               8,
				TimerOffsetBehind:             9,
				TimerCongestNum:               12000,
				TimerEnqueueTPS:               1.2,
				TimerDequeueTPS:               3.4,
				PageCacheLockTimeMS:           "11",
				Hour:                          2,
				CommitLogDiskRatio:            0.03125,
				BrokerActive:                  true,
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"clusterList", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"}, client)
	if err != nil {
		t.Fatalf("run native clusterList: %v", err)
	}
	if !supported {
		t.Fatalf("expected clusterList to be supported")
	}
	expected := fmt.Sprintf("%-22s  %-22s  %-4s  %-22s %-16s  %16s  %30s  %-22s  %-11s  %-12s  %-8s  %-10s\n",
		"#Cluster Name", "#Broker Name", "#BID", "#Addr", "#Version", "#InTPS(LOAD)", "#OutTPS(LOAD)", "#Timer(Progress)", "#PCWait(ms)", "#Hour", "#SPACE", "#ACTIVATED") +
		fmt.Sprintf("%-22s  %-22s  %-4s  %-22s %-16s  %16s  %30s  %-22s  %11s  %-12s  %-8s  %10s\n",
			"DefaultCluster",
			"broker-a",
			"0",
			"127.0.0.1:10911",
			"V5_3_2",
			"     1.25(2,3ms)",
			"     4.50(6,7ms|N,Nms)",
			"8-9(1.2w, 1.2, 3.4)",
			"11",
			"2.00",
			"0.0312",
			strconv.FormatBool(true))
	if output != expected {
		t.Fatalf("clusterList output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeClusterListFormatsOfficialMoreStats(t *testing.T) {
	client := nativeClientFunc{
		clusterListMoreStats: func(ctx context.Context, nameServer string, clusterName string) ([]clusterListMoreStatsRow, error) {
			if nameServer != "127.0.0.1:9876" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected clusterList -m args namesrv=%s cluster=%s", nameServer, clusterName)
			}
			return []clusterListMoreStatsRow{{
				ClusterName:   "DefaultCluster",
				BrokerName:    "broker-a",
				InTotalYest:   10,
				OutTotalYest:  20,
				InTotalToday:  30,
				OutTotalToday: 40,
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"clusterList", "-n", "127.0.0.1:9876", "-m", "-c", "DefaultCluster"}, client)
	if err != nil {
		t.Fatalf("run native clusterList -m: %v", err)
	}
	if !supported {
		t.Fatalf("expected clusterList -m to be supported")
	}
	expected := fmt.Sprintf("%-16s  %-32s %14s %14s %14s %14s\n",
		"#Cluster Name", "#Broker Name", "#InTotalYest", "#OutTotalYest", "#InTotalToday", "#OutTotalToday") +
		fmt.Sprintf("%-16s  %-32s %14d %14d %14d %14d\n",
			"DefaultCluster", "broker-a", int64(10), int64(20), int64(30), int64(40))
	if output != expected {
		t.Fatalf("clusterList -m output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeClusterListFallsBackForContinuousMode(t *testing.T) {
	for _, args := range [][]string{
		{"clusterList", "-n", "127.0.0.1:9876", "-i", "1"},
	} {
		output, supported, err := runNativeCommand(context.Background(), args, nil)
		if err != nil || supported || output != "" {
			t.Fatalf("expected fallback for args=%v, supported=%t output=%q err=%v", args, supported, output, err)
		}
	}
}

func TestRunNativeClusterAclConfigVersionFallsBackForClusterName(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{
		"clusterAclConfigVersion",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
	}, nativeClientFunc{})
	if err != nil || supported || output != "" {
		t.Fatalf("expected fallback for cluster branch, supported=%t output=%q err=%v", supported, output, err)
	}
}

func TestRunNativeClusterAclConfigVersionFallsBackForBrokerAddr(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{
		"clusterAclConfigVersion",
		"-b", "127.0.0.1:10911",
	}, nil)
	if err != nil || supported || output != "" {
		t.Fatalf("expected fallback for broker branch, supported=%t output=%q err=%v", supported, output, err)
	}
}

func TestRunNativeSetCommitLogReadAheadModeBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		setCommitLogReadAheadMode: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, mode string) ([]commitLogReadAheadModeSection, error) {
			if nameServer != "" || brokerAddr != "127.0.0.1:10911" || clusterName != "" || mode != "1" {
				t.Fatalf("unexpected setCommitLogReadAheadMode args namesrv=%s broker=%s cluster=%s mode=%s", nameServer, brokerAddr, clusterName, mode)
			}
			return []commitLogReadAheadModeSection{{
				Header: "============127.0.0.1:10911============",
				Result: "OK",
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"setCommitLogReadAheadMode", "-b", "127.0.0.1:10911", "-m", "1"}, client)
	if err != nil {
		t.Fatalf("run native setCommitLogReadAheadMode -b: %v", err)
	}
	if !supported {
		t.Fatalf("expected setCommitLogReadAheadMode -b to be supported")
	}
	expected := " ============127.0.0.1:10911============\n" +
		"commitLog set readAhead mode rstStrOK\n"
	if output != expected {
		t.Fatalf("setCommitLogReadAheadMode -b output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeSetCommitLogReadAheadModeClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		setCommitLogReadAheadMode: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, mode string) ([]commitLogReadAheadModeSection, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" || mode != "0" {
				t.Fatalf("unexpected setCommitLogReadAheadMode cluster args namesrv=%s broker=%s cluster=%s mode=%s", nameServer, brokerAddr, clusterName, mode)
			}
			return []commitLogReadAheadModeSection{
				{
					Header: "============Master: 10.0.0.1:10911============",
					Result: "MASTER",
				},
				{
					Header: "============My Master: 10.0.0.1:10911=====Slave: 10.0.0.2:10911============",
					Result: "SLAVE",
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"setCommitLogReadAheadMode", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-m", "0"}, client)
	if err != nil {
		t.Fatalf("run native setCommitLogReadAheadMode -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected setCommitLogReadAheadMode -c to be supported")
	}
	expected := " ============Master: 10.0.0.1:10911============\n" +
		"commitLog set readAhead mode rstStrMASTER\n" +
		" ============My Master: 10.0.0.1:10911=====Slave: 10.0.0.2:10911============\n" +
		"commitLog set readAhead mode rstStrSLAVE\n"
	if output != expected {
		t.Fatalf("setCommitLogReadAheadMode -c output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeSetCommitLogReadAheadModeRejectsInvalidModeLikeOfficial(t *testing.T) {
	client := nativeClientFunc{
		setCommitLogReadAheadMode: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, mode string) ([]commitLogReadAheadModeSection, error) {
			t.Fatalf("invalid mode should not call broker")
			return nil, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"setCommitLogReadAheadMode", "-b", "127.0.0.1:10911", "-m", "9"}, client)
	if err != nil {
		t.Fatalf("run native invalid setCommitLogReadAheadMode: %v", err)
	}
	if !supported {
		t.Fatalf("expected invalid setCommitLogReadAheadMode mode to be handled natively")
	}
	expected := "set the read mode error; 0 is default, 1 random read\n"
	if output != expected {
		t.Fatalf("invalid mode output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestBuildClusterListMoreStatsRowUsesOfficialCounterDeltas(t *testing.T) {
	row := buildClusterListMoreStatsRow("DefaultCluster", "broker-a", map[string]string{
		"msgPutTotalYesterdayMorning": "100",
		"msgPutTotalTodayMorning":     "160",
		"msgPutTotalTodayNow":         "230",
		"msgGetTotalYesterdayMorning": "20",
		"msgGetTotalTodayMorning":     "65",
		"msgGetTotalTodayNow":         "90",
	})

	expected := clusterListMoreStatsRow{
		ClusterName:   "DefaultCluster",
		BrokerName:    "broker-a",
		InTotalYest:   60,
		OutTotalYest:  45,
		InTotalToday:  70,
		OutTotalToday: 25,
	}
	if row != expected {
		t.Fatalf("more stats row mismatch\nexpected=%#v\nactual=%#v", expected, row)
	}
}

func TestRunNativeBrokerStatusFormatsOfficialBrokerAddress(t *testing.T) {
	client := nativeClientFunc{
		brokerStatus: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerStatusTable, error) {
			if nameServer != "" || brokerAddr != "127.0.0.1:10911" || clusterName != "" {
				t.Fatalf("unexpected brokerStatus args namesrv=%s broker=%s cluster=%s", nameServer, brokerAddr, clusterName)
			}
			return []brokerStatusTable{{Stats: map[string]string{
				"putTps":                  "1.0 2.0 3.0",
				"brokerVersionDesc":       "V5_3_2",
				"EndTransactionQueueSize": "0",
			}}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"brokerStatus", "-b", "127.0.0.1:10911"}, client)
	if err != nil {
		t.Fatalf("run native brokerStatus -b: %v", err)
	}
	if !supported {
		t.Fatalf("expected brokerStatus -b to be supported")
	}
	expected := fmt.Sprintf("%-32s: %s\n", "EndTransactionQueueSize", "0") +
		fmt.Sprintf("%-32s: %s\n", "brokerVersionDesc", "V5_3_2") +
		fmt.Sprintf("%-32s: %s\n", "putTps", "1.0 2.0 3.0")
	if output != expected {
		t.Fatalf("brokerStatus -b output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeBrokerStatusFormatsOfficialClusterRows(t *testing.T) {
	client := nativeClientFunc{
		brokerStatus: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerStatusTable, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected brokerStatus args namesrv=%s broker=%s cluster=%s", nameServer, brokerAddr, clusterName)
			}
			return []brokerStatusTable{{BrokerAddr: "10.0.0.1:10911", Stats: map[string]string{
				"putTps":            "1.0 2.0 3.0",
				"brokerVersionDesc": "V5_3_2",
			}}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"brokerStatus", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"}, client)
	if err != nil {
		t.Fatalf("run native brokerStatus -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected brokerStatus -c to be supported")
	}
	expected := fmt.Sprintf("%-24s %-32s: %s\n", "10.0.0.1:10911", "brokerVersionDesc", "V5_3_2") +
		fmt.Sprintf("%-24s %-32s: %s\n", "10.0.0.1:10911", "putTps", "1.0 2.0 3.0")
	if output != expected {
		t.Fatalf("brokerStatus -c output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeGetBrokerConfigFormatsOfficialBrokerAddress(t *testing.T) {
	client := nativeClientFunc{
		getBrokerConfig: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerConfigSection, error) {
			if nameServer != "" || brokerAddr != "broker-a:10911" || clusterName != "" {
				t.Fatalf("unexpected getBrokerConfig args namesrv=%s broker=%s cluster=%s", nameServer, brokerAddr, clusterName)
			}
			return []brokerConfigSection{{
				Header: "============broker-a:10911============",
				Entries: []brokerConfigEntry{
					{Key: "brokerName", Value: "broker-a"},
					{Key: "brokerClusterName", Value: "DefaultCluster"},
				},
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"getBrokerConfig", "-b", "broker-a:10911"}, client)
	if err != nil {
		t.Fatalf("run native getBrokerConfig -b: %v", err)
	}
	if !supported {
		t.Fatalf("expected getBrokerConfig -b to be supported")
	}
	expected := "============broker-a:10911============\n" +
		fmt.Sprintf("%-50s=  %s\n", "brokerName", "broker-a") +
		fmt.Sprintf("%-50s=  %s\n", "brokerClusterName", "DefaultCluster") +
		"\n"
	if output != expected {
		t.Fatalf("getBrokerConfig -b output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeGetBrokerConfigFormatsOfficialClusterRows(t *testing.T) {
	client := nativeClientFunc{
		getBrokerConfig: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerConfigSection, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected getBrokerConfig cluster args namesrv=%s broker=%s cluster=%s", nameServer, brokerAddr, clusterName)
			}
			return []brokerConfigSection{
				{
					Header:  "============Master: broker-a:10911============",
					Entries: []brokerConfigEntry{{Key: "brokerName", Value: "broker-a"}},
				},
				{
					Header:  "============My Master: broker-a:10911=====Slave: broker-a:10912============",
					Entries: []brokerConfigEntry{{Key: "brokerRole", Value: "SLAVE"}},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"getBrokerConfig", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"}, client)
	if err != nil {
		t.Fatalf("run native getBrokerConfig -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected getBrokerConfig -c to be supported")
	}
	expected := "============Master: broker-a:10911============\n" +
		fmt.Sprintf("%-50s=  %s\n", "brokerName", "broker-a") +
		"\n" +
		"============My Master: broker-a:10911=====Slave: broker-a:10912============\n" +
		fmt.Sprintf("%-50s=  %s\n", "brokerRole", "SLAVE") +
		"\n"
	if output != expected {
		t.Fatalf("getBrokerConfig -c output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeExportConfigsFormatsOfficialSuccess(t *testing.T) {
	expectedFilePaths := []string{"/tmp/goadmin-export", "/tmp/rocketmq/export"}
	callIndex := 0
	client := nativeClientFunc{
		exportConfigs: func(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error) {
			if nameServer != "127.0.0.1:9876" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected exportConfigs args namesrv=%s cluster=%s", nameServer, clusterName)
			}
			if callIndex >= len(expectedFilePaths) {
				t.Fatalf("unexpected extra exportConfigs call")
			}
			if filePath != expectedFilePaths[callIndex] {
				t.Fatalf("unexpected filePath call=%d expected=%s actual=%s", callIndex, expectedFilePaths[callIndex], filePath)
			}
			callIndex++
			return strings.TrimRight(filePath, `/\`) + "/configs.json", nil
		},
	}

	cases := []struct {
		args     []string
		expected string
	}{
		{
			args:     []string{"exportConfigs", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-f", "/tmp/goadmin-export"},
			expected: "export /tmp/goadmin-export/configs.json success",
		},
		{
			args:     []string{"exportConfigs", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"},
			expected: "export /tmp/rocketmq/export/configs.json success",
		},
	}
	for _, tc := range cases {
		output, supported, err := runNativeCommand(context.Background(), tc.args, client)
		if err != nil {
			t.Fatalf("run native exportConfigs: %v", err)
		}
		if !supported {
			t.Fatalf("expected exportConfigs to be supported")
		}
		if output != tc.expected {
			t.Fatalf("exportConfigs output mismatch\nexpected:%q\nactual:%q", tc.expected, output)
		}
	}
	if callIndex != len(expectedFilePaths) {
		t.Fatalf("expected %d export calls, got %d", len(expectedFilePaths), callIndex)
	}
}

func TestFormatExportConfigsMatchesOfficialJSONOrder(t *testing.T) {
	data := exportConfigsData{
		NameServerSize:   1,
		MasterBrokerSize: 1,
		SlaveBrokerSize:  0,
		BrokerConfigs: []exportBrokerConfig{{
			BrokerName: "55924048bd08",
			Entries:    exportConfigsFixtureEntries(),
		}},
	}

	actual := formatExportConfigsJSON(data)
	expected := "{\n" +
		"\t\"clusterScale\":{\n" +
		"\t\t\"namesrvSize\":1,\n" +
		"\t\t\"slaveBrokerSize\":0,\n" +
		"\t\t\"masterBrokerSize\":1\n" +
		"\t},\n" +
		"\t\"brokerConfigs\":{\n" +
		"\t\t\"55924048bd08\":{\n" +
		"\t\t\t\"brokerId\":\"0\",\n" +
		"\t\t\t\"traceOn\":\"true\",\n" +
		"\t\t\t\"flushDiskType\":\"ASYNC_FLUSH\",\n" +
		"\t\t\t\"msgTraceTopicName\":\"RMQ_SYS_TRACE_TOPIC\",\n" +
		"\t\t\t\"traceTopicEnable\":\"false\",\n" +
		"\t\t\t\"messageDelayLevel\":\"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h\",\n" +
		"\t\t\t\"autoCreateTopicEnable\":\"true\",\n" +
		"\t\t\t\"brokerRole\":\"ASYNC_MASTER\",\n" +
		"\t\t\t\"slaveReadEnable\":\"false\",\n" +
		"\t\t\t\"fileReservedTime\":\"72\",\n" +
		"\t\t\t\"brokerClusterName\":\"DefaultCluster\",\n" +
		"\t\t\t\"brokerName\":\"55924048bd08\",\n" +
		"\t\t\t\"maxMessageSize\":\"4194304\",\n" +
		"\t\t\t\"useTLS\":\"false\",\n" +
		"\t\t\t\"autoCreateSubscriptionGroup\":\"true\"\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}"
	if actual != expected {
		t.Fatalf("exportConfigs JSON mismatch\nexpected:\n%s\nactual:\n%s", expected, actual)
	}
}

func TestClientExportConfigsUsesClusterAndBrokerConfigRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerConfig || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected broker config request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, []byte(exportConfigsFixturePropertiesBody())))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"55924048bd08":{"brokerAddrs":{0:%q},"brokerName":"55924048bd08","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["55924048bd08"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	filePath := strings.ReplaceAll(t.TempDir(), "\\", "/")
	outputPath, err := NewClient(time.Second).ExportConfigs(context.Background(), nameServerListener.Addr().String(), "DefaultCluster", filePath)
	if err != nil {
		t.Fatalf("export configs: %v", err)
	}
	expectedOutputPath := filePath + "/configs.json"
	if outputPath != expectedOutputPath {
		t.Fatalf("unexpected output path expected=%s actual=%s", expectedOutputPath, outputPath)
	}
	content, err := os.ReadFile(expectedOutputPath)
	if err != nil {
		t.Fatalf("read exported configs: %v", err)
	}
	expectedContent := formatExportConfigsJSON(exportConfigsData{
		NameServerSize:   1,
		MasterBrokerSize: 1,
		SlaveBrokerSize:  0,
		BrokerConfigs: []exportBrokerConfig{{
			BrokerName: "55924048bd08",
			Entries:    exportConfigsFixtureEntries(),
		}},
	})
	if string(content) != expectedContent {
		t.Fatalf("exported config file mismatch\nexpected:\n%s\nactual:\n%s", expectedContent, string(content))
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestRunNativeExportMetadataFormatsOfficialSuccess(t *testing.T) {
	expectedFilePaths := []string{"/tmp/goadmin-metadata", "/tmp/rocketmq/export", "/tmp/goadmin-metadata-broker"}
	callIndex := 0
	client := nativeClientFunc{
		exportMetadata: func(ctx context.Context, nameServer string, options exportMetadataOptions) (*exportMetadataResult, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected namesrv %s", nameServer)
			}
			if callIndex >= len(expectedFilePaths) {
				t.Fatalf("unexpected extra exportMetadata call")
			}
			if options.FilePath != expectedFilePaths[callIndex] {
				t.Fatalf("unexpected filePath call=%d expected=%s actual=%s", callIndex, expectedFilePaths[callIndex], options.FilePath)
			}
			switch callIndex {
			case 0:
				if options.ClusterName != "DefaultCluster" || options.TopicOnly || options.SubscriptionGroupOnly || options.BrokerAddr != "" {
					t.Fatalf("unexpected cluster metadata options %#v", options)
				}
				callIndex++
				return &exportMetadataResult{OutputPath: "/tmp/goadmin-metadata/metadata.json", PrintNewline: true}, nil
			case 1:
				if options.ClusterName != "DefaultCluster" || !options.TopicOnly || options.SubscriptionGroupOnly || options.BrokerAddr != "" {
					t.Fatalf("unexpected topic metadata options %#v", options)
				}
				callIndex++
				return &exportMetadataResult{OutputPath: "/tmp/rocketmq/export/topic.json", PrintNewline: true}, nil
			default:
				if options.BrokerAddr != "broker-a:10911" || !options.TopicOnly || options.ClusterName != "" {
					t.Fatalf("unexpected broker metadata options %#v", options)
				}
				callIndex++
				return &exportMetadataResult{OutputPath: "/tmp/goadmin-metadata-broker/topic.json"}, nil
			}
		},
	}

	cases := []struct {
		args     []string
		expected string
	}{
		{
			args:     []string{"exportMetadata", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-f", "/tmp/goadmin-metadata"},
			expected: "export /tmp/goadmin-metadata/metadata.json success\n",
		},
		{
			args:     []string{"exportMetadata", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-t"},
			expected: "export /tmp/rocketmq/export/topic.json success\n",
		},
		{
			args:     []string{"exportMetadata", "-n", "127.0.0.1:9876", "-b", "broker-a:10911", "-t", "-f", "/tmp/goadmin-metadata-broker"},
			expected: "export /tmp/goadmin-metadata-broker/topic.json success",
		},
	}
	for _, tc := range cases {
		output, supported, err := runNativeCommand(context.Background(), tc.args, client)
		if err != nil {
			t.Fatalf("run native exportMetadata: %v", err)
		}
		if !supported {
			t.Fatalf("expected exportMetadata to be supported")
		}
		if output != tc.expected {
			t.Fatalf("exportMetadata output mismatch\nexpected:%q\nactual:%q", tc.expected, output)
		}
	}
	if callIndex != len(expectedFilePaths) {
		t.Fatalf("expected %d exportMetadata calls, got %d", len(expectedFilePaths), callIndex)
	}
}

func TestFormatExportMetadataClusterMatchesOfficialJSONOrder(t *testing.T) {
	data := exportMetadataData{
		ExportTime: 1234567890,
		TopicConfigs: []exportMetadataTopicConfig{{
			Name:  "GoadminQueryKeyTest",
			Value: exportMetadataTopicConfigValue("GoadminQueryKeyTest", 4),
		}},
		SubscriptionGroups: []orderedJSONPair{},
		IncludeTopics:      true,
		IncludeGroups:      true,
	}

	actual := formatExportMetadataJSON(data)
	expected := "{\n" +
		"\t\"exportTime\":1234567890,\n" +
		"\t\"topicConfigTable\":{\n" +
		"\t\t\"GoadminQueryKeyTest\":{\n" +
		"\t\t\t\"attributes\":{},\n" +
		"\t\t\t\"order\":false,\n" +
		"\t\t\t\"perm\":6,\n" +
		"\t\t\t\"readQueueNums\":4,\n" +
		"\t\t\t\"topicFilterType\":\"SINGLE_TAG\",\n" +
		"\t\t\t\"topicName\":\"GoadminQueryKeyTest\",\n" +
		"\t\t\t\"topicSysFlag\":0,\n" +
		"\t\t\t\"writeQueueNums\":4\n" +
		"\t\t}\n" +
		"\t},\n" +
		"\t\"subscriptionGroupTable\":{}\n" +
		"}"
	if actual != expected {
		t.Fatalf("exportMetadata JSON mismatch\nexpected:\n%s\nactual:\n%s", expected, actual)
	}
}

func TestExportMetadataTopicConfigsFromOrderedPairsKeepsHashMapBucketChainOrder(t *testing.T) {
	firstTopic, secondTopic := exportMetadataCollidingTopicNamesForTest(t, 2)
	pairs := []orderedJSONPair{
		{Key: firstTopic, Value: exportMetadataTopicConfigValue(firstTopic, 4)},
		{Key: secondTopic, Value: exportMetadataTopicConfigValue(secondTopic, 8)},
	}

	configs := exportMetadataTopicConfigsFromOrderedPairs(pairs)

	actual := []string{configs[0].Name, configs[1].Name}
	expected := []string{firstTopic, secondTopic}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected equal-bucket topic configs to keep insertion order, expected=%v actual=%v", expected, actual)
	}
}

func TestJavaConcurrentHashMapOrderedJSONPairsMatchesExportMetadataWrapperEntrySet(t *testing.T) {
	pairs := []orderedJSONPair{
		{Key: "GoadminM6TraceRichTest", Value: exportMetadataTopicConfigValue("GoadminM6TraceRichTest", 4)},
		{Key: "SCHEDULE_TOPIC_XXXX", Value: exportMetadataTopicConfigValue("SCHEDULE_TOPIC_XXXX", 1)},
		{Key: "TopicTest", Value: exportMetadataTopicConfigValue("TopicTest", 4)},
		{Key: "55924048bd08", Value: exportMetadataTopicConfigValue("55924048bd08", 1)},
		{Key: "rmq_sys_SYNC_BROKER_MEMBER_55924048bd08", Value: exportMetadataTopicConfigValue("rmq_sys_SYNC_BROKER_MEMBER_55924048bd08", 1)},
		{Key: "GoadminM6SendTraceOfficial20260620115320", Value: exportMetadataTopicConfigValue("GoadminM6SendTraceOfficial20260620115320", 1)},
		{Key: "SELF_TEST_TOPIC", Value: exportMetadataTopicConfigValue("SELF_TEST_TOPIC", 1)},
		{Key: "GoadminM6SendTraceNative20260620115651", Value: exportMetadataTopicConfigValue("GoadminM6SendTraceNative20260620115651", 1)},
		{Key: "GoadminQueryKeyTest", Value: exportMetadataTopicConfigValue("GoadminQueryKeyTest", 4)},
		{Key: "rmq_sys_wheel_timer", Value: exportMetadataTopicConfigValue("rmq_sys_wheel_timer", 1)},
		{Key: "%RETRY%GoadminConsumerStatusM575", Value: exportMetadataTopicConfigValue("%RETRY%GoadminConsumerStatusM575", 1)},
		{Key: "%RETRY%CID_JODIE_1", Value: exportMetadataTopicConfigValue("%RETRY%CID_JODIE_1", 1)},
		{Key: "DefaultCluster", Value: exportMetadataTopicConfigValue("DefaultCluster", 1)},
		{Key: "%RETRY%GoadminConsumerStatusM575Group", Value: exportMetadataTopicConfigValue("%RETRY%GoadminConsumerStatusM575Group", 1)},
		{Key: "DefaultCluster_REPLY_TOPIC", Value: exportMetadataTopicConfigValue("DefaultCluster_REPLY_TOPIC", 1)},
		{Key: "GoadminStaticProbe0612", Value: exportMetadataTopicConfigValue("GoadminStaticProbe0612", 4)},
		{Key: "%RETRY%TOOLS_CONSUMER", Value: exportMetadataTopicConfigValue("%RETRY%TOOLS_CONSUMER", 1)},
		{Key: "rmq_sys_REVIVE_LOG_DefaultCluster", Value: exportMetadataTopicConfigValue("rmq_sys_REVIVE_LOG_DefaultCluster", 1)},
		{Key: "GoadminM6TraceMsgChain_20260618153619", Value: exportMetadataTopicConfigValue("GoadminM6TraceMsgChain_20260618153619", 1)},
		{Key: "RMQ_SYS_TRANS_HALF_TOPIC", Value: exportMetadataTopicConfigValue("RMQ_SYS_TRANS_HALF_TOPIC", 1)},
		{Key: "RMQ_SYS_TRACE_TOPIC", Value: exportMetadataTopicConfigValue("RMQ_SYS_TRACE_TOPIC", 1)},
		{Key: "RMQ_SYS_TRANS_OP_HALF_TOPIC", Value: exportMetadataTopicConfigValue("RMQ_SYS_TRANS_OP_HALF_TOPIC", 1)},
		{Key: "GoadminConsumerStatusM575", Value: exportMetadataTopicConfigValue("GoadminConsumerStatusM575", 8)},
		{Key: "TBW102", Value: exportMetadataTopicConfigValue("TBW102", 1)},
		{Key: "BenchmarkTest", Value: exportMetadataTopicConfigValue("BenchmarkTest", 1)},
		{Key: "%RETRY%__MONITOR_CONSUMER", Value: exportMetadataTopicConfigValue("%RETRY%__MONITOR_CONSUMER", 1)},
		{Key: "%RETRY%GoadminConsumerStatusM587Group", Value: exportMetadataTopicConfigValue("%RETRY%GoadminConsumerStatusM587Group", 1)},
		{Key: "OFFSET_MOVED_EVENT", Value: exportMetadataTopicConfigValue("OFFSET_MOVED_EVENT", 1)},
	}

	ordered := javaConcurrentHashMapOrderedJSONPairs(pairs)
	orderedNames := orderedJSONPairKeys(ordered)
	expectedOrderedNames := []string{
		"RMQ_SYS_TRANS_HALF_TOPIC",
		"GoadminM6SendTraceNative20260620115651",
		"GoadminConsumerStatusM575",
		"BenchmarkTest",
		"%RETRY%GoadminConsumerStatusM575Group",
		"TBW102",
		"GoadminM6TraceRichTest",
		"rmq_sys_SYNC_BROKER_MEMBER_55924048bd08",
		"rmq_sys_REVIVE_LOG_DefaultCluster",
		"SELF_TEST_TOPIC",
		"SCHEDULE_TOPIC_XXXX",
		"%RETRY%CID_JODIE_1",
		"RMQ_SYS_TRACE_TOPIC",
		"GoadminQueryKeyTest",
		"DefaultCluster_REPLY_TOPIC",
		"rmq_sys_wheel_timer",
		"RMQ_SYS_TRANS_OP_HALF_TOPIC",
		"TopicTest",
		"GoadminStaticProbe0612",
		"GoadminM6SendTraceOfficial20260620115320",
		"%RETRY%__MONITOR_CONSUMER",
		"%RETRY%GoadminConsumerStatusM587Group",
		"GoadminM6TraceMsgChain_20260618153619",
		"OFFSET_MOVED_EVENT",
		"DefaultCluster",
		"%RETRY%GoadminConsumerStatusM575",
		"55924048bd08",
		"%RETRY%TOOLS_CONSUMER",
	}
	if !reflect.DeepEqual(orderedNames, expectedOrderedNames) {
		t.Fatalf("expected decoded TopicConfigSerializeWrapper entrySet order, expected=%v actual=%v", expectedOrderedNames, orderedNames)
	}

	wrapper := orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "dataVersion", Value: orderedJSONValue{Kind: orderedJSONObject}},
			{Key: "topicConfigTable", Value: orderedJSONValue{Kind: orderedJSONObject, Pairs: pairs}},
		},
	}
	systemTopics := map[string]bool{
		"BenchmarkTest":               true,
		"DefaultCluster":              true,
		"DefaultCluster_REPLY_TOPIC":  true,
		"RMQ_SYS_TRANS_OP_HALF_TOPIC": true,
		"TBW102":                      true,
		"55924048bd08":                true,
	}
	filterTopicConfigWrapper(&wrapper, systemTopics, false)
	table, ok := wrapper.objectField("topicConfigTable")
	if !ok {
		t.Fatalf("expected filtered wrapper to keep topicConfigTable")
	}
	filteredNames := orderedJSONPairKeys(table.Pairs)
	expectedFilteredNames := []string{
		"GoadminM6SendTraceNative20260620115651",
		"GoadminConsumerStatusM575",
		"GoadminM6TraceRichTest",
		"GoadminQueryKeyTest",
		"TopicTest",
		"GoadminStaticProbe0612",
		"GoadminM6SendTraceOfficial20260620115320",
		"GoadminM6TraceMsgChain_20260618153619",
	}
	if !reflect.DeepEqual(filteredNames, expectedFilteredNames) {
		t.Fatalf("expected user topic filter to preserve decoded entrySet order, expected=%v actual=%v", expectedFilteredNames, filteredNames)
	}

	configs := exportMetadataTopicConfigsFromOrderedPairs(table.Pairs)
	finalNames := exportMetadataTopicConfigNames(configs)
	expectedFinalNames := []string{
		"GoadminQueryKeyTest",
		"GoadminM6SendTraceNative20260620115651",
		"GoadminConsumerStatusM575",
		"GoadminM6TraceMsgChain_20260618153619",
		"TopicTest",
		"GoadminStaticProbe0612",
		"GoadminM6SendTraceOfficial20260620115320",
		"GoadminM6TraceRichTest",
	}
	if !reflect.DeepEqual(finalNames, expectedFinalNames) {
		t.Fatalf("expected cluster HashMap output order to match official metadata.json, expected=%v actual=%v", expectedFinalNames, finalNames)
	}
}

func orderedJSONPairKeys(pairs []orderedJSONPair) []string {
	keys := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		keys = append(keys, pair.Key)
	}
	return keys
}

func exportMetadataTopicConfigNames(configs []exportMetadataTopicConfig) []string {
	names := make([]string, 0, len(configs))
	for _, config := range configs {
		names = append(names, config.Name)
	}
	return names
}

func exportMetadataCollidingTopicNamesForTest(t *testing.T, count int) (string, string) {
	t.Helper()
	capacity := javaHashMapCapacity(count)
	seen := make(map[int]string, capacity)
	for index := 0; index < 1024; index++ {
		name := fmt.Sprintf("GoadminExportCollisionTopic%03d", index)
		bucket := javaHashMapBucketWithCapacity(name, capacity)
		if previous, ok := seen[bucket]; ok {
			return previous, name
		}
		seen[bucket] = name
	}
	t.Fatalf("expected to find two topic names in the same Java HashMap bucket")
	return "", ""
}

func TestFilterExportMetadataWrappersMatchOfficialUserFilters(t *testing.T) {
	topicWrapper := orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "dataVersion", Value: orderedJSONValue{Kind: orderedJSONObject}},
			{Key: "mappingDataVersion", Value: orderedJSONValue{Kind: orderedJSONObject}},
			{Key: "topicConfigTable", Value: orderedJSONValue{Kind: orderedJSONObject, Pairs: []orderedJSONPair{
				{Key: "%RETRY%TOOLS_CONSUMER", Value: exportMetadataTopicConfigValue("%RETRY%TOOLS_CONSUMER", 1)},
				{Key: "GoadminQueryKeyTest", Value: exportMetadataTopicConfigValue("GoadminQueryKeyTest", 4)},
				{Key: "SYSTEM_TOPIC", Value: exportMetadataTopicConfigValue("SYSTEM_TOPIC", 1)},
			}}},
			{Key: "topicQueueMappingDetailMap", Value: orderedJSONValue{Kind: orderedJSONObject}},
			{Key: "topicQueueMappingInfoMap", Value: orderedJSONValue{Kind: orderedJSONObject}},
		},
	}

	filterTopicConfigWrapper(&topicWrapper, map[string]bool{"SYSTEM_TOPIC": true}, false)
	if len(topicWrapper.Pairs) != 2 || topicWrapper.Pairs[0].Key != "dataVersion" || topicWrapper.Pairs[1].Key != "topicConfigTable" {
		t.Fatalf("unexpected topic wrapper fields %#v", topicWrapper.Pairs)
	}
	topicTable, ok := topicWrapper.objectField("topicConfigTable")
	if !ok || len(topicTable.Pairs) != 1 || topicTable.Pairs[0].Key != "GoadminQueryKeyTest" {
		t.Fatalf("unexpected filtered topic table %#v", topicTable)
	}

	groupWrapper := orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "dataVersion", Value: orderedJSONValue{Kind: orderedJSONObject}},
			{Key: "forbiddenTable", Value: orderedJSONValue{Kind: orderedJSONObject}},
			{Key: "subscriptionGroupTable", Value: orderedJSONValue{Kind: orderedJSONObject, Pairs: []orderedJSONPair{
				{Key: "DEFAULT_CONSUMER", Value: orderedJSONValue{Kind: orderedJSONObject}},
				{Key: "CID_RMQ_SYS_TRANS", Value: orderedJSONValue{Kind: orderedJSONObject}},
				{Key: "CID_ONSAPI_OWNER", Value: orderedJSONValue{Kind: orderedJSONObject}},
				{Key: "GoadminUserGroup", Value: orderedJSONValue{Kind: orderedJSONObject}},
			}}},
		},
	}

	filterSubscriptionGroupWrapper(&groupWrapper)
	if len(groupWrapper.Pairs) != 2 || groupWrapper.Pairs[0].Key != "dataVersion" || groupWrapper.Pairs[1].Key != "subscriptionGroupTable" {
		t.Fatalf("unexpected group wrapper fields %#v", groupWrapper.Pairs)
	}
	groupTable, ok := groupWrapper.objectField("subscriptionGroupTable")
	if !ok || len(groupTable.Pairs) != 1 || groupTable.Pairs[0].Key != "GoadminUserGroup" {
		t.Fatalf("unexpected filtered group table %#v", groupTable)
	}
}

func TestClientExportMetadataUsesOfficialRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		expectedCodes := []int{
			requestCodeGetAllTopicConfig,
			requestCodeGetSystemTopicListFromBroker,
			requestCodeGetAllSubscriptionGroupConfig,
		}
		for _, expectedCode := range expectedCodes {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				brokerDone <- err
				return
			}
			if request.Code != expectedCode || len(request.ExtFields) != 0 || len(request.Body) != 0 {
				conn.Close()
				brokerDone <- &testError{message: fmt.Sprintf("unexpected metadata request code=%d expected=%d fields=%#v body=%d", request.Code, expectedCode, request.ExtFields, len(request.Body))}
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			var body []byte
			switch expectedCode {
			case requestCodeGetAllTopicConfig:
				body = []byte(`{"dataVersion":{"counter":3,"stateVersion":0,"timestamp":1780894480298},"topicConfigTable":{"GoadminQueryKeyTest":{"attributes":{},"order":false,"perm":6,"readQueueNums":4,"topicFilterType":"SINGLE_TAG","topicName":"GoadminQueryKeyTest","topicSysFlag":0,"writeQueueNums":4},"%RETRY%TOOLS_CONSUMER":{"attributes":{},"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"%RETRY%TOOLS_CONSUMER","topicSysFlag":0,"writeQueueNums":1}}}`)
			case requestCodeGetSystemTopicListFromBroker:
				body = []byte(`{"topicList":[]}`)
			case requestCodeGetAllSubscriptionGroupConfig:
				body = []byte(`{"dataVersion":{"counter":0,"stateVersion":0,"timestamp":1780962035608},"subscriptionGroupTable":{}}`)
			}
			_, err = conn.Write(remotingFrameForTest(t, response, body))
			conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"55924048bd08":{"brokerAddrs":{0:%q},"brokerName":"55924048bd08","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["55924048bd08"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	filePath := strings.ReplaceAll(t.TempDir(), "\\", "/")
	result, err := NewClient(time.Second).ExportMetadata(context.Background(), nameServerListener.Addr().String(), exportMetadataOptions{
		ClusterName: "DefaultCluster",
		FilePath:    filePath,
		NowMillis:   1234567890,
	})
	if err != nil {
		t.Fatalf("export metadata: %v", err)
	}
	expectedOutputPath := filePath + "/metadata.json"
	if result == nil || result.OutputPath != expectedOutputPath || !result.PrintNewline {
		t.Fatalf("unexpected export metadata result %#v", result)
	}
	content, err := os.ReadFile(expectedOutputPath)
	if err != nil {
		t.Fatalf("read exported metadata: %v", err)
	}
	expectedContent := formatExportMetadataJSON(exportMetadataData{
		ExportTime: 1234567890,
		TopicConfigs: []exportMetadataTopicConfig{{
			Name:  "GoadminQueryKeyTest",
			Value: exportMetadataTopicConfigValue("GoadminQueryKeyTest", 4),
		}},
		SubscriptionGroups: []orderedJSONPair{},
		IncludeTopics:      true,
		IncludeGroups:      true,
	})
	if string(content) != expectedContent {
		t.Fatalf("exported metadata file mismatch\nexpected:\n%s\nactual:\n%s", expectedContent, string(content))
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestRunNativeExportMetricsFormatsOfficialSuccess(t *testing.T) {
	expectedFilePaths := []string{"/tmp/goadmin-metrics", "/tmp/rocketmq/export"}
	callIndex := 0
	client := nativeClientFunc{
		exportMetrics: func(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error) {
			if nameServer != "127.0.0.1:9876" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected exportMetrics args namesrv=%s cluster=%s", nameServer, clusterName)
			}
			if callIndex >= len(expectedFilePaths) {
				t.Fatalf("unexpected extra exportMetrics call")
			}
			if filePath != expectedFilePaths[callIndex] {
				t.Fatalf("unexpected filePath call=%d expected=%s actual=%s", callIndex, expectedFilePaths[callIndex], filePath)
			}
			callIndex++
			return strings.TrimRight(filePath, `/\`) + "/metrics.json", nil
		},
	}

	cases := []struct {
		args     []string
		expected string
	}{
		{
			args:     []string{"exportMetrics", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-f", "/tmp/goadmin-metrics"},
			expected: "export /tmp/goadmin-metrics/metrics.json success",
		},
		{
			args:     []string{"exportMetrics", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"},
			expected: "export /tmp/rocketmq/export/metrics.json success",
		},
	}
	for _, tc := range cases {
		output, supported, err := runNativeCommand(context.Background(), tc.args, client)
		if err != nil {
			t.Fatalf("run native exportMetrics: %v", err)
		}
		if !supported {
			t.Fatalf("expected exportMetrics to be supported")
		}
		if output != tc.expected {
			t.Fatalf("exportMetrics output mismatch\nexpected:%q\nactual:%q", tc.expected, output)
		}
	}
	if callIndex != len(expectedFilePaths) {
		t.Fatalf("expected %d exportMetrics calls, got %d", len(expectedFilePaths), callIndex)
	}
}

func TestDecodeBrokerRuntimeStatsBodyAcceptsFastJSONNumericKeys(t *testing.T) {
	body := []byte(`{"table":{0:"plain-zero-key","putTps":"1.0 2.0 3.0","getTransferredTps":"4.0 5.0 6.0"}}`)

	stats, err := decodeBrokerRuntimeStatsBody(body)
	if err != nil {
		t.Fatalf("decode broker runtime stats: %v", err)
	}
	if stats["putTps"] != "1.0 2.0 3.0" || stats["0"] != "plain-zero-key" {
		t.Fatalf("unexpected runtime stats %#v", stats)
	}
}

func TestDecodeOrderedJSONValueAcceptsFastJSONNumericKeys(t *testing.T) {
	value, err := decodeOrderedJSONValue(`{"topicQueueMappingInfoMap":{0:{"topic":"TopicA"}},"topicConfigTable":{"TopicA":{"readQueueNums":4}}}`)
	if err != nil {
		t.Fatalf("decode ordered JSON value: %v", err)
	}
	mapping, ok := value.objectField("topicQueueMappingInfoMap")
	if !ok || len(mapping.Pairs) != 1 || mapping.Pairs[0].Key != "0" {
		t.Fatalf("unexpected numeric key mapping %#v", mapping)
	}
}

func TestFormatExportMetricsMatchesOfficialJSONOrder(t *testing.T) {
	data := exportMetricsData{
		Total: exportMetricsTotal{
			NormalInTps:         1.5,
			NormalOutTps:        2.5,
			TransInTps:          0,
			ScheduleInTps:       0,
			NormalOneDayInNum:   2,
			NormalOneDayOutNum:  3,
			TransOneDayInNum:    0,
			ScheduleOneDayInNum: 0,
		},
		Reports: []exportMetricsBrokerReport{{
			BrokerName: "55924048bd08",
			RuntimeEnv: exportMetricsRuntimeEnv{
				CPUNum:         "8",
				TotalMemKBytes: "",
			},
			RuntimeQuota: exportMetricsRuntimeQuota{
				CommitLogDiskRatio:    "0.06",
				ConsumeQueueDiskRatio: "0.06",
				NormalInTps:           1.5,
				NormalOutTps:          2.5,
				TransInTps:            0,
				ScheduleInTps:         0,
				NormalOneDayInNum:     2,
				NormalOneDayOutNum:    3,
				TransOneDayInNum:      0,
				ScheduleOneDayInNum:   0,
				MessageAverageSize:    "233.66666666666666",
				TopicSize:             1,
				GroupSize:             0,
			},
			RuntimeVersion: exportMetricsRuntimeVersion{
				RocketMQVersion: "V5_3_2",
				ClientInfo:      []string{},
			},
		}},
	}

	actual := formatExportMetricsJSON(data)
	expected := "{\n" +
		"\t\"totalData\":{\n" +
		"\t\t\"totalOneDayNum\":{\n" +
		"\t\t\t\"transOneDayInNum\":0,\n" +
		"\t\t\t\"scheduleOneDayInNum\":0,\n" +
		"\t\t\t\"normalOneDayOutNum\":3,\n" +
		"\t\t\t\"normalOneDayInNum\":2\n" +
		"\t\t},\n" +
		"\t\t\"totalTps\":{\n" +
		"\t\t\t\"totalScheduleInTps\":0.0,\n" +
		"\t\t\t\"totalNormalInTps\":1.5,\n" +
		"\t\t\t\"totalTransInTps\":0.0,\n" +
		"\t\t\t\"totalNormalOutTps\":2.5\n" +
		"\t\t}\n" +
		"\t},\n" +
		"\t\"evaluateReport\":{\n" +
		"\t\t\"55924048bd08\":{\n" +
		"\t\t\t\"runtimeVersion\":{\n" +
		"\t\t\t\t\"rocketmqVersion\":\"V5_3_2\",\n" +
		"\t\t\t\t\"clientInfo\":[]\n" +
		"\t\t\t},\n" +
		"\t\t\t\"runtimeQuota\":{\n" +
		"\t\t\t\t\"diskRatio\":{\n" +
		"\t\t\t\t\t\"commitLogDiskRatio\":\"0.06\",\n" +
		"\t\t\t\t\t\"consumeQueueDiskRatio\":\"0.06\"\n" +
		"\t\t\t\t},\n" +
		"\t\t\t\t\"topicSize\":1,\n" +
		"\t\t\t\t\"tps\":{\n" +
		"\t\t\t\t\t\"scheduleInTps\":0.0,\n" +
		"\t\t\t\t\t\"normalInTps\":1.5,\n" +
		"\t\t\t\t\t\"normalOutTps\":2.5,\n" +
		"\t\t\t\t\t\"transInTps\":0.0\n" +
		"\t\t\t\t},\n" +
		"\t\t\t\t\"groupSize\":0,\n" +
		"\t\t\t\t\"oneDayNum\":{\n" +
		"\t\t\t\t\t\"transOneDayInNum\":0,\n" +
		"\t\t\t\t\t\"scheduleOneDayInNum\":0,\n" +
		"\t\t\t\t\t\"normalOneDayOutNum\":3,\n" +
		"\t\t\t\t\t\"normalOneDayInNum\":2\n" +
		"\t\t\t\t},\n" +
		"\t\t\t\t\"messageAverageSize\":\"233.66666666666666\"\n" +
		"\t\t\t},\n" +
		"\t\t\t\"runtimeEnv\":{\n" +
		"\t\t\t\t\"cpuNum\":\"8\"\n" +
		"\t\t\t}\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}"
	if actual != expected {
		t.Fatalf("exportMetrics JSON mismatch\nexpected:\n%s\nactual:\n%s", expected, actual)
	}
}

func TestClientExportMetricsUsesOfficialRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		expectedCodes := []int{
			requestCodeGetBrokerRuntimeInfo,
			requestCodeGetBrokerConfig,
			requestCodeGetAllSubscriptionGroupConfig,
			requestCodeGetAllTopicConfig,
			requestCodeGetSystemTopicListFromBroker,
			requestCodeViewBrokerStatsData,
			requestCodeViewBrokerStatsData,
		}
		for _, expectedCode := range expectedCodes {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			if request.Code != expectedCode {
				_ = conn.Close()
				brokerDone <- &testError{message: fmt.Sprintf("unexpected metrics request code=%d expected=%d", request.Code, expectedCode)}
				return
			}
			if expectedCode == requestCodeViewBrokerStatsData {
				statsKey := request.ExtFields["statsKey"]
				if request.ExtFields["statsName"] != statsNameTopicPutNums || (statsKey != "RMQ_SYS_TRANS_HALF_TOPIC" && statsKey != "SCHEDULE_TOPIC_XXXX") {
					_ = conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected stats fields %#v", request.ExtFields)}
					return
				}
			} else if len(request.ExtFields) != 0 || len(request.Body) != 0 {
				_ = conn.Close()
				brokerDone <- &testError{message: fmt.Sprintf("unexpected metrics request fields=%#v body=%d", request.ExtFields, len(request.Body))}
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			var body []byte
			switch expectedCode {
			case requestCodeGetBrokerRuntimeInfo:
				body = []byte(exportMetricsFixtureRuntimeBody())
			case requestCodeGetBrokerConfig:
				body = []byte(exportMetricsFixtureBrokerConfigBody())
			case requestCodeGetAllSubscriptionGroupConfig:
				body = []byte(`{"subscriptionGroupTable":{}}`)
			case requestCodeGetAllTopicConfig:
				body = []byte(`{"topicConfigTable":{"GoadminQueryKeyTest":{"topicName":"GoadminQueryKeyTest"}}}`)
			case requestCodeGetSystemTopicListFromBroker:
				body = []byte(`{"topicList":["RMQ_SYS_TRANS_HALF_TOPIC","SCHEDULE_TOPIC_XXXX"]}`)
			case requestCodeViewBrokerStatsData:
				body = []byte(`{"statsMinute":{"sum":0,"tps":0.0,"avgpt":0},"statsHour":{"sum":0,"tps":0.0,"avgpt":0},"statsDay":{"sum":0,"tps":0.0,"avgpt":0}}`)
			}
			_, err = conn.Write(remotingFrameForTest(t, response, body))
			_ = conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"55924048bd08":{"brokerAddrs":{0:%q},"brokerName":"55924048bd08","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["55924048bd08"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	filePath := strings.ReplaceAll(t.TempDir(), "\\", "/")
	outputPath, err := NewClient(time.Second).ExportMetrics(context.Background(), nameServerListener.Addr().String(), "DefaultCluster", filePath)
	if err != nil {
		t.Fatalf("export metrics: %v", err)
	}
	expectedOutputPath := filePath + "/metrics.json"
	if outputPath != expectedOutputPath {
		t.Fatalf("unexpected output path expected=%s actual=%s", expectedOutputPath, outputPath)
	}
	content, err := os.ReadFile(expectedOutputPath)
	if err != nil {
		t.Fatalf("read exported metrics: %v", err)
	}
	if !strings.Contains(string(content), `"totalData"`) || !strings.Contains(string(content), `"evaluateReport"`) {
		t.Fatalf("exported metrics missing expected sections: %s", string(content))
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func exportMetricsFixtureRuntimeBody() string {
	return `{"table":{"commitLogDiskRatio":"0.06","consumeQueueDiskRatio":"0.06","putTps":"1.5 0.0 0.0","getTransferredTps":"2.5 0.0 0.0","msgPutTotalTodayMorning":"2","msgPutTotalYesterdayMorning":"0","msgGetTotalTodayMorning":"3","msgGetTotalYesterdayMorning":"0","putMessageAverageSize":"233.66666666666666"}}`
}

func exportMetricsFixtureBrokerConfigBody() string {
	return "clientCallbackExecutorThreads=8\n"
}

func exportConfigsFixtureEntries() []brokerConfigEntry {
	return []brokerConfigEntry{
		{Key: "brokerId", Value: "0"},
		{Key: "traceOn", Value: "true"},
		{Key: "flushDiskType", Value: "ASYNC_FLUSH"},
		{Key: "msgTraceTopicName", Value: "RMQ_SYS_TRACE_TOPIC"},
		{Key: "traceTopicEnable", Value: "false"},
		{Key: "messageDelayLevel", Value: "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"},
		{Key: "autoCreateTopicEnable", Value: "true"},
		{Key: "brokerRole", Value: "ASYNC_MASTER"},
		{Key: "slaveReadEnable", Value: "false"},
		{Key: "fileReservedTime", Value: "72"},
		{Key: "brokerClusterName", Value: "DefaultCluster"},
		{Key: "brokerName", Value: "55924048bd08"},
		{Key: "maxMessageSize", Value: "4194304"},
		{Key: "useTLS", Value: "false"},
		{Key: "autoCreateSubscriptionGroup", Value: "true"},
	}
}

func exportConfigsFixturePropertiesBody() string {
	var builder strings.Builder
	for _, entry := range exportConfigsFixtureEntries() {
		builder.WriteString(entry.Key)
		builder.WriteByte('=')
		builder.WriteString(entry.Value)
		builder.WriteByte('\n')
	}
	builder.WriteString("transientStorePoolEnable=false\n")
	return builder.String()
}

func TestRunNativeGetNamesrvConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		getNamesrvConfig: func(ctx context.Context, nameServers string) ([]namesrvConfigSection, error) {
			if nameServers != "127.0.0.1:9876" {
				t.Fatalf("unexpected getNamesrvConfig namesrv=%s", nameServers)
			}
			return []namesrvConfigSection{{
				NameServer: "127.0.0.1:9876",
				Entries: []brokerConfigEntry{
					{Key: "rocketmqHome", Value: "/opt/rocketmq"},
					{Key: "clusterTest", Value: "false"},
				},
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"getNamesrvConfig", "-n", "127.0.0.1:9876"}, client)
	if err != nil {
		t.Fatalf("run native getNamesrvConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected getNamesrvConfig to be supported")
	}
	expected := "============127.0.0.1:9876============\n" +
		fmt.Sprintf("%-50s=  %s\n", "rocketmqHome", "/opt/rocketmq") +
		fmt.Sprintf("%-50s=  %s\n", "clusterTest", "false")
	if output != expected {
		t.Fatalf("getNamesrvConfig output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeGetConsumerConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		getConsumerConfig: func(ctx context.Context, nameServer string, groupName string) ([]consumerConfigSection, error) {
			if nameServer != "127.0.0.1:9876" || groupName != "TOOLS_CONSUMER" {
				t.Fatalf("unexpected getConsumerConfig args namesrv=%s group=%s", nameServer, groupName)
			}
			return []consumerConfigSection{{
				Header: "=============================DefaultCluster:broker-a=============================",
				Entries: []consumerConfigEntry{
					{Name: "groupName", Value: "TOOLS_CONSUMER"},
					{Name: "consumeEnable", Value: "true"},
					{Name: "consumeFromMinEnable", Value: "true"},
					{Name: "consumeBroadcastEnable", Value: "true"},
					{Name: "consumeMessageOrderly", Value: "false"},
					{Name: "retryQueueNums", Value: "1"},
					{Name: "retryMaxTimes", Value: "16"},
					{Name: "groupRetryPolicy", Value: "GroupRetryPolicy{type=CUSTOMIZED, exponentialRetryPolicy=null, customizedRetryPolicy=null}"},
					{Name: "brokerId", Value: "0"},
					{Name: "whichBrokerWhenConsumeSlowly", Value: "1"},
					{Name: "notifyConsumerIdsChangedEnable", Value: "true"},
					{Name: "groupSysFlag", Value: "0"},
					{Name: "consumeTimeoutMinute", Value: "15"},
					{Name: "subscriptionDataSet", Value: ""},
					{Name: "attributes", Value: "{}"},
				},
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"getConsumerConfig", "-n", "127.0.0.1:9876", "-g", "TOOLS_CONSUMER"}, client)
	if err != nil {
		t.Fatalf("run native getConsumerConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected getConsumerConfig to be supported")
	}
	expected := "=============================DefaultCluster:broker-a=============================\n" +
		fmt.Sprintf("%-40s=  %s\n", "groupName", "TOOLS_CONSUMER") +
		fmt.Sprintf("%-40s=  %s\n", "consumeEnable", "true") +
		fmt.Sprintf("%-40s=  %s\n", "consumeFromMinEnable", "true") +
		fmt.Sprintf("%-40s=  %s\n", "consumeBroadcastEnable", "true") +
		fmt.Sprintf("%-40s=  %s\n", "consumeMessageOrderly", "false") +
		fmt.Sprintf("%-40s=  %s\n", "retryQueueNums", "1") +
		fmt.Sprintf("%-40s=  %s\n", "retryMaxTimes", "16") +
		fmt.Sprintf("%-40s=  %s\n", "groupRetryPolicy", "GroupRetryPolicy{type=CUSTOMIZED, exponentialRetryPolicy=null, customizedRetryPolicy=null}") +
		fmt.Sprintf("%-40s=  %s\n", "brokerId", "0") +
		fmt.Sprintf("%-40s=  %s\n", "whichBrokerWhenConsumeSlowly", "1") +
		fmt.Sprintf("%-40s=  %s\n", "notifyConsumerIdsChangedEnable", "true") +
		fmt.Sprintf("%-40s=  %s\n", "groupSysFlag", "0") +
		fmt.Sprintf("%-40s=  %s\n", "consumeTimeoutMinute", "15") +
		fmt.Sprintf("%-40s=  %s\n", "subscriptionDataSet", "") +
		fmt.Sprintf("%-40s=  %s\n", "attributes", "{}")
	if output != expected {
		t.Fatalf("getConsumerConfig output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeBrokerConsumeStatsFormatsOfficialOutput(t *testing.T) {
	const lastTimestamp int64 = 1780883297000
	client := nativeClientFunc{
		brokerConsumeStats: func(ctx context.Context, brokerAddr string, isOrder bool, timeout time.Duration) (*brokerConsumeStats, error) {
			if brokerAddr != "broker-a:10911" || !isOrder {
				t.Fatalf("unexpected brokerConsumeStats args broker=%s isOrder=%v", brokerAddr, isOrder)
			}
			if timeout != 50000*time.Millisecond {
				t.Fatalf("unexpected brokerConsumeStats timeout %s", timeout)
			}
			return &brokerConsumeStats{
				TotalDiff: 27,
				Groups: []brokerConsumeStatsGroup{
					{
						Group: "GroupA",
						Stats: []consumerProgress{
							{
								Entries: []consumerProgressEntry{
									{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1, BrokerOffset: 12, ConsumerOffset: 10, PullOffset: 12},
									{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0, BrokerOffset: 120, ConsumerOffset: 100, PullOffset: 118, LastTimestamp: lastTimestamp},
								},
							},
						},
					},
					{
						Group: "GroupB",
						Stats: []consumerProgress{
							{
								Entries: []consumerProgressEntry{
									{Topic: "TopicTest", BrokerName: "broker-b", QueueID: 0, BrokerOffset: 50, ConsumerOffset: 45, PullOffset: 50, LastTimestamp: lastTimestamp},
								},
							},
						},
					},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"brokerConsumeStats", "-b", "broker-a:10911", "-o", "true", "-l", "10"}, client)
	if err != nil {
		t.Fatalf("run native brokerConsumeStats: %v", err)
	}
	if !supported {
		t.Fatalf("expected brokerConsumeStats to be supported")
	}
	expected := fmt.Sprintf("%-64s  %-64s  %-32s  %-4s  %-20s  %-20s  %-20s  %s\n", "#Topic", "#Group", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff", "#LastTime")
	expected += fmt.Sprintf("%-64s  %-64s  %-32s  %-4d  %-20d  %-20d  %-20d  %s\n",
		frontStringAtLeast("TopicTest", 64),
		"GroupA",
		frontStringAtLeast("broker-a", 32),
		0,
		int64(120),
		int64(100),
		int64(20),
		formatTraceTime(lastTimestamp),
	)
	expected += "\nDiff Total: 27\n"
	if output != expected {
		t.Fatalf("brokerConsumeStats output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeBrokerConsumeStatsPassesTimeoutMillis(t *testing.T) {
	client := nativeClientFunc{
		brokerConsumeStats: func(ctx context.Context, brokerAddr string, isOrder bool, timeout time.Duration) (*brokerConsumeStats, error) {
			if brokerAddr != "broker-a:10911" {
				t.Fatalf("unexpected brokerConsumeStats broker=%s", brokerAddr)
			}
			if !isOrder {
				t.Fatalf("expected brokerConsumeStats order flag")
			}
			if timeout != 12345*time.Millisecond {
				t.Fatalf("unexpected brokerConsumeStats timeout %s", timeout)
			}
			return &brokerConsumeStats{}, nil
		},
	}

	_, supported, err := runNativeCommand(context.Background(), []string{"brokerConsumeStats", "-b", "broker-a:10911", "-o", "true", "-t", "12345"}, client)
	if err != nil {
		t.Fatalf("run native brokerConsumeStats: %v", err)
	}
	if !supported {
		t.Fatalf("expected brokerConsumeStats to be supported")
	}
}

func TestRunNativeStatsAllFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		statsAll: func(ctx context.Context, nameServer string, topic string, activeOnly bool) ([]statsAllRow, error) {
			if nameServer != "127.0.0.1:9876" || topic != "TopicTest" || !activeOnly {
				t.Fatalf("unexpected statsAll args namesrv=%s topic=%s active=%v", nameServer, topic, activeOnly)
			}
			return []statsAllRow{
				{Topic: "TopicTest", Accumulation: 0, InTPS: 1.25, InMsg24Hour: 24, NoConsumer: true},
				{Topic: "TopicWithGroup", ConsumerGroup: "GroupA", Accumulation: 7, InTPS: 2.50, OutTPS: 3.75, InMsg24Hour: 100, OutMsg24Hour: 80},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"statsAll", "-n", "127.0.0.1:9876", "-t", "TopicTest", "-a"}, client)
	if err != nil {
		t.Fatalf("run native statsAll: %v", err)
	}
	if !supported {
		t.Fatalf("expected statsAll to be supported")
	}
	expected := fmt.Sprintf("%-64s  %-64s %12s %11s %11s %14s %14s\n", "#Topic", "#Consumer Group", "#Accumulation", "#InTPS", "#OutTPS", "#InMsg24Hour", "#OutMsg24Hour") +
		fmt.Sprintf("%-64s  %-64s %12d %11.2f %11s %14d %14s\n", "TopicTest", "", int64(0), 1.25, "", int64(24), "NO_CONSUMER") +
		fmt.Sprintf("%-64s  %-64s %12d %11.2f %11.2f %14d %14d\n", "TopicWithGroup", "GroupA", int64(7), 2.50, 3.75, int64(100), int64(80))
	if output != expected {
		t.Fatalf("statsAll output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeQueryCqFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		queryConsumeQueue: func(ctx context.Context, nameServer string, brokerAddr string, topic string, queueID int, index int64, count int, consumerGroup string) (*queryConsumeQueueResult, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "broker-a:10911" || topic != "TopicTest" || queueID != 0 || index != 7 || count != 2 || consumerGroup != "GroupA" {
				t.Fatalf("unexpected queryCq args namesrv=%s broker=%s topic=%s queue=%d index=%d count=%d group=%s", nameServer, brokerAddr, topic, queueID, index, count, consumerGroup)
			}
			return &queryConsumeQueueResult{
				FilterData:    "GroupA@TopicTest is not online!",
				MaxQueueIndex: 9,
				MinQueueIndex: 0,
				QueueData: []consumeQueueData{
					{PhysicOffset: 731, PhysicSize: 225, TagsCode: 0, ExtendDataJSON: "null", BitMap: "null", Eval: false, Msg: "null"},
					{PhysicOffset: 1631, PhysicSize: 225, TagsCode: 12, ExtendDataJSON: "{\"k\":\"v\"}", BitMap: "1010", Eval: true, Msg: "hello"},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"queryCq", "-n", "127.0.0.1:9876", "-b", "broker-a:10911", "-t", "TopicTest", "-q", "0", "-i", "7", "-c", "2", "-g", "GroupA"}, client)
	if err != nil {
		t.Fatalf("run native queryCq: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryCq to be supported")
	}
	expected := "Filter data: \nGroupA@TopicTest is not online!\n" +
		"======================================\n" +
		"Queue data: \nmax: 9, min: 0\n" +
		"======================================\n" +
		"idx: 7\n" +
		"ConsumeQueueData{physicOffset=731, physicSize=225, tagsCode=0, extendDataJson='null', bitMap='null', eval=false, msg='null'}\n" +
		"======================================\n" +
		"idx: 8\n" +
		"ConsumeQueueData{physicOffset=1631, physicSize=225, tagsCode=12, extendDataJson='{\"k\":\"v\"}', bitMap='1010', eval=true, msg='hello'}\n" +
		"======================================\n"
	if output != expected {
		t.Fatalf("queryCq output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeQueryCqRequiresTopicQueueAndIndex(t *testing.T) {
	cases := [][]string{
		{"queryCq", "-n", "127.0.0.1:9876", "-q", "0", "-i", "0"},
		{"queryCq", "-n", "127.0.0.1:9876", "-t", "TopicTest", "-i", "0"},
		{"queryCq", "-n", "127.0.0.1:9876", "-t", "TopicTest", "-q", "0"},
	}
	for _, args := range cases {
		output, supported, err := runNativeCommand(context.Background(), args, nativeClientFunc{})
		if err == nil {
			t.Fatalf("expected queryCq args %v to fail", args)
		}
		if !supported || output != "" {
			t.Fatalf("expected queryCq missing args to be handled by native command, supported=%t output=%q", supported, output)
		}
	}
}

func TestRunNativeHAStatusFormatsOfficialMasterOutput(t *testing.T) {
	client := nativeClientFunc{
		brokerHAStatus: func(ctx context.Context, brokerAddr string) (*haStatusResult, error) {
			if brokerAddr != "broker-a:10911" {
				t.Fatalf("unexpected haStatus broker=%s", brokerAddr)
			}
			return &haStatusResult{
				Master:                   true,
				MasterCommitLogMaxOffset: 397814681,
				InSyncSlaveNums:          0,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"haStatus", "-b", "broker-a:10911"}, client)
	if err != nil {
		t.Fatalf("run native haStatus: %v", err)
	}
	if !supported {
		t.Fatalf("expected haStatus to be supported")
	}
	expected := "\n" +
		"#MasterAddr\tbroker-a:10911\n" +
		"#MasterCommitLogMaxOffset\t397814681\n" +
		"#SlaveNum\t0\n" +
		"#InSyncSlaveNum\t0\n" +
		fmt.Sprintf("%-32s  %-16s %16s %16s %16s %16s\n", "#SlaveAddr", "#SlaveAckOffset", "#Diff", "#TransferSpeed(KB/s)", "#Status", "#TransferFromWhere")
	if output != expected {
		t.Fatalf("haStatus master output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeHAStatusFormatsOfficialSlaveOutput(t *testing.T) {
	client := nativeClientFunc{
		brokerHAStatus: func(ctx context.Context, brokerAddr string) (*haStatusResult, error) {
			if brokerAddr != "broker-a:10911" {
				t.Fatalf("unexpected haStatus broker=%s", brokerAddr)
			}
			return &haStatusResult{
				Master: false,
				HAClientRuntimeInfo: haClientRuntimeInfo{
					MasterAddr:              "broker-master:10911",
					TransferredByteInSecond: 1536,
					MaxOffset:               397814681,
					LastReadTimestamp:       1780926000000,
					LastWriteTimestamp:      1780926060000,
					MasterFlushOffset:       397814680,
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"haStatus", "-b", "broker-a:10911"}, client)
	if err != nil {
		t.Fatalf("run native haStatus slave: %v", err)
	}
	if !supported {
		t.Fatalf("expected haStatus to be supported")
	}
	expected := "\n" +
		"#MasterAddr\tbroker-master:10911\n" +
		"#CommitLogMaxOffset\t397814681\n" +
		"#TransferSpeed(KB/s)\t1.50\n" +
		fmt.Sprintf("#LastReadTime\t%s\n", formatTraceTime(1780926000000)) +
		fmt.Sprintf("#LastWriteTime\t%s\n", formatTraceTime(1780926060000)) +
		"#MasterFlushOffset\t397814680\n"
	if output != expected {
		t.Fatalf("haStatus slave output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeHAStatusRequiresBrokerOrCluster(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{"haStatus"}, nativeClientFunc{})
	if err == nil {
		t.Fatalf("expected haStatus without args to fail")
	}
	if !supported || output != "" {
		t.Fatalf("expected haStatus missing args to be handled by native command, supported=%t output=%q", supported, output)
	}
}

func TestRunNativeCheckRocksdbCqWriteProgressFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		checkRocksdbCqWriteProgress: func(ctx context.Context, nameServer string, clusterName string, topic string, checkStoreTime int64) ([]checkRocksdbCqWriteProgressRow, error) {
			if nameServer != "127.0.0.1:9876" || clusterName != "DefaultCluster" || topic != "TopicTest" || checkStoreTime != 123 {
				t.Fatalf("unexpected checkRocksdbCqWriteProgress args namesrv=%s cluster=%s topic=%s checkStoreTime=%d", nameServer, clusterName, topic, checkStoreTime)
			}
			return []checkRocksdbCqWriteProgressRow{{BrokerName: "broker-a"}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"checkRocksdbCqWriteProgress",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-t", "TopicTest",
		"-cf", "123",
	}, client)
	if err != nil {
		t.Fatalf("run native checkRocksdbCqWriteProgress: %v", err)
	}
	if !supported {
		t.Fatalf("expected checkRocksdbCqWriteProgress to be supported")
	}
	expected := "broker-a check doing, please wait and get the result from log... \n"
	if output != expected {
		t.Fatalf("checkRocksdbCqWriteProgress output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestFormatCheckRocksdbCqWriteProgressFormatsOfficialError(t *testing.T) {
	output := formatCheckRocksdbCqWriteProgress([]checkRocksdbCqWriteProgressRow{{
		BrokerName: "broker-a",
		CheckError: true,
		ErrorInfo:  "rocksdb mismatch",
	}})
	expected := "broker-a check error, please check log... errInfo:rocksdb mismatch"
	if output != expected {
		t.Fatalf("checkRocksdbCqWriteProgress error format mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeRocksDBConfigToJsonFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		rocksDBConfigToJson: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, configTypes []string) error {
			if nameServer != "" || brokerAddr != "127.0.0.1:10911" || clusterName != "" {
				t.Fatalf("unexpected rocksDBConfigToJson broker args namesrv=%s broker=%s cluster=%s", nameServer, brokerAddr, clusterName)
			}
			expectedTypes := []string{"topics"}
			if !reflect.DeepEqual(configTypes, expectedTypes) {
				t.Fatalf("unexpected rocksDBConfigToJson broker configTypes expected=%#v actual=%#v", expectedTypes, configTypes)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-b", "127.0.0.1:10911",
		"-t", "topics",
	}, client)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson broker mode to be supported")
	}
	expected := "Use [rpc mode] call broker to export to json file \n" +
		"broker export done."
	if output != expected {
		t.Fatalf("rocksDBConfigToJson broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRocksDBConfigToJsonFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		rocksDBConfigToJson: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, configTypes []string) error {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected rocksDBConfigToJson cluster args namesrv=%s broker=%s cluster=%s", nameServer, brokerAddr, clusterName)
			}
			expectedTypes := []string{"topics", "subscriptionGroups", "consumerOffsets"}
			if !reflect.DeepEqual(configTypes, expectedTypes) {
				t.Fatalf("unexpected rocksDBConfigToJson cluster configTypes expected=%#v actual=%#v", expectedTypes, configTypes)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
	}, client)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson cluster mode to be supported")
	}
	expected := "Use [rpc mode] call all brokers in cluster to export to json file \n" +
		"broker export done."
	if output != expected {
		t.Fatalf("rocksDBConfigToJson cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRocksDBConfigToJsonLocalDefaultsToPrettyJSON(t *testing.T) {
	parentPath := writeRocksDBConfigToJsonFixtureForTest(t, "topics", map[string]string{
		"TopicA": `{"topicName":"TopicA","readQueueNums":4,"writeQueueNums":4}`,
		"TopicB": `{"topicName":"TopicB","readQueueNums":8,"writeQueueNums":8}`,
	})

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-p", parentPath,
		"-t", "topics",
	}, nil)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson local json: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson local mode to be supported")
	}
	expected := "Use [local mode] load rocksdb to print or export file \n" +
		"{\n" +
		"\t\"topicConfigTable\":{\n" +
		"\t\t\"TopicB\":{\n" +
		"\t\t\t\"readQueueNums\":8,\n" +
		"\t\t\t\"writeQueueNums\":8,\n" +
		"\t\t\t\"topicName\":\"TopicB\"\n" +
		"\t\t},\n" +
		"\t\t\"TopicA\":{\n" +
		"\t\t\t\"readQueueNums\":4,\n" +
		"\t\t\t\"writeQueueNums\":4,\n" +
		"\t\t\t\"topicName\":\"TopicA\"\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}\n"
	if output != expected {
		t.Fatalf("rocksDBConfigToJson local json output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRocksDBConfigToJsonLocalRawWhenJSONDisabled(t *testing.T) {
	parentPath := writeRocksDBConfigToJsonFixtureForTest(t, "subscriptionGroups", map[string]string{
		"GroupA": `{"groupName":"GroupA","consumeEnable":true}`,
		"GroupB": `{"groupName":"GroupB","consumeEnable":false}`,
	})

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-p", parentPath,
		"-t", "subscriptionGroups",
		"-j", "false",
	}, nil)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson local raw: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson local raw mode to be supported")
	}
	expected := "Use [local mode] load rocksdb to print or export file \n" +
		"type: subscriptionGroupTable" +
		"1, Key: GroupA, Value: {\"groupName\":\"GroupA\",\"consumeEnable\":true}\n" +
		"2, Key: GroupB, Value: {\"groupName\":\"GroupB\",\"consumeEnable\":false}\n"
	if output != expected {
		t.Fatalf("rocksDBConfigToJson local raw output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRocksDBConfigToJsonLocalExportFileIgnoresJSONDisable(t *testing.T) {
	parentPath := writeRocksDBConfigToJsonFixtureForTest(t, "topics", map[string]string{
		"TopicA": `{"topicName":"TopicA","readQueueNums":4,"writeQueueNums":4}`,
		"TopicB": `{"topicName":"TopicB","readQueueNums":8,"writeQueueNums":8}`,
	})
	exportFile := filepath.Join(t.TempDir(), "rocksdb-config.json")

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-p", parentPath,
		"-t", "topics",
		"-j", "false",
		"-e", exportFile,
	}, nil)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson local export file: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson local export file mode to be supported")
	}
	if output != "Use [local mode] load rocksdb to print or export file \n" {
		t.Fatalf("rocksDBConfigToJson local export stdout mismatch: %q", output)
	}
	content, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("read export file: %v", err)
	}
	expected := "{\n" +
		"\t\"topicConfigTable\":{\n" +
		"\t\t\"TopicB\":{\n" +
		"\t\t\t\"readQueueNums\":8,\n" +
		"\t\t\t\"writeQueueNums\":8,\n" +
		"\t\t\t\"topicName\":\"TopicB\"\n" +
		"\t\t},\n" +
		"\t\t\"TopicA\":{\n" +
		"\t\t\t\"readQueueNums\":4,\n" +
		"\t\t\t\"writeQueueNums\":4,\n" +
		"\t\t\t\"topicName\":\"TopicA\"\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}"
	if string(content) != expected {
		t.Fatalf("rocksDBConfigToJson local export file mismatch\nexpected:%q\nactual:%q", expected, string(content))
	}
}

func TestRunNativeRocksDBConfigToJsonLocalConsumerOffsetsPrettyJSON(t *testing.T) {
	parentPath := writeRocksDBConfigToJsonFixtureForTest(t, "consumerOffsets", map[string]string{
		"GoadminTopicA@GoadminGroupA": `{"offsetTable":{"0":123,"1":456}}`,
		"GoadminTopicB@GoadminGroupB": `{"offsetTable":{"0":789}}`,
	})

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-p", parentPath,
		"-t", "consumerOffsets",
	}, nil)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson local consumerOffsets json: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson local consumerOffsets mode to be supported")
	}
	expected := "Use [local mode] load rocksdb to print or export file \n" +
		"{\n" +
		"\t\"offsetTable\":{\n" +
		"\t\t\"GoadminTopicA@GoadminGroupA\":{0:123,1:456\n" +
		"\t\t},\n" +
		"\t\t\"GoadminTopicB@GoadminGroupB\":{0:789\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}\n"
	if output != expected {
		t.Fatalf("rocksDBConfigToJson local consumerOffsets json output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRocksDBConfigToJsonLocalConsumerOffsetsRawWhenJSONDisabled(t *testing.T) {
	parentPath := writeRocksDBConfigToJsonFixtureForTest(t, "consumerOffsets", map[string]string{
		"GoadminTopicA@GoadminGroupA": `{"offsetTable":{"0":123,"1":456}}`,
		"GoadminTopicB@GoadminGroupB": `{"offsetTable":{"0":789}}`,
	})

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-p", parentPath,
		"-t", "consumerOffsets",
		"-j", "false",
	}, nil)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson local consumerOffsets raw: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson local consumerOffsets raw mode to be supported")
	}
	expected := "Use [local mode] load rocksdb to print or export file \n" +
		"type: offsetTable" +
		"1, Key: GoadminTopicA@GoadminGroupA, Value: {0=123, 1=456}\n" +
		"2, Key: GoadminTopicB@GoadminGroupB, Value: {0=789}\n"
	if output != expected {
		t.Fatalf("rocksDBConfigToJson local consumerOffsets raw output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRocksDBConfigToJsonLocalConsumerOffsetsExportFile(t *testing.T) {
	parentPath := writeRocksDBConfigToJsonFixtureForTest(t, "consumerOffsets", map[string]string{
		"GoadminTopicA@GoadminGroupA": `{"offsetTable":{"0":123,"1":456}}`,
		"GoadminTopicB@GoadminGroupB": `{"offsetTable":{"0":789}}`,
	})
	exportFile := filepath.Join(t.TempDir(), "consumer-offsets.json")

	output, supported, err := runNativeCommand(context.Background(), []string{
		"rocksDBConfigToJson",
		"-p", parentPath,
		"-t", "consumerOffsets",
		"-j", "false",
		"-e", exportFile,
	}, nil)
	if err != nil {
		t.Fatalf("run native rocksDBConfigToJson local consumerOffsets export file: %v", err)
	}
	if !supported {
		t.Fatalf("expected rocksDBConfigToJson local consumerOffsets export mode to be supported")
	}
	if output != "Use [local mode] load rocksdb to print or export file \n" {
		t.Fatalf("rocksDBConfigToJson local consumerOffsets export stdout mismatch: %q", output)
	}
	content, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("read consumerOffsets export file: %v", err)
	}
	expected := "{\n" +
		"\t\"offsetTable\":{\n" +
		"\t\t\"GoadminTopicA@GoadminGroupA\":{0:123,1:456\n" +
		"\t\t},\n" +
		"\t\t\"GoadminTopicB@GoadminGroupB\":{0:789\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}"
	if string(content) != expected {
		t.Fatalf("rocksDBConfigToJson local consumerOffsets export file mismatch\nexpected:%q\nactual:%q", expected, string(content))
	}
}

func TestRunNativeExportMetadataInRocksDBFormatsOfficialLocalValidation(t *testing.T) {
	existingPath := t.TempDir()
	cases := []struct {
		name     string
		args     []string
		expected string
	}{
		{
			name:     "missing path",
			args:     []string{"exportMetadataInRocksDB", "-p", filepath.Join(t.TempDir(), "missing"), "-t", "topics"},
			expected: "RocksDB path is invalid.\n",
		},
		{
			name:     "invalid config type",
			args:     []string{"exportMetadataInRocksDB", "-p", existingPath, "-t", "consumerOffsets"},
			expected: "RocksDB load error, path=" + existingPath + "/consumerOffsets\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, supported, err := runNativeCommand(context.Background(), tc.args, nil)
			if err != nil {
				t.Fatalf("run native exportMetadataInRocksDB: %v", err)
			}
			if !supported {
				t.Fatalf("expected exportMetadataInRocksDB to be supported")
			}
			if output != tc.expected {
				t.Fatalf("exportMetadataInRocksDB output mismatch\nexpected:%q\nactual:%q", tc.expected, output)
			}
		})
	}
}

func TestRunNativeExportMetadataInRocksDBFormatsRawRows(t *testing.T) {
	parentPath := writeExportMetadataFixtureForTest(t, "topics", map[string]string{
		"TopicA": `{"topicName":"TopicA","readQueueNums":4,"writeQueueNums":4}`,
		"TopicB": `{"topicName":"TopicB","readQueueNums":8,"writeQueueNums":8}`,
	})

	output, supported, err := runNativeCommand(context.Background(), []string{
		"exportMetadataInRocksDB",
		"-p", parentPath,
		"-t", "topics",
	}, nil)
	if err != nil {
		t.Fatalf("run native exportMetadataInRocksDB raw: %v", err)
	}
	if !supported {
		t.Fatalf("expected exportMetadataInRocksDB to be supported")
	}
	expected := "1, Key: TopicA, Value: {\"topicName\":\"TopicA\",\"readQueueNums\":4,\"writeQueueNums\":4}\n" +
		"2, Key: TopicB, Value: {\"topicName\":\"TopicB\",\"readQueueNums\":8,\"writeQueueNums\":8}\n"
	if output != expected {
		t.Fatalf("exportMetadataInRocksDB raw output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeExportMetadataInRocksDBFormatsPrettyJSON(t *testing.T) {
	parentPath := writeExportMetadataFixtureForTest(t, "subscriptionGroups", map[string]string{
		"GroupA": `{"groupName":"GroupA","consumeEnable":true}`,
		"GroupB": `{"groupName":"GroupB","consumeEnable":false}`,
	})

	output, supported, err := runNativeCommand(context.Background(), []string{
		"exportMetadataInRocksDB",
		"-p", parentPath,
		"-t", "subscriptionGroups",
		"-j", "true",
	}, nil)
	if err != nil {
		t.Fatalf("run native exportMetadataInRocksDB json: %v", err)
	}
	if !supported {
		t.Fatalf("expected exportMetadataInRocksDB to be supported")
	}
	expected := "{\n" +
		"\t\"subscriptionGroupTable\":{\n" +
		"\t\t\"GroupA\":{\n" +
		"\t\t\t\"groupName\":\"GroupA\",\n" +
		"\t\t\t\"consumeEnable\":true\n" +
		"\t\t},\n" +
		"\t\t\"GroupB\":{\n" +
		"\t\t\t\"groupName\":\"GroupB\",\n" +
		"\t\t\t\"consumeEnable\":false\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}\n"
	if output != expected {
		t.Fatalf("exportMetadataInRocksDB json output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeExportPopRecordFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		exportPopRecord: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, dryRun bool) ([]exportPopRecordRow, error) {
			if nameServer != "" || brokerAddr != "127.0.0.1:10911" || clusterName != "" || !dryRun {
				t.Fatalf("unexpected exportPopRecord broker args namesrv=%s broker=%s cluster=%s dryRun=%v", nameServer, brokerAddr, clusterName, dryRun)
			}
			return []exportPopRecordRow{{BrokerName: "broker-a", BrokerAddr: "127.0.0.1:10911", DryRun: dryRun}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"exportPopRecord",
		"-b", "127.0.0.1:10911",
		"-d", "false",
	}, client)
	if err != nil {
		t.Fatalf("run native exportPopRecord broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected exportPopRecord broker mode to be supported")
	}
	expected := "Export broker records, brokerName=broker-a, brokerAddr=127.0.0.1:10911, dryRun=true\n"
	if output != expected {
		t.Fatalf("exportPopRecord broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeExportPopRecordFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		exportPopRecord: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, dryRun bool) ([]exportPopRecordRow, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" || !dryRun {
				t.Fatalf("unexpected exportPopRecord cluster args namesrv=%s broker=%s cluster=%s dryRun=%v", nameServer, brokerAddr, clusterName, dryRun)
			}
			return []exportPopRecordRow{
				{BrokerName: "broker-a", BrokerAddr: "127.0.0.1:10911", DryRun: dryRun},
				{BrokerName: "broker-b", BrokerAddr: "127.0.0.1:10912", DryRun: dryRun, Err: fmt.Errorf("request failed")},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"exportPopRecord",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-d", "false",
	}, client)
	if err != nil {
		t.Fatalf("run native exportPopRecord cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected exportPopRecord cluster mode to be supported")
	}
	expected := "Export broker records, brokerName=broker-a, brokerAddr=127.0.0.1:10911, dryRun=true\n" +
		"Export broker records error, brokerName=broker-b, brokerAddr=127.0.0.1:10912, dryRun=true\nrequest failed"
	if output != expected {
		t.Fatalf("exportPopRecord cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateKvConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateKvConfig: func(ctx context.Context, nameServers string, namespace string, key string, value string) error {
			if nameServers != "127.0.0.1:9876" || namespace != "GoadminParityKV" || key != "sample-key" || value != "sample-value" {
				t.Fatalf("unexpected updateKvConfig args namesrv=%s namespace=%s key=%s value=%s", nameServers, namespace, key, value)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateKvConfig",
		"-n", "127.0.0.1:9876",
		"-s", "GoadminParityKV",
		"-k", "sample-key",
		"-v", "sample-value",
	}, client)
	if err != nil {
		t.Fatalf("run native updateKvConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateKvConfig to be supported")
	}
	expected := "create or update kv config to namespace success.\n"
	if output != expected {
		t.Fatalf("updateKvConfig output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeDeleteKvConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteKvConfig: func(ctx context.Context, nameServers string, namespace string, key string) error {
			if nameServers != "127.0.0.1:9876" || namespace != "GoadminParityKV" || key != "sample-key" {
				t.Fatalf("unexpected deleteKvConfig args namesrv=%s namespace=%s key=%s", nameServers, namespace, key)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteKvConfig",
		"-n", "127.0.0.1:9876",
		"-s", "GoadminParityKV",
		"-k", "sample-key",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteKvConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteKvConfig to be supported")
	}
	expected := "delete kv config from namespace success.\n"
	if output != expected {
		t.Fatalf("deleteKvConfig output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateOrderConfFormatsOfficialOutput(t *testing.T) {
	calls := make([]string, 0, 3)
	client := nativeClientFunc{
		updateKvConfig: func(ctx context.Context, nameServers string, namespace string, key string, value string) error {
			calls = append(calls, "put")
			if nameServers != "127.0.0.1:9876" || namespace != "ORDER_TOPIC_CONFIG" || key != "GoadminOrderTopic" || value != "broker-a:1" {
				t.Fatalf("unexpected updateOrderConf put args namesrv=%s namespace=%s key=%s value=%s", nameServers, namespace, key, value)
			}
			return nil
		},
		getKvConfig: func(ctx context.Context, nameServers string, namespace string, key string) (string, error) {
			calls = append(calls, "get")
			if nameServers != "127.0.0.1:9876" || namespace != "ORDER_TOPIC_CONFIG" || key != "GoadminOrderTopic" {
				t.Fatalf("unexpected updateOrderConf get args namesrv=%s namespace=%s key=%s", nameServers, namespace, key)
			}
			return "broker-a:1", nil
		},
		deleteKvConfig: func(ctx context.Context, nameServers string, namespace string, key string) error {
			calls = append(calls, "delete")
			if nameServers != "127.0.0.1:9876" || namespace != "ORDER_TOPIC_CONFIG" || key != "GoadminOrderTopic" {
				t.Fatalf("unexpected updateOrderConf delete args namesrv=%s namespace=%s key=%s", nameServers, namespace, key)
			}
			return nil
		},
	}

	cases := []struct {
		args     []string
		expected string
	}{
		{
			args:     []string{"updateOrderConf", "-n", "127.0.0.1:9876", "-m", "put", "-t", "GoadminOrderTopic", "-v", "broker-a:1"},
			expected: "update orderConf success. topic=[GoadminOrderTopic], orderConf=[broker-a:1]",
		},
		{
			args:     []string{"updateOrderConf", "-n", "127.0.0.1:9876", "-m", "get", "-t", "GoadminOrderTopic"},
			expected: "get orderConf success. topic=[GoadminOrderTopic], orderConf=[broker-a:1] ",
		},
		{
			args:     []string{"updateOrderConf", "-n", "127.0.0.1:9876", "-m", "delete", "-t", "GoadminOrderTopic"},
			expected: "delete orderConf success. topic=[GoadminOrderTopic]",
		},
	}
	for _, tc := range cases {
		output, supported, err := runNativeCommand(context.Background(), tc.args, client)
		if err != nil {
			t.Fatalf("run native updateOrderConf args=%v: %v", tc.args, err)
		}
		if !supported {
			t.Fatalf("expected updateOrderConf to be supported")
		}
		if output != tc.expected {
			t.Fatalf("updateOrderConf output mismatch\nexpected:%q\nactual:%q", tc.expected, output)
		}
	}
	if !reflect.DeepEqual(calls, []string{"put", "get", "delete"}) {
		t.Fatalf("unexpected updateOrderConf call order %#v", calls)
	}
}

func TestRunNativeUpdateBrokerConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateBrokerConfig: func(ctx context.Context, nameServer string, options updateBrokerConfigOptions) ([]string, error) {
			expected := updateBrokerConfigOptions{
				NameServer:      "127.0.0.1:9876",
				ClusterName:     "DefaultCluster",
				Key:             "enableDetailStat",
				Value:           "true",
				UpdateAllBroker: true,
			}
			if options != expected {
				t.Fatalf("unexpected updateBrokerConfig options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected updateBrokerConfig namesrv %s", nameServer)
			}
			return []string{"broker-a:10911", "broker-b:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateBrokerConfig",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-k", "enableDetailStat",
		"-v", "true",
		"-a", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native updateBrokerConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateBrokerConfig to be supported")
	}
	expected := "update broker config success, broker-a:10911\n" +
		"update broker config success, broker-b:10911\n"
	if output != expected {
		t.Fatalf("updateBrokerConfig output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateBrokerConfigBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateBrokerConfig: func(ctx context.Context, nameServer string, options updateBrokerConfigOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected updateBrokerConfig namesrv %s", nameServer)
			}
			if options.BrokerAddr != "broker-a:10911" || options.Key != "enableDetailStat" || options.Value != "true" || options.UpdateAllBroker {
				t.Fatalf("unexpected broker updateBrokerConfig options %#v", options)
			}
			return []string{"broker-a:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateBrokerConfig",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-k", "enableDetailStat",
		"-v", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native updateBrokerConfig broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateBrokerConfig broker to be supported")
	}
	expected := "update broker config success, broker-a:10911\n"
	if output != expected {
		t.Fatalf("updateBrokerConfig broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateNamesrvConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateNamesrvConfig: func(ctx context.Context, nameServers string, options updateNamesrvConfigOptions) ([]string, error) {
			expected := updateNamesrvConfigOptions{
				NameServers: "ns-a:9876;ns-b:9876",
				Key:         "clusterTest",
				Value:       "false",
			}
			if options != expected {
				t.Fatalf("unexpected updateNamesrvConfig options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if nameServers != "ns-a:9876;ns-b:9876" {
				t.Fatalf("unexpected updateNamesrvConfig namesrv %s", nameServers)
			}
			return []string{"ns-a:9876", "ns-b:9876"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateNamesrvConfig",
		"-n", "ns-a:9876;ns-b:9876",
		"-k", "clusterTest",
		"-v", "false",
	}, client)
	if err != nil {
		t.Fatalf("run native updateNamesrvConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateNamesrvConfig to be supported")
	}
	expected := "update name server config success![ns-a:9876, ns-b:9876]\nclusterTest : false\n"
	if output != expected {
		t.Fatalf("updateNamesrvConfig output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateControllerConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateControllerConfig: func(ctx context.Context, controllerAddrs string, options updateControllerConfigOptions) ([]string, error) {
			expected := updateControllerConfigOptions{
				ControllerAddrs: "127.0.0.1:9878",
				Key:             "controllerDLegerGroup",
				Value:           "group1",
			}
			if options != expected {
				t.Fatalf("unexpected updateControllerConfig options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if controllerAddrs != "127.0.0.1:9878" {
				t.Fatalf("unexpected updateControllerConfig controllerAddrs %s", controllerAddrs)
			}
			return []string{"127.0.0.1:9878"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateControllerConfig",
		"-a", "127.0.0.1:9878",
		"-k", "controllerDLegerGroup",
		"-v", "group1",
	}, client)
	if err != nil {
		t.Fatalf("run native updateControllerConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateControllerConfig to be supported")
	}
	expected := "update controller config success![127.0.0.1:9878]\ncontrollerDLegerGroup : group1\n"
	if output != expected {
		t.Fatalf("updateControllerConfig output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeWipeWritePermFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		wipeWritePerm: func(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error) {
			if nameServers != "ns-a:9876;ns-b:9876" {
				t.Fatalf("unexpected wipeWritePerm namesrv %s", nameServers)
			}
			if brokerName != "broker-a" {
				t.Fatalf("unexpected wipeWritePerm brokerName %s", brokerName)
			}
			return []writePermResult{
				{NameServer: "ns-a:9876", Count: 12},
				{NameServer: "ns-b:9876", Count: 13},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"wipeWritePerm",
		"-n", "ns-a:9876;ns-b:9876",
		"-b", "broker-a",
	}, client)
	if err != nil {
		t.Fatalf("run native wipeWritePerm: %v", err)
	}
	if !supported {
		t.Fatalf("expected wipeWritePerm to be supported")
	}
	expected := "wipe write perm of broker[broker-a] in name server[ns-a:9876] OK, 12\n" +
		"wipe write perm of broker[broker-a] in name server[ns-b:9876] OK, 13\n"
	if output != expected {
		t.Fatalf("wipeWritePerm output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeAddWritePermFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		addWritePerm: func(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error) {
			if nameServers != "ns-a:9876;ns-b:9876" {
				t.Fatalf("unexpected addWritePerm namesrv %s", nameServers)
			}
			if brokerName != "broker-a" {
				t.Fatalf("unexpected addWritePerm brokerName %s", brokerName)
			}
			return []writePermResult{
				{NameServer: "ns-a:9876", Count: 12},
				{NameServer: "ns-b:9876", Count: 13},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"addWritePerm",
		"-n", "ns-a:9876;ns-b:9876",
		"-b", "broker-a",
	}, client)
	if err != nil {
		t.Fatalf("run native addWritePerm: %v", err)
	}
	if !supported {
		t.Fatalf("expected addWritePerm to be supported")
	}
	expected := "add write perm of broker[broker-a] in name server[ns-a:9876] OK, 12\n" +
		"add write perm of broker[broker-a] in name server[ns-b:9876] OK, 13\n"
	if output != expected {
		t.Fatalf("addWritePerm output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeCloneGroupOffsetFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		cloneGroupOffset: func(ctx context.Context, nameServer string, srcGroup string, destGroup string, topic string) error {
			if nameServer != "ns-a:9876" {
				t.Fatalf("unexpected cloneGroupOffset namesrv %s", nameServer)
			}
			if srcGroup != "src-group" || destGroup != "dest-group" || topic != "TopicTest" {
				t.Fatalf("unexpected cloneGroupOffset args src=%s dest=%s topic=%s", srcGroup, destGroup, topic)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"cloneGroupOffset",
		"-n", "ns-a:9876",
		"-s", "src-group",
		"-d", "dest-group",
		"-t", "TopicTest",
		"-o", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native cloneGroupOffset: %v", err)
	}
	if !supported {
		t.Fatalf("expected cloneGroupOffset to be supported")
	}
	expected := "clone group offset success. srcGroup[src-group], destGroup=[dest-group], topic[TopicTest]"
	if output != expected {
		t.Fatalf("cloneGroupOffset output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeCloneGroupOffsetOfflineRequiresValue(t *testing.T) {
	client := nativeClientFunc{
		cloneGroupOffset: func(ctx context.Context, nameServer string, srcGroup string, destGroup string, topic string) error {
			t.Fatalf("cloneGroupOffset should not run when offline value is missing")
			return nil
		},
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{
			name: "short option at end",
			args: []string{
				"cloneGroupOffset",
				"-n", "ns-a:9876",
				"-s", "src-group",
				"-d", "dest-group",
				"-t", "TopicTest",
				"-o",
			},
		},
		{
			name: "long option at end",
			args: []string{
				"cloneGroupOffset",
				"-n", "ns-a:9876",
				"-s", "src-group",
				"-d", "dest-group",
				"-t", "TopicTest",
				"--offline",
			},
		},
		{
			name: "known option after offline",
			args: []string{
				"cloneGroupOffset",
				"-n", "ns-a:9876",
				"-o",
				"-s", "src-group",
				"-d", "dest-group",
				"-t", "TopicTest",
			},
		},
		{
			name: "second offline option at end",
			args: []string{
				"cloneGroupOffset",
				"-n", "ns-a:9876",
				"-s", "src-group",
				"-d", "dest-group",
				"-t", "TopicTest",
				"-o", "true",
				"-o",
			},
		},
	} {
		output, supported, err := runNativeCommand(context.Background(), tc.args, client)
		if err == nil || err.Error() != "Missing argument for option: o" {
			t.Fatalf("expected official offline missing-value error for %s, output=%q supported=%t err=%v", tc.name, output, supported, err)
		}
		if !supported || !strings.HasPrefix(output, "usage: mqadmin cloneGroupOffset") {
			t.Fatalf("expected cloneGroupOffset offline missing-value to be handled by native command for %s, supported=%t output=%q", tc.name, supported, output)
		}
	}
}

func TestRunNativeSendMessageFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		sendMessage: func(ctx context.Context, nameServer string, options sendMessageOptions) (*sendMessageResult, error) {
			expected := sendMessageOptions{
				Topic:      "TopicTest",
				Body:       "hello",
				Keys:       "KeyA",
				Tags:       "TagA",
				BrokerName: "broker-a",
				QueueID:    2,
				HasQueueID: true,
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected sendMessage args namesrv=%s options=%#v", nameServer, options)
			}
			return &sendMessageResult{BrokerName: "broker-a", QueueID: 2, SendStatus: "SEND_OK", MessageID: "UNIQ-1"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"sendMessage",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-p", "hello",
		"-k", "KeyA",
		"-c", "TagA",
		"-b", "broker-a",
		"-i", "2",
	}, client)
	if err != nil {
		t.Fatalf("run native sendMessage: %v", err)
	}
	if !supported {
		t.Fatalf("expected sendMessage to be supported")
	}
	expected := fmt.Sprintf("%-32s  %-4s  %-20s    %s\n", "#Broker Name", "#QID", "#Send Result", "#MsgId") +
		fmt.Sprintf("%-32s  %-4d  %-20s    %s\n", "broker-a", 2, "SEND_OK", "UNIQ-1")
	if output != expected {
		t.Fatalf("sendMessage output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeSendMessageTraceFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		sendMessage: func(ctx context.Context, nameServer string, options sendMessageOptions) (*sendMessageResult, error) {
			expected := sendMessageOptions{
				Topic:          "TopicTest",
				Body:           "hello",
				MsgTraceEnable: true,
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected sendMessage trace args namesrv=%s options=%#v", nameServer, options)
			}
			return &sendMessageResult{BrokerName: "broker-a", QueueID: 0, SendStatus: "SEND_OK", MessageID: "UNIQ-TRACE"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"sendMessage",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-p", "hello",
		"-m", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native sendMessage trace: %v", err)
	}
	if !supported {
		t.Fatalf("expected sendMessage -m true to be supported")
	}
	expected := fmt.Sprintf("%-32s  %-4s  %-20s    %s\n", "#Broker Name", "#QID", "#Send Result", "#MsgId") +
		fmt.Sprintf("%-32s  %-4d  %-20s    %s\n", "broker-a", 0, "SEND_OK", "UNIQ-TRACE")
	if output != expected {
		t.Fatalf("sendMessage trace output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeSendMessageRequiresBrokerWhenQueueChosen(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{
		"sendMessage",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-p", "hello",
		"-i", "2",
	}, nativeClientFunc{})
	if err != nil {
		t.Fatalf("run native sendMessage: %v", err)
	}
	if !supported {
		t.Fatalf("expected sendMessage to be supported")
	}
	if output != "Broker name must be set if the queue is chosen!" {
		t.Fatalf("unexpected queue validation output %q", output)
	}
}

func TestRunNativeSendMsgStatusFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		sendMsgStatus: func(ctx context.Context, nameServer string, options sendMsgStatusOptions) ([]sendMsgStatusResult, error) {
			expected := sendMsgStatusOptions{
				BrokerName:  "broker-a",
				MessageSize: 16,
				Count:       2,
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected sendMsgStatus args namesrv=%s options=%#v", nameServer, options)
			}
			return []sendMsgStatusResult{
				{RTMillis: 3, SendResult: sendMessageResult{Topic: "broker-a", BrokerName: "broker-a", QueueID: 0, SendStatus: "SEND_OK", MessageID: "UNIQ-1", OffsetMessageID: "OFFSET-1", QueueOffset: 7}},
				{RTMillis: 4, SendResult: sendMessageResult{Topic: "broker-a", BrokerName: "broker-a", QueueID: 0, SendStatus: "SEND_OK", MessageID: "UNIQ-2", OffsetMessageID: "OFFSET-2", QueueOffset: 8}},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"sendMsgStatus",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a",
		"-s", "16",
		"-c", "2",
	}, client)
	if err != nil {
		t.Fatalf("run native sendMsgStatus: %v", err)
	}
	if !supported {
		t.Fatalf("expected sendMsgStatus to be supported")
	}
	expected := "rt=3ms, SendResult=SendResult [sendStatus=SEND_OK, msgId=UNIQ-1, offsetMsgId=OFFSET-1, messageQueue=MessageQueue [topic=broker-a, brokerName=broker-a, queueId=0], queueOffset=7, recallHandle=null]\n" +
		"rt=4ms, SendResult=SendResult [sendStatus=SEND_OK, msgId=UNIQ-2, offsetMsgId=OFFSET-2, messageQueue=MessageQueue [topic=broker-a, brokerName=broker-a, queueId=0], queueOffset=8, recallHandle=null]\n"
	if output != expected {
		t.Fatalf("sendMsgStatus output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeCheckMsgSendRTFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		checkMsgSendRT: func(ctx context.Context, nameServer string, options checkMsgSendRTOptions) (*checkMsgSendRTResult, error) {
			expected := checkMsgSendRTOptions{
				Topic:  "TopicTest",
				Amount: 2,
				Size:   16,
			}
			if nameServer != "127.0.0.1:9876" || options != expected {
				t.Fatalf("unexpected checkMsgSendRT args namesrv=%s options=%#v", nameServer, options)
			}
			return &checkMsgSendRTResult{
				Rows: []checkMsgSendRTRow{
					{BrokerName: "broker-a", QueueID: 0, SendSuccess: true, RTMillis: 11},
					{BrokerName: "broker-a", QueueID: 1, SendSuccess: false, RTMillis: 3},
				},
				AvgRT: 3,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"checkMsgSendRT",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-s", "16",
		"-a", "2",
	}, client)
	if err != nil {
		t.Fatalf("run native checkMsgSendRT: %v", err)
	}
	if !supported {
		t.Fatalf("expected checkMsgSendRT to be supported")
	}
	expected := "#Broker Name                      #QID  #Send Result            #RT\n" +
		"broker-a                          0     true                    11\n" +
		"broker-a                          1     false                   3\n" +
		"Avg RT: 3.00\n"
	if output != expected {
		t.Fatalf("checkMsgSendRT output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeClusterRTFormatsOfficialTable(t *testing.T) {
	client := nativeClientFunc{
		clusterRT: func(ctx context.Context, nameServer string, options clusterRTOptions) (*clusterRTResult, error) {
			expected := clusterRTOptions{
				ClusterName: "DefaultCluster",
				Amount:      2,
				Size:        16,
				MachineRoom: "noname",
			}
			if nameServer != "127.0.0.1:9876" || options != expected {
				t.Fatalf("unexpected clusterRT args namesrv=%s options=%#v", nameServer, options)
			}
			return &clusterRTResult{Rows: []clusterRTRow{{
				ClusterName:  "DefaultCluster",
				BrokerName:   "broker-a",
				RT:           3.5,
				SuccessCount: 2,
				FailCount:    0,
			}}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"clusterRT",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-a", "2",
		"-s", "16",
	}, client)
	if err != nil {
		t.Fatalf("run native clusterRT: %v", err)
	}
	if !supported {
		t.Fatalf("expected clusterRT to be supported")
	}
	expected := "#Cluster Name             #Broker Name              #RT   #successCount  #failCount\n" +
		"DefaultCluster            broker-a                  3.50      2                 0               \n"
	if output != expected {
		t.Fatalf("clusterRT output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeClusterRTFormatsOfficialTlog(t *testing.T) {
	timestamp := time.Date(2026, 6, 12, 1, 2, 3, 0, time.FixedZone("GMT+8", 8*60*60))
	client := nativeClientFunc{
		clusterRT: func(ctx context.Context, nameServer string, options clusterRTOptions) (*clusterRTResult, error) {
			expected := clusterRTOptions{
				ClusterName: "DefaultCluster",
				Amount:      2,
				Size:        16,
				PrintAsTlog: true,
				MachineRoom: "room-a",
			}
			if nameServer != "127.0.0.1:9876" || options != expected {
				t.Fatalf("unexpected clusterRT tlog args namesrv=%s options=%#v", nameServer, options)
			}
			return &clusterRTResult{Rows: []clusterRTRow{{
				ClusterName:  "DefaultCluster",
				BrokerName:   "broker-a",
				RT:           3.5,
				SuccessCount: 2,
				Timestamp:    timestamp,
			}}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"clusterRT",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-a", "2",
		"-s", "16",
		"-p", "true",
		"-m", "room-a",
	}, client)
	if err != nil {
		t.Fatalf("run native clusterRT tlog: %v", err)
	}
	if !supported {
		t.Fatalf("expected clusterRT tlog to be supported")
	}
	expected := "2026-06-12 01:02:03|room-a|DefaultCluster|broker-a|4\n"
	if output != expected {
		t.Fatalf("clusterRT tlog output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeResetMasterFlushOffsetFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		resetMasterFlushOffset: func(ctx context.Context, brokerAddr string, offset int64) error {
			if brokerAddr != "broker-a:10911" || offset != 42 {
				t.Fatalf("unexpected resetMasterFlushOffset args broker=%s offset=%d", brokerAddr, offset)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"resetMasterFlushOffset",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-o", "42",
	}, client)
	if err != nil {
		t.Fatalf("run native resetMasterFlushOffset: %v", err)
	}
	if !supported {
		t.Fatalf("expected resetMasterFlushOffset to be supported")
	}
	expected := "reset master flush offset to 42 success\n"
	if output != expected {
		t.Fatalf("resetMasterFlushOffset output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeCleanBrokerMetadataFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		cleanBrokerMetadata: func(ctx context.Context, controllerAddr string, options cleanBrokerMetadataOptions) error {
			expected := cleanBrokerMetadataOptions{
				ControllerAddr:             "127.0.0.1:9878",
				ClusterName:                "GoadminControllerCluster",
				BrokerName:                 "goadmin-controller-broker",
				BrokerControllerIDsToClean: "1;2",
				CleanLivingBroker:          true,
			}
			if controllerAddr != expected.ControllerAddr {
				t.Fatalf("unexpected cleanBrokerMetadata controllerAddr %s", controllerAddr)
			}
			if !reflect.DeepEqual(options, expected) {
				t.Fatalf("cleanBrokerMetadata options mismatch\nexpected:%#v\nactual:%#v", expected, options)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"cleanBrokerMetadata",
		"-a", "127.0.0.1:9878",
		"-c", "GoadminControllerCluster",
		"-bn", "goadmin-controller-broker",
		"-b", "1;2",
		"-l",
	}, client)
	if err != nil {
		t.Fatalf("run native cleanBrokerMetadata: %v", err)
	}
	if !supported {
		t.Fatalf("expected cleanBrokerMetadata to be supported")
	}
	expected := "clear broker goadmin-controller-broker metadata from controller success! \n"
	if output != expected {
		t.Fatalf("cleanBrokerMetadata output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeCleanBrokerMetadataRejectsMissingClusterWithoutLivingFlag(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{
		"cleanBrokerMetadata",
		"-a", "127.0.0.1:9878",
		"-bn", "goadmin-controller-broker",
	}, nil)
	if err == nil {
		t.Fatalf("expected missing clusterName error, output=%q supported=%v", output, supported)
	}
	if !supported {
		t.Fatalf("missing clusterName is a native validation error, not fallback")
	}
	if err.Error() != "cleanLivingBroker option is false, clusterName option can not be empty." {
		t.Fatalf("unexpected error %q", err.Error())
	}
}

func TestRunNativeElectMasterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		electMaster: func(ctx context.Context, controllerAddr string, options electMasterOptions) (*electMasterResult, error) {
			expected := electMasterOptions{
				ControllerAddr: "127.0.0.1:9878",
				ClusterName:    "GoadminElectPairCluster",
				BrokerName:     "goadmin-elect-pair-broker",
				BrokerID:       4,
			}
			if controllerAddr != expected.ControllerAddr {
				t.Fatalf("unexpected electMaster controllerAddr %s", controllerAddr)
			}
			if !reflect.DeepEqual(options, expected) {
				t.Fatalf("electMaster options mismatch\nexpected:%#v\nactual:%#v", expected, options)
			}
			return &electMasterResult{
				ClusterName:         options.ClusterName,
				BrokerName:          options.BrokerName,
				BrokerMasterAddr:    "172.24.0.4:30992",
				MasterEpoch:         6,
				SyncStateSetEpoch:   4,
				BrokerMemberAddrs:   nil,
				BrokerMemberAddrsOK: true,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"electMaster",
		"-a", "127.0.0.1:9878",
		"-c", "GoadminElectPairCluster",
		"-bn", "goadmin-elect-pair-broker",
		"-b", "4",
	}, client)
	if err != nil {
		t.Fatalf("run native electMaster: %v", err)
	}
	if !supported {
		t.Fatalf("expected electMaster to be supported")
	}
	expected := "\n#ClusterName\tGoadminElectPairCluster\n#BrokerName\tgoadmin-elect-pair-broker\n#BrokerMasterAddr\t172.24.0.4:30992\n#MasterEpoch\t6\n#SyncStateSetEpoch\t4\n"
	if output != expected {
		t.Fatalf("electMaster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeElectMasterRejectsMissingRequiredOptions(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{
		"electMaster",
		"-a", "127.0.0.1:9878",
		"-c", "GoadminElectPairCluster",
		"-bn", "goadmin-elect-pair-broker",
	}, nil)
	if err == nil {
		t.Fatalf("expected missing brokerId error, output=%q supported=%v", output, supported)
	}
	if !supported {
		t.Fatalf("missing brokerId is a native validation error, not fallback")
	}
	if err.Error() != "BrokerId 必填" {
		t.Fatalf("unexpected error %q", err.Error())
	}
}

func TestRunNativeGetControllerMetaDataFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		getControllerMetaData: func(ctx context.Context, controllerAddr string) (*controllerMetaData, error) {
			if controllerAddr != "127.0.0.1:9878" {
				t.Fatalf("unexpected controller address %s", controllerAddr)
			}
			return &controllerMetaData{
				Group:                   "group1",
				ControllerLeaderID:      "n0",
				ControllerLeaderAddress: "127.0.0.1:9878",
				Peers:                   "n0:127.0.0.1:9878;n1:127.0.0.1:9879",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getControllerMetaData",
		"-a", "127.0.0.1:9878",
	}, client)
	if err != nil {
		t.Fatalf("run native getControllerMetaData: %v", err)
	}
	if !supported {
		t.Fatalf("expected getControllerMetaData to be supported")
	}
	expected := "\n#ControllerGroup\tgroup1" +
		"\n#ControllerLeaderId\tn0" +
		"\n#ControllerLeaderAddress\t127.0.0.1:9878" +
		"\n#Peer:\tn0:127.0.0.1:9878" +
		"\n#Peer:\tn1:127.0.0.1:9879" +
		"\n"
	if output != expected {
		t.Fatalf("getControllerMetaData output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeGetControllerConfigFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		getControllerConfig: func(ctx context.Context, controllerAddrs string) ([]namesrvConfigSection, error) {
			if controllerAddrs != "127.0.0.1:9878;127.0.0.1:9879" {
				t.Fatalf("unexpected controller addresses %s", controllerAddrs)
			}
			return []namesrvConfigSection{
				{
					NameServer: "127.0.0.1:9878",
					Entries: []brokerConfigEntry{
						{Key: "listenPort", Value: "9878"},
						{Key: "controllerDLegerGroup", Value: "group1"},
					},
				},
				{
					NameServer: "127.0.0.1:9879",
					Entries: []brokerConfigEntry{
						{Key: "listenPort", Value: "9879"},
					},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getControllerConfig",
		"-a", "127.0.0.1:9878;127.0.0.1:9879",
	}, client)
	if err != nil {
		t.Fatalf("run native getControllerConfig: %v", err)
	}
	if !supported {
		t.Fatalf("expected getControllerConfig to be supported")
	}
	expected := "============127.0.0.1:9878============\n" +
		"listenPort                                        =  9878\n" +
		"controllerDLegerGroup                             =  group1\n" +
		"============127.0.0.1:9879============\n" +
		"listenPort                                        =  9879\n"
	if output != expected {
		t.Fatalf("getControllerConfig output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeGetSyncStateSetFormatsOfficialOutput(t *testing.T) {
	alive := true
	notAlive := false
	client := nativeClientFunc{
		getSyncStateSet: func(ctx context.Context, controllerAddr string, brokerNames []string) (*syncStateSetResult, error) {
			if controllerAddr != "127.0.0.1:9878" {
				t.Fatalf("unexpected controller address %s", controllerAddr)
			}
			if !reflect.DeepEqual(brokerNames, []string{"broker-a"}) {
				t.Fatalf("unexpected broker names %#v", brokerNames)
			}
			return &syncStateSetResult{Brokers: []syncStateSetBrokerInfo{{
				BrokerName:        "broker-a",
				MasterBrokerID:    0,
				MasterAddress:     "127.0.0.1:30911",
				MasterEpoch:       3,
				SyncStateSetEpoch: 4,
				InSyncReplicas: []syncStateSetReplicaIdentity{{
					BrokerName:    "broker-a",
					BrokerID:      0,
					BrokerAddress: "127.0.0.1:30911",
					Alive:         &alive,
				}},
				NotInSyncReplicas: []syncStateSetReplicaIdentity{{
					BrokerName:    "broker-a",
					BrokerID:      1,
					BrokerAddress: "127.0.0.1:30921",
					Alive:         &notAlive,
				}},
			}}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getSyncStateSet",
		"-a", "127.0.0.1:9878",
		"-b", "broker-a",
	}, client)
	if err != nil {
		t.Fatalf("run native getSyncStateSet: %v", err)
	}
	if !supported {
		t.Fatalf("expected getSyncStateSet to be supported")
	}
	expected := "\n#brokerName\tbroker-a\n#MasterBrokerId\t0\n#MasterAddr\t127.0.0.1:30911\n#MasterEpoch\t3\n#SyncStateSetEpoch\t4\n#SyncStateSetNums\t1\n" +
		"\nInSyncReplica:\tReplicaIdentity{brokerName='broker-a', brokerId=0, brokerAddress='127.0.0.1:30911', alive=true}\n" +
		"\nNotInSyncReplica:\tReplicaIdentity{brokerName='broker-a', brokerId=1, brokerAddress='127.0.0.1:30921', alive=false}\n"
	if output != expected {
		t.Fatalf("getSyncStateSet output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeGetSyncStateSetClusterFormatsOfficialOutput(t *testing.T) {
	alive := true
	client := nativeClientFunc{
		getSyncStateSetByCluster: func(ctx context.Context, nameServer string, controllerAddr string, clusterName string) (*syncStateSetResult, error) {
			if nameServer != "127.0.0.1:9876" || controllerAddr != "127.0.0.1:9878" || clusterName != "GoadminControllerCluster" {
				t.Fatalf("unexpected getSyncStateSet cluster args namesrv=%s controller=%s cluster=%s", nameServer, controllerAddr, clusterName)
			}
			return &syncStateSetResult{Brokers: []syncStateSetBrokerInfo{{
				BrokerName:        "broker-a",
				MasterBrokerID:    0,
				MasterAddress:     "127.0.0.1:30911",
				MasterEpoch:       3,
				SyncStateSetEpoch: 4,
				InSyncReplicas: []syncStateSetReplicaIdentity{{
					BrokerName:    "broker-a",
					BrokerID:      0,
					BrokerAddress: "127.0.0.1:30911",
					Alive:         &alive,
				}},
			}}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getSyncStateSet",
		"-n", "127.0.0.1:9876",
		"-a", "127.0.0.1:9878",
		"-c", "GoadminControllerCluster",
	}, client)
	if err != nil {
		t.Fatalf("run native getSyncStateSet cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected getSyncStateSet cluster to be supported")
	}
	expected := "\n#brokerName\tbroker-a\n#MasterBrokerId\t0\n#MasterAddr\t127.0.0.1:30911\n#MasterEpoch\t3\n#SyncStateSetEpoch\t4\n#SyncStateSetNums\t1\n" +
		"\nInSyncReplica:\tReplicaIdentity{brokerName='broker-a', brokerId=0, brokerAddress='127.0.0.1:30911', alive=true}\n"
	if output != expected {
		t.Fatalf("getSyncStateSet cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeListUserFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		listUser: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, filter string) ([]listUserRow, error) {
			if nameServer != "" || brokerAddr != "127.0.0.1:10911" || clusterName != "" || filter != "admin" {
				t.Fatalf("unexpected listUser args namesrv=%s broker=%s cluster=%s filter=%s", nameServer, brokerAddr, clusterName, filter)
			}
			return []listUserRow{{
				Username:   "admin",
				Password:   "******",
				UserType:   "Super",
				UserStatus: "enable",
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"listUser",
		"-b", "127.0.0.1:10911",
		"-f", "admin",
	}, client)
	if err != nil {
		t.Fatalf("run native listUser broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected listUser broker mode to be supported")
	}
	expected := fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "#UserName", "#Password", "#UserType", "#UserStatus") +
		fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "admin", "******", "Super", "enable")
	if output != expected {
		t.Fatalf("listUser broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeListUserClusterStopsAfterFirstNonEmptyBroker(t *testing.T) {
	client := nativeClientFunc{
		listUser: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, filter string) ([]listUserRow, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" || filter != "" {
				t.Fatalf("unexpected listUser cluster args namesrv=%s broker=%s cluster=%s filter=%s", nameServer, brokerAddr, clusterName, filter)
			}
			return []listUserRow{{
				Username:      "admin",
				Password:      "pwd",
				UserType:      "Super",
				UserStatus:    "enable",
				SourceAddress: "127.0.0.1:10911",
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"listUser",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
	}, client)
	if err != nil {
		t.Fatalf("run native listUser cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected listUser cluster mode to be supported")
	}
	expected := fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "#UserName", "#Password", "#UserType", "#UserStatus") +
		fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "admin", "pwd", "Super", "enable") +
		"get user from 127.0.0.1:10911 success.\n"
	if output != expected {
		t.Fatalf("listUser cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeGetUserFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		getUser: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, username string) (*listUserRow, error) {
			if nameServer != "" || brokerAddr != "127.0.0.1:10911" || clusterName != "" || username != "admin" {
				t.Fatalf("unexpected getUser args namesrv=%s broker=%s cluster=%s username=%s", nameServer, brokerAddr, clusterName, username)
			}
			return &listUserRow{
				Username:   "admin",
				Password:   "******",
				UserType:   "Super",
				UserStatus: "enable",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getUser",
		"-b", "127.0.0.1:10911",
		"-u", "admin",
	}, client)
	if err != nil {
		t.Fatalf("run native getUser broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected getUser broker mode to be supported")
	}
	expected := fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "#UserName", "#Password", "#UserType", "#UserStatus") +
		fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "admin", "******", "Super", "enable")
	if output != expected {
		t.Fatalf("getUser broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeGetUserClusterStopsAfterFirstNonEmptyBroker(t *testing.T) {
	client := nativeClientFunc{
		getUser: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, username string) (*listUserRow, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" || username != "admin" {
				t.Fatalf("unexpected getUser cluster args namesrv=%s broker=%s cluster=%s username=%s", nameServer, brokerAddr, clusterName, username)
			}
			return &listUserRow{
				Username:   "admin",
				Password:   "pwd",
				UserType:   "Super",
				UserStatus: "enable",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getUser",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-u", "admin",
	}, client)
	if err != nil {
		t.Fatalf("run native getUser cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected getUser cluster mode to be supported")
	}
	expected := fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "#UserName", "#Password", "#UserType", "#UserStatus") +
		fmt.Sprintf("%-16s  %-22s  %-22s  %-22s\n", "admin", "pwd", "Super", "enable")
	if output != expected {
		t.Fatalf("getUser cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeCopyUserFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		copyUser: func(ctx context.Context, sourceBroker string, targetBroker string, usernames string) ([]copyUserResult, error) {
			if sourceBroker != "127.0.0.1:31072" || targetBroker != "127.0.0.1:31082" || usernames != "alice,bob" {
				t.Fatalf("unexpected copyUser args source=%s target=%s usernames=%s", sourceBroker, targetBroker, usernames)
			}
			return []copyUserResult{
				{Username: "alice", SourceBroker: sourceBroker, TargetBroker: targetBroker},
				{Username: "bob", SourceBroker: sourceBroker, TargetBroker: targetBroker},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"copyUser",
		"-f", "127.0.0.1:31072",
		"-t", "127.0.0.1:31082",
		"-u", "alice,bob",
	}, client)
	if err != nil {
		t.Fatalf("run native copyUser: %v", err)
	}
	if !supported {
		t.Fatalf("expected copyUser to be supported")
	}
	expected := "copy user of alice from 127.0.0.1:31072 to 127.0.0.1:31082 success.\n" +
		"copy user of bob from 127.0.0.1:31072 to 127.0.0.1:31082 success.\n"
	if output != expected {
		t.Fatalf("copyUser output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeCopyUserFallsBackWhenRequiredOptionsMissing(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{
		"copyUser",
		"-f", "127.0.0.1:31072",
	}, nativeClientFunc{})
	if err != nil {
		t.Fatalf("run native copyUser missing target: %v", err)
	}
	if supported || output != "" {
		t.Fatalf("expected copyUser missing target to fallback, supported=%t output=%q", supported, output)
	}
}

func TestRunNativeListAclFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		listAcl: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subjectFilter string, resourceFilter string) ([]aclInfo, error) {
			if nameServer != "" || brokerAddr != "127.0.0.1:10911" || clusterName != "" || subjectFilter != "User:alice" || resourceFilter != "Topic:test" {
				t.Fatalf("unexpected listAcl args namesrv=%s broker=%s cluster=%s subject=%s resource=%s", nameServer, brokerAddr, clusterName, subjectFilter, resourceFilter)
			}
			return []aclInfo{aclInfoForTest("User:alice")}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"listAcl",
		"-b", "127.0.0.1:10911",
		"-s", "User:alice",
		"-r", "Topic:test",
	}, client)
	if err != nil {
		t.Fatalf("run native listAcl broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected listAcl broker mode to be supported")
	}
	expected := aclTableForTest("User:alice")
	if output != expected {
		t.Fatalf("listAcl broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeGetAclFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		getAcl: func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subject string) ([]aclInfo, error) {
			if nameServer != "127.0.0.1:9876" || brokerAddr != "" || clusterName != "DefaultCluster" || subject != "User:alice" {
				t.Fatalf("unexpected getAcl args namesrv=%s broker=%s cluster=%s subject=%s", nameServer, brokerAddr, clusterName, subject)
			}
			acl := aclInfoForTest("User:alice")
			return []aclInfo{acl}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getAcl",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-s", "User:alice",
	}, client)
	if err != nil {
		t.Fatalf("run native getAcl cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected getAcl cluster mode to be supported")
	}
	expected := aclTableForTest("User:alice")
	if output != expected {
		t.Fatalf("getAcl cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeCopyAclFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		copyAcl: func(ctx context.Context, sourceBroker string, targetBroker string, subjects string) ([]copyAclResult, error) {
			if sourceBroker != "127.0.0.1:31092" || targetBroker != "127.0.0.1:31102" || subjects != "User:alice,User:bob" {
				t.Fatalf("unexpected copyAcl args source=%s target=%s subjects=%s", sourceBroker, targetBroker, subjects)
			}
			return []copyAclResult{
				{Subject: "User:alice", SourceBroker: sourceBroker, TargetBroker: targetBroker},
				{Subject: "User:bob", SourceBroker: sourceBroker, TargetBroker: targetBroker},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"copyAcl",
		"-f", "127.0.0.1:31092",
		"-t", "127.0.0.1:31102",
		"-s", "User:alice,User:bob",
	}, client)
	if err != nil {
		t.Fatalf("run native copyAcl: %v", err)
	}
	if !supported {
		t.Fatalf("expected copyAcl to be supported")
	}
	expected := "copy acl of User:alice from 127.0.0.1:31092 to 127.0.0.1:31102 success.\n" +
		"copy acl of User:bob from 127.0.0.1:31092 to 127.0.0.1:31102 success.\n"
	if output != expected {
		t.Fatalf("copyAcl output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeCopyAclFallsBackWhenRequiredOptionsMissing(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{
		"copyAcl",
		"-f", "127.0.0.1:31092",
	}, nativeClientFunc{})
	if err != nil {
		t.Fatalf("run native copyAcl missing target: %v", err)
	}
	if supported || output != "" {
		t.Fatalf("expected copyAcl missing target to fallback, supported=%t output=%q", supported, output)
	}
}

func aclInfoForTest(subject string) aclInfo {
	return aclInfo{
		Subject: subject,
		Policies: []aclPolicyInfo{{
			PolicyType: "CUSTOM",
			Entries: []aclPolicyEntryInfo{{
				Resource:  "Topic:test",
				Actions:   []string{"Pub", "Sub"},
				SourceIps: []string{"10.0.0.1"},
				Decision:  "Allow",
			}},
		}},
	}
}

func aclTableForTest(subject string) string {
	const rowFormat = "%-16s  %-10s  %-22s  %-20s  %-24s  %-10s\n"
	return fmt.Sprintf(rowFormat, "#Subject", "#PolicyType", "#Resource", "#Actions", "#SourceIp", "#Decision") +
		fmt.Sprintf(rowFormat, subject, "CUSTOM", "Topic:test", "[Pub, Sub]", "[10.0.0.1]", "Allow")
}

func TestRunNativeCreateUserFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		createUser: func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
			if nameServer != "" || options.BrokerAddr != "127.0.0.1:10911" || options.ClusterName != "" {
				t.Fatalf("unexpected createUser target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Username != "goadmin-created" || options.Password != "seed-pass" || options.UserType != "Super" {
				t.Fatalf("unexpected createUser auth fields %#v", options)
			}
			return []string{"127.0.0.1:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"createUser",
		"-b", "127.0.0.1:10911",
		"-u", "goadmin-created",
		"-p", "seed-pass",
		"-t", "Super",
	}, client)
	if err != nil {
		t.Fatalf("run native createUser broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected createUser broker mode to be supported")
	}
	expected := "create user to 127.0.0.1:10911 success.\n"
	if output != expected {
		t.Fatalf("createUser broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeUpdateUserFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		updateUser: func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
			if nameServer != "" || options.BrokerAddr != "127.0.0.1:10911" || options.ClusterName != "" {
				t.Fatalf("unexpected updateUser target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Username != "goadmin-created" || options.UserStatus != "disable" || options.Password != "" || options.UserType != "" {
				t.Fatalf("unexpected updateUser auth fields %#v", options)
			}
			return []string{"127.0.0.1:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateUser",
		"-b", "127.0.0.1:10911",
		"-u", "goadmin-created",
		"-s", "disable",
	}, client)
	if err != nil {
		t.Fatalf("run native updateUser broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateUser broker mode to be supported")
	}
	expected := "update user to 127.0.0.1:10911 success.\n"
	if output != expected {
		t.Fatalf("updateUser broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeCreateUserFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		createUser: func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" || options.BrokerAddr != "" || options.ClusterName != "DefaultCluster" {
				t.Fatalf("unexpected createUser target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Username != "goadmin-created" || options.Password != "seed-pass" || options.UserType != "Super" {
				t.Fatalf("unexpected createUser auth fields %#v", options)
			}
			return []string{"127.0.0.1:10911", "127.0.0.1:10912"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"createUser",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-u", "goadmin-created",
		"-p", "seed-pass",
		"-t", "Super",
	}, client)
	if err != nil {
		t.Fatalf("run native createUser cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected createUser cluster mode to be supported")
	}
	expected := "create user to 127.0.0.1:10911 success.\n" +
		"create user to 127.0.0.1:10912 success.\n"
	if output != expected {
		t.Fatalf("createUser cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeUpdateUserFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		updateUser: func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" || options.BrokerAddr != "" || options.ClusterName != "DefaultCluster" {
				t.Fatalf("unexpected updateUser target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Username != "goadmin-created" || options.Password != "rotated-pass" || options.UserType != "" || options.UserStatus != "" {
				t.Fatalf("unexpected updateUser auth fields %#v", options)
			}
			if !options.PasswordSet || options.UserTypeSet || options.UserStatusSet {
				t.Fatalf("unexpected updateUser selected fields %#v", options)
			}
			return []string{"127.0.0.1:10911", "127.0.0.1:10912"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateUser",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-u", "goadmin-created",
		"-p", "rotated-pass",
	}, client)
	if err != nil {
		t.Fatalf("run native updateUser cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateUser cluster mode to be supported")
	}
	expected := "update user to 127.0.0.1:10911 success.\n" +
		"update user to 127.0.0.1:10912 success.\n"
	if output != expected {
		t.Fatalf("updateUser cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeDeleteUserFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteUser: func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" || options.BrokerAddr != "" || options.ClusterName != "DefaultCluster" {
				t.Fatalf("unexpected deleteUser target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Username != "goadmin-created" {
				t.Fatalf("unexpected deleteUser username %#v", options)
			}
			return []string{"127.0.0.1:10911", "127.0.0.1:10912"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteUser",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-u", "goadmin-created",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteUser cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteUser cluster mode to be supported")
	}
	expected := "delete user to 127.0.0.1:10911 success.\n" +
		"delete user to 127.0.0.1:10912 success.\n"
	if output != expected {
		t.Fatalf("deleteUser cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeUpdateUserFallsBackWhenMultipleMutationFields(t *testing.T) {
	client := nativeClientFunc{
		updateUser: func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
			t.Fatalf("updateUser should not run when -p/-t/-s are selected together")
			return nil, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateUser",
		"-b", "127.0.0.1:10911",
		"-u", "goadmin-created",
		"-p", "seed-pass",
		"-s", "disable",
	}, client)
	if err != nil {
		t.Fatalf("run native updateUser with multiple mutation fields: %v", err)
	}
	if supported || output != "" {
		t.Fatalf("expected fallback for multiple update mutation fields, supported=%t output=%q", supported, output)
	}
}

func TestRunNativeAuthUserFormatsOfficialMissingClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteUser: func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
			return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteUser",
		"-n", "127.0.0.1:9876",
		"-c", "MissingCluster",
		"-u", "goadmin-created",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteUser missing cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected missing cluster output to be handled natively")
	}
	expected := "[error] Make sure the specified clusterName exists or the name server connected to is correct."
	if output != expected {
		t.Fatalf("missing cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeCreateAclFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		createAcl: func(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
			if nameServer != "" || options.BrokerAddr != "127.0.0.1:10911" || options.ClusterName != "" {
				t.Fatalf("unexpected createAcl target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Subject != "User:goadmin-created" || options.Decision != "Allow" {
				t.Fatalf("unexpected createAcl subject/decision %#v", options)
			}
			if !reflect.DeepEqual(options.Resources, []string{"Topic:first", "Group:second"}) {
				t.Fatalf("unexpected createAcl resources %#v", options.Resources)
			}
			if !reflect.DeepEqual(options.Actions, []string{"Pub", "Sub"}) {
				t.Fatalf("unexpected createAcl actions %#v", options.Actions)
			}
			if !reflect.DeepEqual(options.SourceIps, []string{"10.0.0.1", "10.0.0.2"}) {
				t.Fatalf("unexpected createAcl sourceIps %#v", options.SourceIps)
			}
			return []string{"127.0.0.1:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"createAcl",
		"-b", "127.0.0.1:10911",
		"-s", "User:goadmin-created",
		"-r", "Topic:first, Group:second",
		"-a", "Pub, Sub",
		"-i", "10.0.0.1, 10.0.0.2",
		"-d", "Allow",
	}, client)
	if err != nil {
		t.Fatalf("run native createAcl broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected createAcl broker mode to be supported")
	}
	expected := "create acl to 127.0.0.1:10911 success.\n"
	if output != expected {
		t.Fatalf("createAcl broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeUpdateAclFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		updateAcl: func(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" || options.BrokerAddr != "" || options.ClusterName != "DefaultCluster" {
				t.Fatalf("unexpected updateAcl target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Subject != "User:goadmin-created" || options.Decision != "Deny" {
				t.Fatalf("unexpected updateAcl subject/decision %#v", options)
			}
			if !reflect.DeepEqual(options.Resources, []string{"Topic:first"}) || !reflect.DeepEqual(options.Actions, []string{"Sub"}) {
				t.Fatalf("unexpected updateAcl policies %#v", options)
			}
			if options.SourceIps != nil {
				t.Fatalf("expected nil sourceIps when -i is absent, got %#v", options.SourceIps)
			}
			return []string{"127.0.0.1:10911", "127.0.0.1:10912"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateAcl",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-s", "User:goadmin-created",
		"-r", "Topic:first",
		"-a", "Sub",
		"-d", "Deny",
	}, client)
	if err != nil {
		t.Fatalf("run native updateAcl cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateAcl cluster mode to be supported")
	}
	expected := "update acl to 127.0.0.1:10911 success.\n" +
		"update acl to 127.0.0.1:10912 success.\n"
	if output != expected {
		t.Fatalf("updateAcl cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeDeleteAclFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteAcl: func(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" || options.BrokerAddr != "" || options.ClusterName != "DefaultCluster" {
				t.Fatalf("unexpected deleteAcl target namesrv=%s options=%#v", nameServer, options)
			}
			if options.Subject != "User:goadmin-created" || options.Resource != "Topic:first, Topic:second" {
				t.Fatalf("unexpected deleteAcl subject/resource %#v", options)
			}
			return []string{"127.0.0.1:10911", "127.0.0.1:10912"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteAcl",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-s", "User:goadmin-created",
		"-r", "Topic:first, Topic:second",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteAcl cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteAcl cluster mode to be supported")
	}
	expected := "delete acl to 127.0.0.1:10911 success.\n" +
		"delete acl to 127.0.0.1:10912 success.\n"
	if output != expected {
		t.Fatalf("deleteAcl cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeAclFallsBackWhenRequiredOptionsMissing(t *testing.T) {
	client := nativeClientFunc{
		createAcl: func(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
			t.Fatalf("createAcl should not run when decision is missing")
			return nil, nil
		},
		updateAcl: func(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
			t.Fatalf("updateAcl should not run when actions are missing")
			return nil, nil
		},
		deleteAcl: func(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
			t.Fatalf("deleteAcl should not run when subject is missing")
			return nil, nil
		},
	}
	for _, args := range [][]string{
		{"createAcl", "-b", "127.0.0.1:10911", "-s", "User:x", "-r", "Topic:x", "-a", "Pub"},
		{"updateAcl", "-b", "127.0.0.1:10911", "-s", "User:x", "-r", "Topic:x", "-d", "Allow"},
		{"deleteAcl", "-b", "127.0.0.1:10911"},
		{"createAcl", "-b", "127.0.0.1:10911", "-c", "DefaultCluster", "-s", "User:x", "-r", "Topic:x", "-a", "Pub", "-d", "Allow"},
	} {
		output, supported, err := runNativeCommand(context.Background(), args, client)
		if err != nil {
			t.Fatalf("run native ACL fallback %v: %v", args, err)
		}
		if supported || output != "" {
			t.Fatalf("expected ACL command to fallback for %v, supported=%t output=%q", args, supported, output)
		}
	}
}

func TestRunNativeAclFormatsOfficialMissingClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteAcl: func(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
			return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteAcl",
		"-n", "127.0.0.1:9876",
		"-c", "MissingCluster",
		"-s", "User:goadmin-created",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteAcl missing cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected missing ACL cluster output to be handled natively")
	}
	expected := "[error] Make sure the specified clusterName exists or the name server connected to is correct."
	if output != expected {
		t.Fatalf("missing ACL cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeUpdateAclConfigFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		updateAclConfig: func(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
			if nameServer != "" || options.BrokerAddr != "127.0.0.1:10911" || options.ClusterName != "" {
				t.Fatalf("unexpected updateAclConfig target namesrv=%s options=%#v", nameServer, options)
			}
			if options.AccessKey != "GoadminLegacyAccess" || options.SecretKey != "legacy-secret" || !options.Admin {
				t.Fatalf("unexpected updateAclConfig identity/admin %#v", options)
			}
			if options.WhiteRemoteAddress != "10.70.*" || options.DefaultTopicPerm != "PUB|SUB" || options.DefaultGroupPerm != "SUB" {
				t.Fatalf("unexpected updateAclConfig defaults %#v", options)
			}
			if !reflect.DeepEqual(options.TopicPerms, []string{"TopicA=PUB", "TopicB=SUB"}) {
				t.Fatalf("unexpected topic perms %#v", options.TopicPerms)
			}
			if !reflect.DeepEqual(options.GroupPerms, []string{"GroupA=SUB"}) {
				t.Fatalf("unexpected group perms %#v", options.GroupPerms)
			}
			return []string{"127.0.0.1:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateAclConfig",
		"-b", "127.0.0.1:10911",
		"-a", "GoadminLegacyAccess",
		"-s", "legacy-secret",
		"-w", "10.70.*",
		"-i", "PUB|SUB",
		"-u", "SUB",
		"-t", "TopicA=PUB,TopicB=SUB",
		"-g", "GroupA=SUB",
		"-m", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native updateAclConfig broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateAclConfig broker mode to be supported")
	}
	expected := "create or update plain access config to 127.0.0.1:10911 success.\n" +
		"PlainAccessConfig{accessKey='GoadminLegacyAccess', whiteRemoteAddress='10.70.*', admin=true, defaultTopicPerm='PUB|SUB', defaultGroupPerm='SUB', topicPerms=[TopicA=PUB, TopicB=SUB], groupPerms=[GroupA=SUB]}"
	if output != expected {
		t.Fatalf("updateAclConfig broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeDeleteAclConfigFormatsOfficialBrokerOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteAclConfig: func(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
			if nameServer != "" || options.BrokerAddr != "127.0.0.1:10911" || options.AccessKey != "GoadminLegacyAccess" {
				t.Fatalf("unexpected deleteAclConfig args namesrv=%s options=%#v", nameServer, options)
			}
			return []string{"127.0.0.1:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteAclConfig",
		"-b", "127.0.0.1:10911",
		"-a", "GoadminLegacyAccess",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteAclConfig broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteAclConfig broker mode to be supported")
	}
	expected := "delete plain access config account from 127.0.0.1:10911 success.\n" +
		"account's accessKey is:GoadminLegacyAccess"
	if output != expected {
		t.Fatalf("deleteAclConfig broker output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeUpdateGlobalWhiteAddrFormatsOfficialClusterOutput(t *testing.T) {
	client := nativeClientFunc{
		updateGlobalWhiteAddr: func(ctx context.Context, nameServer string, options globalWhiteAddrOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" || options.BrokerAddr != "" || options.ClusterName != "DefaultCluster" {
				t.Fatalf("unexpected updateGlobalWhiteAddr target namesrv=%s options=%#v", nameServer, options)
			}
			if options.GlobalWhiteRemoteAddresses != "10.70.*,192.168.1.*" || options.AclFileFullPath != "/tmp/plain_acl.yml" {
				t.Fatalf("unexpected updateGlobalWhiteAddr fields %#v", options)
			}
			return []string{"127.0.0.1:10911", "127.0.0.1:10912"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateGlobalWhiteAddr",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-g", "10.70.*,192.168.1.*",
		"-p", "/tmp/plain_acl.yml",
	}, client)
	if err != nil {
		t.Fatalf("run native updateGlobalWhiteAddr cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateGlobalWhiteAddr cluster mode to be supported")
	}
	expected := "update global white remote addresses to 127.0.0.1:10911 success.\n" +
		"update global white remote addresses to 127.0.0.1:10912 success.\n"
	if output != expected {
		t.Fatalf("updateGlobalWhiteAddr cluster output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeAclConfigFallsBackWhenRequiredOptionsMissing(t *testing.T) {
	client := nativeClientFunc{
		updateAclConfig: func(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
			t.Fatalf("updateAclConfig should not run when required options are missing")
			return nil, nil
		},
		deleteAclConfig: func(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
			t.Fatalf("deleteAclConfig should not run when required options are missing")
			return nil, nil
		},
		updateGlobalWhiteAddr: func(ctx context.Context, nameServer string, options globalWhiteAddrOptions) ([]string, error) {
			t.Fatalf("updateGlobalWhiteAddr should not run when required options are missing")
			return nil, nil
		},
	}
	for _, args := range [][]string{
		{"updateAclConfig", "-b", "127.0.0.1:10911", "-a", "GoadminLegacyAccess"},
		{"deleteAclConfig", "-b", "127.0.0.1:10911"},
		{"updateGlobalWhiteAddr", "-b", "127.0.0.1:10911"},
		{"updateAclConfig", "-b", "127.0.0.1:10911", "-c", "DefaultCluster", "-a", "GoadminLegacyAccess", "-s", "legacy-secret"},
	} {
		output, supported, err := runNativeCommand(context.Background(), args, client)
		if err != nil {
			t.Fatalf("run native ACL config fallback %v: %v", args, err)
		}
		if supported || output != "" {
			t.Fatalf("expected ACL config command to fallback for %v, supported=%t output=%q", args, supported, output)
		}
	}
}

func TestRunNativeGetSyncStateSetFormatsOfficialEmptyOutput(t *testing.T) {
	client := nativeClientFunc{
		getSyncStateSet: func(ctx context.Context, controllerAddr string, brokerNames []string) (*syncStateSetResult, error) {
			return &syncStateSetResult{}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getSyncStateSet",
		"-a", "127.0.0.1:9878",
		"-b", "broker-a",
	}, client)
	if err != nil {
		t.Fatalf("run native getSyncStateSet empty: %v", err)
	}
	if !supported {
		t.Fatalf("expected getSyncStateSet empty to be supported")
	}
	if output != "" {
		t.Fatalf("getSyncStateSet empty output mismatch\nexpected empty\nactual=%q", output)
	}
}

func TestRunNativeResetOffsetByTimeSpecifiedQueueFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		resetOffsetByTime: func(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error) {
			expected := resetOffsetByTimeOptions{
				NameServer:      "127.0.0.1:9876",
				Group:           "GoadminGroup",
				Topic:           "TopicTest",
				TimestampText:   "1781242105254",
				TimestampMillis: 1781242105254,
				BrokerAddr:      "broker-a:10911",
				QueueID:         0,
				ExpectOffset:    7,
				HasQueueID:      true,
				HasExpectOffset: true,
				SpecifiedQueue:  true,
				Force:           true,
			}
			if !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected resetOffsetByTime options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return nil, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"resetOffsetByTime",
		"-n", "127.0.0.1:9876",
		"-g", "GoadminGroup",
		"-t", "TopicTest",
		"-s", "1781242105254",
		"-b", "broker-a:10911",
		"-q", "0",
		"-o", "7",
	}, client)
	if err != nil {
		t.Fatalf("run native resetOffsetByTime: %v", err)
	}
	if !supported {
		t.Fatalf("expected resetOffsetByTime specified queue to be supported")
	}
	expected := "start reset consumer offset by specified, group[GoadminGroup], topic[TopicTest], queueId[0], broker[broker-a:10911], timestamp(string)[1781242105254], timestamp(long)[1781242105254]\n" +
		"reset consumer offset to 7\n"
	if output != expected {
		t.Fatalf("resetOffsetByTime output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeResetOffsetByTimeSpecifiedQueueSearchesOffsetWhenOffsetMissing(t *testing.T) {
	client := nativeClientFunc{
		resetOffsetByTime: func(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error) {
			expected := resetOffsetByTimeOptions{
				NameServer:      "127.0.0.1:9876",
				Group:           "GoadminGroup",
				Topic:           "TopicTest",
				TimestampText:   "1781242105254",
				TimestampMillis: 1781242105254,
				BrokerAddr:      "broker-a:10911",
				QueueID:         0,
				HasQueueID:      true,
				SpecifiedQueue:  true,
				Force:           true,
			}
			if nameServer != expected.NameServer || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected resetOffsetByTime args namesrv=%s options=%#v", nameServer, options)
			}
			return []skipAccumulatedMessageRow{{Offset: 23}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"resetOffsetByTime",
		"-n", "127.0.0.1:9876",
		"-g", "GoadminGroup",
		"-t", "TopicTest",
		"-s", "1781242105254",
		"-b", "broker-a:10911",
		"-q", "0",
	}, client)
	if err != nil {
		t.Fatalf("run native resetOffsetByTime search branch: %v", err)
	}
	if !supported {
		t.Fatalf("expected resetOffsetByTime broker+queue search branch to be supported")
	}
	expected := "start reset consumer offset by specified, group[GoadminGroup], topic[TopicTest], queueId[0], broker[broker-a:10911], timestamp(string)[1781242105254], timestamp(long)[1781242105254]\n" +
		"reset consumer offset to 23\n"
	if output != expected {
		t.Fatalf("resetOffsetByTime search output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeResetOffsetByTimeTimestampFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		resetOffsetByTime: func(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error) {
			expected := resetOffsetByTimeOptions{
				NameServer:      "127.0.0.1:9876",
				Group:           "GoadminGroup",
				Topic:           "TopicTest",
				TimestampText:   "-1",
				TimestampMillis: -1,
				Force:           false,
				QueueID:         -1,
			}
			if nameServer != expected.NameServer || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected resetOffsetByTime args namesrv=%s options=%#v", nameServer, options)
			}
			return []skipAccumulatedMessageRow{
				{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 3}, Offset: 42},
				{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1}, Offset: 43},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"resetOffsetByTime",
		"-n", "127.0.0.1:9876",
		"-g", "GoadminGroup",
		"-t", "TopicTest",
		"-s", "-1",
		"-f", "false",
	}, client)
	if err != nil {
		t.Fatalf("run native resetOffsetByTime timestamp: %v", err)
	}
	if !supported {
		t.Fatalf("expected resetOffsetByTime timestamp branch to be supported")
	}
	expected := "start reset consumer offset by specified, group[GoadminGroup], topic[TopicTest], force[false], timestamp(string)[-1], timestamp(long)[-1]\n" +
		fmt.Sprintf("%-40s  %-40s  %-40s\n", "#brokerName", "#queueId", "#offset") +
		fmt.Sprintf("%-40s  %-40d  %-40d\n", frontStringAtLeast("broker-a", 32), 3, int64(42)) +
		fmt.Sprintf("%-40s  %-40d  %-40d\n", frontStringAtLeast("broker-a", 32), 1, int64(43))
	if output != expected {
		t.Fatalf("resetOffsetByTime timestamp output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeResetOffsetByTimeTimestampIgnoresIncompleteSpecifiedQueueOptions(t *testing.T) {
	cases := []struct {
		name     string
		extra    []string
		expected resetOffsetByTimeOptions
	}{
		{
			name:  "broker without queue",
			extra: []string{"-b", "broker-a:10911"},
			expected: resetOffsetByTimeOptions{
				NameServer:      "127.0.0.1:9876",
				Group:           "GoadminGroup",
				Topic:           "TopicTest",
				TimestampText:   "-1",
				TimestampMillis: -1,
				Force:           true,
				BrokerAddr:      "broker-a:10911",
				QueueID:         -1,
			},
		},
		{
			name:  "queue without broker",
			extra: []string{"-q", "0"},
			expected: resetOffsetByTimeOptions{
				NameServer:      "127.0.0.1:9876",
				Group:           "GoadminGroup",
				Topic:           "TopicTest",
				TimestampText:   "-1",
				TimestampMillis: -1,
				Force:           true,
				QueueID:         0,
				HasQueueID:      true,
			},
		},
		{
			name:  "offset without broker queue pair",
			extra: []string{"-o", "17"},
			expected: resetOffsetByTimeOptions{
				NameServer:      "127.0.0.1:9876",
				Group:           "GoadminGroup",
				Topic:           "TopicTest",
				TimestampText:   "-1",
				TimestampMillis: -1,
				Force:           true,
				QueueID:         -1,
				ExpectOffset:    17,
				HasExpectOffset: true,
			},
		},
		{
			name:  "broker and offset without queue",
			extra: []string{"-b", "broker-a:10911", "-o", "17"},
			expected: resetOffsetByTimeOptions{
				NameServer:      "127.0.0.1:9876",
				Group:           "GoadminGroup",
				Topic:           "TopicTest",
				TimestampText:   "-1",
				TimestampMillis: -1,
				Force:           true,
				BrokerAddr:      "broker-a:10911",
				QueueID:         -1,
				ExpectOffset:    17,
				HasExpectOffset: true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := nativeClientFunc{
				resetOffsetByTime: func(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error) {
					if nameServer != tc.expected.NameServer || !reflect.DeepEqual(options, tc.expected) {
						t.Fatalf("unexpected resetOffsetByTime args namesrv=%s options=%#v", nameServer, options)
					}
					return []skipAccumulatedMessageRow{
						{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1}, Offset: 43},
					}, nil
				},
			}

			args := []string{
				"resetOffsetByTime",
				"-n", "127.0.0.1:9876",
				"-g", "GoadminGroup",
				"-t", "TopicTest",
				"-s", "-1",
			}
			args = append(args, tc.extra...)
			output, supported, err := runNativeCommand(context.Background(), args, client)
			if err != nil {
				t.Fatalf("run native resetOffsetByTime timestamp incomplete specified queue options: %v", err)
			}
			if !supported {
				t.Fatalf("expected resetOffsetByTime timestamp branch to ignore incomplete specified queue options")
			}
			expectedOutput := "start reset consumer offset by specified, group[GoadminGroup], topic[TopicTest], force[true], timestamp(string)[-1], timestamp(long)[-1]\n" +
				fmt.Sprintf("%-40s  %-40s  %-40s\n", "#brokerName", "#queueId", "#offset") +
				fmt.Sprintf("%-40s  %-40d  %-40d\n", frontStringAtLeast("broker-a", 32), 1, int64(43))
			if output != expectedOutput {
				t.Fatalf("resetOffsetByTime timestamp output mismatch\nexpected:\n%s\nactual:\n%s", expectedOutput, output)
			}
		})
	}
}

func TestRunNativeSkipAccumulatedMessageFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		skipAccumulatedMessage: func(ctx context.Context, nameServer string, options skipAccumulatedMessageOptions) ([]skipAccumulatedMessageRow, error) {
			expected := skipAccumulatedMessageOptions{
				NameServer:  "127.0.0.1:9876",
				Group:       "GroupA",
				Topic:       "TopicTest",
				ClusterName: "DefaultCluster",
				Force:       false,
			}
			if nameServer != expected.NameServer || options != expected {
				t.Fatalf("unexpected skipAccumulatedMessage args namesrv=%s options=%#v", nameServer, options)
			}
			return []skipAccumulatedMessageRow{
				{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 3}, Offset: 42},
				{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1}, Offset: 43},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"skipAccumulatedMessage",
		"-n", "127.0.0.1:9876",
		"-g", "GroupA",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-f", "false",
	}, client)
	if err != nil {
		t.Fatalf("run native skipAccumulatedMessage: %v", err)
	}
	if !supported {
		t.Fatalf("expected skipAccumulatedMessage to be supported")
	}
	expected := fmt.Sprintf("%-40s  %-40s  %-40s\n", "#brokerName", "#queueId", "#offset") +
		fmt.Sprintf("%-40s  %-40d  %-40d\n", frontStringAtLeast("broker-a", 32), 3, int64(42)) +
		fmt.Sprintf("%-40s  %-40d  %-40d\n", frontStringAtLeast("broker-a", 32), 1, int64(43))
	if output != expected {
		t.Fatalf("skipAccumulatedMessage output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeUpdateTopicClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateTopic: func(ctx context.Context, nameServer string, options updateTopicOptions) (*updateTopicResult, error) {
			expected := updateTopicOptions{
				NameServer:      "127.0.0.1:9876",
				ClusterName:     "DefaultCluster",
				Topic:           "GoadminParityTopic",
				ReadQueueNums:   2,
				WriteQueueNums:  2,
				Perm:            6,
				TopicFilterType: "SINGLE_TAG",
			}
			if options != expected {
				t.Fatalf("unexpected updateTopic options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return &updateTopicResult{
				Targets: []string{"127.0.0.1:10911"},
				Config:  options.TopicConfig(),
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopic",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-t", "GoadminParityTopic",
		"-r", "2",
		"-w", "2",
		"-p", "6",
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopic: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopic to be supported")
	}
	expected := "create topic to 127.0.0.1:10911 success.\n" +
		"TopicConfig [topicName=GoadminParityTopic, readQueueNums=2, writeQueueNums=2, perm=RW-, topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}]\n"
	if output != expected {
		t.Fatalf("updateTopic cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateTopicBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateTopic: func(ctx context.Context, nameServer string, options updateTopicOptions) (*updateTopicResult, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected namesrv %s", nameServer)
			}
			if options.BrokerAddr != "broker-a:10911" || options.Topic != "GoadminParityBrokerTopic" || options.ReadQueueNums != 1 || options.WriteQueueNums != 1 {
				t.Fatalf("unexpected broker updateTopic options %#v", options)
			}
			return &updateTopicResult{
				Targets: []string{"broker-a:10911"},
				Config:  options.TopicConfig(),
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopic",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-t", "GoadminParityBrokerTopic",
		"-r", "1",
		"-w", "1",
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopic broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopic broker to be supported")
	}
	expected := "create topic to broker-a:10911 success.\n" +
		"TopicConfig [topicName=GoadminParityBrokerTopic, readQueueNums=1, writeQueueNums=1, perm=RW-, topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}]\n"
	if output != expected {
		t.Fatalf("updateTopic broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateStaticTopicBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateStaticTopic: func(ctx context.Context, nameServer string, options updateStaticTopicOptions) (*updateStaticTopicResult, error) {
			expected := updateStaticTopicOptions{
				NameServer:     "127.0.0.1:9876",
				BrokerNames:    []string{"broker-a"},
				ClusterNames:   []string{},
				Topic:          "GoadminStaticTopic",
				TotalQueueNums: 4,
				ForceReplace:   true,
			}
			if !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected updateStaticTopic options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return &updateStaticTopicResult{
				BeforeFile: "/tmp/GoadminStaticTopic-1000.before",
				AfterFile:  "/tmp/GoadminStaticTopic-2000.after",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateStaticTopic",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a",
		"-qn", "4",
		"-t", "GoadminStaticTopic",
		"-fr", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native updateStaticTopic broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateStaticTopic broker to be supported")
	}
	expected := "The old mapping data is written to file /tmp/GoadminStaticTopic-1000.before\n" +
		"The new mapping data is written to file /tmp/GoadminStaticTopic-2000.after\n"
	if output != expected {
		t.Fatalf("updateStaticTopic broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateStaticTopicMapFileUsesOfficialNonFileBranch(t *testing.T) {
	client := nativeClientFunc{
		updateStaticTopic: func(ctx context.Context, nameServer string, options updateStaticTopicOptions) (*updateStaticTopicResult, error) {
			expected := updateStaticTopicOptions{
				NameServer:     "127.0.0.1:9876",
				BrokerNames:    []string{"broker-a"},
				ClusterNames:   []string{},
				Topic:          "GoadminStaticTopic",
				TotalQueueNums: 4,
				MapFile:        "/tmp/not-used.json",
			}
			if !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected updateStaticTopic mapFile options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return &updateStaticTopicResult{
				BeforeFile: "/tmp/GoadminStaticTopic-1000.before",
				AfterFile:  "/tmp/GoadminStaticTopic-2000.after",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateStaticTopic",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a",
		"-qn", "4",
		"-t", "GoadminStaticTopic",
		"-mf", "/tmp/not-used.json",
	}, client)
	if err != nil {
		t.Fatalf("run native updateStaticTopic mapFile: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateStaticTopic mapFile to use official non-file branch")
	}
	expected := "The old mapping data is written to file /tmp/GoadminStaticTopic-1000.before\n" +
		"The new mapping data is written to file /tmp/GoadminStaticTopic-2000.after\n"
	if output != expected {
		t.Fatalf("updateStaticTopic mapFile output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRemappingStaticTopicBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		remappingStaticTopic: func(ctx context.Context, nameServer string, options remappingStaticTopicOptions) (*remappingStaticTopicResult, error) {
			expected := remappingStaticTopicOptions{
				NameServer:   "127.0.0.1:9876",
				BrokerNames:  []string{"broker-a"},
				ClusterNames: []string{},
				Topic:        "GoadminStaticTopic",
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected remappingStaticTopic args namesrv=%s options=%#v", nameServer, options)
			}
			return &remappingStaticTopicResult{
				BeforeFile: "/tmp/GoadminStaticTopic-1000.before",
				AfterFile:  "/tmp/GoadminStaticTopic-2000.after",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"remappingStaticTopic",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a",
		"-t", "GoadminStaticTopic",
	}, client)
	if err != nil {
		t.Fatalf("run native remappingStaticTopic broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected remappingStaticTopic broker to be supported")
	}
	expected := "The old mapping data is written to file /tmp/GoadminStaticTopic-1000.before\n" +
		"The old mapping data is written to file /tmp/GoadminStaticTopic-2000.after\n"
	if output != expected {
		t.Fatalf("remappingStaticTopic broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRemappingStaticTopicMapFileUsesOfficialNonFileBranch(t *testing.T) {
	client := nativeClientFunc{
		remappingStaticTopic: func(ctx context.Context, nameServer string, options remappingStaticTopicOptions) (*remappingStaticTopicResult, error) {
			expected := remappingStaticTopicOptions{
				NameServer:   "127.0.0.1:9876",
				BrokerNames:  []string{"broker-a"},
				ClusterNames: []string{},
				Topic:        "GoadminStaticTopic",
				MapFile:      "/tmp/remap.json",
			}
			if nameServer != "127.0.0.1:9876" || !reflect.DeepEqual(options, expected) {
				t.Fatalf("unexpected remappingStaticTopic mapFile args namesrv=%s options=%#v", nameServer, options)
			}
			return &remappingStaticTopicResult{
				BeforeFile: "/tmp/GoadminStaticTopic-1000.before",
				AfterFile:  "/tmp/GoadminStaticTopic-2000.after",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"remappingStaticTopic",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a",
		"-t", "GoadminStaticTopic",
		"-mf", "/tmp/remap.json",
	}, client)
	if err != nil {
		t.Fatalf("run native remappingStaticTopic mapFile: %v", err)
	}
	if !supported {
		t.Fatalf("expected remappingStaticTopic mapFile to use official non-file branch")
	}
	expected := "The old mapping data is written to file /tmp/GoadminStaticTopic-1000.before\n" +
		"The old mapping data is written to file /tmp/GoadminStaticTopic-2000.after\n"
	if output != expected {
		t.Fatalf("remappingStaticTopic mapFile output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateTopicListBrokerFormatsOfficialOutput(t *testing.T) {
	fileName := writeTopicListFileForTest(t, `[{"topicName":"GoadminBatchTopicA","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicFilterType":"SINGLE_TAG","topicSysFlag":0,"order":false,"attributes":{}}]`)
	client := nativeClientFunc{
		updateTopicList: func(ctx context.Context, nameServer string, options updateTopicListOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected updateTopicList namesrv %s", nameServer)
			}
			if options.NameServer != "127.0.0.1:9876" || options.BrokerAddr != "broker-a:10911" || options.ClusterName != "" || options.FileName != fileName {
				t.Fatalf("unexpected updateTopicList broker options %#v", options)
			}
			if len(options.TopicConfigs) != 1 || options.TopicConfigs[0].TopicName != "GoadminBatchTopicA" {
				t.Fatalf("unexpected updateTopicList configs %#v", options.TopicConfigs)
			}
			return []string{"broker-a:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopicList",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-f", fileName,
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopicList broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopicList broker to be supported")
	}
	expected := "submit batch of topic config to broker-a:10911 success, please check the result later.\n"
	if output != expected {
		t.Fatalf("updateTopicList broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateTopicListClusterFormatsOfficialOutput(t *testing.T) {
	fileName := writeTopicListFileForTest(t, `[{"topicName":"GoadminBatchTopicA","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicFilterType":"SINGLE_TAG","topicSysFlag":0,"order":false,"attributes":{}}]`)
	client := nativeClientFunc{
		updateTopicList: func(ctx context.Context, nameServer string, options updateTopicListOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected updateTopicList cluster namesrv %s", nameServer)
			}
			if options.NameServer != "127.0.0.1:9876" || options.BrokerAddr != "" || options.ClusterName != "DefaultCluster" || options.FileName != fileName {
				t.Fatalf("unexpected updateTopicList cluster options %#v", options)
			}
			if len(options.TopicConfigs) != 1 || options.TopicConfigs[0].TopicName != "GoadminBatchTopicA" {
				t.Fatalf("unexpected updateTopicList cluster configs %#v", options.TopicConfigs)
			}
			return []string{"broker-a:10911", "broker-b:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopicList",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-f", fileName,
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopicList cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopicList cluster to be supported")
	}
	expected := "submit batch of topic config to broker-a:10911 success, please check the result later.\n" +
		"submit batch of topic config to broker-b:10911 success, please check the result later.\n" +
		updateTopicListUsage
	if output != expected {
		t.Fatalf("updateTopicList cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateTopicPermClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateTopicPerm: func(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error) {
			expected := updateTopicPermOptions{
				NameServer:  "127.0.0.1:9876",
				ClusterName: "DefaultCluster",
				Topic:       "GoadminTopicPerm",
				Perm:        4,
			}
			if options != expected {
				t.Fatalf("unexpected updateTopicPerm options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return &updateTopicPermResult{
				Rows: []updateTopicPermRow{{
					OldPerm:    6,
					NewPerm:    4,
					BrokerAddr: "127.0.0.1:10911",
				}},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopicPerm",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-t", "GoadminTopicPerm",
		"-p", "4",
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopicPerm cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopicPerm cluster to be supported")
	}
	expected := "update topic perm from 6 to 4 in 127.0.0.1:10911 success.\n"
	if output != expected {
		t.Fatalf("updateTopicPerm cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateTopicPermBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateTopicPerm: func(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected namesrv %s", nameServer)
			}
			if options.BrokerAddr != "broker-a:10911" || options.Topic != "GoadminTopicPermBroker" || options.Perm != 4 {
				t.Fatalf("unexpected broker updateTopicPerm options %#v", options)
			}
			return &updateTopicPermResult{
				Rows: []updateTopicPermRow{{
					OldPerm:    6,
					NewPerm:    4,
					BrokerAddr: "broker-a:10911",
				}},
				Config: updateTopicConfig{
					TopicName:       options.Topic,
					ReadQueueNums:   2,
					WriteQueueNums:  2,
					Perm:            4,
					TopicFilterType: "SINGLE_TAG",
					TopicSysFlag:    0,
					Order:           false,
					Attributes:      map[string]string{},
				},
				PrintConfig: true,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopicPerm",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-t", "GoadminTopicPermBroker",
		"-p", "4",
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopicPerm broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopicPerm broker to be supported")
	}
	expected := "update topic perm from 6 to 4 in broker-a:10911 success.\n" +
		"TopicConfig [topicName=GoadminTopicPermBroker, readQueueNums=2, writeQueueNums=2, perm=R--, topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}].\n"
	if output != expected {
		t.Fatalf("updateTopicPerm broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateTopicPermSamePermFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateTopicPerm: func(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error) {
			if options.BrokerAddr != "broker-a:10911" || options.Topic != "GoadminTopicPermBroker" || options.Perm != 6 {
				t.Fatalf("unexpected same-perm updateTopicPerm options %#v", options)
			}
			return &updateTopicPermResult{SamePerm: true}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopicPerm",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-t", "GoadminTopicPermBroker",
		"-p", "6",
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopicPerm same perm: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopicPerm same perm to be supported")
	}
	expected := "new perm equals to the old one!\n"
	if output != expected {
		t.Fatalf("updateTopicPerm same-perm output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateTopicPermBrokerNotMasterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateTopicPerm: func(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error) {
			if options.BrokerAddr != "rmq-goadmin-broker:10911" || options.Topic != "GoadminTopicPermBroker" || options.Perm != 4 {
				t.Fatalf("unexpected not-master updateTopicPerm options %#v", options)
			}
			return &updateTopicPermResult{BrokerNotMaster: true}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateTopicPerm",
		"-n", "127.0.0.1:9876",
		"-b", "rmq-goadmin-broker:10911",
		"-t", "GoadminTopicPermBroker",
		"-p", "4",
	}, client)
	if err != nil {
		t.Fatalf("run native updateTopicPerm not-master: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateTopicPerm not-master to be supported")
	}
	expected := "updateTopicPerm error broker not exit or broker is not master!.\n"
	if output != expected {
		t.Fatalf("updateTopicPerm not-master output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeSetConsumeModeBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		setConsumeMode: func(ctx context.Context, nameServer string, options setConsumeModeOptions) (*setConsumeModeResult, error) {
			expected := setConsumeModeOptions{
				NameServer:       "127.0.0.1:9876",
				BrokerAddr:       "broker-a:10911",
				Topic:            "GoadminSetConsumeModeTopic",
				GroupName:        "GoadminSetConsumeModeGroup",
				Mode:             "POP",
				PopShareQueueNum: 3,
			}
			if options != expected {
				t.Fatalf("unexpected setConsumeMode options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected setConsumeMode namesrv %s", nameServer)
			}
			return &setConsumeModeResult{
				Targets:          []string{"broker-a:10911"},
				Topic:            options.Topic,
				GroupName:        options.GroupName,
				Mode:             options.Mode,
				PopShareQueueNum: options.PopShareQueueNum,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"setConsumeMode",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-t", "GoadminSetConsumeModeTopic",
		"-g", "GoadminSetConsumeModeGroup",
		"-m", "POP",
		"-q", "3",
	}, client)
	if err != nil {
		t.Fatalf("run native setConsumeMode broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected setConsumeMode broker to be supported")
	}
	expected := "set consume mode to broker-a:10911 success.\n" +
		"topic[GoadminSetConsumeModeTopic] group[GoadminSetConsumeModeGroup] consume mode[POP] popShareQueueNum[3]"
	if output != expected {
		t.Fatalf("setConsumeMode broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeSetConsumeModeClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		setConsumeMode: func(ctx context.Context, nameServer string, options setConsumeModeOptions) (*setConsumeModeResult, error) {
			expected := setConsumeModeOptions{
				NameServer:  "127.0.0.1:9876",
				ClusterName: "DefaultCluster",
				Topic:       "GoadminSetConsumeModeTopic",
				GroupName:   "GoadminSetConsumeModeGroup",
				Mode:        "PULL",
			}
			if options != expected {
				t.Fatalf("unexpected setConsumeMode cluster options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return &setConsumeModeResult{
				Targets:          []string{"172.24.0.3:10911"},
				Topic:            options.Topic,
				GroupName:        options.GroupName,
				Mode:             options.Mode,
				PopShareQueueNum: options.PopShareQueueNum,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"setConsumeMode",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-t", "GoadminSetConsumeModeTopic",
		"-g", "GoadminSetConsumeModeGroup",
		"-m", "PULL",
	}, client)
	if err != nil {
		t.Fatalf("run native setConsumeMode cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected setConsumeMode cluster to be supported")
	}
	expected := "set consume mode to 172.24.0.3:10911 success.\n" +
		"topic[GoadminSetConsumeModeTopic] group[GoadminSetConsumeModeGroup] consume mode[PULL] popShareQueueNum[0]"
	if output != expected {
		t.Fatalf("setConsumeMode cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeDeleteTopicFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteTopic: func(ctx context.Context, nameServer string, clusterName string, topic string) error {
			if nameServer != "127.0.0.1:9876" || clusterName != "DefaultCluster" || topic != "GoadminParityTopic" {
				t.Fatalf("unexpected deleteTopic args namesrv=%s cluster=%s topic=%s", nameServer, clusterName, topic)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteTopic",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-t", "GoadminParityTopic",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteTopic: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteTopic to be supported")
	}
	expected := "delete topic [GoadminParityTopic] from cluster [DefaultCluster] success.\n" +
		"delete topic [GoadminParityTopic] from NameServer success.\n"
	if output != expected {
		t.Fatalf("deleteTopic output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateSubGroupClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateSubGroup: func(ctx context.Context, nameServer string, options updateSubGroupOptions) (*updateSubGroupResult, error) {
			expected := updateSubGroupOptions{
				NameServer:                   "127.0.0.1:9876",
				ClusterName:                  "DefaultCluster",
				GroupName:                    "GoadminParityGroup",
				ConsumeEnable:                false,
				ConsumeFromMinEnable:         true,
				ConsumeBroadcastEnable:       true,
				ConsumeMessageOrderly:        true,
				RetryQueueNums:               3,
				RetryMaxTimes:                5,
				GroupRetryPolicy:             defaultGroupRetryPolicyJSON,
				BrokerID:                     1,
				WhichBrokerWhenConsumeSlowly: 2,
				NotifyConsumerIdsChanged:     false,
				Attributes:                   "+owner=dev",
			}
			if options != expected {
				t.Fatalf("unexpected updateSubGroup options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return &updateSubGroupResult{
				Targets: []string{"127.0.0.1:10911"},
				Config:  options.SubscriptionGroupConfig(),
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateSubGroup",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-g", "GoadminParityGroup",
		"-s", "false",
		"-m", "true",
		"-d", "true",
		"-o", "true",
		"-q", "3",
		"-r", "5",
		"-i", "1",
		"-w", "2",
		"-a", "false",
		"--attributes", "+owner=dev",
	}, client)
	if err != nil {
		t.Fatalf("run native updateSubGroup: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateSubGroup to be supported")
	}
	expected := "create subscription group to 127.0.0.1:10911 success.\n" +
		"SubscriptionGroupConfig{groupName=GoadminParityGroup, consumeEnable=false, consumeFromMinEnable=true, consumeBroadcastEnable=true, consumeMessageOrderly=true, retryQueueNums=3, retryMaxTimes=5, groupRetryPolicy=GroupRetryPolicy{type=CUSTOMIZED, exponentialRetryPolicy=null, customizedRetryPolicy=null}, brokerId=1, whichBrokerWhenConsumeSlowly=2, notifyConsumerIdsChangedEnable=false, groupSysFlag=0, consumeTimeoutMinute=15, subscriptionDataSet=null, attributes={+owner=dev}}"
	if output != expected {
		t.Fatalf("updateSubGroup cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateSubGroupListBrokerFormatsOfficialOutput(t *testing.T) {
	fileName := writeSubGroupListFileForTest(t, `[{"groupName":"GoadminBatchGroupA","consumeEnable":true,"consumeFromMinEnable":false,"consumeBroadcastEnable":false,"consumeMessageOrderly":false,"retryQueueNums":1,"retryMaxTimes":16,"groupRetryPolicy":{"type":"CUSTOMIZED","exponentialRetryPolicy":null,"customizedRetryPolicy":null},"brokerId":0,"whichBrokerWhenConsumeSlowly":1,"notifyConsumerIdsChangedEnable":true,"groupSysFlag":0,"consumeTimeoutMinute":15,"subscriptionDataSet":null,"attributes":{}}]`)
	client := nativeClientFunc{
		updateSubGroupList: func(ctx context.Context, nameServer string, options updateSubGroupListOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected updateSubGroupList namesrv %s", nameServer)
			}
			if options.BrokerAddr != "127.0.0.1:10911" || options.ClusterName != "" || options.FileName != fileName {
				t.Fatalf("unexpected updateSubGroupList options %#v", options)
			}
			if len(options.GroupConfigs) != 1 || options.GroupConfigs[0].GroupName != "GoadminBatchGroupA" {
				t.Fatalf("unexpected updateSubGroupList configs %#v", options.GroupConfigs)
			}
			return []string{"127.0.0.1:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateSubGroupList",
		"-n", "127.0.0.1:9876",
		"-b", "127.0.0.1:10911",
		"-f", fileName,
	}, client)
	if err != nil {
		t.Fatalf("run native updateSubGroupList broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateSubGroupList to be supported")
	}
	expected := "submit batch of group config to 127.0.0.1:10911 success, please check the result later.\n"
	if output != expected {
		t.Fatalf("updateSubGroupList broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateSubGroupListClusterFormatsOfficialOutput(t *testing.T) {
	fileName := writeSubGroupListFileForTest(t, `[{"groupName":"GoadminBatchGroupA","consumeEnable":true,"consumeFromMinEnable":false,"consumeBroadcastEnable":false,"consumeMessageOrderly":false,"retryQueueNums":1,"retryMaxTimes":16,"groupRetryPolicy":{"type":"CUSTOMIZED","exponentialRetryPolicy":null,"customizedRetryPolicy":null},"brokerId":0,"whichBrokerWhenConsumeSlowly":1,"notifyConsumerIdsChangedEnable":true,"groupSysFlag":0,"consumeTimeoutMinute":15,"subscriptionDataSet":null,"attributes":{}}]`)
	client := nativeClientFunc{
		updateSubGroupList: func(ctx context.Context, nameServer string, options updateSubGroupListOptions) ([]string, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected updateSubGroupList cluster namesrv %s", nameServer)
			}
			if options.ClusterName != "DefaultCluster" || options.BrokerAddr != "" || options.FileName != fileName {
				t.Fatalf("unexpected updateSubGroupList cluster options %#v", options)
			}
			return []string{"127.0.0.1:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateSubGroupList",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-f", fileName,
	}, client)
	if err != nil {
		t.Fatalf("run native updateSubGroupList cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateSubGroupList to be supported")
	}
	expected := "submit batch of subscription group config to 127.0.0.1:10911 success, please check the result later.\n" + updateSubGroupListUsage
	if output != expected {
		t.Fatalf("updateSubGroupList cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeDeleteSubGroupFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteSubGroup: func(ctx context.Context, nameServer string, options deleteSubGroupOptions) ([]deleteSubGroupResult, error) {
			expected := deleteSubGroupOptions{
				NameServer:   "127.0.0.1:9876",
				ClusterName:  "DefaultCluster",
				GroupName:    "GoadminParityGroup",
				RemoveOffset: true,
			}
			if options != expected {
				t.Fatalf("unexpected deleteSubGroup options\nexpected:%#v\nactual:%#v", expected, options)
			}
			return []deleteSubGroupResult{{BrokerAddr: "127.0.0.1:10911", ClusterName: "DefaultCluster"}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteSubGroup",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-g", "GoadminParityGroup",
		"-r", "true",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteSubGroup: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteSubGroup to be supported")
	}
	expected := "delete subscription group [GoadminParityGroup] from broker [127.0.0.1:10911] in cluster [DefaultCluster] success.\n"
	if output != expected {
		t.Fatalf("deleteSubGroup output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestDecodeBrokerConfigBodyUsesJDK21PropertiesOrder(t *testing.T) {
	body := []byte("z=last\n" +
		"timerStopEnqueue=false\n" +
		"metricsOtelCardinalityLimit=50000\n" +
		"serverSocketBacklog=1024\n" +
		"channelNotActiveInterval=60000\n" +
		"haHousekeepingInterval=20000\n" +
		"consumerManageThreadPoolNums=32\n" +
		"mappedFileSizeTimerLog=104857600\n" +
		"splitRegistrationSize=800\n" +
		"transactionCheckInterval=30000\n" +
		"autoDeleteUnusedStats=true\n" +
		"clientSocketRcvBufSize=0\n" +
		"enableDetailStat=true\n" +
		"queryMessageThreadPoolNums=16\n" +
		"useReentrantLockWhenPutMessage=true\n" +
		"appendCkAsync=false\n")

	entries, err := decodeBrokerConfigBody(body)
	if err != nil {
		t.Fatalf("decode broker config body: %v", err)
	}
	expected := []brokerConfigEntry{
		{Key: "timerStopEnqueue", Value: "false"},
		{Key: "metricsOtelCardinalityLimit", Value: "50000"},
		{Key: "serverSocketBacklog", Value: "1024"},
		{Key: "channelNotActiveInterval", Value: "60000"},
		{Key: "haHousekeepingInterval", Value: "20000"},
		{Key: "consumerManageThreadPoolNums", Value: "32"},
		{Key: "mappedFileSizeTimerLog", Value: "104857600"},
		{Key: "splitRegistrationSize", Value: "800"},
		{Key: "transactionCheckInterval", Value: "30000"},
		{Key: "autoDeleteUnusedStats", Value: "true"},
		{Key: "clientSocketRcvBufSize", Value: "0"},
		{Key: "enableDetailStat", Value: "true"},
		{Key: "z", Value: "last"},
		{Key: "queryMessageThreadPoolNums", Value: "16"},
		{Key: "useReentrantLockWhenPutMessage", Value: "true"},
		{Key: "appendCkAsync", Value: "false"},
	}
	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("broker config entry order mismatch\nexpected=%#v\nactual=%#v", expected, entries)
	}
}

func TestClientGetNamesrvConfigUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeGetNamesrvConfig {
			done <- fmt.Errorf("expected GET_NAMESRV_CONFIG code %d, got %d", requestCodeGetNamesrvConfig, request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		frame := remotingFrameForTest(t, response, []byte("rocketmqHome=/opt/rocketmq\nclusterTest=false\n"))
		_, err = conn.Write(frame)
		done <- err
	}()

	sections, err := NewClient(time.Second).GetNamesrvConfig(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatalf("get namesrv config: %v", err)
	}
	if len(sections) != 1 || sections[0].NameServer != listener.Addr().String() {
		t.Fatalf("unexpected sections %#v", sections)
	}
	seen := map[string]string{}
	for _, entry := range sections[0].Entries {
		seen[entry.Key] = entry.Value
	}
	if seen["rocketmqHome"] != "/opt/rocketmq" || seen["clusterTest"] != "false" {
		t.Fatalf("unexpected config entries %#v", sections[0].Entries)
	}
	if err := <-done; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func TestClientGetConsumerConfigUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetSubscriptionGroupConfig {
			brokerDone <- fmt.Errorf("expected GET_SUBSCRIPTIONGROUP_CONFIG code %d, got %d", requestCodeGetSubscriptionGroupConfig, request.Code)
			return
		}
		if request.ExtFields["group"] != "TOOLS_CONSUMER" {
			brokerDone <- fmt.Errorf("unexpected subscription group fields %#v", request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"groupName":"TOOLS_CONSUMER","consumeEnable":true,"consumeFromMinEnable":true,"consumeBroadcastEnable":true,"consumeMessageOrderly":false,"retryQueueNums":1,"retryMaxTimes":16,"groupRetryPolicy":{"type":"CUSTOMIZED"},"brokerId":0,"whichBrokerWhenConsumeSlowly":1,"notifyConsumerIdsChangedEnable":true,"groupSysFlag":0,"consumeTimeoutMinute":15,"attributes":{}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- fmt.Errorf("unexpected cluster request code %d", request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	sections, err := NewClient(time.Second).GetConsumerConfig(context.Background(), nameServerListener.Addr().String(), "TOOLS_CONSUMER")
	if err != nil {
		t.Fatalf("get consumer config: %v", err)
	}
	if len(sections) != 1 || len(sections[0].Entries) != 15 || sections[0].Entries[0].Name != "groupName" {
		t.Fatalf("unexpected consumer config sections %#v", sections)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientBrokerConsumeStatsUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeGetBrokerConsumeStats {
			done <- fmt.Errorf("expected GET_BROKER_CONSUME_STATS code %d, got %d", requestCodeGetBrokerConsumeStats, request.Code)
			return
		}
		if request.ExtFields["isOrder"] != "true" {
			done <- fmt.Errorf("unexpected broker consume stats fields %#v", request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"consumeStatsList":[],"brokerAddr":"broker-a:10911","totalDiff":0,"totalInflightDiff":0}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		done <- err
	}()

	stats, err := NewClient(time.Second).BrokerConsumeStats(context.Background(), brokerListener.Addr().String(), true, 0)
	if err != nil {
		t.Fatalf("broker consume stats: %v", err)
	}
	if stats == nil || stats.TotalDiff != 0 || len(stats.Groups) != 0 {
		t.Fatalf("unexpected broker consume stats %#v", stats)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientBrokerConsumeStatsUsesCommandTimeout(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeGetBrokerConsumeStats {
			done <- fmt.Errorf("expected GET_BROKER_CONSUME_STATS code %d, got %d", requestCodeGetBrokerConsumeStats, request.Code)
			return
		}
		if request.ExtFields["isOrder"] != "false" {
			done <- fmt.Errorf("unexpected broker consume stats fields %#v", request.ExtFields)
			return
		}
		if _, ok := request.ExtFields["timeoutMillis"]; ok {
			done <- fmt.Errorf("timeoutMillis must stay local, got %#v", request.ExtFields)
			return
		}
		time.Sleep(250 * time.Millisecond)
		done <- nil
	}()

	start := time.Now()
	_, err = NewClient(time.Second).BrokerConsumeStats(context.Background(), brokerListener.Addr().String(), false, 100*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected broker consume stats timeout")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("broker consume stats timeout took too long: %s", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientQueryConsumeQueueUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeQueryConsumeQueue {
			brokerDone <- fmt.Errorf("expected QUERY_CONSUME_QUEUE code %d, got %d", requestCodeQueryConsumeQueue, request.Code)
			return
		}
		expectedFields := map[string]string{
			"topic":         "TopicTest",
			"queueId":       "1",
			"index":         "5",
			"count":         "3",
			"consumerGroup": "GroupA",
		}
		for key, expected := range expectedFields {
			if request.ExtFields[key] != expected {
				brokerDone <- fmt.Errorf("unexpected queryCq field %s=%q in %#v", key, request.ExtFields[key], request.ExtFields)
				return
			}
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"queueData":[{"physicOffset":42,"physicSize":128,"tagsCode":7,"extendDataJson":null,"bitMap":null,"eval":false,"msg":null}],"maxQueueIndex":8,"minQueueIndex":0}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic {
			nameServerDone <- fmt.Errorf("unexpected route request code %d", request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	result, err := NewClient(time.Second).QueryConsumeQueue(context.Background(), nameServerListener.Addr().String(), "", "TopicTest", 1, 5, 3, "GroupA")
	if err != nil {
		t.Fatalf("query consume queue: %v", err)
	}
	if result == nil || result.MaxQueueIndex != 8 || len(result.QueueData) != 1 || result.QueueData[0].PhysicOffset != 42 {
		t.Fatalf("unexpected queryCq result %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientHAStatusUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerHAStatus {
			brokerDone <- fmt.Errorf("expected GET_BROKER_HA_STATUS code %d, got %d", requestCodeGetBrokerHAStatus, request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"master":true,"masterCommitLogMaxOffset":397814681,"inSyncSlaveNums":0,"haConnectionInfo":[],"haClientRuntimeInfo":{"masterAddr":"broker-a:10911","transferredByteInSecond":0,"maxOffset":0,"lastReadTimestamp":0,"lastWriteTimestamp":0,"masterFlushOffset":0}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- fmt.Errorf("unexpected cluster request code %d", request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	rows, err := NewClient(time.Second).BrokerHAStatusByCluster(context.Background(), nameServerListener.Addr().String(), "DefaultCluster")
	if err != nil {
		t.Fatalf("broker ha status: %v", err)
	}
	if len(rows) != 1 || rows[0].Addr != brokerListener.Addr().String() || rows[0].Result == nil || !rows[0].Result.Master || rows[0].Result.MasterCommitLogMaxOffset != 397814681 {
		t.Fatalf("unexpected ha status %#v", rows)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientCheckRocksdbCqWriteProgressUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeCheckRocksdbCqWriteProgress {
			brokerDone <- fmt.Errorf("expected CHECK_ROCKSDB_CQ_WRITE_PROGRESS code %d, got %d", requestCodeCheckRocksdbCqWriteProgress, request.Code)
			return
		}
		if request.ExtFields["topic"] != "TopicTest" || request.ExtFields["checkStoreTime"] != "123" {
			brokerDone <- fmt.Errorf("unexpected check rocksdb fields %#v", request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"checkStatus":0,"checkResult":""}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- fmt.Errorf("unexpected cluster request code %d", request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	rows, err := NewClient(time.Second).CheckRocksdbCqWriteProgress(context.Background(), nameServerListener.Addr().String(), "DefaultCluster", "TopicTest", 123)
	if err != nil {
		t.Fatalf("check rocksdb cq write progress: %v", err)
	}
	if len(rows) != 1 || rows[0].BrokerName != "broker-a" || rows[0].CheckError {
		t.Fatalf("unexpected check rocksdb rows %#v", rows)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientRocksDBConfigToJsonUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeExportRocksDBConfigToJson {
			brokerDone <- fmt.Errorf("expected EXPORT_ROCKSDB_CONFIG_TO_JSON code %d, got %d", requestCodeExportRocksDBConfigToJson, request.Code)
			return
		}
		if request.ExtFields["configType"] != "topics;" {
			brokerDone <- fmt.Errorf("unexpected rocksDBConfigToJson fields %#v", request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- fmt.Errorf("unexpected cluster request code %d", request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	err = NewClient(time.Second).RocksDBConfigToJson(context.Background(), nameServerListener.Addr().String(), "", "DefaultCluster", []string{"topics"})
	if err != nil {
		t.Fatalf("rocksDBConfigToJson: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientExportPopRecordUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeExportPopRecord {
			brokerDone <- fmt.Errorf("expected EXPORT_POP_RECORD code %d, got %d", requestCodeExportPopRecord, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			brokerDone <- fmt.Errorf("expected no exportPopRecord ext fields, got %#v", request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- fmt.Errorf("unexpected cluster request code %d", request.Code)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	rows, err := NewClient(time.Second).ExportPopRecord(context.Background(), nameServerListener.Addr().String(), "", "DefaultCluster", false)
	if err != nil {
		t.Fatalf("exportPopRecord: %v", err)
	}
	if len(rows) != 1 || rows[0].BrokerName != "broker-a" || rows[0].BrokerAddr != brokerListener.Addr().String() || rows[0].DryRun {
		t.Fatalf("unexpected exportPopRecord rows %#v", rows)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientKvConfigUsesOfficialRequestCodes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		expected := []struct {
			code   int
			fields map[string]string
		}{
			{
				code: requestCodePutKVConfig,
				fields: map[string]string{
					"namespace": "GoadminParityKV",
					"key":       "sample-key",
					"value":     "sample-value",
				},
			},
			{
				code: requestCodeDeleteKVConfig,
				fields: map[string]string{
					"namespace": "GoadminParityKV",
					"key":       "sample-key",
				},
			},
		}
		for _, item := range expected {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				done <- err
				return
			}
			if request.Code != item.code {
				_ = conn.Close()
				done <- fmt.Errorf("expected kv request code %d, got %d", item.code, request.Code)
				return
			}
			if len(request.Body) != 0 {
				_ = conn.Close()
				done <- fmt.Errorf("expected empty kv request body, got %d", len(request.Body))
				return
			}
			if !reflect.DeepEqual(request.ExtFields, item.fields) {
				_ = conn.Close()
				done <- fmt.Errorf("unexpected kv fields expected=%#v actual=%#v", item.fields, request.ExtFields)
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			_ = conn.Close()
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	client := NewClient(time.Second)
	if err := client.UpdateKvConfig(context.Background(), listener.Addr().String(), "GoadminParityKV", "sample-key", "sample-value"); err != nil {
		t.Fatalf("update kv config: %v", err)
	}
	if err := client.DeleteKvConfig(context.Background(), listener.Addr().String(), "GoadminParityKV", "sample-key"); err != nil {
		t.Fatalf("delete kv config: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
}

func TestClientGetKvConfigUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		expectedFields := map[string]string{"namespace": "ORDER_TOPIC_CONFIG", "key": "GoadminOrderTopic"}
		if request.Code != requestCodeGetKVConfig || !reflect.DeepEqual(request.ExtFields, expectedFields) || len(request.Body) != 0 {
			done <- fmt.Errorf("unexpected get kv request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
			ExtFields: map[string]string{
				"value": "broker-a:1",
			},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	value, err := client.GetKvConfig(context.Background(), listener.Addr().String(), "ORDER_TOPIC_CONFIG", "GoadminOrderTopic")
	if err != nil {
		t.Fatalf("get kv config: %v", err)
	}
	if value != "broker-a:1" {
		t.Fatalf("unexpected kv value %q", value)
	}
	if err := <-done; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
}

func TestClientUpdateBrokerConfigUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeUpdateBrokerConfig {
			done <- fmt.Errorf("expected UPDATE_BROKER_CONFIG code %d, got %d", requestCodeUpdateBrokerConfig, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("unexpected update broker config fields %#v", request.ExtFields)
			return
		}
		if string(request.Body) != "enableDetailStat=true\n" {
			done <- fmt.Errorf("unexpected update broker config body %q", string(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	targets, err := client.UpdateBrokerConfig(context.Background(), "", updateBrokerConfigOptions{
		BrokerAddr: listener.Addr().String(),
		Key:        "enableDetailStat",
		Value:      "true",
	})
	if err != nil {
		t.Fatalf("update broker config: %v", err)
	}
	if !reflect.DeepEqual(targets, []string{listener.Addr().String()}) {
		t.Fatalf("unexpected update broker config targets %#v", targets)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker server: %v", err)
	}
}

func TestClientUpdateNamesrvConfigUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen namesrv: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeUpdateNamesrvConfig {
			done <- fmt.Errorf("expected UPDATE_NAMESRV_CONFIG code %d, got %d", requestCodeUpdateNamesrvConfig, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("unexpected update namesrv config fields %#v", request.ExtFields)
			return
		}
		if string(request.Body) != "clusterTest=false\n" {
			done <- fmt.Errorf("unexpected update namesrv config body %q", string(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	targets, err := client.UpdateNamesrvConfig(context.Background(), listener.Addr().String(), updateNamesrvConfigOptions{
		NameServers: listener.Addr().String(),
		Key:         "clusterTest",
		Value:       "false",
	})
	if err != nil {
		t.Fatalf("update namesrv config: %v", err)
	}
	if !reflect.DeepEqual(targets, []string{listener.Addr().String()}) {
		t.Fatalf("unexpected update namesrv config targets %#v", targets)
	}
	if err := <-done; err != nil {
		t.Fatalf("namesrv server: %v", err)
	}
}

func TestClientUpdateControllerConfigUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeUpdateControllerConfig {
			done <- fmt.Errorf("expected UPDATE_CONTROLLER_CONFIG code %d, got %d", requestCodeUpdateControllerConfig, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("unexpected update controller config fields %#v", request.ExtFields)
			return
		}
		if string(request.Body) != "controllerDLegerGroup=group1\n" {
			done <- fmt.Errorf("unexpected update controller config body %q", string(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	targets, err := client.UpdateControllerConfig(context.Background(), listener.Addr().String(), updateControllerConfigOptions{
		ControllerAddrs: listener.Addr().String(),
		Key:             "controllerDLegerGroup",
		Value:           "group1",
	})
	if err != nil {
		t.Fatalf("update controller config: %v", err)
	}
	if !reflect.DeepEqual(targets, []string{listener.Addr().String()}) {
		t.Fatalf("unexpected update controller config targets %#v", targets)
	}
	if err := <-done; err != nil {
		t.Fatalf("controller server: %v", err)
	}
}

func TestClientWipeWritePermUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen namesrv: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeWipeWritePermOfBroker {
			done <- fmt.Errorf("expected WIPE_WRITE_PERM_OF_BROKER code %d, got %d", requestCodeWipeWritePermOfBroker, request.Code)
			return
		}
		if request.ExtFields["brokerName"] != "broker-a" {
			done <- fmt.Errorf("expected brokerName broker-a, got %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty wipeWritePerm body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{
			Code:      responseCodeSuccess,
			Language:  "JAVA",
			Version:   0,
			Opaque:    request.Opaque,
			Flag:      1,
			ExtFields: map[string]string{"wipeTopicCount": "12"},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	results, err := client.WipeWritePerm(context.Background(), listener.Addr().String(), "broker-a")
	if err != nil {
		t.Fatalf("wipe write perm: %v", err)
	}
	expected := []writePermResult{{NameServer: listener.Addr().String(), Count: 12}}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("unexpected wipeWritePerm results\nexpected=%#v\nactual=%#v", expected, results)
	}
	if err := <-done; err != nil {
		t.Fatalf("namesrv server: %v", err)
	}
}

func TestClientAddWritePermUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen namesrv: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeAddWritePermOfBroker {
			done <- fmt.Errorf("expected ADD_WRITE_PERM_OF_BROKER code %d, got %d", requestCodeAddWritePermOfBroker, request.Code)
			return
		}
		if request.ExtFields["brokerName"] != "broker-a" {
			done <- fmt.Errorf("expected brokerName broker-a, got %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty addWritePerm body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{
			Code:      responseCodeSuccess,
			Language:  "JAVA",
			Version:   0,
			Opaque:    request.Opaque,
			Flag:      1,
			ExtFields: map[string]string{"addTopicCount": "12"},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	results, err := client.AddWritePerm(context.Background(), listener.Addr().String(), "broker-a")
	if err != nil {
		t.Fatalf("add write perm: %v", err)
	}
	expected := []writePermResult{{NameServer: listener.Addr().String(), Count: 12}}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("unexpected addWritePerm results\nexpected=%#v\nactual=%#v", expected, results)
	}
	if err := <-done; err != nil {
		t.Fatalf("namesrv server: %v", err)
	}
}

func TestClientCloneGroupOffsetUsesConsumeStatsAndUpdateOffsetRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for index := 0; index < 2; index++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			if index == 0 {
				if request.Code != requestCodeGetConsumeStats {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("expected GET_CONSUME_STATS code %d, got %d", requestCodeGetConsumeStats, request.Code)
					return
				}
				if request.ExtFields["consumerGroup"] != "src-group" {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("unexpected consume stats fields %#v", request.ExtFields)
					return
				}
				if _, ok := request.ExtFields["topic"]; ok {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("cloneGroupOffset examineConsumeStats should not send topic, fields=%#v", request.ExtFields)
					return
				}
				body := []byte(`{"consumeTps":0.0,"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"brokerOffset":200,"consumerOffset":123,"lastTimestamp":0,"pullOffset":123}}}`)
				if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
					_ = conn.Close()
					brokerDone <- err
					return
				}
				_ = conn.Close()
				continue
			}
			if request.Code != requestCodeUpdateConsumerOffset {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("expected UPDATE_CONSUMER_OFFSET code %d, got %d", requestCodeUpdateConsumerOffset, request.Code)
				return
			}
			expectedFields := map[string]string{
				"consumerGroup": "dest-group",
				"topic":         "TopicTest",
				"queueId":       "0",
				"commitOffset":  "123",
				"brokerName":    "broker-a",
			}
			for key, expected := range expectedFields {
				if request.ExtFields[key] != expected {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("expected update offset field %s=%s, got %#v", key, expected, request.ExtFields)
					return
				}
			}
			if len(request.Body) != 0 {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("expected empty update offset body, got %d bytes", len(request.Body))
				return
			}
			if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			_ = conn.Close()
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		expectedTopics := []string{retryGroupTopicPrefix + "src-group", "TopicTest"}
		for _, expectedTopic := range expectedTopics {
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != expectedTopic {
				_ = conn.Close()
				nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
			if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			_ = conn.Close()
		}
		nameServerDone <- nil
	}()

	if err := NewClient(time.Second).CloneGroupOffset(context.Background(), nameServerListener.Addr().String(), "src-group", "dest-group", "TopicTest"); err != nil {
		t.Fatalf("clone group offset: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientSendMessageUsesOfficialV2Request(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	uniqID := make(chan string, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeSendMessageV2 {
			brokerDone <- fmt.Errorf("expected SEND_MESSAGE_V2 code %d, got %d", requestCodeSendMessageV2, request.Code)
			return
		}
		expectedFields := map[string]string{
			"b": "TopicTest",
			"c": "TBW102",
			"d": "4",
			"e": "1",
			"f": "0",
			"h": "0",
			"j": "0",
			"k": "false",
			"m": "false",
			"n": "broker-a",
		}
		for key, expected := range expectedFields {
			if request.ExtFields[key] != expected {
				brokerDone <- fmt.Errorf("expected send field %s=%s, got %#v", key, expected, request.ExtFields)
				return
			}
		}
		if strings.TrimSpace(request.ExtFields["a"]) == "" {
			brokerDone <- fmt.Errorf("producerGroup should be set, fields=%#v", request.ExtFields)
			return
		}
		if strings.TrimSpace(request.ExtFields["g"]) == "" {
			brokerDone <- fmt.Errorf("bornTimestamp should be set, fields=%#v", request.ExtFields)
			return
		}
		properties := request.ExtFields["i"]
		if !strings.Contains(properties, "KEYS\x01KeyA\x02") || !strings.Contains(properties, "TAGS\x01TagA\x02") || !strings.Contains(properties, "UNIQ_KEY\x01") {
			brokerDone <- fmt.Errorf("unexpected properties %q", properties)
			return
		}
		for _, part := range strings.Split(properties, "\x02") {
			if strings.HasPrefix(part, "UNIQ_KEY\x01") {
				uniqID <- strings.TrimPrefix(part, "UNIQ_KEY\x01")
				break
			}
		}
		if string(request.Body) != "hello" {
			brokerDone <- fmt.Errorf("unexpected send body %q", string(request.Body))
			return
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
			ExtFields: map[string]string{
				"msgId":       "OFFSET-ID",
				"queueId":     "1",
				"queueOffset": "7",
			},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}]}`, brokerListener.Addr().String()))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	client := NewClient(time.Second)
	result, err := client.SendMessage(context.Background(), nameServerListener.Addr().String(), sendMessageOptions{
		Topic:      "TopicTest",
		Body:       "hello",
		Keys:       "KeyA",
		Tags:       "TagA",
		BrokerName: "broker-a",
		QueueID:    1,
		HasQueueID: true,
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	generatedUniqID := <-uniqID
	expected := &sendMessageResult{
		Topic:           "TopicTest",
		BrokerName:      "broker-a",
		QueueID:         1,
		SendStatus:      "SEND_OK",
		MessageID:       generatedUniqID,
		OffsetMessageID: "OFFSET-ID",
		QueueOffset:     7,
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("unexpected send result\nexpected=%#v\nactual=%#v", expected, result)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
}

func TestClientSendMessageTraceSendsOfficialPubTrace(t *testing.T) {
	businessBrokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen business broker: %v", err)
	}
	defer businessBrokerListener.Close()
	traceBrokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen trace broker: %v", err)
	}
	defer traceBrokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	businessMsgID := make(chan string, 1)
	producerGroup := make(chan string, 1)
	nameServerDone := make(chan error, 1)
	go func() {
		tcpListener, ok := nameServerListener.(*net.TCPListener)
		if !ok {
			nameServerDone <- fmt.Errorf("expected TCP nameserver listener, got %T", nameServerListener)
			return
		}
		routes := []struct {
			topic      string
			brokerAddr string
		}{
			{topic: "TopicTest", brokerAddr: businessBrokerListener.Addr().String()},
			{topic: defaultTraceTopic, brokerAddr: traceBrokerListener.Addr().String()},
		}
		for _, route := range routes {
			if err := tcpListener.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
				nameServerDone <- err
				return
			}
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != route.topic {
				_ = conn.Close()
				nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
				return
			}
			body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}]}`, route.brokerAddr))
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			_ = conn.Close()
		}
		nameServerDone <- nil
	}()

	businessBrokerDone := make(chan error, 1)
	go func() {
		conn, err := businessBrokerListener.Accept()
		if err != nil {
			businessBrokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			businessBrokerDone <- err
			return
		}
		if request.Code != requestCodeSendMessageV2 {
			businessBrokerDone <- fmt.Errorf("expected business SEND_MESSAGE_V2 code %d, got %d", requestCodeSendMessageV2, request.Code)
			return
		}
		if request.ExtFields["b"] != "TopicTest" || request.ExtFields["e"] != "1" || request.ExtFields["n"] != "broker-a" || string(request.Body) != "hello" {
			businessBrokerDone <- fmt.Errorf("unexpected business send request fields=%#v body=%q", request.ExtFields, string(request.Body))
			return
		}
		producerGroup <- request.ExtFields["a"]
		properties := decodeMessageProperties(request.ExtFields["i"])
		uniqID := properties.Get("UNIQ_KEY")
		if uniqID == "" || properties.Get("KEYS") != "KeyA" || properties.Get("TAGS") != "TagA" {
			businessBrokerDone <- fmt.Errorf("unexpected business properties %#v", properties)
			return
		}
		businessMsgID <- uniqID
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
			ExtFields: map[string]string{
				"msgId":       "OFFSET-ID",
				"queueId":     "1",
				"queueOffset": "7",
			},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		businessBrokerDone <- err
	}()

	traceBrokerDone := make(chan error, 1)
	go func() {
		tcpListener, ok := traceBrokerListener.(*net.TCPListener)
		if !ok {
			traceBrokerDone <- fmt.Errorf("expected TCP trace listener, got %T", traceBrokerListener)
			return
		}
		if err := tcpListener.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			traceBrokerDone <- err
			return
		}
		conn, err := traceBrokerListener.Accept()
		if err != nil {
			traceBrokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			traceBrokerDone <- err
			return
		}
		if request.Code != requestCodeSendMessageV2 {
			traceBrokerDone <- fmt.Errorf("expected trace SEND_MESSAGE_V2 code %d, got %d", requestCodeSendMessageV2, request.Code)
			return
		}
		if request.ExtFields["b"] != defaultTraceTopic || request.ExtFields["n"] != "broker-a" {
			traceBrokerDone <- fmt.Errorf("unexpected trace send fields %#v", request.ExtFields)
			return
		}
		properties := decodeMessageProperties(request.ExtFields["i"])
		traceKeys := splitMessageKeys(properties.Get("KEYS"))
		msgID := <-businessMsgID
		if !containsMessageKey(traceKeys, msgID) || !containsMessageKey(traceKeys, "KeyA") {
			traceBrokerDone <- fmt.Errorf("trace keys should contain msgId and business key, got %#v", traceKeys)
			return
		}
		if properties.Get("TAGS") != "" {
			traceBrokerDone <- fmt.Errorf("trace message should not set tags, properties=%#v", properties)
			return
		}
		contexts := javaStyleSplit(string(request.Body), string(traceFieldSplitter))
		if len(contexts) != 1 {
			traceBrokerDone <- fmt.Errorf("unexpected trace context count %d body=%q", len(contexts), string(request.Body))
			return
		}
		fields := javaStyleSplit(contexts[0], string(traceContentSplitter))
		if len(fields) != 14 {
			traceBrokerDone <- fmt.Errorf("unexpected trace field count %d body=%q", len(fields), string(request.Body))
			return
		}
		group := <-producerGroup
		expectedFields := map[int]string{
			0:  "Pub",
			2:  "DefaultRegion",
			3:  group,
			4:  "TopicTest",
			5:  msgID,
			6:  "TagA",
			7:  "KeyA",
			8:  businessBrokerListener.Addr().String(),
			9:  "5",
			11: "0",
			12: "OFFSET-ID",
			13: "true",
		}
		for index, expected := range expectedFields {
			if fields[index] != expected {
				traceBrokerDone <- fmt.Errorf("unexpected trace field[%d], expected=%q actual=%q body=%q", index, expected, fields[index], string(request.Body))
				return
			}
		}
		if _, err := strconv.ParseInt(fields[1], 10, 64); err != nil {
			traceBrokerDone <- fmt.Errorf("trace timestamp should be int64, got %q", fields[1])
			return
		}
		if costTime, err := strconv.Atoi(fields[10]); err != nil || costTime < 0 {
			traceBrokerDone <- fmt.Errorf("trace costTime should be non-negative int, got %q", fields[10])
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		traceBrokerDone <- err
	}()

	if _, err := NewClient(time.Second).SendMessage(context.Background(), nameServerListener.Addr().String(), sendMessageOptions{
		Topic:          "TopicTest",
		Body:           "hello",
		Keys:           "KeyA",
		Tags:           "TagA",
		BrokerName:     "broker-a",
		QueueID:        1,
		HasQueueID:     true,
		MsgTraceEnable: true,
	}); err != nil {
		t.Fatalf("send message trace: %v", err)
	}
	if err := <-businessBrokerDone; err != nil {
		t.Fatalf("business broker side: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-traceBrokerDone; err != nil {
		t.Fatalf("trace broker side: %v", err)
	}
}

func TestClientSendMsgStatusSendsWarmupAndCountMessages(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for index := 0; index < 3; index++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			if request.Code != requestCodeSendMessageV2 {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("expected SEND_MESSAGE_V2 code %d, got %d", requestCodeSendMessageV2, request.Code)
				return
			}
			expectedFields := map[string]string{
				"a": "PID_SMSC",
				"b": "broker-a",
				"c": "TBW102",
				"d": "4",
				"e": "0",
				"f": "0",
				"h": "0",
				"j": "0",
				"k": "false",
				"m": "false",
				"n": "broker-a",
			}
			for key, expected := range expectedFields {
				if request.ExtFields[key] != expected {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("expected sendMsgStatus field %s=%s, got %#v", key, expected, request.ExtFields)
					return
				}
			}
			if strings.Contains(request.ExtFields["i"], "WAIT\x01") || !strings.Contains(request.ExtFields["i"], "UNIQ_KEY\x01") {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected sendMsgStatus properties %q", request.ExtFields["i"])
				return
			}
			expectedBody := "hello jodie"
			if index == 0 {
				expectedBody = "hello jodiehello jodie"
			}
			if string(request.Body) != expectedBody {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected sendMsgStatus body index=%d body=%q", index, string(request.Body))
				return
			}
			response := remotingCommand{
				Code:     responseCodeSuccess,
				Language: "JAVA",
				Version:  0,
				Opaque:   request.Opaque,
				Flag:     1,
				ExtFields: map[string]string{
					"msgId":       fmt.Sprintf("OFFSET-%d", index),
					"queueId":     "0",
					"queueOffset": strconv.Itoa(100 + index),
				},
			}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			_ = conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "broker-a" {
			nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":1,"topicSysFlag":0,"writeQueueNums":1}]}`, brokerListener.Addr().String()))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	client := NewClient(time.Second)
	results, err := client.SendMsgStatus(context.Background(), nameServerListener.Addr().String(), sendMsgStatusOptions{
		BrokerName:  "broker-a",
		MessageSize: 1,
		Count:       2,
	})
	if err != nil {
		t.Fatalf("send msg status: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 printed send results, got %#v", results)
	}
	for index, result := range results {
		if result.SendResult.Topic != "broker-a" || result.SendResult.BrokerName != "broker-a" || result.SendResult.QueueID != 0 || result.SendResult.SendStatus != "SEND_OK" {
			t.Fatalf("unexpected sendMsgStatus result[%d]=%#v", index, result)
		}
		if result.SendResult.OffsetMessageID != fmt.Sprintf("OFFSET-%d", index+1) || result.SendResult.QueueOffset != int64(101+index) {
			t.Fatalf("unexpected sendMsgStatus offset result[%d]=%#v", index, result)
		}
		if strings.TrimSpace(result.SendResult.MessageID) == "" || result.RTMillis < 0 {
			t.Fatalf("unexpected sendMsgStatus dynamic result[%d]=%#v", index, result)
		}
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
}

func TestClientCheckMsgSendRTCyclesQueuesAndAveragesAfterFirstSend(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for index := 0; index < 2; index++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			if request.Code != requestCodeSendMessageV2 {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("expected SEND_MESSAGE_V2 code %d, got %d", requestCodeSendMessageV2, request.Code)
				return
			}
			expectedFields := map[string]string{
				"b": "TopicTest",
				"c": "TBW102",
				"d": "4",
				"e": strconv.Itoa(index),
				"f": "0",
				"h": "0",
				"j": "0",
				"k": "false",
				"m": "false",
				"n": "broker-a",
			}
			for key, expected := range expectedFields {
				if request.ExtFields[key] != expected {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("expected checkMsgSendRT field %s=%s, got %#v", key, expected, request.ExtFields)
					return
				}
			}
			if _, err := strconv.ParseInt(request.ExtFields["a"], 10, 64); err != nil {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("producerGroup should be current millis, fields=%#v", request.ExtFields)
				return
			}
			properties := request.ExtFields["i"]
			if !strings.Contains(properties, "WAIT\x01true\x02") || !strings.Contains(properties, "UNIQ_KEY\x01") {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected checkMsgSendRT properties %q", properties)
				return
			}
			if string(request.Body) != strings.Repeat("a", 16) {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected checkMsgSendRT body %q", string(request.Body))
				return
			}
			response := remotingCommand{
				Code:     responseCodeSuccess,
				Language: "JAVA",
				Version:  0,
				Opaque:   request.Opaque,
				Flag:     1,
				ExtFields: map[string]string{
					"msgId":       fmt.Sprintf("OFFSET-%d", index),
					"queueId":     strconv.Itoa(index),
					"queueOffset": strconv.Itoa(200 + index),
				},
			}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			_ = conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":2,"topicSysFlag":0,"writeQueueNums":2}]}`, brokerListener.Addr().String()))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	result, err := NewClient(time.Second).CheckMsgSendRT(context.Background(), nameServerListener.Addr().String(), checkMsgSendRTOptions{
		Topic:  "TopicTest",
		Amount: 2,
		Size:   16,
	})
	if err != nil {
		t.Fatalf("check message send rt: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 send rows, got %#v", result)
	}
	for index, row := range result.Rows {
		if row.BrokerName != "broker-a" || row.QueueID != index || !row.SendSuccess || row.RTMillis < 0 {
			t.Fatalf("unexpected checkMsgSendRT row[%d]=%#v", index, row)
		}
	}
	if result.AvgRT != float64(result.Rows[1].RTMillis) {
		t.Fatalf("expected avg to ignore first send, result=%#v", result)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
}

func TestClientClusterRTFetchesClusterAndSendsToBrokerNameTopic(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for index := 0; index < 2; index++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			if request.Code != requestCodeSendMessageV2 {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("expected SEND_MESSAGE_V2 code %d, got %d", requestCodeSendMessageV2, request.Code)
				return
			}
			expectedFields := map[string]string{
				"b": "broker-a",
				"c": "TBW102",
				"d": "4",
				"e": strconv.Itoa(index),
				"f": "0",
				"h": "0",
				"j": "0",
				"k": "false",
				"m": "false",
				"n": "broker-a",
			}
			for key, expected := range expectedFields {
				if request.ExtFields[key] != expected {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("expected clusterRT field %s=%s, got %#v", key, expected, request.ExtFields)
					return
				}
			}
			properties := request.ExtFields["i"]
			if strings.Contains(properties, "WAIT\x01true\x02") || !strings.Contains(properties, "UNIQ_KEY\x01") {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected clusterRT properties %q", properties)
				return
			}
			if string(request.Body) != strings.Repeat("a", 16) {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected clusterRT body %q", string(request.Body))
				return
			}
			response := remotingCommand{
				Code:     responseCodeSuccess,
				Language: "JAVA",
				Version:  0,
				Opaque:   request.Opaque,
				Flag:     1,
				ExtFields: map[string]string{
					"msgId":       fmt.Sprintf("OFFSET-%d", index),
					"queueId":     strconv.Itoa(index),
					"queueOffset": strconv.Itoa(300 + index),
				},
			}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			_ = conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		for index := 0; index < 2; index++ {
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			switch index {
			case 0:
				if request.Code != requestCodeGetBrokerClusterInfo {
					_ = conn.Close()
					nameServerDone <- fmt.Errorf("unexpected clusterInfo request code=%d fields=%#v", request.Code, request.ExtFields)
					return
				}
				body := []byte(fmt.Sprintf(`{"clusterAddrTable":{"DefaultCluster":["broker-a"]},"brokerAddrTable":{"broker-a":{"brokerName":"broker-a","brokerAddrs":{0:%q},"cluster":"DefaultCluster"}}}`, brokerListener.Addr().String()))
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				_, err = conn.Write(remotingFrameForTest(t, response, body))
			case 1:
				if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "broker-a" {
					_ = conn.Close()
					nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
					return
				}
				body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":2,"topicSysFlag":0,"writeQueueNums":2}]}`, brokerListener.Addr().String()))
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				_, err = conn.Write(remotingFrameForTest(t, response, body))
			}
			_ = conn.Close()
			if err != nil {
				nameServerDone <- err
				return
			}
		}
		nameServerDone <- nil
	}()

	result, err := NewClient(time.Second).ClusterRT(context.Background(), nameServerListener.Addr().String(), clusterRTOptions{
		ClusterName: "DefaultCluster",
		Amount:      2,
		Size:        16,
		MachineRoom: "noname",
	})
	if err != nil {
		t.Fatalf("clusterRT: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 clusterRT row, got %#v", result)
	}
	row := result.Rows[0]
	if row.ClusterName != "DefaultCluster" || row.BrokerName != "broker-a" || row.SuccessCount != 2 || row.FailCount != 0 || row.RT < 0 {
		t.Fatalf("unexpected clusterRT row %#v", row)
	}
	if row.Timestamp.IsZero() {
		t.Fatalf("expected clusterRT timestamp to be set")
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
}

func TestClientResetMasterFlushOffsetUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeResetMasterFlushOffset {
			done <- fmt.Errorf("expected RESET_MASTER_FLUSH_OFFSET code %d, got %d", requestCodeResetMasterFlushOffset, request.Code)
			return
		}
		if !reflect.DeepEqual(request.ExtFields, map[string]string{"masterFlushOffset": "42"}) {
			done <- fmt.Errorf("unexpected resetMasterFlushOffset fields %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty resetMasterFlushOffset body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	if err := NewClient(time.Second).ResetMasterFlushOffset(context.Background(), brokerListener.Addr().String(), 42); err != nil {
		t.Fatalf("reset master flush offset: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestRunNativeRemoveBrokerNegativeBrokerIDFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		removeBroker: func(ctx context.Context, brokerContainerAddr string, options removeBrokerOptions) error {
			t.Fatalf("negative brokerId should not call removeBroker: addr=%s options=%#v", brokerContainerAddr, options)
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"removeBroker", "-c", "127.0.0.1:30911", "-b", "DefaultCluster:broker-a:-1"}, client)
	if err != nil {
		t.Fatalf("removeBroker negative brokerId: %v", err)
	}
	if !supported {
		t.Fatalf("expected removeBroker to be supported")
	}
	if output != "brokerId can't be negative\n" {
		t.Fatalf("unexpected removeBroker negative output %q", output)
	}
}

func TestRunNativeAddBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		addBroker: func(ctx context.Context, brokerContainerAddr string, options addBrokerOptions) error {
			if brokerContainerAddr != "container-a:10911" {
				t.Fatalf("unexpected broker container addr %s", brokerContainerAddr)
			}
			expected := addBrokerOptions{
				BrokerContainerAddr: "container-a:10911",
				BrokerConfigPath:    "/tmp/broker.conf",
			}
			if !reflect.DeepEqual(options, expected) {
				t.Fatalf("addBroker options mismatch\nexpected=%#v\nactual=%#v", expected, options)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"addBroker", "-c", "container-a:10911", "-b", "/tmp/broker.conf"}, client)
	if err != nil {
		t.Fatalf("addBroker: %v", err)
	}
	if !supported {
		t.Fatalf("expected addBroker to be supported")
	}
	if output != "add broker to container-a:10911 success\n" {
		t.Fatalf("unexpected addBroker output %q", output)
	}
}

func TestClientAddBrokerUsesOfficialRequestCode(t *testing.T) {
	brokerContainerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker container: %v", err)
	}
	defer brokerContainerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerContainerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeAddBroker {
			done <- fmt.Errorf("expected ADD_BROKER code %d, got %d", requestCodeAddBroker, request.Code)
			return
		}
		if !reflect.DeepEqual(request.ExtFields, map[string]string{"configPath": "/tmp/broker.conf"}) {
			done <- fmt.Errorf("unexpected addBroker fields %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty addBroker body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	err = NewClient(time.Second).AddBroker(context.Background(), brokerContainerListener.Addr().String(), addBrokerOptions{
		BrokerConfigPath: "/tmp/broker.conf",
	})
	if err != nil {
		t.Fatalf("addBroker: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker container side: %v", err)
	}
}

func TestRunNativeRemoveBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		removeBroker: func(ctx context.Context, brokerContainerAddr string, options removeBrokerOptions) error {
			if brokerContainerAddr != "container-a:10911" {
				t.Fatalf("unexpected broker container addr %s", brokerContainerAddr)
			}
			expected := removeBrokerOptions{
				BrokerContainerAddr: "container-a:10911",
				BrokerIdentity:      "DefaultCluster:broker-a:0",
				ClusterName:         "DefaultCluster",
				BrokerName:          "broker-a",
				BrokerID:            0,
			}
			if !reflect.DeepEqual(options, expected) {
				t.Fatalf("removeBroker options mismatch\nexpected=%#v\nactual=%#v", expected, options)
			}
			return nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"removeBroker", "-c", "container-a:10911", "-b", "DefaultCluster:broker-a:0"}, client)
	if err != nil {
		t.Fatalf("removeBroker: %v", err)
	}
	if !supported {
		t.Fatalf("expected removeBroker to be supported")
	}
	if output != "remove broker from container-a:10911 success\n" {
		t.Fatalf("unexpected removeBroker output %q", output)
	}
}

func TestRunNativeGetBrokerEpochFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		getBrokerEpoch: func(ctx context.Context, nameServer string, brokerName string) ([]brokerEpochResult, error) {
			if nameServer != "127.0.0.1:9876" || brokerName != "goadmin-controller-broker" {
				t.Fatalf("unexpected getBrokerEpoch args namesrv=%s brokerName=%s", nameServer, brokerName)
			}
			return []brokerEpochResult{{
				ClusterName: "DefaultCluster",
				BrokerName:  "goadmin-controller-broker",
				BrokerAddr:  "172.24.0.4:30941",
				BrokerID:    0,
				MaxOffset:   0,
				EpochList: []epochEntry{{
					Epoch:       1,
					StartOffset: 0,
					EndOffset:   9223372036854775807,
				}},
			}}, nil
		},
	}
	output, supported, err := runNativeCommand(context.Background(), []string{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-b", "goadmin-controller-broker"}, client)
	if err != nil {
		t.Fatalf("getBrokerEpoch: %v", err)
	}
	if !supported {
		t.Fatalf("getBrokerEpoch should be supported")
	}
	expected := "\n#clusterName\tDefaultCluster\n#brokerName\tgoadmin-controller-broker\n#brokerAddr\t172.24.0.4:30941\n#brokerId\t0\n#Epoch: EpochEntry{epoch=1, startOffset=0, endOffset=0}\n"
	if output != expected {
		t.Fatalf("unexpected output\nwant=%q\n got=%q", expected, output)
	}
}

func TestRunNativeGetBrokerEpochClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		getBrokerEpochByCluster: func(ctx context.Context, nameServer string, clusterName string) ([]brokerEpochResult, error) {
			if nameServer != "127.0.0.1:9876" || clusterName != "GoadminControllerCluster" {
				t.Fatalf("unexpected getBrokerEpoch cluster args namesrv=%s clusterName=%s", nameServer, clusterName)
			}
			return []brokerEpochResult{{
				ClusterName: "GoadminControllerCluster",
				BrokerName:  "goadmin-controller-broker",
				BrokerAddr:  "172.24.0.4:30951",
				BrokerID:    0,
				MaxOffset:   12,
				EpochList: []epochEntry{{
					Epoch:       1,
					StartOffset: 0,
					EndOffset:   9223372036854775807,
				}},
			}}, nil
		},
	}
	output, supported, err := runNativeCommand(context.Background(), []string{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-c", "GoadminControllerCluster"}, client)
	if err != nil {
		t.Fatalf("getBrokerEpoch cluster: %v", err)
	}
	if !supported {
		t.Fatalf("getBrokerEpoch cluster should be supported")
	}
	expected := "\n#clusterName\tGoadminControllerCluster\n#brokerName\tgoadmin-controller-broker\n#brokerAddr\t172.24.0.4:30951\n#brokerId\t0\n#Epoch: EpochEntry{epoch=1, startOffset=0, endOffset=12}\n"
	if output != expected {
		t.Fatalf("unexpected cluster output\nwant=%q\n got=%q", expected, output)
	}
}

func TestRunNativeGetBrokerEpochFallsBackForInterval(t *testing.T) {
	for _, args := range [][]string{
		{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-b", "broker-a", "-i", "1"},
	} {
		output, supported, err := runNativeCommand(context.Background(), args, nil)
		if err != nil || supported || output != "" {
			t.Fatalf("args=%v should fallback cleanly, output=%q supported=%v err=%v", args, output, supported, err)
		}
	}
}

func TestClientRemoveBrokerUsesOfficialRequestCode(t *testing.T) {
	brokerContainerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker container: %v", err)
	}
	defer brokerContainerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerContainerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeRemoveBroker {
			done <- fmt.Errorf("expected REMOVE_BROKER code %d, got %d", requestCodeRemoveBroker, request.Code)
			return
		}
		expectedFields := map[string]string{
			"brokerClusterName": "DefaultCluster",
			"brokerName":        "broker-a",
			"brokerId":          "0",
		}
		if !reflect.DeepEqual(request.ExtFields, expectedFields) {
			done <- fmt.Errorf("unexpected removeBroker fields %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty removeBroker body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	err = NewClient(time.Second).RemoveBroker(context.Background(), brokerContainerListener.Addr().String(), removeBrokerOptions{
		ClusterName: "DefaultCluster",
		BrokerName:  "broker-a",
		BrokerID:    0,
	})
	if err != nil {
		t.Fatalf("removeBroker: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker container side: %v", err)
	}
}

func TestClientGetBrokerEpochUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerEpochCache {
			brokerDone <- fmt.Errorf("expected GET_BROKER_EPOCH_CACHE code %d, got %d", requestCodeGetBrokerEpochCache, request.Code)
			return
		}
		if len(request.ExtFields) != 0 || len(request.Body) != 0 {
			brokerDone <- fmt.Errorf("expected empty getBrokerEpoch request fields/body, fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		body := []byte(`{"brokerId":0,"brokerName":"goadmin-controller-broker","clusterName":"DefaultCluster","epochList":[{"endOffset":9223372036854775807,"epoch":1,"startOffset":0}],"maxOffset":0}`)
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- fmt.Errorf("expected GET_BROKER_CLUSTER_INFO code %d, got %d", requestCodeGetBrokerClusterInfo, request.Code)
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"goadmin-controller-broker":{"brokerAddrs":{"0":"%s"},"brokerName":"goadmin-controller-broker","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["goadmin-controller-broker"]}}`, brokerListener.Addr().String()))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	results, err := NewClient(time.Second).GetBrokerEpoch(context.Background(), nameServerListener.Addr().String(), "goadmin-controller-broker")
	if err != nil {
		t.Fatalf("getBrokerEpoch: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
	expected := []brokerEpochResult{{
		ClusterName: "DefaultCluster",
		BrokerName:  "goadmin-controller-broker",
		BrokerAddr:  brokerListener.Addr().String(),
		BrokerID:    0,
		MaxOffset:   0,
		EpochList:   []epochEntry{{Epoch: 1, StartOffset: 0, EndOffset: 9223372036854775807}},
	}}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("unexpected epoch results\nexpected=%#v\nactual=%#v", expected, results)
	}
}

func TestClientGetBrokerEpochByClusterUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerEpochCache {
			brokerDone <- fmt.Errorf("expected GET_BROKER_EPOCH_CACHE code %d, got %d", requestCodeGetBrokerEpochCache, request.Code)
			return
		}
		if len(request.ExtFields) != 0 || len(request.Body) != 0 {
			brokerDone <- fmt.Errorf("expected empty getBrokerEpoch cluster request fields/body, fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		body := []byte(`{"brokerId":0,"brokerName":"goadmin-controller-broker","clusterName":"GoadminControllerCluster","epochList":[{"endOffset":9223372036854775807,"epoch":1,"startOffset":0}],"maxOffset":12}`)
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- fmt.Errorf("expected GET_BROKER_CLUSTER_INFO code %d, got %d", requestCodeGetBrokerClusterInfo, request.Code)
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"goadmin-controller-broker":{"brokerAddrs":{"0":"%s"},"brokerName":"goadmin-controller-broker","cluster":"GoadminControllerCluster"}},"clusterAddrTable":{"GoadminControllerCluster":["goadmin-controller-broker"]}}`, brokerListener.Addr().String()))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	results, err := NewClient(time.Second).GetBrokerEpochByCluster(context.Background(), nameServerListener.Addr().String(), "GoadminControllerCluster")
	if err != nil {
		t.Fatalf("getBrokerEpoch cluster: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
	expected := []brokerEpochResult{{
		ClusterName: "GoadminControllerCluster",
		BrokerName:  "goadmin-controller-broker",
		BrokerAddr:  brokerListener.Addr().String(),
		BrokerID:    0,
		MaxOffset:   12,
		EpochList:   []epochEntry{{Epoch: 1, StartOffset: 0, EndOffset: 9223372036854775807}},
	}}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("unexpected cluster epoch results\nexpected=%#v\nactual=%#v", expected, results)
	}
}

func TestClientGetControllerMetaDataUsesOfficialRequestCode(t *testing.T) {
	controllerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	defer controllerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := controllerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeControllerGetMetadataInfo {
			done <- fmt.Errorf("expected CONTROLLER_GET_METADATA_INFO code %d, got %d", requestCodeControllerGetMetadataInfo, request.Code)
			return
		}
		if len(request.ExtFields) != 0 || len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty controller metadata request fields/body, got fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
			ExtFields: map[string]string{
				"controllerLeaderAddress": "127.0.0.1:9878",
				"controllerLeaderId":      "n0",
				"group":                   "group1",
				"isLeader":                "true",
				"peers":                   "n0:127.0.0.1:9878",
			},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	meta, err := client.GetControllerMetaData(context.Background(), controllerListener.Addr().String())
	if err != nil {
		t.Fatalf("get controller metadata: %v", err)
	}
	expected := &controllerMetaData{
		Group:                   "group1",
		ControllerLeaderID:      "n0",
		ControllerLeaderAddress: "127.0.0.1:9878",
		IsLeader:                true,
		Peers:                   "n0:127.0.0.1:9878",
	}
	if !reflect.DeepEqual(meta, expected) {
		t.Fatalf("controller metadata mismatch\nexpected=%#v\nactual=%#v", expected, meta)
	}
	if err := <-done; err != nil {
		t.Fatalf("controller side: %v", err)
	}
}

func TestClientGetSyncStateSetUsesOfficialRequestCode(t *testing.T) {
	controllerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	defer controllerListener.Close()

	done := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := controllerListener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				done <- err
				return
			}
			switch i {
			case 0:
				if request.Code != requestCodeControllerGetMetadataInfo {
					conn.Close()
					done <- fmt.Errorf("expected CONTROLLER_GET_METADATA_INFO code %d, got %d", requestCodeControllerGetMetadataInfo, request.Code)
					return
				}
				if len(request.ExtFields) != 0 || len(request.Body) != 0 {
					conn.Close()
					done <- fmt.Errorf("expected empty controller metadata request fields/body, got fields=%#v body=%d", request.ExtFields, len(request.Body))
					return
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"controllerLeaderAddress": controllerListener.Addr().String(),
						"controllerLeaderId":      "n0",
						"group":                   "group1",
						"isLeader":                "true",
						"peers":                   "n0:" + controllerListener.Addr().String(),
					},
				}
				if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
					conn.Close()
					done <- err
					return
				}
			case 1:
				if request.Code != requestCodeControllerGetSyncStateData {
					conn.Close()
					done <- fmt.Errorf("expected CONTROLLER_GET_SYNC_STATE_DATA code %d, got %d", requestCodeControllerGetSyncStateData, request.Code)
					return
				}
				if len(request.ExtFields) != 0 {
					conn.Close()
					done <- fmt.Errorf("expected empty getSyncStateSet ext fields, got %#v", request.ExtFields)
					return
				}
				if string(request.Body) != `["broker-a"]` {
					conn.Close()
					done <- fmt.Errorf("expected broker list JSON body, got %q", string(request.Body))
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				body := []byte(`{"replicasInfoTable":{"broker-a":{"masterBrokerId":0,"masterAddress":"127.0.0.1:30911","masterEpoch":3,"syncStateSetEpoch":4,"inSyncReplicas":[{"brokerName":"broker-a","brokerId":0,"brokerAddress":"127.0.0.1:30911","alive":true}],"notInSyncReplicas":[]}}}`)
				if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
					conn.Close()
					done <- err
					return
				}
			}
			conn.Close()
		}
		done <- nil
	}()

	result, err := NewClient(time.Second).GetSyncStateSet(context.Background(), controllerListener.Addr().String(), []string{"broker-a"})
	if err != nil {
		t.Fatalf("get sync state set: %v", err)
	}
	if len(result.Brokers) != 1 || result.Brokers[0].BrokerName != "broker-a" || result.Brokers[0].MasterBrokerID != 0 {
		t.Fatalf("unexpected sync state set %#v", result)
	}
	if len(result.Brokers[0].InSyncReplicas) != 1 || result.Brokers[0].InSyncReplicas[0].Alive == nil || !*result.Brokers[0].InSyncReplicas[0].Alive {
		t.Fatalf("unexpected in-sync replicas %#v", result.Brokers[0].InSyncReplicas)
	}
	if err := <-done; err != nil {
		t.Fatalf("controller side: %v", err)
	}
}

func TestClientGetSyncStateSetByClusterUsesOfficialRequestCodes(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()
	controllerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	defer controllerListener.Close()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- fmt.Errorf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"brokerAddrTable":{"broker-b":{"brokerAddrs":{"0":"127.0.0.1:30921"},"brokerName":"broker-b","cluster":"GoadminControllerCluster"},"broker-a":{"brokerAddrs":{"0":"127.0.0.1:30911"},"brokerName":"broker-a","cluster":"GoadminControllerCluster"}},"clusterAddrTable":{"GoadminControllerCluster":["broker-b","broker-a"]}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	controllerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := controllerListener.Accept()
			if err != nil {
				controllerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				controllerDone <- err
				return
			}
			switch i {
			case 0:
				if request.Code != requestCodeControllerGetMetadataInfo {
					conn.Close()
					controllerDone <- fmt.Errorf("expected CONTROLLER_GET_METADATA_INFO code %d, got %d", requestCodeControllerGetMetadataInfo, request.Code)
					return
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"controllerLeaderAddress": controllerListener.Addr().String(),
						"controllerLeaderId":      "n0",
						"group":                   "group1",
						"isLeader":                "true",
						"peers":                   "n0:" + controllerListener.Addr().String(),
					},
				}
				if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
					conn.Close()
					controllerDone <- err
					return
				}
			case 1:
				if request.Code != requestCodeControllerGetSyncStateData {
					conn.Close()
					controllerDone <- fmt.Errorf("expected CONTROLLER_GET_SYNC_STATE_DATA code %d, got %d", requestCodeControllerGetSyncStateData, request.Code)
					return
				}
				if len(request.ExtFields) != 0 {
					conn.Close()
					controllerDone <- fmt.Errorf("expected empty getSyncStateSet ext fields, got %#v", request.ExtFields)
					return
				}
				if string(request.Body) != `["broker-a","broker-b"]` {
					conn.Close()
					controllerDone <- fmt.Errorf("expected sorted broker list JSON body, got %q", string(request.Body))
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				body := []byte(`{"replicasInfoTable":{"broker-a":{"masterBrokerId":0,"masterAddress":"127.0.0.1:30911","masterEpoch":3,"syncStateSetEpoch":4,"inSyncReplicas":[{"brokerName":"broker-a","brokerId":0,"brokerAddress":"127.0.0.1:30911","alive":true}],"notInSyncReplicas":[]},"broker-b":{"masterBrokerId":0,"masterAddress":"127.0.0.1:30921","masterEpoch":5,"syncStateSetEpoch":6,"inSyncReplicas":[],"notInSyncReplicas":[]}}}`)
				if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
					conn.Close()
					controllerDone <- err
					return
				}
			}
			conn.Close()
		}
		controllerDone <- nil
	}()

	result, err := NewClient(time.Second).GetSyncStateSetByCluster(context.Background(), nameServerListener.Addr().String(), controllerListener.Addr().String(), "GoadminControllerCluster")
	if err != nil {
		t.Fatalf("get sync state set by cluster: %v", err)
	}
	if len(result.Brokers) != 2 || result.Brokers[0].BrokerName != "broker-a" || result.Brokers[1].BrokerName != "broker-b" {
		t.Fatalf("unexpected sync state set cluster result %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-controllerDone; err != nil {
		t.Fatalf("controller side: %v", err)
	}
}

func TestClientListUserUsesOfficialRequestCodeAndFilter(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeAuthListUser {
			done <- fmt.Errorf("expected AUTH_LIST_USER code %d, got %d", requestCodeAuthListUser, request.Code)
			return
		}
		if request.ExtFields["filter"] != "admin" || len(request.Body) != 0 {
			done <- fmt.Errorf("unexpected listUser request fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`[{"username":"admin","password":"******","userType":"Super","userStatus":"enable"}]`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		done <- err
	}()

	rows, err := NewClient(time.Second).ListUser(context.Background(), "", brokerListener.Addr().String(), "", "admin")
	if err != nil {
		t.Fatalf("list user: %v", err)
	}
	if len(rows) != 1 || rows[0].Username != "admin" || rows[0].Password != "******" || rows[0].UserType != "Super" || rows[0].UserStatus != "enable" {
		t.Fatalf("unexpected listUser rows %#v", rows)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientListUserOmitsFilterWhenBlankAndAcceptsEmptyBody(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeAuthListUser {
			done <- fmt.Errorf("expected AUTH_LIST_USER code %d, got %d", requestCodeAuthListUser, request.Code)
			return
		}
		if _, exists := request.ExtFields["filter"]; exists || len(request.Body) != 0 {
			done <- fmt.Errorf("unexpected blank-filter request fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	rows, err := NewClient(time.Second).ListUser(context.Background(), "", brokerListener.Addr().String(), "", " ")
	if err != nil {
		t.Fatalf("list user empty body: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected empty listUser rows, got %#v", rows)
	}
	if output := formatListUser(rows, false); output != "" {
		t.Fatalf("expected empty listUser output, got %q", output)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientGetUserUsesOfficialRequestCodeAndUsername(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeAuthGetUser {
			done <- fmt.Errorf("expected AUTH_GET_USER code %d, got %d", requestCodeAuthGetUser, request.Code)
			return
		}
		if request.ExtFields["username"] != "admin" || len(request.Body) != 0 {
			done <- fmt.Errorf("unexpected getUser request fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"username":"admin","password":"******","userType":"Super","userStatus":"enable"}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		done <- err
	}()

	row, err := NewClient(time.Second).GetUser(context.Background(), "", brokerListener.Addr().String(), "", "admin")
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if row == nil || row.Username != "admin" || row.Password != "******" || row.UserType != "Super" || row.UserStatus != "enable" {
		t.Fatalf("unexpected getUser row %#v", row)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientAuthUserWritesUseOfficialRequestCodesAndBody(t *testing.T) {
	cases := []struct {
		name         string
		code         int
		call         func(context.Context, *Client, string, authUserOptions) ([]string, error)
		expectedBody map[string]string
	}{
		{
			name: "create",
			code: requestCodeAuthCreateUser,
			call: func(ctx context.Context, client *Client, brokerAddr string, options authUserOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				return client.CreateUser(ctx, "", options)
			},
			expectedBody: map[string]string{
				"username": "goadmin-created",
				"password": "seed-pass",
				"userType": "Super",
			},
		},
		{
			name: "update",
			code: requestCodeAuthUpdateUser,
			call: func(ctx context.Context, client *Client, brokerAddr string, options authUserOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				options.UserStatusSet = true
				return client.UpdateUser(ctx, "", options)
			},
			expectedBody: map[string]string{
				"username":   "goadmin-created",
				"userStatus": "disable",
			},
		},
		{
			name: "update_password",
			code: requestCodeAuthUpdateUser,
			call: func(ctx context.Context, client *Client, brokerAddr string, options authUserOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				options.PasswordSet = true
				options.UserStatus = ""
				return client.UpdateUser(ctx, "", options)
			},
			expectedBody: map[string]string{
				"username": "goadmin-created",
				"password": "seed-pass",
			},
		},
		{
			name: "update_user_type",
			code: requestCodeAuthUpdateUser,
			call: func(ctx context.Context, client *Client, brokerAddr string, options authUserOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				options.UserTypeSet = true
				options.UserStatus = ""
				return client.UpdateUser(ctx, "", options)
			},
			expectedBody: map[string]string{
				"username": "goadmin-created",
				"userType": "Super",
			},
		},
		{
			name: "update_empty_password",
			code: requestCodeAuthUpdateUser,
			call: func(ctx context.Context, client *Client, brokerAddr string, options authUserOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				options.Password = ""
				options.PasswordSet = true
				options.UserStatus = ""
				return client.UpdateUser(ctx, "", options)
			},
			expectedBody: map[string]string{
				"username": "goadmin-created",
				"password": "",
			},
		},
		{
			name: "delete",
			code: requestCodeAuthDeleteUser,
			call: func(ctx context.Context, client *Client, brokerAddr string, options authUserOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				return client.DeleteUser(ctx, "", options)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen broker: %v", err)
			}
			defer brokerListener.Close()

			done := make(chan error, 1)
			go func() {
				conn, err := brokerListener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				request, err := decodeCommand(conn)
				if err != nil {
					done <- err
					return
				}
				if request.Code != tc.code {
					done <- fmt.Errorf("expected auth request code %d, got %d", tc.code, request.Code)
					return
				}
				if request.ExtFields["username"] != "goadmin-created" {
					done <- fmt.Errorf("expected username header, got fields=%#v", request.ExtFields)
					return
				}
				if len(tc.expectedBody) == 0 {
					if len(request.Body) != 0 {
						done <- fmt.Errorf("expected empty body, got %s", string(request.Body))
						return
					}
				} else {
					var body map[string]string
					if err := json.Unmarshal(request.Body, &body); err != nil {
						done <- fmt.Errorf("decode body %q: %w", string(request.Body), err)
						return
					}
					if !reflect.DeepEqual(body, tc.expectedBody) {
						done <- fmt.Errorf("auth body mismatch expected=%#v actual=%#v", tc.expectedBody, body)
						return
					}
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				_, err = conn.Write(remotingFrameForTest(t, response, nil))
				done <- err
			}()

			options := authUserOptions{
				Username:   "goadmin-created",
				Password:   "seed-pass",
				UserType:   "Super",
				UserStatus: "disable",
			}
			targets, err := tc.call(context.Background(), NewClient(time.Second), brokerListener.Addr().String(), options)
			if err != nil {
				t.Fatalf("%s user: %v", tc.name, err)
			}
			if !reflect.DeepEqual(targets, []string{brokerListener.Addr().String()}) {
				t.Fatalf("unexpected targets %#v", targets)
			}
			if err := <-done; err != nil {
				t.Fatalf("broker side: %v", err)
			}
		})
	}
}

func TestClientAclWritesUseOfficialRequestCodesAndBody(t *testing.T) {
	cases := []struct {
		name         string
		code         int
		call         func(context.Context, *Client, string, aclOptions) ([]string, error)
		expectedBody string
		expectedExt  map[string]string
	}{
		{
			name: "create",
			code: requestCodeAuthCreateAcl,
			call: func(ctx context.Context, client *Client, brokerAddr string, options aclOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				return client.CreateAcl(ctx, "", options)
			},
			expectedExt:  map[string]string{"subject": "User:goadmin-acl"},
			expectedBody: `{"subject":"User:goadmin-acl","policies":[{"entries":[{"resource":"Topic:first","actions":["Pub","Sub"],"sourceIps":["10.0.0.1","10.0.0.2"],"decision":"Allow"},{"resource":"Group:second","actions":["Pub","Sub"],"sourceIps":["10.0.0.1","10.0.0.2"],"decision":"Allow"}]}]}`,
		},
		{
			name: "update_without_source_ips",
			code: requestCodeAuthUpdateAcl,
			call: func(ctx context.Context, client *Client, brokerAddr string, options aclOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				options.SourceIps = nil
				return client.UpdateAcl(ctx, "", options)
			},
			expectedExt:  map[string]string{"subject": "User:goadmin-acl"},
			expectedBody: `{"subject":"User:goadmin-acl","policies":[{"entries":[{"resource":"Topic:first","actions":["Pub","Sub"],"decision":"Allow"},{"resource":"Group:second","actions":["Pub","Sub"],"decision":"Allow"}]}]}`,
		},
		{
			name: "delete_with_resource",
			code: requestCodeAuthDeleteAcl,
			call: func(ctx context.Context, client *Client, brokerAddr string, options aclOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				return client.DeleteAcl(ctx, "", options)
			},
			expectedExt: map[string]string{"subject": "User:goadmin-acl", "resource": "Topic:first, Group:second"},
		},
		{
			name: "delete_without_resource",
			code: requestCodeAuthDeleteAcl,
			call: func(ctx context.Context, client *Client, brokerAddr string, options aclOptions) ([]string, error) {
				options.BrokerAddr = brokerAddr
				options.Resource = ""
				return client.DeleteAcl(ctx, "", options)
			},
			expectedExt: map[string]string{"subject": "User:goadmin-acl"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen broker: %v", err)
			}
			defer brokerListener.Close()

			done := make(chan error, 1)
			go func() {
				conn, err := brokerListener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				request, err := decodeCommand(conn)
				if err != nil {
					done <- err
					return
				}
				if request.Code != tc.code {
					done <- fmt.Errorf("expected ACL request code %d, got %d", tc.code, request.Code)
					return
				}
				if !reflect.DeepEqual(request.ExtFields, tc.expectedExt) {
					done <- fmt.Errorf("ACL ext fields mismatch expected=%#v actual=%#v", tc.expectedExt, request.ExtFields)
					return
				}
				if tc.expectedBody == "" {
					if len(request.Body) != 0 {
						done <- fmt.Errorf("expected empty ACL body, got %s", string(request.Body))
						return
					}
				} else if string(request.Body) != tc.expectedBody {
					done <- fmt.Errorf("ACL body mismatch expected=%s actual=%s", tc.expectedBody, string(request.Body))
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				_, err = conn.Write(remotingFrameForTest(t, response, nil))
				done <- err
			}()

			options := aclOptions{
				Subject:   "User:goadmin-acl",
				Resources: []string{"Topic:first", "Group:second"},
				Actions:   []string{"Pub", "Sub"},
				SourceIps: []string{"10.0.0.1", "10.0.0.2"},
				Decision:  "Allow",
				Resource:  "Topic:first, Group:second",
			}
			targets, err := tc.call(context.Background(), NewClient(time.Second), brokerListener.Addr().String(), options)
			if err != nil {
				t.Fatalf("%s acl: %v", tc.name, err)
			}
			if !reflect.DeepEqual(targets, []string{brokerListener.Addr().String()}) {
				t.Fatalf("unexpected ACL targets %#v", targets)
			}
			if err := <-done; err != nil {
				t.Fatalf("broker side: %v", err)
			}
		})
	}
}

func TestClientAclClusterWritesAllMasterAndSlaveTargets(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerListeners := make([]net.Listener, 0, 3)
	for i := 0; i < 3; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen broker %d: %v", i, err)
		}
		defer listener.Close()
		brokerListeners = append(brokerListeners, listener)
	}

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- fmt.Errorf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		body := []byte(fmt.Sprintf(
			`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q,1:%q},"brokerName":"broker-a","cluster":"DefaultCluster"},"broker-b":{"brokerAddrs":{0:%q},"brokerName":"broker-b","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-b","broker-a"]}}`,
			brokerListeners[0].Addr().String(),
			brokerListeners[1].Addr().String(),
			brokerListeners[2].Addr().String(),
		))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	brokerDone := make(chan error, len(brokerListeners))
	for _, listener := range brokerListeners {
		go func(listener net.Listener) {
			conn, err := listener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			defer conn.Close()
			request, err := decodeCommand(conn)
			if err != nil {
				brokerDone <- err
				return
			}
			if request.Code != requestCodeAuthDeleteAcl {
				brokerDone <- fmt.Errorf("expected AUTH_DELETE_ACL code %d, got %d", requestCodeAuthDeleteAcl, request.Code)
				return
			}
			if !reflect.DeepEqual(request.ExtFields, map[string]string{"subject": "User:goadmin-acl"}) || len(request.Body) != 0 {
				brokerDone <- fmt.Errorf("unexpected deleteAcl request fields=%#v body=%s", request.ExtFields, string(request.Body))
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			brokerDone <- err
		}(listener)
	}

	targets, err := NewClient(time.Second).DeleteAcl(context.Background(), nameServerListener.Addr().String(), aclOptions{
		ClusterName: "DefaultCluster",
		Subject:     "User:goadmin-acl",
	})
	if err != nil {
		t.Fatalf("delete acl by cluster: %v", err)
	}
	expectedTargets := []string{
		brokerListeners[0].Addr().String(),
		brokerListeners[1].Addr().String(),
		brokerListeners[2].Addr().String(),
	}
	if !reflect.DeepEqual(targets, expectedTargets) {
		t.Fatalf("cluster ACL targets mismatch\nexpected=%#v\nactual=%#v", expectedTargets, targets)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	for range brokerListeners {
		if err := <-brokerDone; err != nil {
			t.Fatalf("broker side: %v", err)
		}
	}
}

func TestClientAclConfigUsesOfficialRequestCodesAndHeaders(t *testing.T) {
	cases := []struct {
		name        string
		code        int
		call        func(context.Context, *Client, string) ([]string, error)
		expectedExt map[string]string
	}{
		{
			name: "update_acl_config",
			code: requestCodeUpdateAndCreateAclConfig,
			call: func(ctx context.Context, client *Client, brokerAddr string) ([]string, error) {
				return client.UpdateAclConfig(ctx, "", aclConfigOptions{
					BrokerAddr:         brokerAddr,
					AccessKey:          "GoadminLegacyAccess",
					SecretKey:          "legacy-secret",
					WhiteRemoteAddress: "10.70.*",
					DefaultTopicPerm:   "PUB|SUB",
					DefaultGroupPerm:   "SUB",
					TopicPerms:         []string{"TopicA=PUB", "TopicB=SUB"},
					TopicPermsSet:      true,
					GroupPerms:         []string{"GroupA=SUB"},
					GroupPermsSet:      true,
					Admin:              true,
					AdminSet:           true,
				})
			},
			expectedExt: map[string]string{
				"accessKey":          "GoadminLegacyAccess",
				"secretKey":          "legacy-secret",
				"admin":              "true",
				"defaultGroupPerm":   "SUB",
				"defaultTopicPerm":   "PUB|SUB",
				"whiteRemoteAddress": "10.70.*",
				"topicPerms":         "TopicA=PUB,TopicB=SUB",
				"groupPerms":         "GroupA=SUB",
			},
		},
		{
			name: "delete_acl_config",
			code: requestCodeDeleteAclConfig,
			call: func(ctx context.Context, client *Client, brokerAddr string) ([]string, error) {
				return client.DeleteAclConfig(ctx, "", aclConfigOptions{
					BrokerAddr: brokerAddr,
					AccessKey:  "GoadminLegacyAccess",
				})
			},
			expectedExt: map[string]string{"accessKey": "GoadminLegacyAccess"},
		},
		{
			name: "update_global_white_addr",
			code: requestCodeUpdateGlobalWhiteAddrsConfig,
			call: func(ctx context.Context, client *Client, brokerAddr string) ([]string, error) {
				return client.UpdateGlobalWhiteAddr(ctx, "", globalWhiteAddrOptions{
					BrokerAddr:                 brokerAddr,
					GlobalWhiteRemoteAddresses: "10.70.*,192.168.1.*",
					AclFileFullPath:            "/tmp/plain_acl.yml",
				})
			},
			expectedExt: map[string]string{
				"globalWhiteAddrs": "10.70.*,192.168.1.*",
				"aclFileFullPath":  "/tmp/plain_acl.yml",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen broker: %v", err)
			}
			defer brokerListener.Close()

			done := make(chan error, 1)
			go func() {
				conn, err := brokerListener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				request, err := decodeCommand(conn)
				if err != nil {
					done <- err
					return
				}
				if request.Code != tc.code {
					done <- fmt.Errorf("expected request code %d, got %d", tc.code, request.Code)
					return
				}
				if !reflect.DeepEqual(request.ExtFields, tc.expectedExt) {
					done <- fmt.Errorf("ext fields mismatch expected=%#v actual=%#v", tc.expectedExt, request.ExtFields)
					return
				}
				if len(request.Body) != 0 {
					done <- fmt.Errorf("expected empty body, got %s", string(request.Body))
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				_, err = conn.Write(remotingFrameForTest(t, response, nil))
				done <- err
			}()

			targets, err := tc.call(context.Background(), NewClient(time.Second), brokerListener.Addr().String())
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if !reflect.DeepEqual(targets, []string{brokerListener.Addr().String()}) {
				t.Fatalf("unexpected targets %#v", targets)
			}
			if err := <-done; err != nil {
				t.Fatalf("broker side: %v", err)
			}
		})
	}
}

func TestClientListAclUsesOfficialRequestCodeAndFilters(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer listener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		expectedFields := map[string]string{"subjectFilter": "User:alice", "resourceFilter": "Topic:test"}
		if request.Code != requestCodeAuthListAcl || !reflect.DeepEqual(request.ExtFields, expectedFields) || len(request.Body) != 0 {
			brokerDone <- fmt.Errorf("unexpected listAcl request code=%d fields=%#v body=%s", request.Code, request.ExtFields, string(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, []byte(`[{"subject":"User:alice","policies":[{"policyType":"CUSTOM","entries":[{"resource":"Topic:test","actions":["Pub","Sub"],"sourceIps":["10.0.0.1"],"decision":"Allow"}]}]}]`)))
		brokerDone <- err
	}()

	rows, err := NewClient(time.Second).ListAcl(context.Background(), "", listener.Addr().String(), "", "User:alice", "Topic:test")
	if err != nil {
		t.Fatalf("list acl: %v", err)
	}
	if len(rows) != 1 || rows[0].Subject != "User:alice" || len(rows[0].Policies) != 1 {
		t.Fatalf("unexpected listAcl rows %#v", rows)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientGetAclUsesOfficialRequestCodeAndSubject(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer listener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeAuthGetAcl || !reflect.DeepEqual(request.ExtFields, map[string]string{"subject": "User:alice"}) || len(request.Body) != 0 {
			brokerDone <- fmt.Errorf("unexpected getAcl request code=%d fields=%#v body=%s", request.Code, request.ExtFields, string(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, []byte(`{"subject":"User:alice","policies":[{"policyType":"CUSTOM","entries":[{"resource":"Topic:test","actions":["Pub"],"sourceIps":["10.0.0.1"],"decision":"Allow"}]}]}`)))
		brokerDone <- err
	}()

	rows, err := NewClient(time.Second).GetAcl(context.Background(), "", listener.Addr().String(), "", "User:alice")
	if err != nil {
		t.Fatalf("get acl: %v", err)
	}
	if len(rows) != 1 || rows[0].Subject != "User:alice" || len(rows[0].Policies) != 1 {
		t.Fatalf("unexpected getAcl rows %#v", rows)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientCopyAclUsesOfficialReadThenCreateAndUpdateFlow(t *testing.T) {
	sourceListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen source broker: %v", err)
	}
	defer sourceListener.Close()
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target broker: %v", err)
	}
	defer targetListener.Close()

	sourceDone := make(chan error, 1)
	go func() {
		for _, subject := range []string{"User:alice", "User:bob"} {
			conn, err := sourceListener.Accept()
			if err != nil {
				sourceDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				sourceDone <- err
				return
			}
			if request.Code != requestCodeAuthGetAcl || request.ExtFields["subject"] != subject || len(request.Body) != 0 {
				conn.Close()
				sourceDone <- fmt.Errorf("unexpected source getAcl request subject=%s code=%d fields=%#v body=%s", subject, request.Code, request.ExtFields, string(request.Body))
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			body := []byte(fmt.Sprintf(`{"subject":%q,"policies":[{"policyType":"CUSTOM","entries":[{"resource":"Topic:%s","actions":["Pub"],"sourceIps":["10.0.0.1"],"decision":"Allow"}]}]}`, subject, strings.TrimPrefix(subject, "User:")))
			if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
				conn.Close()
				sourceDone <- err
				return
			}
			conn.Close()
		}
		sourceDone <- nil
	}()

	targetDone := make(chan error, 1)
	go func() {
		steps := []struct {
			code         int
			subject      string
			responseBody []byte
		}{
			{code: requestCodeAuthGetAcl, subject: "User:alice"},
			{code: requestCodeAuthCreateAcl, subject: "User:alice"},
			{code: requestCodeAuthGetAcl, subject: "User:bob", responseBody: []byte(`{"subject":"User:bob","policies":[]}`)},
			{code: requestCodeAuthUpdateAcl, subject: "User:bob"},
		}
		for _, step := range steps {
			conn, err := targetListener.Accept()
			if err != nil {
				targetDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			if request.Code != step.code || request.ExtFields["subject"] != step.subject {
				conn.Close()
				targetDone <- fmt.Errorf("unexpected target request subject=%s code=%d fields=%#v", step.subject, request.Code, request.ExtFields)
				return
			}
			if step.code == requestCodeAuthCreateAcl || step.code == requestCodeAuthUpdateAcl {
				var body aclInfo
				if err := json.Unmarshal(request.Body, &body); err != nil {
					conn.Close()
					targetDone <- fmt.Errorf("decode target ACL body %q: %w", string(request.Body), err)
					return
				}
				if body.Subject != step.subject || len(body.Policies) != 1 {
					conn.Close()
					targetDone <- fmt.Errorf("target ACL body mismatch %#v", body)
					return
				}
			} else if len(request.Body) != 0 {
				conn.Close()
				targetDone <- fmt.Errorf("expected empty target getAcl body, got %s", string(request.Body))
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			if _, err := conn.Write(remotingFrameForTest(t, response, step.responseBody)); err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			conn.Close()
		}
		targetDone <- nil
	}()

	results, err := NewClient(time.Second).CopyAcl(context.Background(), sourceListener.Addr().String(), targetListener.Addr().String(), "User:alice,User:bob")
	if err != nil {
		t.Fatalf("copy acl: %v", err)
	}
	if len(results) != 2 || results[0].Subject != "User:alice" || results[1].Subject != "User:bob" {
		t.Fatalf("unexpected copyAcl results %#v", results)
	}
	if err := <-sourceDone; err != nil {
		t.Fatalf("source broker side: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target broker side: %v", err)
	}
}

func TestClientCopyAclListsSourceWhenSubjectsBlank(t *testing.T) {
	sourceListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen source broker: %v", err)
	}
	defer sourceListener.Close()
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target broker: %v", err)
	}
	defer targetListener.Close()

	sourceDone := make(chan error, 1)
	go func() {
		conn, err := sourceListener.Accept()
		if err != nil {
			sourceDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			sourceDone <- err
			return
		}
		if request.Code != requestCodeAuthListAcl || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			sourceDone <- fmt.Errorf("unexpected source listAcl request code=%d fields=%#v body=%s", request.Code, request.ExtFields, string(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, []byte(`[{"subject":"User:carol","policies":[{"policyType":"CUSTOM","entries":[{"resource":"Topic:carol","actions":["Pub"],"sourceIps":["10.0.0.1"],"decision":"Allow"}]}]}]`)))
		sourceDone <- err
	}()

	targetDone := make(chan error, 1)
	go func() {
		for _, code := range []int{requestCodeAuthGetAcl, requestCodeAuthCreateAcl} {
			conn, err := targetListener.Accept()
			if err != nil {
				targetDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			if request.Code != code || request.ExtFields["subject"] != "User:carol" {
				conn.Close()
				targetDone <- fmt.Errorf("unexpected target request code=%d fields=%#v", request.Code, request.ExtFields)
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			conn.Close()
		}
		targetDone <- nil
	}()

	results, err := NewClient(time.Second).CopyAcl(context.Background(), sourceListener.Addr().String(), targetListener.Addr().String(), "")
	if err != nil {
		t.Fatalf("copy acl by list: %v", err)
	}
	if len(results) != 1 || results[0].Subject != "User:carol" {
		t.Fatalf("unexpected listed copyAcl results %#v", results)
	}
	if err := <-sourceDone; err != nil {
		t.Fatalf("source broker side: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target broker side: %v", err)
	}
}

func TestClientCopyUserUsesOfficialReadThenCreateAndUpdateFlow(t *testing.T) {
	sourceListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen source broker: %v", err)
	}
	defer sourceListener.Close()
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target broker: %v", err)
	}
	defer targetListener.Close()

	sourceDone := make(chan error, 1)
	go func() {
		for _, user := range []string{"alice", "bob"} {
			conn, err := sourceListener.Accept()
			if err != nil {
				sourceDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				sourceDone <- err
				return
			}
			if request.Code != requestCodeAuthGetUser || request.ExtFields["username"] != user || len(request.Body) != 0 {
				conn.Close()
				sourceDone <- fmt.Errorf("unexpected source getUser request user=%s code=%d fields=%#v body=%d", user, request.Code, request.ExtFields, len(request.Body))
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			body := []byte(fmt.Sprintf(`{"username":%q,"password":%q,"userType":"Super","userStatus":"enable"}`, user, user+"-pass"))
			if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
				conn.Close()
				sourceDone <- err
				return
			}
			conn.Close()
		}
		sourceDone <- nil
	}()

	targetDone := make(chan error, 1)
	go func() {
		steps := []struct {
			code         int
			username     string
			responseBody []byte
			expectedBody map[string]string
		}{
			{code: requestCodeAuthGetUser, username: "alice"},
			{code: requestCodeAuthCreateUser, username: "alice", expectedBody: map[string]string{"username": "alice", "password": "alice-pass", "userType": "Super", "userStatus": "enable"}},
			{code: requestCodeAuthGetUser, username: "bob", responseBody: []byte(`{"username":"bob","password":"old-pass","userType":"Super","userStatus":"enable"}`)},
			{code: requestCodeAuthUpdateUser, username: "bob", expectedBody: map[string]string{"username": "bob", "password": "bob-pass", "userType": "Super", "userStatus": "enable"}},
		}
		for _, step := range steps {
			conn, err := targetListener.Accept()
			if err != nil {
				targetDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			if request.Code != step.code || request.ExtFields["username"] != step.username {
				conn.Close()
				targetDone <- fmt.Errorf("unexpected target request username=%s code=%d fields=%#v", step.username, request.Code, request.ExtFields)
				return
			}
			if step.expectedBody == nil {
				if len(request.Body) != 0 {
					conn.Close()
					targetDone <- fmt.Errorf("expected empty target body for %s, got %s", step.username, string(request.Body))
					return
				}
			} else {
				var body map[string]string
				if err := json.Unmarshal(request.Body, &body); err != nil {
					conn.Close()
					targetDone <- fmt.Errorf("decode target body %q: %w", string(request.Body), err)
					return
				}
				if !reflect.DeepEqual(body, step.expectedBody) {
					conn.Close()
					targetDone <- fmt.Errorf("target body mismatch expected=%#v actual=%#v", step.expectedBody, body)
					return
				}
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			if _, err := conn.Write(remotingFrameForTest(t, response, step.responseBody)); err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			conn.Close()
		}
		targetDone <- nil
	}()

	results, err := NewClient(time.Second).CopyUser(context.Background(), sourceListener.Addr().String(), targetListener.Addr().String(), "alice,bob")
	if err != nil {
		t.Fatalf("copy user: %v", err)
	}
	if len(results) != 2 || results[0].Username != "alice" || results[1].Username != "bob" {
		t.Fatalf("unexpected copyUser results %#v", results)
	}
	if err := <-sourceDone; err != nil {
		t.Fatalf("source broker side: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target broker side: %v", err)
	}
}

func TestClientCopyUserListsSourceWhenUsernamesBlank(t *testing.T) {
	sourceListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen source broker: %v", err)
	}
	defer sourceListener.Close()
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target broker: %v", err)
	}
	defer targetListener.Close()

	sourceDone := make(chan error, 1)
	go func() {
		conn, err := sourceListener.Accept()
		if err != nil {
			sourceDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			sourceDone <- err
			return
		}
		if request.Code != requestCodeAuthListUser || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			sourceDone <- fmt.Errorf("unexpected source listUser request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`[{"username":"carol","password":"carol-pass","userType":"Normal","userStatus":"enable"}]`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		sourceDone <- err
	}()

	targetDone := make(chan error, 1)
	go func() {
		for _, step := range []struct {
			code         int
			expectedBody map[string]string
		}{
			{code: requestCodeAuthGetUser},
			{code: requestCodeAuthCreateUser, expectedBody: map[string]string{"username": "carol", "password": "carol-pass", "userType": "Normal", "userStatus": "enable"}},
		} {
			conn, err := targetListener.Accept()
			if err != nil {
				targetDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			if request.Code != step.code || request.ExtFields["username"] != "carol" {
				conn.Close()
				targetDone <- fmt.Errorf("unexpected target list-copy request code=%d fields=%#v", request.Code, request.ExtFields)
				return
			}
			if step.expectedBody != nil {
				var body map[string]string
				if err := json.Unmarshal(request.Body, &body); err != nil {
					conn.Close()
					targetDone <- fmt.Errorf("decode target body %q: %w", string(request.Body), err)
					return
				}
				if !reflect.DeepEqual(body, step.expectedBody) {
					conn.Close()
					targetDone <- fmt.Errorf("target body mismatch expected=%#v actual=%#v", step.expectedBody, body)
					return
				}
			} else if len(request.Body) != 0 {
				conn.Close()
				targetDone <- fmt.Errorf("expected empty target body, got %s", string(request.Body))
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
				conn.Close()
				targetDone <- err
				return
			}
			conn.Close()
		}
		targetDone <- nil
	}()

	results, err := NewClient(time.Second).CopyUser(context.Background(), sourceListener.Addr().String(), targetListener.Addr().String(), "")
	if err != nil {
		t.Fatalf("copy user by list: %v", err)
	}
	if len(results) != 1 || results[0].Username != "carol" {
		t.Fatalf("unexpected listed copyUser results %#v", results)
	}
	if err := <-sourceDone; err != nil {
		t.Fatalf("source broker side: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target broker side: %v", err)
	}
}

func TestClientAuthUserClusterWritesAllMasterAndSlaveTargets(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerListeners := make([]net.Listener, 0, 3)
	for i := 0; i < 3; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen broker %d: %v", i, err)
		}
		defer listener.Close()
		brokerListeners = append(brokerListeners, listener)
	}

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- fmt.Errorf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		body := []byte(fmt.Sprintf(
			`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q,1:%q},"brokerName":"broker-a","cluster":"DefaultCluster"},"broker-b":{"brokerAddrs":{0:%q},"brokerName":"broker-b","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-b","broker-a"]}}`,
			brokerListeners[0].Addr().String(),
			brokerListeners[1].Addr().String(),
			brokerListeners[2].Addr().String(),
		))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	brokerDone := make(chan error, len(brokerListeners))
	for _, listener := range brokerListeners {
		go func(listener net.Listener) {
			conn, err := listener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			defer conn.Close()
			request, err := decodeCommand(conn)
			if err != nil {
				brokerDone <- err
				return
			}
			if request.Code != requestCodeAuthDeleteUser {
				brokerDone <- fmt.Errorf("expected AUTH_DELETE_USER code %d, got %d", requestCodeAuthDeleteUser, request.Code)
				return
			}
			if request.ExtFields["username"] != "goadmin-created" || len(request.Body) != 0 {
				brokerDone <- fmt.Errorf("unexpected deleteUser request fields=%#v body=%s", request.ExtFields, string(request.Body))
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			brokerDone <- err
		}(listener)
	}

	targets, err := NewClient(time.Second).DeleteUser(context.Background(), nameServerListener.Addr().String(), authUserOptions{
		ClusterName: "DefaultCluster",
		Username:    "goadmin-created",
	})
	if err != nil {
		t.Fatalf("delete user by cluster: %v", err)
	}
	expectedTargets := []string{
		brokerListeners[0].Addr().String(),
		brokerListeners[1].Addr().String(),
		brokerListeners[2].Addr().String(),
	}
	if !reflect.DeepEqual(targets, expectedTargets) {
		t.Fatalf("cluster auth targets mismatch\nexpected=%#v\nactual=%#v", expectedTargets, targets)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	for range brokerListeners {
		if err := <-brokerDone; err != nil {
			t.Fatalf("broker side: %v", err)
		}
	}
}

func TestClientSetCommitLogReadAheadModeUsesOfficialRequestCodeAndMode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeSetCommitLogReadMode {
			done <- fmt.Errorf("expected SET_COMMITLOG_READ_MODE code %d, got %d", requestCodeSetCommitLogReadMode, request.Code)
			return
		}
		if request.ExtFields["READ_AHEAD_MODE"] != "1" || len(request.Body) != 0 {
			done <- fmt.Errorf("unexpected readAhead request fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
			Remark:   "OK",
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	sections, err := NewClient(time.Second).SetCommitLogReadAheadMode(context.Background(), "", brokerListener.Addr().String(), "", "1")
	if err != nil {
		t.Fatalf("set commitLog readAhead mode: %v", err)
	}
	expected := []commitLogReadAheadModeSection{{
		Header: "============" + brokerListener.Addr().String() + "============",
		Result: "OK",
	}}
	if !reflect.DeepEqual(sections, expected) {
		t.Fatalf("sections mismatch\nexpected=%#v\nactual=%#v", expected, sections)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientCleanBrokerMetadataUsesOfficialRequestCode(t *testing.T) {
	controllerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	defer controllerListener.Close()

	done := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := controllerListener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				done <- err
				return
			}
			switch i {
			case 0:
				if request.Code != requestCodeControllerGetMetadataInfo {
					conn.Close()
					done <- fmt.Errorf("expected CONTROLLER_GET_METADATA_INFO code %d, got %d", requestCodeControllerGetMetadataInfo, request.Code)
					return
				}
				if len(request.ExtFields) != 0 || len(request.Body) != 0 {
					conn.Close()
					done <- fmt.Errorf("expected empty controller metadata request fields/body, got fields=%#v body=%d", request.ExtFields, len(request.Body))
					return
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"controllerLeaderAddress": controllerListener.Addr().String(),
						"controllerLeaderId":      "n0",
						"group":                   "group1",
						"isLeader":                "true",
						"peers":                   "n0:" + controllerListener.Addr().String(),
					},
				}
				if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
					conn.Close()
					done <- err
					return
				}
			case 1:
				if request.Code != requestCodeCleanBrokerData {
					conn.Close()
					done <- fmt.Errorf("expected CLEAN_BROKER_DATA code %d, got %d", requestCodeCleanBrokerData, request.Code)
					return
				}
				if len(request.Body) != 0 {
					conn.Close()
					done <- fmt.Errorf("expected empty cleanBrokerMetadata body, got %d bytes", len(request.Body))
					return
				}
				expectedFields := map[string]string{
					"brokerControllerIdsToClean": "1;2",
					"brokerName":                 "goadmin-controller-broker",
					"clusterName":                "GoadminControllerCluster",
					"isCleanLivingBroker":        "true",
				}
				for key, value := range expectedFields {
					if request.ExtFields[key] != value {
						conn.Close()
						done <- fmt.Errorf("expected cleanBrokerMetadata field %s=%q, got %#v", key, value, request.ExtFields)
						return
					}
				}
				if _, ok := request.ExtFields["invokeTime"]; !ok {
					conn.Close()
					done <- fmt.Errorf("expected dynamic invokeTime field, got %#v", request.ExtFields)
					return
				}
				if len(request.ExtFields) != len(expectedFields)+1 {
					conn.Close()
					done <- fmt.Errorf("unexpected cleanBrokerMetadata fields %#v", request.ExtFields)
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
					conn.Close()
					done <- err
					return
				}
			}
			conn.Close()
		}
		done <- nil
	}()

	err = NewClient(time.Second).CleanBrokerMetadata(context.Background(), controllerListener.Addr().String(), cleanBrokerMetadataOptions{
		ClusterName:                "GoadminControllerCluster",
		BrokerName:                 "goadmin-controller-broker",
		BrokerControllerIDsToClean: "1;2",
		CleanLivingBroker:          true,
	})
	if err != nil {
		t.Fatalf("cleanBrokerMetadata: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("controller side: %v", err)
	}
}

func TestClientElectMasterUsesOfficialRequestCode(t *testing.T) {
	controllerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	defer controllerListener.Close()

	done := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := controllerListener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				done <- err
				return
			}
			switch i {
			case 0:
				if request.Code != requestCodeControllerGetMetadataInfo {
					conn.Close()
					done <- fmt.Errorf("expected CONTROLLER_GET_METADATA_INFO code %d, got %d", requestCodeControllerGetMetadataInfo, request.Code)
					return
				}
				if len(request.ExtFields) != 0 || len(request.Body) != 0 {
					conn.Close()
					done <- fmt.Errorf("expected empty controller metadata request fields/body, got fields=%#v body=%d", request.ExtFields, len(request.Body))
					return
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"controllerLeaderAddress": controllerListener.Addr().String(),
						"controllerLeaderId":      "n0",
						"group":                   "group1",
						"isLeader":                "true",
						"peers":                   "n0:" + controllerListener.Addr().String(),
					},
				}
				if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
					conn.Close()
					done <- err
					return
				}
			case 1:
				if request.Code != requestCodeControllerElectMaster {
					conn.Close()
					done <- fmt.Errorf("expected CONTROLLER_ELECT_MASTER code %d, got %d", requestCodeControllerElectMaster, request.Code)
					return
				}
				if len(request.Body) != 0 {
					conn.Close()
					done <- fmt.Errorf("expected empty electMaster body, got %d bytes", len(request.Body))
					return
				}
				expectedFields := map[string]string{
					"brokerId":       "4",
					"brokerName":     "goadmin-elect-pair-broker",
					"clusterName":    "GoadminElectPairCluster",
					"designateElect": "true",
				}
				for key, value := range expectedFields {
					if request.ExtFields[key] != value {
						conn.Close()
						done <- fmt.Errorf("expected electMaster field %s=%q, got %#v", key, value, request.ExtFields)
						return
					}
				}
				if _, ok := request.ExtFields["invokeTime"]; !ok {
					conn.Close()
					done <- fmt.Errorf("expected dynamic invokeTime field, got %#v", request.ExtFields)
					return
				}
				if len(request.ExtFields) != len(expectedFields)+1 {
					conn.Close()
					done <- fmt.Errorf("unexpected electMaster fields %#v", request.ExtFields)
					return
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"masterAddress":     "172.24.0.4:30992",
						"masterBrokerId":    "4",
						"masterEpoch":       "6",
						"syncStateSetEpoch": "4",
					},
				}
				if _, err := conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
					conn.Close()
					done <- err
					return
				}
			}
			conn.Close()
		}
		done <- nil
	}()

	result, err := NewClient(time.Second).ElectMaster(context.Background(), controllerListener.Addr().String(), electMasterOptions{
		ClusterName: "GoadminElectPairCluster",
		BrokerName:  "goadmin-elect-pair-broker",
		BrokerID:    4,
	})
	if err != nil {
		t.Fatalf("electMaster: %v", err)
	}
	if result.BrokerMasterAddr != "172.24.0.4:30992" || result.MasterEpoch != 6 || result.SyncStateSetEpoch != 4 {
		t.Fatalf("unexpected electMaster result %#v", result)
	}
	if err := <-done; err != nil {
		t.Fatalf("controller side: %v", err)
	}
}

func TestDecodeElectMasterBrokerMembersAcceptsFastJSONNumericKeys(t *testing.T) {
	body := []byte(`{"cluster":"GoadminElectPairCluster","brokerName":"goadmin-elect-pair-broker","brokerAddrs":{4:"172.24.0.4:30992",3:"172.24.0.4:30982"}}`)

	members, ok, err := decodeElectMasterBrokerMembers(body)
	if err != nil {
		t.Fatalf("decode electMaster broker members: %v", err)
	}
	if !ok {
		t.Fatalf("expected brokerAddrs to be present")
	}
	expected := []electMasterBrokerMember{
		{BrokerID: 3, BrokerAddress: "172.24.0.4:30982"},
		{BrokerID: 4, BrokerAddress: "172.24.0.4:30992"},
	}
	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("members mismatch\nexpected:%#v\nactual:%#v", expected, members)
	}
}

func TestClientGetControllerConfigUsesOfficialRequestCode(t *testing.T) {
	controllerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	defer controllerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := controllerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeGetControllerConfig {
			done <- fmt.Errorf("expected GET_CONTROLLER_CONFIG code %d, got %d", requestCodeGetControllerConfig, request.Code)
			return
		}
		if len(request.ExtFields) != 0 || len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty controller config request fields/body, got fields=%#v body=%d", request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
		}
		_, err = conn.Write(remotingFrameForTest(t, response, []byte("listenPort=9878\ncontrollerDLegerGroup=group1\n")))
		done <- err
	}()

	sections, err := NewClient(time.Second).GetControllerConfig(context.Background(), controllerListener.Addr().String())
	if err != nil {
		t.Fatalf("get controller config: %v", err)
	}
	if len(sections) != 1 {
		t.Fatalf("expected one controller config section, got %#v", sections)
	}
	if sections[0].NameServer != controllerListener.Addr().String() {
		t.Fatalf("unexpected controller section address %s", sections[0].NameServer)
	}
	if len(sections[0].Entries) != 2 {
		t.Fatalf("unexpected entries %#v", sections[0].Entries)
	}
	if err := <-done; err != nil {
		t.Fatalf("controller side: %v", err)
	}
}

func TestClientResetOffsetByTimeSpecifiedQueueUsesUpdateConsumerOffset(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeUpdateConsumerOffset {
			done <- fmt.Errorf("expected UPDATE_CONSUMER_OFFSET code %d, got %d", requestCodeUpdateConsumerOffset, request.Code)
			return
		}
		expectedFields := map[string]string{
			"consumerGroup": "GoadminGroup",
			"topic":         "TopicTest",
			"queueId":       "0",
			"commitOffset":  "7",
			"brokerName":    brokerListener.Addr().String(),
		}
		if !reflect.DeepEqual(request.ExtFields, expectedFields) {
			done <- fmt.Errorf("unexpected resetOffsetByTime fields\nexpected:%#v\nactual:%#v", expectedFields, request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty resetOffsetByTime body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	options := resetOffsetByTimeOptions{
		Group:           "GoadminGroup",
		Topic:           "TopicTest",
		BrokerAddr:      brokerListener.Addr().String(),
		QueueID:         0,
		ExpectOffset:    7,
		HasQueueID:      true,
		HasExpectOffset: true,
		SpecifiedQueue:  true,
	}
	if _, err := NewClient(time.Second).ResetOffsetByTime(context.Background(), "127.0.0.1:9876", options); err != nil {
		t.Fatalf("reset offset by time: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientResetOffsetByTimeSpecifiedQueueSearchesThenUpdates(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				done <- err
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			switch i {
			case 0:
				if request.Code != requestCodeSearchOffsetByTimestamp {
					_ = conn.Close()
					done <- fmt.Errorf("expected SEARCH_OFFSET_BY_TIMESTAMP code %d, got %d", requestCodeSearchOffsetByTimestamp, request.Code)
					return
				}
				expectedFields := map[string]string{
					"topic":     "TopicTest",
					"queueId":   "0",
					"timestamp": "1781242105254",
				}
				if !reflect.DeepEqual(request.ExtFields, expectedFields) {
					_ = conn.Close()
					done <- fmt.Errorf("unexpected searchOffset fields\nexpected:%#v\nactual:%#v", expectedFields, request.ExtFields)
					return
				}
				if len(request.Body) != 0 {
					_ = conn.Close()
					done <- fmt.Errorf("expected empty searchOffset body, got %d bytes", len(request.Body))
					return
				}
				response.ExtFields = map[string]string{"offset": "23"}
				_, err = conn.Write(remotingFrameForTest(t, response, nil))
				_ = conn.Close()
				if err != nil {
					done <- err
					return
				}
			case 1:
				if request.Code != requestCodeUpdateConsumerOffset {
					_ = conn.Close()
					done <- fmt.Errorf("expected UPDATE_CONSUMER_OFFSET code %d, got %d", requestCodeUpdateConsumerOffset, request.Code)
					return
				}
				expectedFields := map[string]string{
					"consumerGroup": "GoadminGroup",
					"topic":         "TopicTest",
					"queueId":       "0",
					"commitOffset":  "23",
					"brokerName":    brokerListener.Addr().String(),
				}
				if !reflect.DeepEqual(request.ExtFields, expectedFields) {
					_ = conn.Close()
					done <- fmt.Errorf("unexpected updateConsumerOffset fields\nexpected:%#v\nactual:%#v", expectedFields, request.ExtFields)
					return
				}
				if len(request.Body) != 0 {
					_ = conn.Close()
					done <- fmt.Errorf("expected empty updateConsumerOffset body, got %d bytes", len(request.Body))
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, response, nil))
				_ = conn.Close()
				done <- err
			}
		}
	}()

	options := resetOffsetByTimeOptions{
		Group:           "GoadminGroup",
		Topic:           "TopicTest",
		TimestampMillis: 1781242105254,
		BrokerAddr:      brokerListener.Addr().String(),
		QueueID:         0,
		HasQueueID:      true,
		SpecifiedQueue:  true,
	}
	rows, err := NewClient(time.Second).ResetOffsetByTime(context.Background(), "127.0.0.1:9876", options)
	if err != nil {
		t.Fatalf("reset offset by time search branch: %v", err)
	}
	if len(rows) != 1 || rows[0].Offset != 23 {
		t.Fatalf("unexpected searched rows: %#v", rows)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientResetOffsetByTimeSpecifiedQueueSkipsUpdateWhenSearchReturnsZero(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeSearchOffsetByTimestamp {
			done <- fmt.Errorf("expected SEARCH_OFFSET_BY_TIMESTAMP code %d, got %d", requestCodeSearchOffsetByTimestamp, request.Code)
			return
		}
		response := remotingCommand{
			Code:      responseCodeSuccess,
			Language:  "JAVA",
			Version:   0,
			Opaque:    request.Opaque,
			Flag:      1,
			ExtFields: map[string]string{"offset": "0"},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	options := resetOffsetByTimeOptions{
		Group:           "GoadminGroup",
		Topic:           "TopicTest",
		TimestampMillis: 1781242105254,
		BrokerAddr:      brokerListener.Addr().String(),
		QueueID:         0,
		HasQueueID:      true,
		SpecifiedQueue:  true,
	}
	rows, err := NewClient(time.Second).ResetOffsetByTime(context.Background(), "127.0.0.1:9876", options)
	if err != nil {
		t.Fatalf("reset offset by time zero search branch: %v", err)
	}
	if len(rows) != 1 || rows[0].Offset != 0 {
		t.Fatalf("unexpected zero search rows: %#v", rows)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientResetOffsetByTimeTimestampUsesBrokerResetOffsetRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeInvokeBrokerToResetOffset {
			brokerDone <- fmt.Errorf("expected INVOKE_BROKER_TO_RESET_OFFSET code %d, got %d", requestCodeInvokeBrokerToResetOffset, request.Code)
			return
		}
		expectedFields := map[string]string{
			"group":     "GoadminGroup",
			"topic":     "TopicTest",
			"timestamp": "-1",
			"isForce":   "false",
			"offset":    "-1",
		}
		if !reflect.DeepEqual(request.ExtFields, expectedFields) {
			brokerDone <- fmt.Errorf("unexpected resetOffsetByTime fields\nexpected:%#v\nactual:%#v", expectedFields, request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			brokerDone <- fmt.Errorf("expected empty resetOffsetByTime body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"TopicTest"}:42,{"brokerName":"broker-a","queueId":1,"topic":"TopicTest"}:43}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	rows, err := NewClient(time.Second).ResetOffsetByTime(context.Background(), nameServerListener.Addr().String(), resetOffsetByTimeOptions{
		Group:           "GoadminGroup",
		Topic:           "TopicTest",
		TimestampMillis: -1,
		Force:           false,
	})
	if err != nil {
		t.Fatalf("reset offset by time timestamp: %v", err)
	}
	expected := []skipAccumulatedMessageRow{
		{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 3}, Offset: 42},
		{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1}, Offset: 43},
	}
	if !reflect.DeepEqual(rows, expected) {
		t.Fatalf("unexpected resetOffsetByTime rows\nexpected:%#v\nactual:%#v", expected, rows)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientSkipAccumulatedMessageUsesBrokerResetOffsetRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeInvokeBrokerToResetOffset {
			brokerDone <- fmt.Errorf("expected INVOKE_BROKER_TO_RESET_OFFSET code %d, got %d", requestCodeInvokeBrokerToResetOffset, request.Code)
			return
		}
		expectedFields := map[string]string{
			"group":     "GroupA",
			"topic":     "TopicTest",
			"timestamp": "-1",
			"isForce":   "false",
			"offset":    "-1",
			"queueId":   "-1",
		}
		if !reflect.DeepEqual(request.ExtFields, expectedFields) {
			brokerDone <- fmt.Errorf("unexpected skipAccumulatedMessage fields\nexpected:%#v\nactual:%#v", expectedFields, request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			brokerDone <- fmt.Errorf("expected empty skipAccumulatedMessage body, got %d bytes", len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"TopicTest"}:42,{"brokerName":"broker-a","queueId":1,"topic":"TopicTest"}:43}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	rows, err := NewClient(time.Second).SkipAccumulatedMessage(context.Background(), nameServerListener.Addr().String(), skipAccumulatedMessageOptions{
		Group: "GroupA",
		Topic: "TopicTest",
		Force: false,
	})
	if err != nil {
		t.Fatalf("skip accumulated message: %v", err)
	}
	expected := []skipAccumulatedMessageRow{
		{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 3}, Offset: 42},
		{Queue: messageQueueIdentity{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1}, Offset: 43},
	}
	if !reflect.DeepEqual(rows, expected) {
		t.Fatalf("unexpected skipAccumulatedMessage rows\nexpected:%#v\nactual:%#v", expected, rows)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientUpdateTopicListUsesOfficialRequestBody(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	fileName := writeTopicListFileForTest(t, `[{"topicName":"GoadminBatchTopicA","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicFilterType":"SINGLE_TAG","topicSysFlag":0,"order":false,"attributes":{}}]`)
	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeUpdateAndCreateTopicList {
			done <- fmt.Errorf("expected UPDATE_AND_CREATE_TOPIC_LIST code %d, got %d", requestCodeUpdateAndCreateTopicList, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("unexpected updateTopicList ext fields %#v", request.ExtFields)
			return
		}
		var body struct {
			TopicConfigList []updateTopicConfig `json:"topicConfigList"`
		}
		if err := json.Unmarshal(request.Body, &body); err != nil {
			done <- err
			return
		}
		if len(body.TopicConfigList) != 1 {
			done <- fmt.Errorf("expected one topic config, got %#v", body.TopicConfigList)
			return
		}
		config := body.TopicConfigList[0]
		if config.TopicName != "GoadminBatchTopicA" || config.ReadQueueNums != 4 || config.WriteQueueNums != 4 || config.Perm != 6 || config.TopicFilterType != "SINGLE_TAG" || config.TopicSysFlag != 0 || config.Order {
			done <- fmt.Errorf("unexpected updateTopicList config %#v", config)
			return
		}
		if !reflect.DeepEqual(config.Attributes, map[string]string{}) {
			done <- fmt.Errorf("unexpected updateTopicList attributes %#v", config.Attributes)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	targets, err := client.UpdateTopicList(context.Background(), "", updateTopicListOptions{
		BrokerAddr: brokerListener.Addr().String(),
		FileName:   fileName,
	})
	if err != nil {
		t.Fatalf("update topic list: %v", err)
	}
	if !reflect.DeepEqual(targets, []string{brokerListener.Addr().String()}) {
		t.Fatalf("unexpected updateTopicList targets %#v", targets)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker server: %v", err)
	}
}

func TestClientTopicMutationUsesOfficialRequestCodes(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	clusterInfoBody := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
	nameServerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			if request.Code != requestCodeGetBrokerClusterInfo {
				_ = conn.Close()
				nameServerDone <- fmt.Errorf("unexpected cluster info code %d", request.Code)
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			_, err = conn.Write(remotingFrameForTest(t, response, clusterInfoBody))
			_ = conn.Close()
			if err != nil {
				nameServerDone <- err
				return
			}
		}
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeDeleteTopicInNameServer {
			nameServerDone <- fmt.Errorf("expected DELETE_TOPIC_IN_NAMESRV code %d, got %d", requestCodeDeleteTopicInNameServer, request.Code)
			return
		}
		expectedFields := map[string]string{"clusterName": "DefaultCluster", "topic": "GoadminParityTopic"}
		if !reflect.DeepEqual(request.ExtFields, expectedFields) {
			nameServerDone <- fmt.Errorf("unexpected delete namesrv fields expected=%#v actual=%#v", expectedFields, request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		nameServerDone <- err
	}()

	brokerDone := make(chan error, 1)
	go func() {
		expected := []struct {
			code   int
			fields map[string]string
		}{
			{
				code: requestCodeUpdateAndCreateTopic,
				fields: map[string]string{
					"topic":           "GoadminParityTopic",
					"defaultTopic":    "TBW102",
					"readQueueNums":   "2",
					"writeQueueNums":  "2",
					"perm":            "6",
					"topicFilterType": "SINGLE_TAG",
					"topicSysFlag":    "0",
					"order":           "false",
					"attributes":      "",
				},
			},
			{
				code:   requestCodeDeleteTopicInBroker,
				fields: map[string]string{"topic": "GoadminParityTopic"},
			},
		}
		for _, item := range expected {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			if request.Code != item.code {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("expected topic request code %d, got %d", item.code, request.Code)
				return
			}
			if !reflect.DeepEqual(request.ExtFields, item.fields) {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected topic fields expected=%#v actual=%#v", item.fields, request.ExtFields)
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			_ = conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	client := NewClient(time.Second)
	_, err = client.UpdateTopic(context.Background(), nameServerListener.Addr().String(), updateTopicOptions{
		NameServer:      nameServerListener.Addr().String(),
		ClusterName:     "DefaultCluster",
		Topic:           "GoadminParityTopic",
		ReadQueueNums:   2,
		WriteQueueNums:  2,
		Perm:            6,
		TopicFilterType: "SINGLE_TAG",
	})
	if err != nil {
		t.Fatalf("updateTopic: %v", err)
	}
	if err := client.DeleteTopic(context.Background(), nameServerListener.Addr().String(), "DefaultCluster", "GoadminParityTopic"); err != nil {
		t.Fatalf("deleteTopic: %v", err)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientUpdateStaticTopicUsesOfficialRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- fmt.Errorf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		request, err := decodeCommand(conn)
		if err != nil {
			_ = conn.Close()
			brokerDone <- err
			return
		}
		expectedGetFields := map[string]string{"topic": "GoadminStaticTopic", "lo": "true"}
		if request.Code != requestCodeGetTopicConfig || !reflect.DeepEqual(request.ExtFields, expectedGetFields) || len(request.Body) != 0 {
			_ = conn.Close()
			brokerDone <- fmt.Errorf("unexpected getTopicConfig request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeTopicNotExist, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1, Remark: "topic not exist"}
		if _, err = conn.Write(remotingFrameForTest(t, response, nil)); err != nil {
			_ = conn.Close()
			brokerDone <- err
			return
		}
		_ = conn.Close()

		conn, err = brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err = decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		expectedUpdateFields := map[string]string{
			"topic":           "GoadminStaticTopic",
			"defaultTopic":    "TBW102",
			"readQueueNums":   "4",
			"writeQueueNums":  "4",
			"perm":            "6",
			"topicFilterType": "SINGLE_TAG",
			"topicSysFlag":    "0",
			"order":           "false",
			"force":           "true",
		}
		if request.Code != requestCodeUpdateAndCreateStaticTopic || !reflect.DeepEqual(request.ExtFields, expectedUpdateFields) {
			brokerDone <- fmt.Errorf("unexpected updateStaticTopic request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		var mapping staticTopicQueueMappingDetail
		if err := json.Unmarshal([]byte(normalizeFastJSONNumericKeys(string(request.Body))), &mapping); err != nil {
			brokerDone <- err
			return
		}
		if mapping.Topic != "GoadminStaticTopic" || mapping.BName != "broker-a" || mapping.Scope != "__global__" || mapping.TotalQueues != 4 || mapping.Epoch <= 0 || len(mapping.HostedQueues) != 4 {
			brokerDone <- fmt.Errorf("unexpected static topic mapping %#v", mapping)
			return
		}
		for queueID := 0; queueID < 4; queueID++ {
			items := mapping.HostedQueues[strconv.Itoa(queueID)]
			if len(items) != 1 || items[0].BName != "broker-a" || items[0].QueueID != queueID || items[0].LogicOffset != 0 || items[0].EndOffset != -1 {
				brokerDone <- fmt.Errorf("unexpected queue %d mapping %#v", queueID, items)
				return
			}
		}
		response = remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		brokerDone <- err
	}()

	result, err := NewClient(time.Second).UpdateStaticTopic(context.Background(), nameServerListener.Addr().String(), updateStaticTopicOptions{
		NameServer:     nameServerListener.Addr().String(),
		BrokerNames:    []string{"broker-a"},
		Topic:          "GoadminStaticTopic",
		TotalQueueNums: 4,
		ForceReplace:   true,
	})
	if err != nil {
		t.Fatalf("updateStaticTopic: %v", err)
	}
	if !strings.HasSuffix(result.BeforeFile, ".before") || !strings.Contains(result.BeforeFile, "GoadminStaticTopic-") {
		t.Fatalf("unexpected before file %s", result.BeforeFile)
	}
	if !strings.HasSuffix(result.AfterFile, ".after") || !strings.Contains(result.AfterFile, "GoadminStaticTopic-") {
		t.Fatalf("unexpected after file %s", result.AfterFile)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientRemappingStaticTopicUsesOfficialRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo || len(request.ExtFields) != 0 || len(request.Body) != 0 {
			nameServerDone <- fmt.Errorf("unexpected cluster info request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	config := newStaticTopicConfig("StaticTopicA", "broker-a", 2, 1000)
	config.ReadQueueNums = 2
	config.WriteQueueNums = 2
	for queueID := 0; queueID < 2; queueID++ {
		config.MappingDetail.HostedQueues[strconv.Itoa(queueID)] = []staticLogicQueueMappingItem{{
			Gen:         0,
			QueueID:     queueID,
			BName:       "broker-a",
			LogicOffset: 0,
			StartOffset: 0,
			EndOffset:   -1,
			TimeOfStart: -1,
			TimeOfEnd:   -1,
		}}
		config.MappingDetail.CurrIDMap[strconv.Itoa(queueID)] = queueID
	}

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		request, err := decodeCommand(conn)
		if err != nil {
			_ = conn.Close()
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetTopicConfig || !reflect.DeepEqual(request.ExtFields, map[string]string{"topic": "StaticTopicA", "lo": "true"}) || len(request.Body) != 0 {
			_ = conn.Close()
			brokerDone <- fmt.Errorf("unexpected getTopicConfig request code=%d fields=%#v body=%d", request.Code, request.ExtFields, len(request.Body))
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		if _, err = conn.Write(remotingFrameForTest(t, response, []byte(formatStaticTopicConfigJSON(config)))); err != nil {
			_ = conn.Close()
			brokerDone <- err
			return
		}
		_ = conn.Close()

		conn, err = brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err = decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		expectedFields := map[string]string{
			"topic":           "StaticTopicA",
			"defaultTopic":    "TBW102",
			"readQueueNums":   "2",
			"writeQueueNums":  "2",
			"perm":            "6",
			"topicFilterType": "SINGLE_TAG",
			"topicSysFlag":    "0",
			"order":           "false",
			"force":           "false",
		}
		if request.Code != requestCodeUpdateAndCreateStaticTopic || !reflect.DeepEqual(request.ExtFields, expectedFields) {
			brokerDone <- fmt.Errorf("unexpected remappingStaticTopic request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		var mapping staticTopicQueueMappingDetail
		if err := json.Unmarshal([]byte(normalizeFastJSONNumericKeys(string(request.Body))), &mapping); err != nil {
			brokerDone <- err
			return
		}
		if mapping.Topic != "StaticTopicA" || mapping.BName != "broker-a" || mapping.Scope != "__global__" || mapping.TotalQueues != 2 || mapping.Epoch <= 1000 || len(mapping.HostedQueues) != 2 {
			brokerDone <- fmt.Errorf("unexpected remapped static topic mapping %#v", mapping)
			return
		}
		response = remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		brokerDone <- err
	}()

	result, err := NewClient(time.Second).RemappingStaticTopic(context.Background(), nameServerListener.Addr().String(), remappingStaticTopicOptions{
		NameServer:  nameServerListener.Addr().String(),
		BrokerNames: []string{"broker-a"},
		Topic:       "StaticTopicA",
	})
	if err != nil {
		t.Fatalf("remappingStaticTopic: %v", err)
	}
	if !strings.HasSuffix(result.BeforeFile, ".before") || !strings.Contains(result.BeforeFile, "StaticTopicA-") {
		t.Fatalf("unexpected before file %s", result.BeforeFile)
	}
	if !strings.HasSuffix(result.AfterFile, ".after") || !strings.Contains(result.AfterFile, "StaticTopicA-") {
		t.Fatalf("unexpected after file %s", result.AfterFile)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientUpdateTopicPermUsesOfficialRouteAndUpdateRequest(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic {
			nameServerDone <- fmt.Errorf("expected GET_ROUTEINFO_BY_TOPIC code %d, got %d", requestCodeGetRouteInfoByTopic, request.Code)
			return
		}
		if request.ExtFields["topic"] != "GoadminTopicPerm" {
			nameServerDone <- fmt.Errorf("unexpected route topic fields %#v", request.ExtFields)
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:"%s"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":2,"topicSysFlag":0,"writeQueueNums":2}]}`, brokerListener.Addr().String()))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeUpdateAndCreateTopic {
			brokerDone <- fmt.Errorf("expected UPDATE_AND_CREATE_TOPIC code %d, got %d", requestCodeUpdateAndCreateTopic, request.Code)
			return
		}
		expectedFields := map[string]string{
			"topic":           "GoadminTopicPerm",
			"defaultTopic":    "TBW102",
			"readQueueNums":   "2",
			"writeQueueNums":  "2",
			"perm":            "4",
			"topicFilterType": "SINGLE_TAG",
			"topicSysFlag":    "0",
			"order":           "false",
			"attributes":      "",
		}
		if !reflect.DeepEqual(request.ExtFields, expectedFields) {
			brokerDone <- fmt.Errorf("unexpected updateTopicPerm fields expected=%#v actual=%#v", expectedFields, request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		brokerDone <- err
	}()

	result, err := NewClient(time.Second).UpdateTopicPerm(context.Background(), nameServerListener.Addr().String(), updateTopicPermOptions{
		NameServer: nameServerListener.Addr().String(),
		BrokerAddr: brokerListener.Addr().String(),
		Topic:      "GoadminTopicPerm",
		Perm:       4,
	})
	if err != nil {
		t.Fatalf("updateTopicPerm: %v", err)
	}
	if result == nil || !result.PrintConfig || len(result.Rows) != 1 || result.Rows[0].OldPerm != 6 || result.Rows[0].NewPerm != 4 {
		t.Fatalf("unexpected updateTopicPerm result %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientUpdateTopicPermSamePermSkipsBrokerUpdate(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:"%s"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":2,"topicSysFlag":0,"writeQueueNums":2}]}`, brokerListener.Addr().String()))
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	result, err := NewClient(200*time.Millisecond).UpdateTopicPerm(context.Background(), nameServerListener.Addr().String(), updateTopicPermOptions{
		NameServer: nameServerListener.Addr().String(),
		BrokerAddr: brokerListener.Addr().String(),
		Topic:      "GoadminTopicPerm",
		Perm:       6,
	})
	if err != nil {
		t.Fatalf("updateTopicPerm same perm: %v", err)
	}
	if result == nil || !result.SamePerm {
		t.Fatalf("expected same-perm result, got %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	tcpBrokerListener, ok := brokerListener.(*net.TCPListener)
	if !ok {
		t.Fatalf("expected TCP listener, got %T", brokerListener)
	}
	if err := tcpBrokerListener.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("set broker deadline: %v", err)
	}
	if conn, err := brokerListener.Accept(); err == nil {
		_ = conn.Close()
		t.Fatalf("same-perm path unexpectedly sent broker update request")
	}
}

func TestClientSetConsumeModeUsesOfficialRequestCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeSetMessageRequestMode {
			done <- fmt.Errorf("expected SET_MESSAGE_REQUEST_MODE code %d, got %d", requestCodeSetMessageRequestMode, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("unexpected setConsumeMode fields %#v", request.ExtFields)
			return
		}
		var body struct {
			Topic            string `json:"topic"`
			ConsumerGroup    string `json:"consumerGroup"`
			Mode             string `json:"mode"`
			PopShareQueueNum int    `json:"popShareQueueNum"`
		}
		if err := json.Unmarshal(request.Body, &body); err != nil {
			done <- fmt.Errorf("decode setConsumeMode body %q: %w", string(request.Body), err)
			return
		}
		if body.Topic != "GoadminSetConsumeModeTopic" || body.ConsumerGroup != "GoadminSetConsumeModeGroup" || body.Mode != "POP" || body.PopShareQueueNum != 3 {
			done <- fmt.Errorf("unexpected setConsumeMode body %#v", body)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		done <- err
	}()

	result, err := NewClient(time.Second).SetConsumeMode(context.Background(), "", setConsumeModeOptions{
		BrokerAddr:       listener.Addr().String(),
		Topic:            "GoadminSetConsumeModeTopic",
		GroupName:        "GoadminSetConsumeModeGroup",
		Mode:             "POP",
		PopShareQueueNum: 3,
	})
	if err != nil {
		t.Fatalf("setConsumeMode: %v", err)
	}
	if result == nil || !reflect.DeepEqual(result.Targets, []string{listener.Addr().String()}) || result.Mode != "POP" || result.PopShareQueueNum != 3 {
		t.Fatalf("unexpected setConsumeMode result %#v", result)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientSubscriptionGroupMutationUsesOfficialRequestCodes(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		expected := []struct {
			code       int
			fields     map[string]string
			bodyAssert func([]byte) error
		}{
			{
				code:   requestCodeUpdateAndCreateSubscriptionGroup,
				fields: nil,
				bodyAssert: func(body []byte) error {
					var payload map[string]any
					if err := json.Unmarshal(body, &payload); err != nil {
						return err
					}
					if payload["groupName"] != "GoadminParityGroup" ||
						payload["consumeEnable"] != true ||
						payload["consumeFromMinEnable"] != false ||
						payload["consumeBroadcastEnable"] != false ||
						payload["retryQueueNums"] != float64(1) ||
						payload["retryMaxTimes"] != float64(16) {
						return fmt.Errorf("unexpected subscription group payload %#v", payload)
					}
					return nil
				},
			},
			{
				code:   requestCodeDeleteSubscriptionGroup,
				fields: map[string]string{"groupName": "GoadminParityGroup", "cleanOffset": "true"},
			},
		}
		for _, item := range expected {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			if request.Code != item.code {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("expected subscription group request code %d, got %d", item.code, request.Code)
				return
			}
			if item.fields != nil && !reflect.DeepEqual(request.ExtFields, item.fields) {
				_ = conn.Close()
				brokerDone <- fmt.Errorf("unexpected subscription group fields expected=%#v actual=%#v", item.fields, request.ExtFields)
				return
			}
			if item.bodyAssert != nil {
				if err := item.bodyAssert(request.Body); err != nil {
					_ = conn.Close()
					brokerDone <- err
					return
				}
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			_ = conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	client := NewClient(time.Second)
	_, err = client.UpdateSubGroup(context.Background(), "", updateSubGroupOptions{
		BrokerAddr:       brokerListener.Addr().String(),
		GroupName:        "GoadminParityGroup",
		ConsumeEnable:    true,
		RetryQueueNums:   1,
		RetryMaxTimes:    16,
		GroupRetryPolicy: defaultGroupRetryPolicyJSON,
	})
	if err != nil {
		t.Fatalf("updateSubGroup: %v", err)
	}
	if _, err := client.DeleteSubGroup(context.Background(), "", deleteSubGroupOptions{
		BrokerAddr:   brokerListener.Addr().String(),
		GroupName:    "GoadminParityGroup",
		RemoveOffset: true,
	}); err != nil {
		t.Fatalf("deleteSubGroup: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientUpdateSubGroupListUsesOfficialRequestBody(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeUpdateAndCreateSubscriptionGroupList {
			done <- fmt.Errorf("expected UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST code %d, got %d", requestCodeUpdateAndCreateSubscriptionGroupList, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("expected no ext fields, got %#v", request.ExtFields)
			return
		}
		var payload struct {
			GroupConfigList []map[string]any `json:"groupConfigList"`
		}
		if err := json.Unmarshal(request.Body, &payload); err != nil {
			done <- err
			return
		}
		if len(payload.GroupConfigList) != 1 {
			done <- fmt.Errorf("expected one group config, got %#v", payload.GroupConfigList)
			return
		}
		group := payload.GroupConfigList[0]
		if group["groupName"] != "GoadminBatchGroupA" || group["retryQueueNums"] != float64(1) || group["notifyConsumerIdsChangedEnable"] != true {
			done <- fmt.Errorf("unexpected group config payload %#v", group)
			return
		}
		_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	_, err = client.UpdateSubGroupList(context.Background(), "", updateSubGroupListOptions{
		BrokerAddr: brokerListener.Addr().String(),
		GroupConfigs: []subscriptionGroupConfig{{
			GroupName:                    "GoadminBatchGroupA",
			ConsumeEnable:                true,
			RetryQueueNums:               1,
			RetryMaxTimes:                16,
			GroupRetryPolicy:             defaultGroupRetryPolicyJSON,
			BrokerID:                     0,
			WhichBrokerWhenConsumeSlowly: 1,
			NotifyConsumerIdsChanged:     true,
			ConsumeTimeoutMinute:         15,
			Attributes:                   map[string]string{},
		}},
	})
	if err != nil {
		t.Fatalf("updateSubGroupList: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker request: %v", err)
	}
}

func TestRunNativeTopicRouteFormatsOfficialJSON(t *testing.T) {
	client := nativeClientFunc{
		topicRoute: func(ctx context.Context, nameServer string, topic string) ([]byte, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" {
				t.Fatalf("unexpected topic %s", topic)
			}
			return []byte(`{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}]}`), nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"topicRoute", "-n", "127.0.0.1:9876", "-t", "TopicTest"}, client)
	if err != nil {
		t.Fatalf("run native topicRoute: %v", err)
	}
	if !supported {
		t.Fatalf("expected topicRoute to be supported")
	}
	expected := "{\n\t\"brokerDatas\":[\n\t\t{\n\t\t\t\"brokerAddrs\":{0:\"127.0.0.1:10911\"\n\t\t\t},\n\t\t\t\"brokerName\":\"broker-a\",\n\t\t\t\"cluster\":\"DefaultCluster\",\n\t\t\t\"enableActingMaster\":false\n\t\t}\n\t],\n\t\"filterServerTable\":{},\n\t\"queueDatas\":[\n\t\t{\n\t\t\t\"brokerName\":\"broker-a\",\n\t\t\t\"perm\":6,\n\t\t\t\"readQueueNums\":4,\n\t\t\t\"topicSysFlag\":0,\n\t\t\t\"writeQueueNums\":4\n\t\t}\n\t]\n}\n"
	if output != expected {
		t.Fatalf("topicRoute output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeTopicRouteFormatsOfficialList(t *testing.T) {
	client := nativeClientFunc{
		topicRoute: func(ctx context.Context, nameServer string, topic string) ([]byte, error) {
			if nameServer != "127.0.0.1:9876" || topic != "TopicTest" {
				t.Fatalf("unexpected topicRoute args namesrv=%s topic=%s", nameServer, topic)
			}
			return []byte(`{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}]}`), nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"topicRoute", "-n", "127.0.0.1:9876", "-t", "TopicTest", "-l"}, client)
	if err != nil {
		t.Fatalf("run native topicRoute -l: %v", err)
	}
	if !supported {
		t.Fatalf("expected topicRoute -l to be supported")
	}
	expected := fmt.Sprintf("%-45s %-32s %-50s %-10s %-11s %-5s\n", "#ClusterName", "#BrokerName", "#BrokerAddrs", "#ReadQueue", "#WriteQueue", "#Perm") +
		fmt.Sprintf("%-45s %-32s %-50s %-10s %-11s %-5s\n", "DefaultCluster", "broker-a", "{0=127.0.0.1:10911}", "4", "4", "6") +
		strings.Repeat("-", 158) + "\n" +
		fmt.Sprintf("%-45s %-32s %-50s %-10s %-11s %-5s\n", "Total:", "1", "", "4", "4", "")
	if output != expected {
		t.Fatalf("topicRoute -l output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeTopicRouteRequiresTopic(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{"topicRoute", "-n", "127.0.0.1:9876"}, nil)
	if err == nil || !strings.Contains(err.Error(), "Topic 必填") {
		t.Fatalf("expected missing topic error, got output=%q supported=%t err=%v", output, supported, err)
	}
	if !supported || output != "" {
		t.Fatalf("expected topicRoute missing topic to be handled by native command, supported=%t output=%q", supported, output)
	}
}

func TestRunNativeTopicStatusFormatsOfficialTable(t *testing.T) {
	client := nativeClientFunc{
		topicStatus: func(ctx context.Context, nameServer string, topic string) ([]topicStatusEntry, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" {
				t.Fatalf("unexpected topic %s", topic)
			}
			return []topicStatusEntry{
				{BrokerName: "broker-a", QueueID: 1, MinOffset: 0, MaxOffset: 0},
				{BrokerName: "broker-a", QueueID: 0, MinOffset: 0, MaxOffset: 3, LastUpdateTimestamp: 1780883297044},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"topicStatus", "-n", "127.0.0.1:9876", "-t", "TopicTest"}, client)
	if err != nil {
		t.Fatalf("run native topicStatus: %v", err)
	}
	if !supported {
		t.Fatalf("expected topicStatus to be supported")
	}
	lastUpdated := time.UnixMilli(1780883297044).In(time.Local).Format("2006-01-02 15:04:05,000")
	expected := "#Broker Name                      #QID  #Min Offset           #Max Offset             #Last Updated\n" +
		"broker-a                          0     0                     3                       " + lastUpdated + "\n" +
		"broker-a                          1     0                     0                       \n"
	if output != expected {
		t.Fatalf("topicStatus output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeTopicStatusClusterUsesClusterRoute(t *testing.T) {
	client := nativeClientFunc{
		topicStatusByCluster: func(ctx context.Context, nameServer string, topic string, cluster string) ([]topicStatusEntry, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" {
				t.Fatalf("unexpected topic %s", topic)
			}
			if cluster != "DefaultCluster" {
				t.Fatalf("unexpected cluster %s", cluster)
			}
			return []topicStatusEntry{{BrokerName: "broker-a", QueueID: 0, MinOffset: 1, MaxOffset: 2}}, nil
		},
		topicStatus: func(ctx context.Context, nameServer string, topic string) ([]topicStatusEntry, error) {
			t.Fatalf("topicStatus -c must use cluster route, got namesrv=%s topic=%s", nameServer, topic)
			return nil, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"topicStatus",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
	}, client)
	if err != nil {
		t.Fatalf("run native topicStatus -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected topicStatus -c to be supported")
	}
	expected := "#Broker Name                      #QID  #Min Offset           #Max Offset             #Last Updated\n" +
		"broker-a                          0     1                     2                       \n"
	if output != expected {
		t.Fatalf("topicStatus -c output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeTopicStatusRequiresTopic(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{"topicStatus", "-n", "127.0.0.1:9876"}, nil)
	if err == nil || !strings.Contains(err.Error(), "Topic 必填") {
		t.Fatalf("expected missing topic error, got output=%q supported=%t err=%v", output, supported, err)
	}
	if !supported || output != "" {
		t.Fatalf("expected topicStatus missing topic to be handled by native command, supported=%t output=%q", supported, output)
	}
}

func TestRunNativeTopicClusterListFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		topicClusterList: func(ctx context.Context, nameServer string, topic string) ([]string, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" {
				t.Fatalf("unexpected topic %s", topic)
			}
			return []string{"DefaultCluster"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"topicClusterList", "-n", "127.0.0.1:9876", "-t", "TopicTest"}, client)
	if err != nil {
		t.Fatalf("run native topicClusterList: %v", err)
	}
	if !supported {
		t.Fatalf("expected topicClusterList to be supported")
	}
	if output != "DefaultCluster\n" {
		t.Fatalf("unexpected output %q", output)
	}
}

func TestRunNativeTopicClusterListRequiresTopic(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{"topicClusterList", "-n", "127.0.0.1:9876"}, nil)
	if err == nil || !strings.Contains(err.Error(), "Topic 必填") {
		t.Fatalf("expected missing topic error, got output=%q supported=%t err=%v", output, supported, err)
	}
	if !supported || output != "" {
		t.Fatalf("expected topicClusterList missing topic to be handled by native command, supported=%t output=%q", supported, output)
	}
}

func TestRunNativeConsumerProgressFormatsOfficialGroupTable(t *testing.T) {
	client := nativeClientFunc{
		consumerProgress: func(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "GroupA" || topic != "TopicTest" || clusterName != "" {
				t.Fatalf("unexpected consumer progress args namesrv=%s group=%s topic=%s cluster=%s", nameServer, consumerGroup, topic, clusterName)
			}
			return &consumerProgress{
				ConsumeTPS: 1.25,
				Entries: []consumerProgressEntry{
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1, BrokerOffset: 0, ConsumerOffset: 0, PullOffset: 0},
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0, BrokerOffset: 3, ConsumerOffset: 1, PullOffset: 2, LastTimestamp: 1780883297000},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"consumerProgress", "-n", "127.0.0.1:9876", "-g", "GroupA", "-t", "TopicTest"}, client)
	if err != nil {
		t.Fatalf("run native consumerProgress: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerProgress -g to be supported")
	}
	expected := fmt.Sprintf("%-64s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s%s\n", "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff", "#Inflight", "#LastTime") +
		fmt.Sprintf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20d %-20d %s\n", "TopicTest", "broker-a", 0, int64(3), int64(1), int64(2), int64(1), time.UnixMilli(1780883297000).In(time.Local).Format("2006-01-02 15:04:05")) +
		fmt.Sprintf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20d %-20d %s\n", "TopicTest", "broker-a", 1, int64(0), int64(0), int64(0), int64(0), "N/A") +
		"\n" +
		"Consume TPS: 1.25\n" +
		"Consume Diff Total: 2\n" +
		"Consume Inflight Total: 1\n"
	if output != expected {
		t.Fatalf("consumerProgress output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeConsumerProgressGroupTopicPassesClusterName(t *testing.T) {
	client := nativeClientFunc{
		consumerProgress: func(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "GroupA" || topic != "TopicTest" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected consumer progress args namesrv=%s group=%s topic=%s cluster=%s", nameServer, consumerGroup, topic, clusterName)
			}
			return &consumerProgress{
				Entries: []consumerProgressEntry{
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0, BrokerOffset: 3, ConsumerOffset: 1, PullOffset: 2},
				},
			}, nil
		},
	}

	_, supported, err := runNativeCommand(context.Background(), []string{"consumerProgress", "-n", "127.0.0.1:9876", "-g", "GroupA", "-t", "TopicTest", "-c", "DefaultCluster"}, client)
	if err != nil {
		t.Fatalf("run native consumerProgress -g -t -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerProgress -g -t -c to be supported")
	}
}

func TestRunNativeConsumerProgressFormatsOfficialGroupTableWithClientIP(t *testing.T) {
	client := nativeClientFunc{
		consumerProgressWithClientIP: func(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "GroupA" || topic != "TopicTest" || clusterName != "" {
				t.Fatalf("unexpected consumer progress showClientIP args namesrv=%s group=%s topic=%s cluster=%s", nameServer, consumerGroup, topic, clusterName)
			}
			return &consumerProgress{
				ConsumeTPS: 1.25,
				Entries: []consumerProgressEntry{
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1, BrokerOffset: 0, ConsumerOffset: 0, PullOffset: 0},
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0, BrokerOffset: 3, ConsumerOffset: 1, PullOffset: 2, LastTimestamp: 1780883297000, ClientIP: "10.0.0.1"},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"consumerProgress", "-n", "127.0.0.1:9876", "-g", "GroupA", "-t", "TopicTest", "-s", "true"}, client)
	if err != nil {
		t.Fatalf("run native consumerProgress showClientIP: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerProgress -g -s true to be supported")
	}
	expected := fmt.Sprintf("%-64s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s %-20s%s\n", "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Client IP", "#Diff", "#Inflight", "#LastTime") +
		fmt.Sprintf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20s %-20d %-20d %s\n", "TopicTest", "broker-a", 0, int64(3), int64(1), "10.0.0.1", int64(2), int64(1), time.UnixMilli(1780883297000).In(time.Local).Format("2006-01-02 15:04:05")) +
		fmt.Sprintf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20s %-20d %-20d %s\n", "TopicTest", "broker-a", 1, int64(0), int64(0), "N/A", int64(0), int64(0), "N/A") +
		"\n" +
		"Consume TPS: 1.25\n" +
		"Consume Diff Total: 2\n" +
		"Consume Inflight Total: 1\n"
	if output != expected {
		t.Fatalf("consumerProgress showClientIP output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeConsumerProgressFormatsOfficialSummary(t *testing.T) {
	client := nativeClientFunc{
		consumerProgressSummary: func(ctx context.Context, nameServer string) ([]consumerProgressSummaryRow, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			return []consumerProgressSummaryRow{
				{Group: "GroupA", Count: 2, Version: "V5_3_2", ConsumeType: "PUSH", MessageModel: "CLUSTERING", ConsumeTPS: 12, DiffTotal: 3},
				{Group: "TOOLS_CONSUMER", Version: "OFFLINE"},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"consumerProgress", "-n", "127.0.0.1:9876"}, client)
	if err != nil {
		t.Fatalf("run native consumerProgress summary: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerProgress summary to be supported")
	}
	expected := fmt.Sprintf("%-64s  %-6s  %-24s %-5s  %-14s  %-7s  %s\n", "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS", "#Diff Total") +
		fmt.Sprintf("%-64s  %-6d  %-24s %-5s  %-14s  %-7d  %d\n", "GroupA", 2, "V5_3_2", "PUSH", "CLUSTERING", 12, int64(3)) +
		fmt.Sprintf("%-64s  %-6d  %-24s %-5s  %-14s  %-7d  %d\n", "TOOLS_CONSUMER", 0, "OFFLINE", "", "", 0, int64(0))
	if output != expected {
		t.Fatalf("consumerProgress summary mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeConsumerProgressClusterUsesOfficialSummary(t *testing.T) {
	client := nativeClientFunc{
		consumerProgressSummary: func(ctx context.Context, nameServer string) ([]consumerProgressSummaryRow, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			return []consumerProgressSummaryRow{
				{Group: "TOOLS_CONSUMER", Count: 1, Version: "V5_3_2", ConsumeType: "PUSH", MessageModel: "CLUSTERING"},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"consumerProgress", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"}, client)
	if err != nil {
		t.Fatalf("run native consumerProgress -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerProgress -c summary to be supported")
	}
	expected := fmt.Sprintf("%-64s  %-6s  %-24s %-5s  %-14s  %-7s  %s\n", "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS", "#Diff Total") +
		fmt.Sprintf("%-64s  %-6d  %-24s %-5s  %-14s  %-7d  %d\n", "TOOLS_CONSUMER", 1, "V5_3_2", "PUSH", "CLUSTERING", 0, int64(0))
	if output != expected {
		t.Fatalf("consumerProgress -c summary mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeStartMonitoringWaitsUntilContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := false
	client := nativeClientFunc{
		startMonitoring: func(ctx context.Context, nameServer string) error {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			called = true
			cancel()
			<-ctx.Done()
			return nil
		},
	}

	output, supported, err := runNativeCommand(ctx, []string{"startMonitoring", "-n", "127.0.0.1:9876"}, client)
	if err != nil {
		t.Fatalf("run native startMonitoring: %v", err)
	}
	if !supported {
		t.Fatalf("expected startMonitoring to be supported")
	}
	if output != "" {
		t.Fatalf("expected startMonitoring to keep stdout empty, got %q", output)
	}
	if !called {
		t.Fatalf("expected startMonitoring client hook to be called")
	}
}

func TestRunNativeStartMonitoringRequiresNameServer(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{"startMonitoring"}, nil)
	if err == nil {
		t.Fatalf("expected startMonitoring without nameserver to fail")
	}
	if !strings.Contains(err.Error(), "NameServer 必填") {
		t.Fatalf("unexpected startMonitoring error: %v", err)
	}
	if !supported || output != "" {
		t.Fatalf("expected startMonitoring missing nameserver to be handled by native command, supported=%t output=%q", supported, output)
	}
}

func TestClientStartMonitoringReturnsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := NewClient(time.Second).StartMonitoring(ctx, "127.0.0.1:9876"); err != nil {
		t.Fatalf("startMonitoring canceled context: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("startMonitoring should return immediately after context cancel, elapsed=%s", elapsed)
	}
}

func TestRunNativeConsumerConnectionFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		consumerConnection: func(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string) (*consumerConnectionDetail, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "TOOLS_CONSUMER" || brokerAddr != "" {
				t.Fatalf("unexpected consumerConnection args namesrv=%s group=%s broker=%s", nameServer, consumerGroup, brokerAddr)
			}
			return &consumerConnectionDetail{
				Connections: []consumerConnectionEntry{{
					ClientID:   "172.24.0.2@2620#23792886135672",
					ClientAddr: "172.24.0.2:33848",
					Language:   "JAVA",
					Version:    477,
				}},
				Subscriptions: []consumerSubscriptionEntry{
					{Topic: "TopicTest", Expression: "*"},
					{Topic: "%RETRY%TOOLS_CONSUMER", Expression: "*"},
				},
				ConsumeType:      "CONSUME_PASSIVELY",
				MessageModel:     "CLUSTERING",
				ConsumeFromWhere: "CONSUME_FROM_FIRST_OFFSET",
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"consumerConnection", "-n", "127.0.0.1:9876", "-g", "TOOLS_CONSUMER"}, client)
	if err != nil {
		t.Fatalf("run native consumerConnection: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerConnection to be supported")
	}
	expected := "#ClientId                            #ClientAddr            #Language  #Version\n" +
		"172.24.0.2@2620#23792886135672       172.24.0.2:33848       JAVA       V5_3_2\n" +
		"\nBelow is subscription:\n" +
		"#Topic               #SubExpression\n" +
		"TopicTest            *\n" +
		"%RETRY%TOOLS_CONSUMER *\n" +
		"\nConsumeType: CONSUME_PASSIVELY\n" +
		"MessageModel: CLUSTERING\n" +
		"ConsumeFromWhere: CONSUME_FROM_FIRST_OFFSET\n"
	if output != expected {
		t.Fatalf("consumerConnection output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeConsumerStatusWithClientIDPrintsRunningInfo(t *testing.T) {
	client := nativeClientFunc{
		consumerStatus: func(ctx context.Context, nameServer string, consumerGroup string, clientID string, brokerAddr string, jstack bool) (string, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "TOOLS_CONSUMER" || clientID != "client-a" || brokerAddr != "" || !jstack {
				t.Fatalf("unexpected consumerStatus args namesrv=%s group=%s client=%s broker=%s jstack=%t", nameServer, consumerGroup, clientID, brokerAddr, jstack)
			}
			return "#Consumer Properties#\nconsumerGroup                           : TOOLS_CONSUMER\n", nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"consumerStatus", "-n", "127.0.0.1:9876", "-g", "TOOLS_CONSUMER", "-i", "client-a", "-s"}, client)
	if err != nil {
		t.Fatalf("run native consumerStatus -i: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerStatus -i to be supported")
	}
	expected := "#Consumer Properties#\nconsumerGroup                           : TOOLS_CONSUMER\n"
	if output != expected {
		t.Fatalf("consumerStatus -i output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeConsumerStatusWithClientIDIgnoresBrokerAddr(t *testing.T) {
	client := nativeClientFunc{
		consumerStatus: func(ctx context.Context, nameServer string, consumerGroup string, clientID string, brokerAddr string, jstack bool) (string, error) {
			if brokerAddr != "" {
				t.Fatalf("official consumerStatus -i ignores -b, got brokerAddr=%s", brokerAddr)
			}
			if nameServer != "127.0.0.1:9876" || consumerGroup != "TOOLS_CONSUMER" || clientID != "client-a" || jstack {
				t.Fatalf("unexpected consumerStatus args namesrv=%s group=%s client=%s jstack=%t", nameServer, consumerGroup, clientID, jstack)
			}
			return "#Consumer Properties#\n", nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"consumerStatus",
		"-n", "127.0.0.1:9876",
		"-g", "TOOLS_CONSUMER",
		"-i", "client-a",
		"-b", "127.0.0.1:10911",
	}, client)
	if err != nil {
		t.Fatalf("run native consumerStatus -i -b: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerStatus -i -b to be supported")
	}
	if output != "#Consumer Properties#\n" {
		t.Fatalf("unexpected consumerStatus output %q", output)
	}
}

func TestRunNativeConsumerStatusWithoutClientIDListsRunningInfoFiles(t *testing.T) {
	client := nativeClientFunc{
		consumerStatusList: func(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string, jstack bool) (string, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "TOOLS_CONSUMER" || brokerAddr != "127.0.0.1:10911" || !jstack {
				t.Fatalf("unexpected consumerStatus list args namesrv=%s group=%s broker=%s jstack=%t", nameServer, consumerGroup, brokerAddr, jstack)
			}
			return fmt.Sprintf("%-10s %-40s %-20s %s\n", "#Index", "#ClientId", "#Version", "#ConsumerRunningInfoFile") +
				fmt.Sprintf("%-10d %-40s %-20s %s\n", 1, "client-a", "V5_3_2", "1234567890/client-a") +
				"\n\nSame subscription in the same group of consumer\n\nRebalance OK\n", nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"consumerStatus",
		"-n", "127.0.0.1:9876",
		"-g", "TOOLS_CONSUMER",
		"-b", "127.0.0.1:10911",
		"-s",
	}, client)
	if err != nil {
		t.Fatalf("run native consumerStatus list: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerStatus without -i to be supported")
	}
	if !strings.Contains(output, "#ConsumerRunningInfoFile") || !strings.Contains(output, "1234567890/client-a") || !strings.Contains(output, "Rebalance OK") {
		t.Fatalf("unexpected consumerStatus list output:\n%s", output)
	}
}

func TestRunNativeProducerConnectionFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		producerConnection: func(ctx context.Context, nameServer string, producerGroup string, topic string) (*producerConnectionDetail, error) {
			if nameServer != "127.0.0.1:9876" || producerGroup != "benchmark_producer" || topic != "GoadminQueryKeyTest" {
				t.Fatalf("unexpected producerConnection args namesrv=%s group=%s topic=%s", nameServer, producerGroup, topic)
			}
			return &producerConnectionDetail{
				Connections: []producerConnectionEntry{{
					ClientID:   "172.24.0.2@1780923083637",
					ClientAddr: "172.24.0.2:52628",
					Language:   "JAVA",
					Version:    477,
				}},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"producerConnection", "-n", "127.0.0.1:9876", "-g", "benchmark_producer", "-t", "GoadminQueryKeyTest"}, client)
	if err != nil {
		t.Fatalf("run native producerConnection: %v", err)
	}
	if !supported {
		t.Fatalf("expected producerConnection to be supported")
	}
	expected := fmt.Sprintf("%04d  %-32s %-22s %-8s %s\n", 1, "172.24.0.2@1780923083637", "172.24.0.2:52628", "JAVA", "V5_3_2")
	if output != expected {
		t.Fatalf("producerConnection output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeProducerConnectionRequiresGroupAndTopic(t *testing.T) {
	for _, args := range [][]string{
		{"producerConnection", "-n", "127.0.0.1:9876", "-t", "TopicTest"},
		{"producerConnection", "-n", "127.0.0.1:9876", "-g", "ProducerGroup"},
	} {
		output, supported, err := runNativeCommand(context.Background(), args, nil)
		if err == nil {
			t.Fatalf("expected producerConnection args %v to fail", args)
		}
		if !supported || output != "" {
			t.Fatalf("expected producerConnection missing args to be handled by native command, supported=%t output=%q", supported, output)
		}
	}
}

func TestRunNativeProducerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		producer: func(ctx context.Context, brokerAddr string) (*producerTableInfo, error) {
			if brokerAddr != "127.0.0.1:10911" {
				t.Fatalf("unexpected producer broker addr %s", brokerAddr)
			}
			return &producerTableInfo{
				Groups: []producerGroupInfo{{
					Group: "CLIENT_INNER_PRODUCER",
					Producers: []producerInfo{{
						ClientID:            "172.24.0.2@346#1754392374823",
						RemoteIP:            "/172.24.0.2:54890",
						Language:            "JAVA",
						Version:             477,
						LastUpdateTimestamp: 1781002748808,
					}},
				}},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"producer", "-n", "127.0.0.1:9876", "-b", "127.0.0.1:10911"}, client)
	if err != nil {
		t.Fatalf("run native producer: %v", err)
	}
	if !supported {
		t.Fatalf("expected producer to be supported")
	}
	expected := "producer group (CLIENT_INNER_PRODUCER) instance : clientId=172.24.0.2@346#1754392374823,remoteIP=/172.24.0.2:54890, language=JAVA, version=477, lastUpdateTimestamp=1781002748808\n"
	if output != expected {
		t.Fatalf("producer output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeGetColdDataFlowCtrInfoByBrokerFormatsOfficialOutput(t *testing.T) {
	body := `{"globalAcc":0,"cgColdReadThreshold":3145728,"configTable":{},"globalColdReadThreshold":104857600,"runtimeTable":{}}`
	client := nativeClientFunc{
		getColdDataFlowCtrInfo: func(ctx context.Context, brokerAddr string) (string, error) {
			if brokerAddr != "127.0.0.1:10911" {
				t.Fatalf("unexpected cold ctr broker addr %s", brokerAddr)
			}
			return body, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getColdDataFlowCtrInfo",
		"-n", "127.0.0.1:9876",
		"-b", "127.0.0.1:10911",
	}, client)
	if err != nil {
		t.Fatalf("getColdDataFlowCtrInfo: %v", err)
	}
	if !supported {
		t.Fatal("expected getColdDataFlowCtrInfo to be supported")
	}
	expected := " ============127.0.0.1:10911============\n" +
		"{\n" +
		"\t\"globalAcc\":0,\n" +
		"\t\"cgColdReadThreshold\":3145728,\n" +
		"\t\"configTable\":{},\n" +
		"\t\"globalColdReadThreshold\":104857600,\n" +
		"\t\"runtimeTable\":{}\n" +
		"}\n"
	if output != expected {
		t.Fatalf("unexpected cold ctr output\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeGetColdDataFlowCtrInfoByClusterFormatsMasterHeader(t *testing.T) {
	client := nativeClientFunc{
		getColdDataFlowCtrInfoByCluster: func(ctx context.Context, nameServer string, clusterName string) ([]coldDataFlowCtrInfoSection, error) {
			if nameServer != "127.0.0.1:9876" || clusterName != "DefaultCluster" {
				t.Fatalf("unexpected cold ctr cluster args namesrv=%s cluster=%s", nameServer, clusterName)
			}
			return []coldDataFlowCtrInfoSection{{
				Header:     "============Master: 127.0.0.1:10911============",
				BrokerAddr: "127.0.0.1:10911",
				Body:       "",
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"getColdDataFlowCtrInfo",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
	}, client)
	if err != nil {
		t.Fatalf("getColdDataFlowCtrInfo cluster: %v", err)
	}
	if !supported {
		t.Fatal("expected getColdDataFlowCtrInfo cluster to be supported")
	}
	expected := " ============Master: 127.0.0.1:10911============\n" +
		"Broker[127.0.0.1:10911] has no cold ctr table !\n"
	if output != expected {
		t.Fatalf("unexpected cold ctr cluster output\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeUpdateColdDataFlowCtrGroupConfigBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateColdDataFlowCtrGroupConfig: func(ctx context.Context, nameServer string, options coldDataFlowCtrGroupConfigOptions) ([]string, error) {
			expected := coldDataFlowCtrGroupConfigOptions{
				NameServer:    "127.0.0.1:9876",
				BrokerAddr:    "broker-a:10911",
				ConsumerGroup: "GoadminColdCtrGroup",
				Threshold:     "12345",
			}
			if options != expected {
				t.Fatalf("unexpected update cold ctr options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected update cold ctr namesrv %s", nameServer)
			}
			return []string{"broker-a:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateColdDataFlowCtrGroupConfig",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-g", "GoadminColdCtrGroup",
		"-v", "12345",
	}, client)
	if err != nil {
		t.Fatalf("run native updateColdDataFlowCtrGroupConfig broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateColdDataFlowCtrGroupConfig broker to be supported")
	}
	expected := "updateColdDataFlowCtrGroupConfig success, broker-a:10911\n"
	if output != expected {
		t.Fatalf("updateColdDataFlowCtrGroupConfig broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeUpdateColdDataFlowCtrGroupConfigClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		updateColdDataFlowCtrGroupConfig: func(ctx context.Context, nameServer string, options coldDataFlowCtrGroupConfigOptions) ([]string, error) {
			expected := coldDataFlowCtrGroupConfigOptions{
				NameServer:    "127.0.0.1:9876",
				ClusterName:   "DefaultCluster",
				ConsumerGroup: "GoadminColdCtrGroup",
				Threshold:     "12345",
			}
			if options != expected {
				t.Fatalf("unexpected update cold ctr cluster options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected update cold ctr cluster namesrv %s", nameServer)
			}
			return []string{"broker-a:10911", "broker-b:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"updateColdDataFlowCtrGroupConfig",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-g", "GoadminColdCtrGroup",
		"-v", "12345",
	}, client)
	if err != nil {
		t.Fatalf("run native updateColdDataFlowCtrGroupConfig cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected updateColdDataFlowCtrGroupConfig cluster to be supported")
	}
	expected := "updateColdDataFlowCtrGroupConfig success, broker-a:10911\n" +
		"updateColdDataFlowCtrGroupConfig success, broker-b:10911\n"
	if output != expected {
		t.Fatalf("updateColdDataFlowCtrGroupConfig cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRemoveColdDataFlowCtrGroupConfigBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		removeColdDataFlowCtrGroupConfig: func(ctx context.Context, nameServer string, options removeColdDataFlowCtrGroupConfigOptions) ([]string, error) {
			expected := removeColdDataFlowCtrGroupConfigOptions{
				NameServer:    "127.0.0.1:9876",
				BrokerAddr:    "broker-a:10911",
				ConsumerGroup: "GoadminColdCtrGroup",
			}
			if options != expected {
				t.Fatalf("unexpected remove cold ctr options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected remove cold ctr namesrv %s", nameServer)
			}
			return []string{"broker-a:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"removeColdDataFlowCtrGroupConfig",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
		"-g", "GoadminColdCtrGroup",
	}, client)
	if err != nil {
		t.Fatalf("run native removeColdDataFlowCtrGroupConfig broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected removeColdDataFlowCtrGroupConfig broker to be supported")
	}
	expected := "remove broker cold read threshold success, broker-a:10911\n"
	if output != expected {
		t.Fatalf("removeColdDataFlowCtrGroupConfig broker output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeRemoveColdDataFlowCtrGroupConfigClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		removeColdDataFlowCtrGroupConfig: func(ctx context.Context, nameServer string, options removeColdDataFlowCtrGroupConfigOptions) ([]string, error) {
			expected := removeColdDataFlowCtrGroupConfigOptions{
				NameServer:    "127.0.0.1:9876",
				ClusterName:   "DefaultCluster",
				ConsumerGroup: "GoadminColdCtrGroup",
			}
			if options != expected {
				t.Fatalf("unexpected remove cold ctr cluster options\nexpected:%#v\nactual:%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected remove cold ctr cluster namesrv %s", nameServer)
			}
			return []string{"broker-a:10911", "broker-b:10911"}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"removeColdDataFlowCtrGroupConfig",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"-g", "GoadminColdCtrGroup",
	}, client)
	if err != nil {
		t.Fatalf("run native removeColdDataFlowCtrGroupConfig cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected removeColdDataFlowCtrGroupConfig cluster to be supported")
	}
	expected := "remove broker cold read threshold success, broker-a:10911\n" +
		"remove broker cold read threshold success, broker-b:10911\n"
	if output != expected {
		t.Fatalf("removeColdDataFlowCtrGroupConfig cluster output mismatch\nexpected:%q\nactual:%q", expected, output)
	}
}

func TestRunNativeCleanExpiredCQBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		cleanExpiredCQ: func(ctx context.Context, nameServer string, options cleanExpiredCQOptions) (bool, error) {
			expected := cleanExpiredCQOptions{
				NameServer: "127.0.0.1:9876",
				BrokerAddr: "broker-a:10911",
			}
			if options != expected {
				t.Fatalf("unexpected cleanExpiredCQ broker options\nexpected=%#v\nactual=%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected cleanExpiredCQ broker namesrv %s", nameServer)
			}
			return true, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"cleanExpiredCQ",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
	}, client)
	if err != nil {
		t.Fatalf("run native cleanExpiredCQ broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected cleanExpiredCQ broker to be supported")
	}
	if output != "success" {
		t.Fatalf("cleanExpiredCQ broker output mismatch\nexpected:%q\nactual:%q", "success", output)
	}
}

func TestRunNativeCleanExpiredCQClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		cleanExpiredCQ: func(ctx context.Context, nameServer string, options cleanExpiredCQOptions) (bool, error) {
			expected := cleanExpiredCQOptions{
				NameServer:  "127.0.0.1:9876",
				ClusterName: "DefaultCluster",
			}
			if options != expected {
				t.Fatalf("unexpected cleanExpiredCQ cluster options\nexpected=%#v\nactual=%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected cleanExpiredCQ cluster namesrv %s", nameServer)
			}
			return true, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"cleanExpiredCQ",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
	}, client)
	if err != nil {
		t.Fatalf("run native cleanExpiredCQ cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected cleanExpiredCQ cluster to be supported")
	}
	if output != "success" {
		t.Fatalf("cleanExpiredCQ cluster output mismatch\nexpected:%q\nactual:%q", "success", output)
	}
}

func TestRunNativeCleanUnusedTopicBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		cleanUnusedTopic: func(ctx context.Context, nameServer string, options cleanUnusedTopicOptions) (bool, error) {
			expected := cleanUnusedTopicOptions{
				NameServer: "127.0.0.1:9876",
				BrokerAddr: "broker-a:10911",
			}
			if options != expected {
				t.Fatalf("unexpected cleanUnusedTopic broker options\nexpected=%#v\nactual=%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected cleanUnusedTopic broker namesrv %s", nameServer)
			}
			return true, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"cleanUnusedTopic",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
	}, client)
	if err != nil {
		t.Fatalf("run native cleanUnusedTopic broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected cleanUnusedTopic broker to be supported")
	}
	if output != "success" {
		t.Fatalf("cleanUnusedTopic broker output mismatch\nexpected:%q\nactual:%q", "success", output)
	}
}

func TestRunNativeCleanUnusedTopicClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		cleanUnusedTopic: func(ctx context.Context, nameServer string, options cleanUnusedTopicOptions) (bool, error) {
			expected := cleanUnusedTopicOptions{
				NameServer:  "127.0.0.1:9876",
				ClusterName: "DefaultCluster",
			}
			if options != expected {
				t.Fatalf("unexpected cleanUnusedTopic cluster options\nexpected=%#v\nactual=%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected cleanUnusedTopic cluster namesrv %s", nameServer)
			}
			return true, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"cleanUnusedTopic",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
	}, client)
	if err != nil {
		t.Fatalf("run native cleanUnusedTopic cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected cleanUnusedTopic cluster to be supported")
	}
	if output != "success" {
		t.Fatalf("cleanUnusedTopic cluster output mismatch\nexpected:%q\nactual:%q", "success", output)
	}
}

func TestRunNativeDeleteExpiredCommitLogBrokerFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteExpiredCommitLog: func(ctx context.Context, nameServer string, options deleteExpiredCommitLogOptions) (bool, error) {
			expected := deleteExpiredCommitLogOptions{
				NameServer: "127.0.0.1:9876",
				BrokerAddr: "broker-a:10911",
			}
			if options != expected {
				t.Fatalf("unexpected deleteExpiredCommitLog broker options\nexpected=%#v\nactual=%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected deleteExpiredCommitLog broker namesrv %s", nameServer)
			}
			return true, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteExpiredCommitLog",
		"-n", "127.0.0.1:9876",
		"-b", "broker-a:10911",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteExpiredCommitLog broker: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteExpiredCommitLog broker to be supported")
	}
	if output != "success" {
		t.Fatalf("deleteExpiredCommitLog broker output mismatch\nexpected:%q\nactual:%q", "success", output)
	}
}

func TestRunNativeDeleteExpiredCommitLogClusterFormatsOfficialOutput(t *testing.T) {
	client := nativeClientFunc{
		deleteExpiredCommitLog: func(ctx context.Context, nameServer string, options deleteExpiredCommitLogOptions) (bool, error) {
			expected := deleteExpiredCommitLogOptions{
				NameServer:  "127.0.0.1:9876",
				ClusterName: "DefaultCluster",
			}
			if options != expected {
				t.Fatalf("unexpected deleteExpiredCommitLog cluster options\nexpected=%#v\nactual=%#v", expected, options)
			}
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected deleteExpiredCommitLog cluster namesrv %s", nameServer)
			}
			return true, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"deleteExpiredCommitLog",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
	}, client)
	if err != nil {
		t.Fatalf("run native deleteExpiredCommitLog cluster: %v", err)
	}
	if !supported {
		t.Fatalf("expected deleteExpiredCommitLog cluster to be supported")
	}
	if output != "success" {
		t.Fatalf("deleteExpiredCommitLog cluster output mismatch\nexpected:%q\nactual:%q", "success", output)
	}
}

func TestFormatColdDataFlowCtrInfoFormatsRuntimeTimestamps(t *testing.T) {
	lastCold := int64(1700000000000)
	created := int64(1700000001000)
	sections := []coldDataFlowCtrInfoSection{{
		Header:     "============broker-a:10911============",
		BrokerAddr: "broker-a:10911",
		Body:       fmt.Sprintf(`{"globalAcc":0,"cgColdReadThreshold":3145728,"configTable":{},"globalColdReadThreshold":104857600,"runtimeTable":{"group-a":{"lastColdReadTimeMills":%d,"createTimeMills":%d,"counter":7}}}`, lastCold, created),
	}}

	output, err := formatColdDataFlowCtrInfo(sections)
	if err != nil {
		t.Fatalf("format cold ctr: %v", err)
	}
	expected := " ============broker-a:10911============\n" +
		"{\n" +
		"\t\"globalAcc\":0,\n" +
		"\t\"cgColdReadThreshold\":3145728,\n" +
		"\t\"configTable\":{},\n" +
		"\t\"globalColdReadThreshold\":104857600,\n" +
		"\t\"runtimeTable\":{\n" +
		"\t\t\"group-a\":{\n" +
		"\t\t\t\"createTimeFormat\":\"" + time.UnixMilli(created).In(time.Local).Format("2006-01-02 15:04:05") + "\",\n" +
		"\t\t\t\"lastColdReadTimeFormat\":\"" + time.UnixMilli(lastCold).In(time.Local).Format("2006-01-02 15:04:05") + "\",\n" +
		"\t\t\t\"counter\":7\n" +
		"\t\t}\n" +
		"\t}\n" +
		"}\n"
	if output != expected {
		t.Fatalf("unexpected cold ctr runtime output\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestDecodeProducerTableInfoBodyParsesOfficialData(t *testing.T) {
	body := []byte(`{"data":{"GroupA":[{"clientId":"client-a","remoteIP":"/127.0.0.1:10001","language":"JAVA","version":477,"lastUpdateTimestamp":1781000000001}],"GroupB":[]}}`)

	table, err := decodeProducerTableInfoBody(body)
	if err != nil {
		t.Fatalf("decode producer table info: %v", err)
	}
	output := formatProducerTableInfo(table)
	expected := "producer group (GroupA) instance : clientId=client-a,remoteIP=/127.0.0.1:10001, language=JAVA, version=477, lastUpdateTimestamp=1781000000001\n" +
		"producer group (GroupB) instances are empty\n"
	if output != expected {
		t.Fatalf("producer table output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeConsumerProgressGroupAcceptsClusterWithoutTopic(t *testing.T) {
	client := nativeClientFunc{
		consumerProgress: func(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "GroupA" || topic != "" || clusterName != "" {
				t.Fatalf("unexpected consumerProgress args namesrv=%s group=%s topic=%s cluster=%s", nameServer, consumerGroup, topic, clusterName)
			}
			return &consumerProgress{
				ConsumeTPS: 1.25,
				Entries: []consumerProgressEntry{
					{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0, BrokerOffset: 3, ConsumerOffset: 1, PullOffset: 2, LastTimestamp: 1780883297000},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{"consumerProgress", "-n", "127.0.0.1:9876", "-g", "GroupA", "-c", "DefaultCluster"}, client)
	if err != nil {
		t.Fatalf("run native consumerProgress -g -c: %v", err)
	}
	if !supported {
		t.Fatalf("expected consumerProgress -g -c to be supported")
	}
	expected := fmt.Sprintf("%-64s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s%s\n", "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff", "#Inflight", "#LastTime") +
		fmt.Sprintf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20d %-20d %s\n", "TopicTest", "broker-a", 0, int64(3), int64(1), int64(2), int64(1), time.UnixMilli(1780883297000).In(time.Local).Format("2006-01-02 15:04:05")) +
		"\n" +
		"Consume TPS: 1.25\n" +
		"Consume Diff Total: 2\n" +
		"Consume Inflight Total: 1\n"
	if output != expected {
		t.Fatalf("consumerProgress -g -c mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeConsumerConnectionRequiresGroup(t *testing.T) {
	output, supported, err := runNativeCommand(context.Background(), []string{"consumerConnection", "-n", "127.0.0.1:9876"}, nil)
	if err == nil || !strings.Contains(err.Error(), "ConsumerGroup 必填") {
		t.Fatalf("expected missing group error, got output=%q supported=%t err=%v", output, supported, err)
	}
	if !supported || output != "" {
		t.Fatalf("expected consumerConnection missing group to be handled by native command, supported=%t output=%q", supported, output)
	}
}

func TestRunNativeQueryMsgByKeyFormatsOfficialTable(t *testing.T) {
	client := nativeClientFunc{
		queryMessagesByKey: func(ctx context.Context, nameServer string, topic string, key string, clusterName string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageSearchResult, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" || key != "OrderKey" {
				t.Fatalf("unexpected topic/key %s/%s", topic, key)
			}
			if clusterName != "" {
				t.Fatalf("unexpected cluster %s", clusterName)
			}
			if beginTimestamp != 10 || endTimestamp != 20 || maxNum != 2 {
				t.Fatalf("unexpected query window begin=%d end=%d max=%d", beginTimestamp, endTimestamp, maxNum)
			}
			return []messageSearchResult{
				{MessageID: "AC10000100002A9F00000000000003E8", QueueID: 1, QueueOffset: 7},
				{MessageID: "AC10000100002A9F00000000000007D0", QueueID: 2, QueueOffset: 8},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByKey",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-k", "OrderKey",
		"-b", "10",
		"-e", "20",
		"-m", "2",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByKey: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByKey to be supported")
	}
	expected := fmt.Sprintf("%-50s %4s %40s\n", "#Message ID", "#QID", "#Offset") +
		fmt.Sprintf("%-50s %4d %40d\n", "AC10000100002A9F00000000000003E8", 1, int64(7)) +
		fmt.Sprintf("%-50s %4d %40d\n", "AC10000100002A9F00000000000007D0", 2, int64(8))
	if output != expected {
		t.Fatalf("queryMsgByKey output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeQueryMsgByKeyAcceptsOfficialClusterFlag(t *testing.T) {
	client := nativeClientFunc{
		queryMessagesByKey: func(ctx context.Context, nameServer string, topic string, key string, clusterName string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageSearchResult, error) {
			if clusterName != "DefaultCluster" {
				t.Fatalf("unexpected cluster %s", clusterName)
			}
			if maxNum != 1 {
				t.Fatalf("unexpected query maxNum %d", maxNum)
			}
			return []messageSearchResult{{MessageID: "AC10000100002A9F00000000000003E8", QueueID: 0, QueueOffset: 3}}, nil
		},
	}

	_, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByKey",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-k", "OrderKey",
		"-c", "DefaultCluster",
		"-m", "1",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByKey: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByKey to be supported")
	}
}

func TestRunNativeQueryMsgByOffsetFormatsOfficialDetail(t *testing.T) {
	bornTimestamp := int64(1780891116911)
	storeTimestamp := int64(1780891116954)
	message := &messageDetail{
		OffsetMessageID:  "AC10000100002A9F00000000000003E8",
		DisplayMessageID: "UNIQ-1",
		Topic:            "TopicTest",
		Keys:             "OrderKey",
		QueueID:          0,
		QueueOffset:      7,
		CommitLogOffset:  1000,
		ReconsumeTimes:   0,
		BornTimestamp:    bornTimestamp,
		StoreTimestamp:   storeTimestamp,
		BornHost:         "127.0.0.1:1000",
		StoreHost:        "172.16.0.1:10911",
		SysFlag:          0,
		Body:             []byte("hello"),
		Properties: []messageProperty{
			{Key: "KEYS", Value: "OrderKey"},
			{Key: "UNIQ_KEY", Value: "UNIQ-1"},
			{Key: "WAIT", Value: "true"},
			{Key: "TRACE_ON", Value: "true"},
			{Key: "MSG_REGION", Value: "DefaultRegion"},
			{Key: "CLUSTER", Value: "DefaultCluster"},
			{Key: "MIN_OFFSET", Value: "0"},
			{Key: "MAX_OFFSET", Value: "8"},
		},
	}
	client := nativeClientFunc{
		queryMessageByOffset: func(ctx context.Context, nameServer string, topic string, brokerName string, queueID int, offset int64) (*messageDetail, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" || brokerName != "broker-a" || queueID != 0 || offset != 7 {
				t.Fatalf("unexpected query args topic=%s broker=%s queue=%d offset=%d", topic, brokerName, queueID, offset)
			}
			return message, nil
		},
		messageTrackDetail: func(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error) {
			if detail != message {
				t.Fatalf("messageTrackDetail should receive the queried message detail")
			}
			return []messageTrack{
				{ConsumerGroup: "GroupA", TrackType: "PULL"},
				{ConsumerGroup: "GroupB", TrackType: "NOT_ONLINE", ExceptionDesc: "CODE:206 DESC:consumer not online"},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByOffset",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-b", "broker-a",
		"-i", "0",
		"-o", "7",
		"-f", "UTF-8",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByOffset: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByOffset to be supported")
	}
	expected :=
		fmt.Sprintf("%-20s %s\n", "OffsetID:", "AC10000100002A9F00000000000003E8") +
			fmt.Sprintf("%-20s %s\n", "Topic:", "TopicTest") +
			fmt.Sprintf("%-20s %s\n", "Tags:", "[null]") +
			fmt.Sprintf("%-20s %s\n", "Keys:", "[OrderKey]") +
			fmt.Sprintf("%-20s %d\n", "Queue ID:", 0) +
			fmt.Sprintf("%-20s %d\n", "Queue Offset:", int64(7)) +
			fmt.Sprintf("%-20s %d\n", "CommitLog Offset:", int64(1000)) +
			fmt.Sprintf("%-20s %d\n", "Reconsume Times:", 0) +
			fmt.Sprintf("%-20s %s\n", "Born Timestamp:", formatRocketMQMillis(bornTimestamp)) +
			fmt.Sprintf("%-20s %s\n", "Store Timestamp:", formatRocketMQMillis(storeTimestamp)) +
			fmt.Sprintf("%-20s %s\n", "Born Host:", "127.0.0.1:1000") +
			fmt.Sprintf("%-20s %s\n", "Store Host:", "172.16.0.1:10911") +
			fmt.Sprintf("%-20s %d\n", "System Flag:", 0) +
			fmt.Sprintf("%-20s %s\n", "Properties:", "{MSG_REGION=DefaultRegion, UNIQ_KEY=UNIQ-1, CLUSTER=DefaultCluster, MIN_OFFSET=0, KEYS=OrderKey, WAIT=true, TRACE_ON=true, MAX_OFFSET=8}") +
			fmt.Sprintf("%-20s %s\n", "Message Body Path:", "/tmp/rocketmq/msgbodys/UNIQ-1") +
			fmt.Sprintf("%-20s %s\n", "Message Body:", "hello") +
			"\n\n" +
			"MessageTrack [consumerGroup=GroupA, trackType=PULL, exceptionDesc=null]\n" +
			"MessageTrack [consumerGroup=GroupB, trackType=NOT_ONLINE, exceptionDesc=CODE:206 DESC:consumer not online]\n"
	if output != expected {
		t.Fatalf("queryMsgByOffset output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeQueryMsgByOffsetPrintsWarnWhenNoConsumer(t *testing.T) {
	message := &messageDetail{
		OffsetMessageID: "AC10000100002A9F00000000000003E8",
		Topic:           "TopicTest",
		QueueID:         0,
		QueueOffset:     7,
		CommitLogOffset: 1000,
		BornHost:        "127.0.0.1:1000",
		StoreHost:       "172.16.0.1:10911",
		Body:            []byte("hello"),
		Properties: []messageProperty{
			{Key: "UNIQ_KEY", Value: "UNIQ-1"},
		},
	}
	client := nativeClientFunc{
		queryMessageByOffset: func(ctx context.Context, nameServer string, topic string, brokerName string, queueID int, offset int64) (*messageDetail, error) {
			return message, nil
		},
		messageTrackDetail: func(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error) {
			if detail != message {
				t.Fatalf("messageTrackDetail should receive the queried message detail")
			}
			return nil, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByOffset",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-b", "broker-a",
		"-i", "0",
		"-o", "7",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByOffset: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByOffset to be supported")
	}
	if !strings.Contains(output, "WARN: No Consumer") {
		t.Fatalf("queryMsgByOffset should keep WARN output when MessageTrack rows are empty:\n%s", output)
	}
}

func TestRunNativeQueryMsgByIdFormatsOfficialDetail(t *testing.T) {
	client := nativeClientFunc{
		queryMessageByID: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" || clusterName != "DefaultCluster" || msgID != "AC10000100002A9F00000000000003E8" {
				t.Fatalf("unexpected query args topic=%s cluster=%s msgID=%s", topic, clusterName, msgID)
			}
			return &messageDetail{
				OffsetMessageID:  "AC10000100002A9F00000000000003E8",
				DisplayMessageID: "UNIQ-1",
				Topic:            "TopicTest",
				Keys:             "OrderKey",
				QueueID:          0,
				QueueOffset:      7,
				CommitLogOffset:  1000,
				BornHost:         "127.0.0.1:1000",
				StoreHost:        "172.16.0.1:10911",
				Body:             []byte("hello"),
				Properties: []messageProperty{
					{Key: "KEYS", Value: "OrderKey"},
					{Key: "UNIQ_KEY", Value: "UNIQ-1"},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgById",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-i", "AC10000100002A9F00000000000003E8",
		"-f", "UTF-8",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgById: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgById to be supported")
	}
	if !strings.Contains(output, "OffsetID:            AC10000100002A9F00000000000003E8") || !strings.Contains(output, "Message Body:        hello") {
		t.Fatalf("unexpected queryMsgById output:\n%s", output)
	}
}

func TestRunNativeQueryMsgByIdPrintsMessageTrackDetails(t *testing.T) {
	message := &messageDetail{
		OffsetMessageID: "AC10000100002A9F00000000000003E8",
		Topic:           "TopicTest",
		QueueID:         0,
		QueueOffset:     7,
		CommitLogOffset: 1000,
		BornHost:        "127.0.0.1:1000",
		StoreHost:       "172.16.0.1:10911",
		Body:            []byte("hello"),
		Properties: []messageProperty{
			{Key: "UNIQ_KEY", Value: "UNIQ-1"},
		},
	}
	client := nativeClientFunc{
		queryMessageByID: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
			return message, nil
		},
		messageTrackDetail: func(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error) {
			if detail != message {
				t.Fatalf("messageTrackDetail should receive the queried message detail")
			}
			return []messageTrack{
				{ConsumerGroup: "GroupA", TrackType: "PULL"},
				{ConsumerGroup: "GroupB", TrackType: "NOT_ONLINE", ExceptionDesc: "CODE:206 DESC:consumer not online"},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgById",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-i", "AC10000100002A9F00000000000003E8",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgById: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgById to be supported")
	}
	if strings.Contains(output, "WARN: No Consumer") {
		t.Fatalf("queryMsgById should not print WARN when MessageTrack rows exist:\n%s", output)
	}
	if !strings.Contains(output, "MessageTrack [consumerGroup=GroupA, trackType=PULL, exceptionDesc=null]") {
		t.Fatalf("expected PULL MessageTrack row, got:\n%s", output)
	}
	if !strings.Contains(output, "MessageTrack [consumerGroup=GroupB, trackType=NOT_ONLINE, exceptionDesc=CODE:206 DESC:consumer not online]") {
		t.Fatalf("expected NOT_ONLINE MessageTrack row, got:\n%s", output)
	}
}

func TestClientMessageTrackDetailReturnsPullTrack(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	routeBody := []byte(fmt.Sprintf(`{"queueDatas":[],"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":%q}}]}`, brokerListener.Addr().String()))
	nameServerDone := make(chan error, 1)
	go func() {
		for _, expectedTopic := range []string{"TopicTest", "%RETRY%GroupA"} {
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != expectedTopic {
				_ = conn.Close()
				nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			if _, err := conn.Write(remotingFrameForTest(t, response, routeBody)); err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			_ = conn.Close()
		}
		nameServerDone <- nil
	}()

	brokerDone := make(chan error, 1)
	go func() {
		for step := 0; step < 2; step++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			var body []byte
			switch step {
			case 0:
				if request.Code != requestCodeQueryTopicConsumeByWho || request.ExtFields["topic"] != "TopicTest" {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("unexpected group request code=%d fields=%#v", request.Code, request.ExtFields)
					return
				}
				body = []byte(`{"groupList":["GroupA"]}`)
			case 1:
				if request.Code != requestCodeGetConsumerConnectionList || request.ExtFields["consumerGroup"] != "GroupA" {
					_ = conn.Close()
					brokerDone <- fmt.Errorf("unexpected connection request code=%d fields=%#v", request.Code, request.ExtFields)
					return
				}
				body = []byte(`{"connectionSet":[{"clientId":"client-a","clientAddr":"127.0.0.1:1000","language":"JAVA","version":477}],"subscriptionTable":{},"consumeType":"CONSUME_ACTIVELY","messageModel":"CLUSTERING","consumeFromWhere":"CONSUME_FROM_LAST_OFFSET"}`)
			}
			if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			_ = conn.Close()
		}
		brokerDone <- nil
	}()

	tracks, err := NewClient(time.Second).MessageTrackDetail(context.Background(), nameServerListener.Addr().String(), &messageDetail{
		Topic:       "TopicTest",
		QueueID:     0,
		QueueOffset: 7,
		StoreHost:   brokerListener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("message track detail: %v", err)
	}
	expected := []messageTrack{{ConsumerGroup: "GroupA", TrackType: "PULL"}}
	if !reflect.DeepEqual(tracks, expected) {
		t.Fatalf("tracks mismatch\nexpected=%#v\nactual=%#v", expected, tracks)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientMessageTrackDetailFormatsRetryRouteErrorLikeOfficial(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	retryRemark := "No topic route info in name server for the topic: %RETRY%GroupA\nSee https://rocketmq.apache.org/docs/bestPractice/06FAQ for further details."
	routeBody := []byte(fmt.Sprintf(`{"queueDatas":[],"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"broker-a","brokerAddrs":{"0":%q}}]}`, brokerListener.Addr().String()))
	nameServerDone := make(chan error, 1)
	go func() {
		for step, expectedTopic := range []string{"TopicTest", "%RETRY%GroupA"} {
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != expectedTopic {
				_ = conn.Close()
				nameServerDone <- fmt.Errorf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)
				return
			}
			response := remotingCommand{Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			var body []byte
			if step == 0 {
				response.Code = responseCodeSuccess
				body = routeBody
			} else {
				response.Code = responseCodeTopicNotExist
				response.Remark = retryRemark
			}
			if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
				_ = conn.Close()
				nameServerDone <- err
				return
			}
			_ = conn.Close()
		}
		nameServerDone <- nil
	}()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeQueryTopicConsumeByWho || request.ExtFields["topic"] != "TopicTest" {
			brokerDone <- fmt.Errorf("unexpected group request code=%d fields=%#v", request.Code, request.ExtFields)
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		if _, err := conn.Write(remotingFrameForTest(t, response, []byte(`{"groupList":["GroupA"]}`))); err != nil {
			brokerDone <- err
			return
		}
		brokerDone <- nil
	}()

	tracks, err := NewClient(time.Second).MessageTrackDetail(context.Background(), nameServerListener.Addr().String(), &messageDetail{
		Topic:       "TopicTest",
		QueueID:     0,
		QueueOffset: 7,
		StoreHost:   brokerListener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("message track detail: %v", err)
	}
	expected := []messageTrack{{
		ConsumerGroup: "GroupA",
		TrackType:     "UNKNOWN",
		ExceptionDesc: "org.apache.rocketmq.client.exception.MQClientException: CODE: 17  DESC: " + retryRemark + ", org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicRouteInfoFromNameServer(MQClientAPIImpl.java:2110)",
	}}
	if !reflect.DeepEqual(tracks, expected) {
		t.Fatalf("tracks mismatch\nexpected=%#v\nactual=%#v", expected, tracks)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestRunNativeQueryMsgByIdDecodesGBKBodyFormat(t *testing.T) {
	client := nativeClientFunc{
		queryMessageByID: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
			return &messageDetail{
				OffsetMessageID:  "AC10000100002A9F00000000000003E8",
				DisplayMessageID: "UNIQ-GBK",
				Topic:            "TopicTest",
				QueueID:          0,
				QueueOffset:      7,
				CommitLogOffset:  1000,
				BornHost:         "127.0.0.1:1000",
				StoreHost:        "172.16.0.1:10911",
				// Java Charset.forName("GBK") 会把这三个字节解码成“中文”。
				Body: []byte{0xD6, 0xD0, 0xCE, 0xC4},
				Properties: []messageProperty{
					{Key: "UNIQ_KEY", Value: "UNIQ-GBK"},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgById",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-i", "AC10000100002A9F00000000000003E8",
		"-f", "GBK",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgById GBK bodyFormat: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgById to be supported")
	}
	if !strings.Contains(output, "Message Body:        中文") {
		t.Fatalf("expected GBK body to be decoded as Chinese text, got:\n%s", output)
	}
}

func TestRunNativeQueryMsgByOffsetReplacesInvalidUTF8Bytes(t *testing.T) {
	client := nativeClientFunc{
		queryMessageByOffset: func(ctx context.Context, nameServer string, topic string, brokerName string, queueID int, offset int64) (*messageDetail, error) {
			return &messageDetail{
				OffsetMessageID:  "AC10000100002A9F00000000000003E8",
				DisplayMessageID: "UNIQ-BADUTF8",
				Topic:            "TopicTest",
				QueueID:          3,
				QueueOffset:      442017,
				CommitLogOffset:  1000,
				BornHost:         "127.0.0.1:1000",
				StoreHost:        "172.16.0.1:10911",
				// 官方 Java UTF-8 解码会把这四个非法字节逐个替换成 U+FFFD。
				Body: []byte{0xD6, 0xD0, 0xCE, 0xC4},
				Properties: []messageProperty{
					{Key: "UNIQ_KEY", Value: "UNIQ-BADUTF8"},
				},
			}, nil
		},
		messageTrackDetail: func(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error) {
			return []messageTrack{{ConsumerGroup: "GroupA", TrackType: "PULL"}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByOffset",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-b", "broker-a",
		"-i", "3",
		"-o", "442017",
		"-f", "UTF-8",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByOffset invalid UTF-8 bodyFormat: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByOffset to be supported")
	}
	if !strings.Contains(output, "Message Body:        ����") {
		t.Fatalf("expected invalid UTF-8 bytes to be replaced like official Java decoder, got:\n%s", output)
	}
}

func TestRunNativeQueryMsgByUniqueKeyFormatsOfficialDetailWithoutOffsetID(t *testing.T) {
	bornTimestamp := int64(1780891116911)
	storeTimestamp := int64(1780891116954)
	client := nativeClientFunc{
		queryMessageByID: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
			t.Fatalf("queryMsgByUniqueKey must not use queryMsgById offset-aware path")
			return nil, nil
		},
		queryMessageByUniqueKey: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if topic != "TopicTest" || clusterName != "DefaultCluster" || msgID != "UNIQ-1" {
				t.Fatalf("unexpected query args topic=%s cluster=%s msgID=%s", topic, clusterName, msgID)
			}
			return &messageDetail{
				OffsetMessageID:  "AC10000100002A9F00000000000003E8",
				DisplayMessageID: "UNIQ-1",
				Topic:            "TopicTest",
				Keys:             "OrderKey",
				QueueID:          0,
				QueueOffset:      7,
				CommitLogOffset:  1000,
				ReconsumeTimes:   0,
				BornTimestamp:    bornTimestamp,
				StoreTimestamp:   storeTimestamp,
				BornHost:         "127.0.0.1:1000",
				StoreHost:        "172.16.0.1:10911",
				SysFlag:          0,
				Body:             []byte("hello"),
				Properties: []messageProperty{
					{Key: "KEYS", Value: "OrderKey"},
					{Key: "UNIQ_KEY", Value: "UNIQ-1"},
					{Key: "WAIT", Value: "true"},
					{Key: "TRACE_ON", Value: "true"},
					{Key: "MSG_REGION", Value: "DefaultRegion"},
					{Key: "CLUSTER", Value: "DefaultCluster"},
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByUniqueKey",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-i", "UNIQ-1",
		"-d", "false",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByUniqueKey: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByUniqueKey to be supported")
	}
	expected :=
		fmt.Sprintf("%-20s %s\n", "Topic:", "TopicTest") +
			fmt.Sprintf("%-20s %s\n", "Tags:", "[null]") +
			fmt.Sprintf("%-20s %s\n", "Keys:", "[OrderKey]") +
			fmt.Sprintf("%-20s %d\n", "Queue ID:", 0) +
			fmt.Sprintf("%-20s %d\n", "Queue Offset:", int64(7)) +
			fmt.Sprintf("%-20s %d\n", "CommitLog Offset:", int64(1000)) +
			fmt.Sprintf("%-20s %d\n", "Reconsume Times:", 0) +
			fmt.Sprintf("%-20s %s\n", "Born Timestamp:", formatRocketMQMillis(bornTimestamp)) +
			fmt.Sprintf("%-20s %s\n", "Store Timestamp:", formatRocketMQMillis(storeTimestamp)) +
			fmt.Sprintf("%-20s %s\n", "Born Host:", "127.0.0.1:1000") +
			fmt.Sprintf("%-20s %s\n", "Store Host:", "172.16.0.1:10911") +
			fmt.Sprintf("%-20s %d\n", "System Flag:", 0) +
			fmt.Sprintf("%-20s %s\n", "Properties:", "{MSG_REGION=DefaultRegion, UNIQ_KEY=UNIQ-1, CLUSTER=DefaultCluster, KEYS=OrderKey, WAIT=true, TRACE_ON=true}") +
			fmt.Sprintf("%-20s %s\n", "Message Body Path:", "/tmp/rocketmq/msgbodys/UNIQ-1") +
			"\n\nWARN: No Consumer"
	if output != expected {
		t.Fatalf("queryMsgByUniqueKey output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
	if strings.Contains(output, "OffsetID:") || strings.Contains(output, "Message Body:") {
		t.Fatalf("queryMsgByUniqueKey should not print OffsetID or decoded body:\n%s", output)
	}
}

func TestRunNativeQueryMsgByUniqueKeyShowAllUsesNativeList(t *testing.T) {
	client := nativeClientFunc{
		queryMessagesByUniqueKey: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) ([]messageDetail, error) {
			if nameServer != "127.0.0.1:9876" || topic != "TopicTest" || clusterName != "DefaultCluster" || msgID != "UNIQ-1" {
				t.Fatalf("unexpected query args namesrv=%s topic=%s cluster=%s msgID=%s", nameServer, topic, clusterName, msgID)
			}
			return []messageDetail{{
				OffsetMessageID:  "AC10000100002A9F00000000000003E8",
				DisplayMessageID: "UNIQ-1",
				Topic:            "TopicTest",
				Keys:             "OrderKey",
				QueueID:          0,
				QueueOffset:      7,
				CommitLogOffset:  1000,
				BornHost:         "127.0.0.1:1000",
				StoreHost:        "172.16.0.1:10911",
				Body:             []byte("hello"),
				Properties: []messageProperty{
					{Key: "KEYS", Value: "OrderKey"},
					{Key: "UNIQ_KEY", Value: "UNIQ-1"},
				},
			}}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByUniqueKey",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-i", "UNIQ-1",
		"-a",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByUniqueKey -a: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByUniqueKey -a to be supported")
	}
	if strings.Contains(output, "OffsetID:") || !strings.Contains(output, "Topic:               TopicTest") {
		t.Fatalf("unexpected queryMsgByUniqueKey -a output:\n%s", output)
	}
}

func TestRunNativeQueryMsgByUniqueKeyDirectlyConsumesOnClient(t *testing.T) {
	client := nativeClientFunc{
		consumeMessageDirectly: func(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error) {
			if nameServer != "127.0.0.1:9876" || consumerGroup != "TOOLS_CONSUMER" || clientID != "client-a" || topic != "TopicTest" || clusterName != "DefaultCluster" || msgID != "UNIQ-1" {
				t.Fatalf("unexpected direct consume args namesrv=%s group=%s client=%s topic=%s cluster=%s msgID=%s", nameServer, consumerGroup, clientID, topic, clusterName, msgID)
			}
			return &consumeMessageDirectlyResult{
				Order:          false,
				AutoCommit:     true,
				ConsumeResult:  "CR_SUCCESS",
				Remark:         nil,
				SpentTimeMills: 1,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgByUniqueKey",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-i", "UNIQ-1",
		"-g", "TOOLS_CONSUMER",
		"-d", "client-a",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgByUniqueKey direct consume: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgByUniqueKey -g -d to be supported")
	}
	expected := "ConsumeMessageDirectlyResult [order=false, autoCommit=true, consumeResult=CR_SUCCESS, remark=null, spentTimeMills=1]"
	if output != expected {
		t.Fatalf("direct consume output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeQueryMsgByIdDirectlyConsumesOnClient(t *testing.T) {
	called := false
	client := nativeClientFunc{
		consumeMessageDirectlyByID: func(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error) {
			called = true
			if nameServer != "127.0.0.1:9876" || consumerGroup != "TOOLS_CONSUMER" || clientID != "client-a" || topic != "TopicTest" || clusterName != "DefaultCluster" || msgID != "AC10000100002A9F00000000000003E8" {
				t.Fatalf("unexpected direct consume by id args namesrv=%s group=%s client=%s topic=%s cluster=%s msgID=%s", nameServer, consumerGroup, clientID, topic, clusterName, msgID)
			}
			return &consumeMessageDirectlyResult{
				Order:          false,
				AutoCommit:     true,
				ConsumeResult:  "CR_SUCCESS",
				Remark:         nil,
				SpentTimeMills: 1,
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgById",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-i", "AC10000100002A9F00000000000003E8",
		"-g", "TOOLS_CONSUMER",
		"-d", "client-a",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgById direct consume: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgById -g -d to be supported")
	}
	if !called {
		t.Fatalf("expected queryMsgById -g -d to call ConsumeMessageDirectlyByID")
	}
	expected := "ConsumeMessageDirectlyResult [order=false, autoCommit=true, consumeResult=CR_SUCCESS, remark=null, spentTimeMills=1]"
	if output != expected {
		t.Fatalf("direct consume by id output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestRunNativeQueryMsgByIdResendsMessage(t *testing.T) {
	called := false
	client := nativeClientFunc{
		resendMessageByID: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string, unitName string) (*queryMsgByIDResendResult, error) {
			called = true
			if nameServer != "127.0.0.1:9876" || topic != "TopicTest" || clusterName != "DefaultCluster" || msgID != "AC10000100002A9F00000000000003E8" || unitName != "UnitA" {
				t.Fatalf("unexpected resend args namesrv=%s topic=%s cluster=%s msgID=%s unitName=%s", nameServer, topic, clusterName, msgID, unitName)
			}
			return &queryMsgByIDResendResult{
				OriginalMsgID: msgID,
				SendResult: &sendMessageResult{
					Topic:           "TopicTest",
					BrokerName:      "broker-a",
					QueueID:         3,
					SendStatus:      "SEND_OK",
					MessageID:       "AC10000100002A9F00000000000003E8",
					OffsetMessageID: "AC10000100002A9F00000000000007D0",
					QueueOffset:     17,
				},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgById",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-c", "DefaultCluster",
		"-i", "AC10000100002A9F00000000000003E8",
		"-s", "true",
		"-u", "UnitA",
	}, client)
	if err != nil {
		t.Fatalf("queryMsgById resend: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgById -s true to be supported")
	}
	if !called {
		t.Fatalf("expected queryMsgById -s true to call ResendMessageByID")
	}
	expected := "prepare resend msg. originalMsgId=AC10000100002A9F00000000000003E8" +
		"SendResult [sendStatus=SEND_OK, msgId=AC10000100002A9F00000000000003E8, offsetMsgId=AC10000100002A9F00000000000007D0, messageQueue=MessageQueue [topic=TopicTest, brokerName=broker-a, queueId=3], queueOffset=17, recallHandle=null]"
	if output != expected {
		t.Fatalf("queryMsgById resend output mismatch\nexpected=%q\nactual=%q", expected, output)
	}
}

func TestRunNativeQueryMsgByIdResendPrintsNoMessage(t *testing.T) {
	client := nativeClientFunc{
		resendMessageByID: func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string, unitName string) (*queryMsgByIDResendResult, error) {
			return &queryMsgByIDResendResult{OriginalMsgID: msgID}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgById",
		"-n", "127.0.0.1:9876",
		"-t", "TopicTest",
		"-i", "MISSING-ID",
		"-s", "true",
	}, client)
	if err != nil {
		t.Fatalf("queryMsgById resend no message: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgById -s true to be supported")
	}
	if output != "no message. msgId=MISSING-ID" {
		t.Fatalf("unexpected no message output %q", output)
	}
}

func TestRunNativeQueryMsgTraceByIdFormatsOfficialTrace(t *testing.T) {
	pubTimestamp := int64(1780883297000)
	subTimestamp := int64(1780883298000)
	client := nativeClientFunc{
		queryMessageTraceByID: func(ctx context.Context, nameServer string, traceTopic string, msgID string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageTraceView, error) {
			if nameServer != "127.0.0.1:9876" {
				t.Fatalf("unexpected nameserver %s", nameServer)
			}
			if traceTopic != "RMQ_SYS_TRACE_TOPIC" || msgID != "MSG-1" || beginTimestamp != 10 || endTimestamp != 20 || maxNum != 8 {
				t.Fatalf("unexpected trace args topic=%s msgID=%s begin=%d end=%d max=%d", traceTopic, msgID, beginTimestamp, endTimestamp, maxNum)
			}
			return []messageTraceView{
				{MsgType: "Pub", GroupName: "PG-A", ClientHost: "127.0.0.1", TimeStamp: pubTimestamp, CostTime: 7, Status: "success"},
				{MsgType: "SubAfter", GroupName: "CG-A", ClientHost: "127.0.0.1", TimeStamp: subTimestamp, CostTime: 3, Status: "failed"},
			}, nil
		},
	}

	output, supported, err := runNativeCommand(context.Background(), []string{
		"queryMsgTraceById",
		"-n", "127.0.0.1:9876",
		"-i", "MSG-1",
		"-b", "10",
		"-e", "20",
		"-c", "8",
	}, client)
	if err != nil {
		t.Fatalf("run native queryMsgTraceById: %v", err)
	}
	if !supported {
		t.Fatalf("expected queryMsgTraceById to be supported")
	}
	expected := fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "#Type", "#ProducerGroup", "#ClientHost", "#SendTime", "#CostTimes", "#Status") +
		fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "Pub", "PG-A", "127.0.0.1", time.UnixMilli(pubTimestamp).In(time.Local).Format("2006-01-02 15:04:05"), "7ms", "success") +
		"\n" +
		fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "#Type", "#ConsumerGroup", "#ClientHost", "#ConsumerTime", "#CostTimes", "#Status") +
		fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "Sub", "CG-A", "127.0.0.1", time.UnixMilli(subTimestamp).In(time.Local).Format("2006-01-02 15:04:05"), "3ms", "failed") +
		"\n"
	if output != expected {
		t.Fatalf("queryMsgTraceById output mismatch\nexpected:\n%s\nactual:\n%s", expected, output)
	}
}

func TestDecodeQueryMessageBodyParsesRocketMQRecord(t *testing.T) {
	body := queryMessageRecordForTest(t, queryMessageRecordFixture{
		Topic:           "TopicTest",
		Keys:            "OtherKey OrderKey",
		UniqKey:         "UNIQ-1",
		QueueID:         3,
		QueueOffset:     9,
		CommitLogOffset: 1000,
		StoreHostIP:     []byte{172, 16, 0, 1},
		StoreHostPort:   10911,
	})

	results, err := decodeQueryMessageBody(body)
	if err != nil {
		t.Fatalf("decode query message body: %v", err)
	}
	expected := []messageSearchResult{{
		MessageID:   "UNIQ-1",
		QueueID:     3,
		QueueOffset: 9,
		Topic:       "TopicTest",
		Keys:        []string{"OtherKey", "OrderKey"},
		UniqKey:     "UNIQ-1",
	}}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("message search results mismatch\nexpected=%#v\nactual=%#v", expected, results)
	}
}

func TestDecodeTraceViewsFromMessagesParsesPubAndSubAfter(t *testing.T) {
	pubTimestamp := int64(1780883297000)
	subTimestamp := int64(1780883298000)
	traceBody := strings.Join([]string{
		"Pub", strconv.FormatInt(pubTimestamp, 10), "DefaultRegion", "PG-A", "TopicTest", "MSG-1", "TagA", "KeyA", "172.16.0.1:10911", "5", "7", "0", "OFFSET-1", "true",
	}, string(rune(1))) + string(rune(2)) +
		strings.Join([]string{
			"SubAfter", "REQ-1", "MSG-1", "3", "false", "KeyA", "0", strconv.FormatInt(subTimestamp, 10), "CG-A",
		}, string(rune(1))) + string(rune(2))

	views, err := decodeTraceViewsFromMessages("MSG-1", []messageDetail{{
		Body:     []byte(traceBody),
		BornHost: "127.0.0.1:1000",
	}})
	if err != nil {
		t.Fatalf("decode trace views: %v", err)
	}
	expected := []messageTraceView{
		{MsgID: "MSG-1", Keys: "KeyA", Tags: "TagA", Topic: "TopicTest", MsgType: "Pub", GroupName: "PG-A", ClientHost: "127.0.0.1", StoreHost: "172.16.0.1:10911", OffsetMessageID: "OFFSET-1", TimeStamp: pubTimestamp, CostTime: 7, Status: "success"},
		{MsgID: "MSG-1", Keys: "KeyA", MsgType: "SubAfter", GroupName: "CG-A", ClientHost: "127.0.0.1", TimeStamp: subTimestamp, CostTime: 3, Status: "failed"},
	}
	if !reflect.DeepEqual(views, expected) {
		t.Fatalf("trace views mismatch\nexpected=%#v\nactual=%#v", expected, views)
	}
}

func TestDecodeTopicStatsBodyParsesFastJSONMapKeys(t *testing.T) {
	body := []byte(`{"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"TopicTest"}:{"lastUpdateTimestamp":0,"maxOffset":0,"minOffset":0},{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"lastUpdateTimestamp":1780883297044,"maxOffset":3,"minOffset":0}}}`)
	entries, err := decodeTopicStatsBody(body)
	if err != nil {
		t.Fatalf("decode topic stats body: %v", err)
	}
	expected := []topicStatusEntry{
		{BrokerName: "broker-a", QueueID: 1, MinOffset: 0, MaxOffset: 0},
		{BrokerName: "broker-a", QueueID: 0, MinOffset: 0, MaxOffset: 3, LastUpdateTimestamp: 1780883297044},
	}
	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("entries mismatch\nexpected=%#v\nactual=%#v", expected, entries)
	}
}

func TestDecodeTopicClustersUsesFirstRouteBroker(t *testing.T) {
	clusterBody := []byte(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"],"OtherCluster":["broker-b"]}}`)
	routeBody := []byte(`{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`)
	clusters, err := decodeTopicClusters(clusterBody, routeBody)
	if err != nil {
		t.Fatalf("decode topic clusters: %v", err)
	}
	expected := []string{"DefaultCluster"}
	if !reflect.DeepEqual(clusters, expected) {
		t.Fatalf("clusters mismatch\nexpected=%#v\nactual=%#v", expected, clusters)
	}
}

func TestDecodeConsumeStatsBodyParsesFastJSONMapKeys(t *testing.T) {
	body := []byte(`{"consumeTps":1.25,"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"TopicTest"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0,"pullOffset":0},{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"brokerOffset":3,"consumerOffset":1,"lastTimestamp":1780883297000,"pullOffset":2}}}`)
	progress, err := decodeConsumeStatsBody(body)
	if err != nil {
		t.Fatalf("decode consume stats body: %v", err)
	}
	expected := &consumerProgress{
		ConsumeTPS: 1.25,
		Entries: []consumerProgressEntry{
			{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1, BrokerOffset: 0, ConsumerOffset: 0, PullOffset: 0},
			{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0, BrokerOffset: 3, ConsumerOffset: 1, PullOffset: 2, LastTimestamp: 1780883297000},
		},
	}
	if !reflect.DeepEqual(progress, expected) {
		t.Fatalf("consume progress mismatch\nexpected=%#v\nactual=%#v", expected, progress)
	}
}

func TestDecodeBrokerConsumeStatsBodyParsesOfficialWrapper(t *testing.T) {
	body := []byte(`{"consumeStatsList":[{"GroupA":[{"consumeTps":1.25,"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"TopicTest"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0,"pullOffset":0},{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"brokerOffset":3,"consumerOffset":1,"lastTimestamp":1780883297000,"pullOffset":2}}}]}],"brokerAddr":"broker-a:10911","totalDiff":2,"totalInflightDiff":1}`)
	stats, err := decodeBrokerConsumeStatsBody(body)
	if err != nil {
		t.Fatalf("decode broker consume stats body: %v", err)
	}
	expected := &brokerConsumeStats{
		BrokerAddr:        "broker-a:10911",
		TotalDiff:         2,
		TotalInflightDiff: 1,
		Groups: []brokerConsumeStatsGroup{
			{
				Group: "GroupA",
				Stats: []consumerProgress{
					{
						ConsumeTPS: 1.25,
						Entries: []consumerProgressEntry{
							{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 1, BrokerOffset: 0, ConsumerOffset: 0, PullOffset: 0},
							{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0, BrokerOffset: 3, ConsumerOffset: 1, PullOffset: 2, LastTimestamp: 1780883297000},
						},
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(stats, expected) {
		t.Fatalf("broker consume stats mismatch\nexpected=%#v\nactual=%#v", expected, stats)
	}
}

func TestDecodeBrokerStatsDataBodyUsesOfficial24HourFallback(t *testing.T) {
	stats, err := decodeBrokerStatsDataBody([]byte(`{"statsMinute":{"sum":1,"tps":1.25,"avgpt":0},"statsHour":{"sum":12,"tps":0,"avgpt":0},"statsDay":{"sum":24,"tps":0,"avgpt":0}}`))
	if err != nil {
		t.Fatalf("decode broker stats day body: %v", err)
	}
	if stats.StatsMinute.Sum != 1 || stats.StatsMinute.TPS != 1.25 || brokerStats24HourSum(stats) != 24 {
		t.Fatalf("unexpected day stats %#v sum=%d", stats, brokerStats24HourSum(stats))
	}

	stats, err = decodeBrokerStatsDataBody([]byte(`{"statsMinute":{"sum":3,"tps":2.50,"avgpt":0},"statsHour":{"sum":30,"tps":0,"avgpt":0},"statsDay":{"sum":0,"tps":0,"avgpt":0}}`))
	if err != nil {
		t.Fatalf("decode broker stats hour body: %v", err)
	}
	if brokerStats24HourSum(stats) != 30 {
		t.Fatalf("expected hour sum fallback, got %d", brokerStats24HourSum(stats))
	}

	stats, err = decodeBrokerStatsDataBody([]byte(`{"statsMinute":{"sum":5,"tps":3.75,"avgpt":0},"statsHour":{"sum":0,"tps":0,"avgpt":0},"statsDay":{"sum":0,"tps":0,"avgpt":0}}`))
	if err != nil {
		t.Fatalf("decode broker stats minute body: %v", err)
	}
	if brokerStats24HourSum(stats) != 5 {
		t.Fatalf("expected minute sum fallback, got %d", brokerStats24HourSum(stats))
	}
}

func TestDecodeConsumerConnectionBodyUsesOfficialSummaryFields(t *testing.T) {
	body := []byte(`{"connectionSet":[{"clientId":"client-a","clientAddr":"127.0.0.1:10001","language":"JAVA","version":477},{"clientId":"client-b","clientAddr":"127.0.0.1:10002","language":"JAVA","version":479}],"consumeType":"CONSUME_PASSIVELY","messageModel":"CLUSTERING","consumeFromWhere":"CONSUME_FROM_LAST_OFFSET"}`)
	summary, err := decodeConsumerConnectionBody(body)
	if err != nil {
		t.Fatalf("decode consumer connection body: %v", err)
	}
	expected := &consumerConnectionSummary{
		Count:        2,
		Version:      "V5_3_2",
		ConsumeType:  "PUSH",
		MessageModel: "CLUSTERING",
		ClientIDs:    []string{"client-a", "client-b"},
	}
	if !reflect.DeepEqual(summary, expected) {
		t.Fatalf("consumer connection summary mismatch\nexpected=%#v\nactual=%#v", expected, summary)
	}
}

func TestDecodeProducerConnectionBodyUsesOfficialConnectionSet(t *testing.T) {
	body := []byte(`{"connectionSet":[{"clientId":"client-a","clientAddr":"127.0.0.1:10001","language":"JAVA","version":477}]}`)
	detail, err := decodeProducerConnectionBody(body)
	if err != nil {
		t.Fatalf("decode producer connection body: %v", err)
	}
	expected := &producerConnectionDetail{
		Connections: []producerConnectionEntry{{
			ClientID:   "client-a",
			ClientAddr: "127.0.0.1:10001",
			Language:   "JAVA",
			Version:    477,
		}},
	}
	if !reflect.DeepEqual(detail, expected) {
		t.Fatalf("producer connection mismatch\nexpected=%#v\nactual=%#v", expected, detail)
	}
}

func TestDecodeQueryConsumeQueueBodyParsesOfficialResponse(t *testing.T) {
	body := []byte(`{"filterData":"GroupA@TopicTest is not online!","queueData":[{"physicOffset":731,"physicSize":225,"tagsCode":0,"extendDataJson":null,"bitMap":null,"eval":false,"msg":null},{"physicOffset":1631,"physicSize":225,"tagsCode":12,"extendDataJson":"{\"k\":\"v\"}","bitMap":"1010","eval":true,"msg":"hello"}],"maxQueueIndex":9,"minQueueIndex":0}`)
	result, err := decodeQueryConsumeQueueBody(body)
	if err != nil {
		t.Fatalf("decode queryCq body: %v", err)
	}
	expected := &queryConsumeQueueResult{
		FilterData:    "GroupA@TopicTest is not online!",
		MaxQueueIndex: 9,
		MinQueueIndex: 0,
		QueueData: []consumeQueueData{
			{PhysicOffset: 731, PhysicSize: 225, TagsCode: 0, ExtendDataJSON: "null", BitMap: "null", Eval: false, Msg: "null"},
			{PhysicOffset: 1631, PhysicSize: 225, TagsCode: 12, ExtendDataJSON: "{\"k\":\"v\"}", BitMap: "1010", Eval: true, Msg: "hello"},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("queryCq body mismatch\nexpected=%#v\nactual=%#v", expected, result)
	}
}

func TestDecodeHAStatusBodyParsesOfficialResponse(t *testing.T) {
	body := []byte(`{"master":true,"masterCommitLogMaxOffset":1000,"inSyncSlaveNums":1,"haConnectionInfo":[{"addr":"slave-a:10912","slaveAckOffset":900,"diff":100,"inSync":false,"transferredByteInSecond":2048,"transferFromWhere":800}],"haClientRuntimeInfo":{"masterAddr":"master-a:10911","transferredByteInSecond":0,"maxOffset":0,"lastReadTimestamp":0,"lastWriteTimestamp":0,"masterFlushOffset":0}}`)
	result, err := decodeHAStatusBody(body)
	if err != nil {
		t.Fatalf("decode haStatus body: %v", err)
	}
	expected := &haStatusResult{
		Master:                   true,
		MasterCommitLogMaxOffset: 1000,
		InSyncSlaveNums:          1,
		HAConnectionInfo: []haConnectionRuntimeInfo{{
			Addr:                    "slave-a:10912",
			SlaveAckOffset:          900,
			Diff:                    100,
			InSync:                  false,
			TransferredByteInSecond: 2048,
			TransferFromWhere:       800,
		}},
		HAClientRuntimeInfo: haClientRuntimeInfo{MasterAddr: "master-a:10911"},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("haStatus body mismatch\nexpected=%#v\nactual=%#v", expected, result)
	}
}

func TestDecodeConsumerRunningInfoBodyParsesMqTableKeys(t *testing.T) {
	body := []byte(`{"properties":{"CONSUME_TYPE":"CONSUME_PASSIVELY"},"mqTable":{{"brokerName":"broker-a","queueId":1,"topic":"%RETRY%GroupA"}:{"commitOffset":0},{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"commitOffset":3}}}`)
	queues, err := decodeConsumerRunningInfoBody(body)
	if err != nil {
		t.Fatalf("decode consumer running info body: %v", err)
	}
	expected := []messageQueueIdentity{
		{Topic: "%RETRY%GroupA", BrokerName: "broker-a", QueueID: 1},
		{Topic: "TopicTest", BrokerName: "broker-a", QueueID: 0},
	}
	if !reflect.DeepEqual(queues, expected) {
		t.Fatalf("running info queues mismatch\nexpected=%#v\nactual=%#v", expected, queues)
	}
}

func TestRocketMQVersionDescMatchesOfficialOrdinals(t *testing.T) {
	cases := map[int]string{
		407: "V4_9_7",
		453: "V5_2_0",
		477: "V5_3_2",
		999: "V5_9_9",
	}
	for value, expected := range cases {
		if actual := rocketMQVersionDesc(value); actual != expected {
			t.Fatalf("version desc %d mismatch: expected %s actual %s", value, expected, actual)
		}
	}
}

func TestClientQueryMessageByIDUsesViewMessageRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	host, portText, err := net.SplitHostPort(brokerListener.Addr().String())
	if err != nil {
		t.Fatalf("split broker addr: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse broker port: %v", err)
	}
	msgID := createMessageID(net.ParseIP(host).To4(), int32(port), 1000)

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeViewMessageByID {
			brokerDone <- &testError{message: "unexpected view message request code"}
			return
		}
		if request.ExtFields["offset"] != "1000" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected offset %q", request.ExtFields["offset"])}
			return
		}
		if len(request.Body) != 0 {
			brokerDone <- &testError{message: "view message request should not have body"}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := queryMessageRecordForTest(t, queryMessageRecordFixture{
			Topic:           "TopicTest",
			Keys:            "OrderKey",
			UniqKey:         "UNIQ-1",
			QueueID:         0,
			QueueOffset:     7,
			CommitLogOffset: 1000,
			Body:            []byte("hello"),
			StoreHostIP:     net.ParseIP(host).To4(),
			StoreHostPort:   int32(port),
		})
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	detail, err := NewClient(time.Second).QueryMessageByID(context.Background(), "", "TopicTest", "", msgID)
	if err != nil {
		t.Fatalf("query message by id: %v", err)
	}
	if detail.OffsetMessageID != msgID || detail.DisplayMessageID != "UNIQ-1" || string(detail.Body) != "hello" {
		t.Fatalf("unexpected detail %#v", detail)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumeMessageDirectlyByIDQueriesMessageBeforePush(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	host, portText, err := net.SplitHostPort(brokerListener.Addr().String())
	if err != nil {
		t.Fatalf("split broker addr: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse broker port: %v", err)
	}
	msgID := createMessageID(net.ParseIP(host).To4(), int32(port), 1000)

	brokerDone := make(chan error, 1)
	go func() {
		for step := 0; step < 3; step++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				brokerDone <- err
				return
			}
			switch step {
			case 0:
				if request.Code != requestCodeGetConsumerRunningInfo || request.ExtFields["consumerGroup"] != "TOOLS_CONSUMER" || request.ExtFields["clientId"] != "client-a" || request.ExtFields["jstackEnable"] != "false" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected running info request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				body := []byte(`{"properties":{"PROP_CONSUME_TYPE":"CONSUME_PASSIVELY","consumerGroup":"TOOLS_CONSUMER"},"subscriptionSet":[],"mqTable":{},"mqPopTable":{},"statusTable":{},"userConsumerInfo":{}}`)
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, body))
			case 1:
				if request.Code != requestCodeViewMessageByID || request.ExtFields["offset"] != "1000" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected query by id request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				body := queryMessageRecordForTest(t, queryMessageRecordFixture{
					Topic:           "TopicTest",
					Keys:            "OrderKey",
					UniqKey:         "UNIQ-1",
					QueueID:         0,
					QueueOffset:     7,
					CommitLogOffset: 1000,
					Body:            []byte("hello"),
					StoreHostIP:     net.ParseIP(host).To4(),
					StoreHostPort:   int32(port),
				})
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, body))
			case 2:
				if request.Code != requestCodeConsumeMessageDirectly {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected direct consume request code=%d", request.Code)}
					return
				}
				expectedFields := map[string]string{
					"consumerGroup": "TOOLS_CONSUMER",
					"clientId":      "client-a",
					"topic":         "TopicTest",
					"msgId":         msgID,
				}
				for key, expected := range expectedFields {
					if request.ExtFields[key] != expected {
						conn.Close()
						brokerDone <- &testError{message: fmt.Sprintf("unexpected direct consume field %s=%q fields=%#v", key, request.ExtFields[key], request.ExtFields)}
						return
					}
				}
				body := []byte(`{"order":false,"autoCommit":true,"consumeResult":"CR_SUCCESS","remark":null,"spentTimeMills":1}`)
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, body))
			}
			conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != retryGroupTopicPrefix+"TOOLS_CONSUMER" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, body))
		nameServerDone <- err
	}()

	result, err := NewClient(time.Second).ConsumeMessageDirectlyByID(context.Background(), nameServerListener.Addr().String(), "TOOLS_CONSUMER", "client-a", "TopicTest", "DefaultCluster", msgID)
	if err != nil {
		t.Fatalf("consume message directly by id: %v", err)
	}
	if result == nil || result.Order || !result.AutoCommit || result.ConsumeResult != "CR_SUCCESS" || result.Remark != nil || result.SpentTimeMills != 1 {
		t.Fatalf("unexpected direct consume result %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientResendMessageByIDQueriesAndSendsOriginalMessage(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	host, portText, err := net.SplitHostPort(brokerListener.Addr().String())
	if err != nil {
		t.Fatalf("split broker addr: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse broker port: %v", err)
	}
	msgID := createMessageID(net.ParseIP(host).To4(), int32(port), 1000)

	brokerDone := make(chan error, 1)
	go func() {
		for step := 0; step < 2; step++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				brokerDone <- err
				return
			}
			switch step {
			case 0:
				if request.Code != requestCodeViewMessageByID || request.ExtFields["offset"] != "1000" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected query by id request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				body := queryMessageRecordForTest(t, queryMessageRecordFixture{
					Topic:           "TopicTest",
					Keys:            "OrderKey",
					UniqKey:         "UNIQ-ORIGINAL",
					QueueID:         0,
					QueueOffset:     7,
					CommitLogOffset: 1000,
					Body:            []byte("resend-body"),
					Flag:            9,
					StoreHostIP:     net.ParseIP(host).To4(),
					StoreHostPort:   int32(port),
				})
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, body))
			case 1:
				if request.Code != requestCodeSendMessageV2 {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected resend request code=%d", request.Code)}
					return
				}
				expectedFields := map[string]string{
					"a": "ReSendMsgById",
					"b": "TopicTest",
					"e": "0",
					"j": "9",
					"k": "true",
					"n": "broker-a",
				}
				for key, expected := range expectedFields {
					if request.ExtFields[key] != expected {
						conn.Close()
						brokerDone <- &testError{message: fmt.Sprintf("unexpected resend field %s=%q fields=%#v", key, request.ExtFields[key], request.ExtFields)}
						return
					}
				}
				properties := decodeMessageProperties(request.ExtFields["i"])
				if properties.Get("UNIQ_KEY") != "UNIQ-ORIGINAL" || properties.Get("KEYS") != "OrderKey" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("resend should preserve original properties, got %#v", properties)}
					return
				}
				if string(request.Body) != "resend-body" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected resend body %q", string(request.Body))}
					return
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"msgId":       "OFFSET-NEW",
						"queueId":     "0",
						"queueOffset": "31",
					},
				}
				_, err = conn.Write(remotingFrameForTest(t, response, nil))
			}
			conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, body))
		nameServerDone <- err
	}()

	result, err := NewClient(time.Second).ResendMessageByID(context.Background(), nameServerListener.Addr().String(), "TopicTest", "DefaultCluster", msgID, "UnitA")
	if err != nil {
		t.Fatalf("resend message by id: %v", err)
	}
	if result == nil || result.OriginalMsgID != msgID || result.SendResult == nil {
		t.Fatalf("unexpected nil resend result %#v", result)
	}
	expectedSendResult := &sendMessageResult{
		Topic:           "TopicTest",
		BrokerName:      "broker-a",
		QueueID:         0,
		SendStatus:      "SEND_OK",
		MessageID:       "UNIQ-ORIGINAL",
		OffsetMessageID: "OFFSET-NEW",
		QueueOffset:     31,
	}
	if !reflect.DeepEqual(result.SendResult, expectedSendResult) {
		t.Fatalf("unexpected resend send result\nexpected=%#v\nactual=%#v", expectedSendResult, result.SendResult)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientQueryMessageTraceByIDUsesTraceTopicQuery(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	traceBody := strings.Join([]string{
		"Pub", "1780883297000", "DefaultRegion", "PG-A", "TopicTest", "MSG-1", "TagA", "KeyA", "172.16.0.1:10911", "5", "7", "0", "OFFSET-1", "true",
	}, string(rune(1))) + string(rune(2))

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeQueryMessage {
			brokerDone <- &testError{message: "unexpected query message request code"}
			return
		}
		if request.ExtFields["topic"] != defaultTraceTopic || request.ExtFields["key"] != "MSG-1" || request.ExtFields["beginTimestamp"] != "10" || request.ExtFields["endTimestamp"] != "20" || request.ExtFields["maxNum"] != "8" || request.ExtFields[uniqueMsgQueryFlag] != "false" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected query fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := queryMessageRecordForTest(t, queryMessageRecordFixture{
			Topic:           defaultTraceTopic,
			Keys:            "MSG-1",
			UniqKey:         "TRACE-MSG-1",
			Body:            []byte(traceBody),
			StoreHostIP:     []byte{172, 16, 0, 1},
			StoreHostPort:   10911,
			CommitLogOffset: 1000,
		})
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != defaultTraceTopic {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	views, err := NewClient(time.Second).QueryMessageTraceByID(context.Background(), nameServerListener.Addr().String(), defaultTraceTopic, "MSG-1", 10, 20, 8)
	if err != nil {
		t.Fatalf("query trace by id: %v", err)
	}
	if len(views) != 1 || views[0].MsgType != "Pub" || views[0].GroupName != "PG-A" {
		t.Fatalf("unexpected trace views %#v", views)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientQueryMessagesByKeyUsesClusterRouteAndMaxNum(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeQueryMessage {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected query message request code %d", request.Code)}
			return
		}
		expectedFields := map[string]string{
			"topic":            "TopicTest",
			"key":              "OrderKey",
			"maxNum":           "7",
			"beginTimestamp":   "10",
			"endTimestamp":     "20",
			uniqueMsgQueryFlag: "false",
		}
		for key, expected := range expectedFields {
			if request.ExtFields[key] != expected {
				brokerDone <- &testError{message: fmt.Sprintf("unexpected query field %s=%q fields=%#v", key, request.ExtFields[key], request.ExtFields)}
				return
			}
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := queryMessageRecordForTest(t, queryMessageRecordFixture{
			Topic:           "TopicTest",
			Keys:            "OrderKey ExtraKey",
			UniqKey:         "UNIQ-ORDER-1",
			QueueID:         1,
			QueueOffset:     17,
			CommitLogOffset: 1000,
			Body:            []byte("hello"),
			StoreHostIP:     []byte{172, 16, 0, 1},
			StoreHostPort:   10911,
		})
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	results, err := NewClient(time.Second).QueryMessagesByKey(context.Background(), nameServerListener.Addr().String(), "TopicTest", "OrderKey", "DefaultCluster", 10, 20, 7)
	if err != nil {
		t.Fatalf("query messages by key: %v", err)
	}
	if len(results) != 1 || results[0].MessageID != "UNIQ-ORDER-1" || results[0].QueueID != 1 || results[0].QueueOffset != 17 {
		t.Fatalf("unexpected query results %#v", results)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumerProgressUsesConsumeStatsRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetConsumeStats {
			brokerDone <- &testError{message: "unexpected consume stats request code"}
			return
		}
		if request.ExtFields["consumerGroup"] != "GroupA" || request.ExtFields["topic"] != "TopicTest" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected consume stats fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"consumeTps":1.25,"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"brokerOffset":3,"consumerOffset":1,"lastTimestamp":1780883297000,"pullOffset":2}}}`)
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	progress, err := NewClient(time.Second).ConsumerProgress(context.Background(), nameServerListener.Addr().String(), "GroupA", "TopicTest", "")
	if err != nil {
		t.Fatalf("consumer progress: %v", err)
	}
	if progress.ConsumeTPS != 1.25 || len(progress.Entries) != 1 || progress.Entries[0].BrokerOffset != 3 {
		t.Fatalf("unexpected progress %#v", progress)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumerProgressClusterUsesClusterRouteAndStatsTopic(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetConsumeStats {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected consume stats request code %d", request.Code)}
			return
		}
		if request.ExtFields["consumerGroup"] != "GroupA" || request.ExtFields["topic"] != "TopicTest" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected consume stats fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"consumeTps":1.25,"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"brokerOffset":3,"consumerOffset":1,"lastTimestamp":1780883297000,"pullOffset":2}}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "DefaultCluster" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	progress, err := NewClient(time.Second).ConsumerProgress(context.Background(), nameServerListener.Addr().String(), "GroupA", "TopicTest", "DefaultCluster")
	if err != nil {
		t.Fatalf("consumer progress with cluster: %v", err)
	}
	if progress.ConsumeTPS != 1.25 || len(progress.Entries) != 1 || progress.Entries[0].BrokerOffset != 3 {
		t.Fatalf("unexpected progress %#v", progress)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumerConnectionUsesConnectionRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetConsumerConnectionList {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected consumer connection request code %d", request.Code)}
			return
		}
		if request.ExtFields["consumerGroup"] != "GroupA" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected consumer connection fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"connectionSet":[{"clientId":"client-a","clientAddr":"127.0.0.1:10001","language":"JAVA","version":477}],"consumeType":"CONSUME_PASSIVELY","messageModel":"CLUSTERING","consumeFromWhere":"CONSUME_FROM_LAST_OFFSET"}`)
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != retryGroupTopicPrefix+"GroupA" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	summary, err := NewClient(time.Second).ConsumerConnectionSummary(context.Background(), nameServerListener.Addr().String(), "GroupA")
	if err != nil {
		t.Fatalf("consumer connection: %v", err)
	}
	expected := &consumerConnectionSummary{Count: 1, Version: "V5_3_2", ConsumeType: "PUSH", MessageModel: "CLUSTERING", ClientIDs: []string{"client-a"}}
	if !reflect.DeepEqual(summary, expected) {
		t.Fatalf("consumer connection summary mismatch\nexpected=%#v\nactual=%#v", expected, summary)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumerStatusWithClientIDSendsRunningInfoRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetConsumerRunningInfo {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected consumer running info request code %d", request.Code)}
			return
		}
		if request.ExtFields["consumerGroup"] != "GroupA" || request.ExtFields["clientId"] != "client-a" || request.ExtFields["jstackEnable"] != "true" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected consumer running info fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"properties":{"PROP_CONSUME_TYPE":"CONSUME_PASSIVELY","PROP_CONSUMER_START_TIMESTAMP":"1780900199326","consumerGroup":"GroupA"},"subscriptionSet":[{"topic":"TopicTest","subString":"*","classFilterMode":false}],"mqTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"commitOffset":3,"cachedMsgMinOffset":1,"cachedMsgMaxOffset":2,"cachedMsgCount":4,"cachedMsgSizeInMiB":5,"transactionMsgMinOffset":6,"transactionMsgMaxOffset":7,"transactionMsgCount":8,"locked":true,"tryUnlockTimes":9,"lastLockTimestamp":1780900200000,"droped":false,"lastPullTimestamp":1780900201000,"lastConsumeTimestamp":1780900202000}},"mqPopTable":{},"statusTable":{"TopicTest":{"pullRT":1.25,"pullTPS":2.5,"consumeRT":3.75,"consumeOKTPS":4.5,"consumeFailedTPS":5.25,"consumeFailedMsgs":6}},"userConsumerInfo":{"userKey":"userValue"},"jstack":"stack-line\n"}`)
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != retryGroupTopicPrefix+"GroupA" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	output, err := NewClient(time.Second).ConsumerStatus(context.Background(), nameServerListener.Addr().String(), "GroupA", "client-a", "", true)
	if err != nil {
		t.Fatalf("consumer status: %v", err)
	}
	for _, expected := range []string{
		"#Consumer Properties#\n",
		"PROP_CONSUME_TYPE                       : CONSUME_PASSIVELY\n",
		"001 Topic: TopicTest                                ClassFilter: false    SubExpression: *\n",
		"TopicTest                         broker-a                          0     3                   \n",
		fmt.Sprintf("ProcessQueueInfo [commitOffset=3, cachedMsgMinOffset=1, cachedMsgMaxOffset=2, cachedMsgCount=4, cachedMsgSizeInMiB=5, transactionMsgMinOffset=6, transactionMsgMaxOffset=7, transactionMsgCount=8, locked=true, tryUnlockTimes=9, lastLockTimestamp=%s, droped=false, lastPullTimestamp=%s, lastConsumeTimestamp=%s]",
			formatRocketMQHumanMillis(1780900200000),
			formatRocketMQHumanMillis(1780900201000),
			formatRocketMQHumanMillis(1780900202000)),
		fmt.Sprintf("%-32s  %14.2f %14.2f %14.2f %14.2f %18.2f %25d\n", "TopicTest", 1.25, 2.5, 3.75, 4.5, 5.25, int64(6)),
		"userKey                                 : userValue\n",
		"#Consumer jstack#\nstack-line\n",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("consumer status output missing %q\nactual:\n%s", expected, output)
		}
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumeMessageDirectlyAtBrokerSendsOfficialRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeConsumeMessageDirectly {
			done <- &testError{message: fmt.Sprintf("unexpected consumeMessageDirectly request code %d", request.Code)}
			return
		}
		expectedFields := map[string]string{
			"consumerGroup": "TOOLS_CONSUMER",
			"clientId":      "client-a",
			"topic":         "TopicTest",
			"msgId":         "AC10000100002A9F00000000000003E8",
		}
		for key, expected := range expectedFields {
			if request.ExtFields[key] != expected {
				done <- &testError{message: fmt.Sprintf("unexpected field %s=%q fields=%#v", key, request.ExtFields[key], request.ExtFields)}
				return
			}
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"order":false,"autoCommit":true,"consumeResult":"CR_SUCCESS","remark":null,"spentTimeMills":1}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		done <- err
	}()

	result, err := NewClient(time.Second).consumeMessageDirectlyAtBroker(context.Background(), brokerListener.Addr().String(), "TOOLS_CONSUMER", "client-a", "TopicTest", "AC10000100002A9F00000000000003E8")
	if err != nil {
		t.Fatalf("consume message directly at broker: %v", err)
	}
	if result == nil || result.Order || !result.AutoCommit || result.ConsumeResult != "CR_SUCCESS" || result.Remark != nil || result.SpentTimeMills != 1 {
		t.Fatalf("unexpected direct consume result %#v", result)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientProducerConnectionUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetProducerConnectionList {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected producer connection request code %d", request.Code)}
			return
		}
		if request.ExtFields["producerGroup"] != "ProducerGroupA" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected producer connection fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"connectionSet":[{"clientId":"client-a","clientAddr":"127.0.0.1:10001","language":"JAVA","version":477}]}`)
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	detail, err := NewClient(time.Second).ProducerConnection(context.Background(), nameServerListener.Addr().String(), "ProducerGroupA", "TopicTest")
	if err != nil {
		t.Fatalf("producer connection: %v", err)
	}
	expected := &producerConnectionDetail{
		Connections: []producerConnectionEntry{{
			ClientID:   "client-a",
			ClientAddr: "127.0.0.1:10001",
			Language:   "JAVA",
			Version:    477,
		}},
	}
	if !reflect.DeepEqual(detail, expected) {
		t.Fatalf("producer connection mismatch\nexpected=%#v\nactual=%#v", expected, detail)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientProducerUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeGetAllProducerInfo {
			done <- &testError{message: fmt.Sprintf("unexpected producer request code %d", request.Code)}
			return
		}
		if len(request.ExtFields) != 0 {
			done <- &testError{message: fmt.Sprintf("unexpected producer fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"data":{"CLIENT_INNER_PRODUCER":[{"clientId":"client-a","remoteIP":"/127.0.0.1:10001","language":"JAVA","version":477,"lastUpdateTimestamp":1781000000001}]}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		done <- err
	}()

	table, err := NewClient(time.Second).Producer(context.Background(), brokerListener.Addr().String())
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	expected := &producerTableInfo{
		Groups: []producerGroupInfo{{
			Group: "CLIENT_INNER_PRODUCER",
			Producers: []producerInfo{{
				ClientID:            "client-a",
				RemoteIP:            "/127.0.0.1:10001",
				Language:            "JAVA",
				Version:             477,
				LastUpdateTimestamp: 1781000000001,
			}},
		}},
	}
	if !reflect.DeepEqual(table, expected) {
		t.Fatalf("producer table mismatch\nexpected=%#v\nactual=%#v", expected, table)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientGetColdDataFlowCtrInfoUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeGetColdDataFlowCtrInfo {
			done <- &testError{message: fmt.Sprintf("unexpected cold ctr request code %d", request.Code)}
			return
		}
		if len(request.ExtFields) != 0 {
			done <- &testError{message: fmt.Sprintf("unexpected cold ctr fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"globalAcc":0,"cgColdReadThreshold":3145728,"configTable":{},"globalColdReadThreshold":104857600,"runtimeTable":{}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		done <- err
	}()

	body, err := NewClient(time.Second).GetColdDataFlowCtrInfo(context.Background(), brokerListener.Addr().String())
	if err != nil {
		t.Fatalf("get cold ctr info: %v", err)
	}
	expected := `{"globalAcc":0,"cgColdReadThreshold":3145728,"configTable":{},"globalColdReadThreshold":104857600,"runtimeTable":{}}`
	if body != expected {
		t.Fatalf("unexpected cold ctr body\nexpected=%s\nactual=%s", expected, body)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientColdDataFlowCtrGroupConfigUsesOfficialRequestCodes(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		expected := []struct {
			code int
			body string
		}{
			{code: requestCodeUpdateColdDataFlowCtrConfig, body: "GoadminColdCtrGroup=12345\n"},
			{code: requestCodeRemoveColdDataFlowCtrConfig, body: "GoadminColdCtrGroup"},
		}
		for _, item := range expected {
			conn, err := brokerListener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				done <- err
				return
			}
			if request.Code != item.code {
				conn.Close()
				done <- fmt.Errorf("unexpected cold ctr config request code got=%d want=%d", request.Code, item.code)
				return
			}
			if len(request.ExtFields) != 0 {
				conn.Close()
				done <- fmt.Errorf("unexpected cold ctr config fields %#v", request.ExtFields)
				return
			}
			if string(request.Body) != item.body {
				conn.Close()
				done <- fmt.Errorf("unexpected cold ctr config body got=%q want=%q", string(request.Body), item.body)
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			_, err = conn.Write(remotingFrameForTest(t, response, nil))
			conn.Close()
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	client := NewClient(time.Second)
	targets, err := client.UpdateColdDataFlowCtrGroupConfig(context.Background(), "", coldDataFlowCtrGroupConfigOptions{
		BrokerAddr:    brokerListener.Addr().String(),
		ConsumerGroup: "GoadminColdCtrGroup",
		Threshold:     "12345",
	})
	if err != nil {
		t.Fatalf("update cold ctr group config: %v", err)
	}
	if !reflect.DeepEqual(targets, []string{brokerListener.Addr().String()}) {
		t.Fatalf("unexpected update cold ctr targets %#v", targets)
	}
	targets, err = client.RemoveColdDataFlowCtrGroupConfig(context.Background(), "", removeColdDataFlowCtrGroupConfigOptions{
		BrokerAddr:    brokerListener.Addr().String(),
		ConsumerGroup: "GoadminColdCtrGroup",
	})
	if err != nil {
		t.Fatalf("remove cold ctr group config: %v", err)
	}
	if !reflect.DeepEqual(targets, []string{brokerListener.Addr().String()}) {
		t.Fatalf("unexpected remove cold ctr targets %#v", targets)
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientCleanExpiredCQUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeCleanExpiredConsumeQueue {
			done <- fmt.Errorf("expected CLEAN_EXPIRED_CONSUMEQUEUE code %d, got %d", requestCodeCleanExpiredConsumeQueue, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("expected no cleanExpiredCQ ext fields, got %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty cleanExpiredCQ body, got %d bytes", len(request.Body))
			return
		}
		_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	ok, err := client.CleanExpiredCQ(context.Background(), "", cleanExpiredCQOptions{
		BrokerAddr: brokerListener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("cleanExpiredCQ: %v", err)
	}
	if !ok {
		t.Fatalf("expected cleanExpiredCQ to return success")
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientCleanUnusedTopicUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeCleanUnusedTopic {
			done <- fmt.Errorf("expected CLEAN_UNUSED_TOPIC code %d, got %d", requestCodeCleanUnusedTopic, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("expected no cleanUnusedTopic ext fields, got %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty cleanUnusedTopic body, got %d bytes", len(request.Body))
			return
		}
		_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	ok, err := client.CleanUnusedTopic(context.Background(), "", cleanUnusedTopicOptions{
		BrokerAddr: brokerListener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("cleanUnusedTopic: %v", err)
	}
	if !ok {
		t.Fatalf("expected cleanUnusedTopic to return success")
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientDeleteExpiredCommitLogUsesOfficialRequestCode(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeDeleteExpiredCommitLog {
			done <- fmt.Errorf("expected DELETE_EXPIRED_COMMITLOG code %d, got %d", requestCodeDeleteExpiredCommitLog, request.Code)
			return
		}
		if len(request.ExtFields) != 0 {
			done <- fmt.Errorf("expected no deleteExpiredCommitLog ext fields, got %#v", request.ExtFields)
			return
		}
		if len(request.Body) != 0 {
			done <- fmt.Errorf("expected empty deleteExpiredCommitLog body, got %d bytes", len(request.Body))
			return
		}
		_, err = conn.Write(remotingFrameForTest(t, remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}, nil))
		done <- err
	}()

	client := NewClient(time.Second)
	ok, err := client.DeleteExpiredCommitLog(context.Background(), "", deleteExpiredCommitLogOptions{
		BrokerAddr: brokerListener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("deleteExpiredCommitLog: %v", err)
	}
	if !ok {
		t.Fatalf("expected deleteExpiredCommitLog to return success")
	}
	if err := <-done; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumerProgressWithClientIPUsesRunningInfoRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 3; i++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			switch i {
			case 0:
				if request.Code != requestCodeGetConsumeStats {
					_ = conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected consume stats request code %d", request.Code)}
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				body := []byte(`{"consumeTps":1.25,"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"brokerOffset":3,"consumerOffset":1,"lastTimestamp":1780883297000,"pullOffset":2}}}`)
				if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
					_ = conn.Close()
					brokerDone <- err
					return
				}
			case 1:
				if request.Code != requestCodeGetConsumerConnectionList {
					_ = conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected connection request code %d", request.Code)}
					return
				}
				if request.ExtFields["consumerGroup"] != "GroupA" {
					_ = conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected connection fields %#v", request.ExtFields)}
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				body := []byte(`{"connectionSet":[{"clientId":"10.0.0.1@client-a","clientAddr":"10.0.0.1:10001","language":"JAVA","version":477}],"consumeType":"CONSUME_PASSIVELY","messageModel":"CLUSTERING","consumeFromWhere":"CONSUME_FROM_LAST_OFFSET"}`)
				if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
					_ = conn.Close()
					brokerDone <- err
					return
				}
			case 2:
				if request.Code != requestCodeGetConsumerRunningInfo {
					_ = conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected running info request code %d", request.Code)}
					return
				}
				if request.ExtFields["consumerGroup"] != "GroupA" || request.ExtFields["clientId"] != "10.0.0.1@client-a" || request.ExtFields["jstackEnable"] != "false" {
					_ = conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected running info fields %#v", request.ExtFields)}
					return
				}
				response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
				body := []byte(`{"properties":{},"mqTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"commitOffset":3}}}`)
				if _, err := conn.Write(remotingFrameForTest(t, response, body)); err != nil {
					_ = conn.Close()
					brokerDone <- err
					return
				}
			}
			_ = conn.Close()
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 3; i++ {
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				nameServerDone <- err
				return
			}
			if request.Code != requestCodeGetRouteInfoByTopic {
				conn.Close()
				nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
			_, err = conn.Write(remotingFrameForTest(t, response, body))
			conn.Close()
			if err != nil {
				nameServerDone <- err
				return
			}
		}
		nameServerDone <- nil
	}()

	progress, err := NewClient(time.Second).ConsumerProgressWithClientIP(context.Background(), nameServerListener.Addr().String(), "GroupA", "TopicTest", "")
	if err != nil {
		t.Fatalf("consumer progress showClientIP: %v", err)
	}
	if len(progress.Entries) != 1 || progress.Entries[0].ClientIP != "10.0.0.1" {
		t.Fatalf("unexpected progress with client IP %#v", progress)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientQueryMessageByIDFallsBackToUniqKeyQuery(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeQueryMessage {
			brokerDone <- &testError{message: "unexpected query message request code"}
			return
		}
		if request.ExtFields["key"] != "AC18000400091152471124E6F96F0000" || request.ExtFields[uniqueMsgQueryFlag] != "true" || request.ExtFields["maxNum"] != "32" {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected query fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := queryMessageRecordForTest(t, queryMessageRecordFixture{
			Topic:           "TopicTest",
			Keys:            "OrderKey",
			UniqKey:         "AC18000400091152471124E6F96F0000",
			QueueID:         0,
			QueueOffset:     7,
			CommitLogOffset: 1000,
			Body:            []byte("hello"),
			StoreHostIP:     []byte{172, 16, 0, 1},
			StoreHostPort:   10911,
		})
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic {
			nameServerDone <- &testError{message: "unexpected route request code"}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	detail, err := NewClient(time.Second).QueryMessageByID(context.Background(), nameServerListener.Addr().String(), "TopicTest", "", "AC18000400091152471124E6F96F0000")
	if err != nil {
		t.Fatalf("query uniq message by id: %v", err)
	}
	if detail.DisplayMessageID != "AC18000400091152471124E6F96F0000" || detail.OffsetMessageID == detail.DisplayMessageID {
		t.Fatalf("unexpected detail ids offset=%s display=%s", detail.OffsetMessageID, detail.DisplayMessageID)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientQueryMessageByOffsetUsesPullRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodePullMessage {
			brokerDone <- &testError{message: "unexpected pull request code"}
			return
		}
		expectedFields := map[string]string{
			"consumerGroup":        "TOOLS_CONSUMER",
			"topic":                "TopicTest",
			"queueId":              "0",
			"queueOffset":          "7",
			"maxMsgNums":           "1",
			"sysFlag":              "4",
			"commitOffset":         "0",
			"suspendTimeoutMillis": "20000",
			"subscription":         "*",
			"subVersion":           "0",
			"expressionType":       "TAG",
			"maxMsgBytes":          "2147483647",
			"bname":                "broker-a",
		}
		for key, expected := range expectedFields {
			if request.ExtFields[key] != expected {
				brokerDone <- &testError{message: fmt.Sprintf("unexpected pull field %s=%q", key, request.ExtFields[key])}
				return
			}
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
			ExtFields: map[string]string{
				"suggestWhichBrokerId": "0",
				"nextBeginOffset":      "8",
				"minOffset":            "0",
				"maxOffset":            "8",
			},
		}
		body := queryMessageRecordForTest(t, queryMessageRecordFixture{
			Topic:           "TopicTest",
			Keys:            "OrderKey",
			UniqKey:         "UNIQ-1",
			QueueID:         0,
			QueueOffset:     7,
			CommitLogOffset: 1000,
			Body:            []byte("hello"),
			StoreHostIP:     []byte{172, 16, 0, 1},
			StoreHostPort:   10911,
		})
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic {
			nameServerDone <- &testError{message: "unexpected route request code"}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	detail, err := NewClient(time.Second).QueryMessageByOffset(context.Background(), nameServerListener.Addr().String(), "TopicTest", "broker-a", 0, 7)
	if err != nil {
		t.Fatalf("query message by offset: %v", err)
	}
	if detail.OffsetMessageID != "AC10000100002A9F00000000000003E8" || detail.DisplayMessageID != "UNIQ-1" {
		t.Fatalf("unexpected message ids offset=%s display=%s", detail.OffsetMessageID, detail.DisplayMessageID)
	}
	if detail.QueueID != 0 || detail.QueueOffset != 7 || detail.Properties.Get("MIN_OFFSET") != "0" || detail.Properties.Get("MAX_OFFSET") != "8" {
		t.Fatalf("unexpected detail %#v", detail)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientPrintMsgByQueueUsesOffsetAndPullRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for step := 0; step < 5; step++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				brokerDone <- err
				return
			}
			switch step {
			case 0:
				if request.Code != requestCodeGetMinOffset || request.ExtFields["topic"] != "TopicTest" || request.ExtFields["brokerName"] != "broker-a" || request.ExtFields["queueId"] != "0" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected min offset request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "0"},
				}, nil))
			case 1:
				if request.Code != requestCodeGetMaxOffset || request.ExtFields["topic"] != "TopicTest" || request.ExtFields["brokerName"] != "broker-a" || request.ExtFields["queueId"] != "0" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected max offset request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "12"},
				}, nil))
			case 2:
				if request.Code != requestCodeSearchOffsetByTimestamp || request.ExtFields["timestamp"] != "10" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected begin search request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "7"},
				}, nil))
			case 3:
				if request.Code != requestCodeSearchOffsetByTimestamp || request.ExtFields["timestamp"] != "20" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected end search request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "8"},
				}, nil))
			case 4:
				expectedFields := map[string]string{
					"consumerGroup":        "TOOLS_CONSUMER",
					"topic":                "TopicTest",
					"queueId":              "0",
					"queueOffset":          "7",
					"maxMsgNums":           "32",
					"sysFlag":              "4",
					"commitOffset":         "0",
					"suspendTimeoutMillis": "20000",
					"subscription":         "TagA",
					"subVersion":           "0",
					"expressionType":       "TAG",
					"maxMsgBytes":          "2147483647",
					"bname":                "broker-a",
				}
				if request.Code != requestCodePullMessage {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected pull request code %d", request.Code)}
					return
				}
				for key, expected := range expectedFields {
					if request.ExtFields[key] != expected {
						conn.Close()
						brokerDone <- &testError{message: fmt.Sprintf("unexpected pull field %s=%q", key, request.ExtFields[key])}
						return
					}
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"nextBeginOffset": "8",
						"minOffset":       "0",
						"maxOffset":       "12",
					},
				}
				body := queryMessageRecordForTest(t, queryMessageRecordFixture{
					Topic:           "TopicTest",
					Keys:            "OrderKey",
					UniqKey:         "UNIQ-1",
					QueueID:         0,
					QueueOffset:     7,
					CommitLogOffset: 1000,
					Body:            []byte("hello"),
					StoreHostIP:     []byte{172, 16, 0, 1},
					StoreHostPort:   10911,
				})
				_, err = conn.Write(remotingFrameForTest(t, response, body))
			}
			conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	result, err := NewClient(time.Second).PrintMessagesByQueue(context.Background(), nameServerListener.Addr().String(), printMsgByQueueOptions{
		Topic:             "TopicTest",
		BrokerName:        "broker-a",
		QueueID:           0,
		HasBeginTimestamp: true,
		BeginTimestamp:    10,
		HasEndTimestamp:   true,
		EndTimestamp:      20,
		SubExpression:     "TagA",
	})
	if err != nil {
		t.Fatalf("print messages by queue: %v", err)
	}
	if len(result.Messages) != 1 || result.Messages[0].QueueOffset != 7 || result.Messages[0].Properties.Get("MIN_OFFSET") != "0" || result.Messages[0].Properties.Get("MAX_OFFSET") != "12" {
		t.Fatalf("unexpected printMsgByQueue result %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientConsumeMessageUsesConsumerGroupAndOfficialPullBatch(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for step := 0; step < 3; step++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				brokerDone <- err
				return
			}
			switch step {
			case 0:
				if request.Code != requestCodeGetMinOffset || request.ExtFields["topic"] != "TopicTest" || request.ExtFields["brokerName"] != "broker-a" || request.ExtFields["queueId"] != "0" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected min offset request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "0"},
				}, nil))
			case 1:
				if request.Code != requestCodeGetMaxOffset || request.ExtFields["topic"] != "TopicTest" || request.ExtFields["brokerName"] != "broker-a" || request.ExtFields["queueId"] != "0" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected max offset request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "8"},
				}, nil))
			case 2:
				expectedFields := map[string]string{
					"consumerGroup":        "GroupA",
					"topic":                "TopicTest",
					"queueId":              "0",
					"queueOffset":          "7",
					"maxMsgNums":           "2",
					"sysFlag":              "4",
					"commitOffset":         "0",
					"suspendTimeoutMillis": "20000",
					"subscription":         "*",
					"subVersion":           "0",
					"expressionType":       "TAG",
					"maxMsgBytes":          "2147483647",
					"bname":                "broker-a",
				}
				if request.Code != requestCodePullMessage {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected pull request code %d", request.Code)}
					return
				}
				for key, expected := range expectedFields {
					if request.ExtFields[key] != expected {
						conn.Close()
						brokerDone <- &testError{message: fmt.Sprintf("unexpected pull field %s=%q", key, request.ExtFields[key])}
						return
					}
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"nextBeginOffset": "9",
						"minOffset":       "0",
						"maxOffset":       "8",
					},
				}
				body := queryMessageRecordForTest(t, queryMessageRecordFixture{
					Topic:           "TopicTest",
					Keys:            "OrderKey",
					UniqKey:         "UNIQ-1",
					QueueID:         0,
					QueueOffset:     7,
					CommitLogOffset: 1000,
					Body:            []byte("hello"),
					StoreHostIP:     []byte{172, 16, 0, 1},
					StoreHostPort:   10911,
				})
				_, err = conn.Write(remotingFrameForTest(t, response, body))
			}
			conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	result, err := NewClient(time.Second).ConsumeMessages(context.Background(), nameServerListener.Addr().String(), consumeMessageOptions{
		Topic:         "TopicTest",
		BrokerName:    "broker-a",
		QueueID:       0,
		HasQueueID:    true,
		Offset:        7,
		HasOffset:     true,
		ConsumerGroup: "GroupA",
		MessageCount:  128,
	})
	if err != nil {
		t.Fatalf("consume messages: %v", err)
	}
	if len(result.Entries) != 1 || len(result.Entries[0].Messages) != 1 || result.Entries[0].Messages[0].QueueOffset != 7 || result.Entries[0].Messages[0].Properties.Get("MIN_OFFSET") != "0" || result.Entries[0].Messages[0].Properties.Get("MAX_OFFSET") != "8" {
		t.Fatalf("unexpected consumeMessage result %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientPrintMsgUsesRouteQueuesAndPullRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		for step := 0; step < 3; step++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				conn.Close()
				brokerDone <- err
				return
			}
			switch step {
			case 0:
				if request.Code != requestCodeGetMinOffset || request.ExtFields["topic"] != "TopicTest" || request.ExtFields["brokerName"] != "broker-a" || request.ExtFields["queueId"] != "0" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected min offset request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "0"},
				}, nil))
			case 1:
				if request.Code != requestCodeGetMaxOffset || request.ExtFields["topic"] != "TopicTest" || request.ExtFields["brokerName"] != "broker-a" || request.ExtFields["queueId"] != "0" {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected max offset request code=%d fields=%#v", request.Code, request.ExtFields)}
					return
				}
				_, err = conn.Write(remotingFrameForTest(t, remotingCommand{
					Code:      responseCodeSuccess,
					Language:  "JAVA",
					Version:   0,
					Opaque:    request.Opaque,
					Flag:      1,
					ExtFields: map[string]string{"offset": "1"},
				}, nil))
			case 2:
				expectedFields := map[string]string{
					"consumerGroup":        "TOOLS_CONSUMER",
					"topic":                "TopicTest",
					"queueId":              "0",
					"queueOffset":          "0",
					"maxMsgNums":           "32",
					"sysFlag":              "4",
					"commitOffset":         "0",
					"suspendTimeoutMillis": "20000",
					"subscription":         "*",
					"subVersion":           "0",
					"expressionType":       "TAG",
					"maxMsgBytes":          "2147483647",
					"bname":                "broker-a",
				}
				if request.Code != requestCodePullMessage {
					conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected pull request code %d", request.Code)}
					return
				}
				for key, expected := range expectedFields {
					if request.ExtFields[key] != expected {
						conn.Close()
						brokerDone <- &testError{message: fmt.Sprintf("unexpected pull field %s=%q", key, request.ExtFields[key])}
						return
					}
				}
				response := remotingCommand{
					Code:     responseCodeSuccess,
					Language: "JAVA",
					Version:  0,
					Opaque:   request.Opaque,
					Flag:     1,
					ExtFields: map[string]string{
						"nextBeginOffset": "1",
						"minOffset":       "0",
						"maxOffset":       "1",
					},
				}
				body := queryMessageRecordForTest(t, queryMessageRecordFixture{
					Topic:           "TopicTest",
					Keys:            "OrderKey",
					UniqKey:         "UNIQ-1",
					QueueID:         0,
					QueueOffset:     0,
					CommitLogOffset: 1000,
					Body:            []byte("hello"),
					StoreHostIP:     []byte{172, 16, 0, 1},
					StoreHostPort:   10911,
				})
				_, err = conn.Write(remotingFrameForTest(t, response, body))
			}
			conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic || request.ExtFields["topic"] != "TopicTest" {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code=%d fields=%#v", request.Code, request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":1,"topicSysFlag":0,"writeQueueNums":1}]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	result, err := NewClient(time.Second).PrintMessages(context.Background(), nameServerListener.Addr().String(), printMsgOptions{Topic: "TopicTest"})
	if err != nil {
		t.Fatalf("print messages: %v", err)
	}
	if len(result.Queues) != 1 || result.Queues[0].Queue.QueueID != 0 || result.Queues[0].MinOffset != 0 || result.Queues[0].MaxOffset != 1 || len(result.Queues[0].Messages) != 1 {
		t.Fatalf("unexpected printMsg result %#v", result)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientTopicStatusUsesRouteBrokerAndStatsRequest(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetTopicStatsInfo {
			brokerDone <- &testError{message: "unexpected topic stats request code"}
			return
		}
		if request.ExtFields["topic"] != "TopicTest" {
			brokerDone <- &testError{message: "unexpected topic stats topic field"}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		frame := remotingFrameForTest(t, response, []byte(`{"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"lastUpdateTimestamp":0,"maxOffset":2,"minOffset":0}}}`))
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic {
			nameServerDone <- &testError{message: "unexpected route request code"}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	entries, err := NewClient(time.Second).TopicStatus(context.Background(), nameServerListener.Addr().String(), "TopicTest")
	if err != nil {
		t.Fatalf("topic status: %v", err)
	}
	expected := []topicStatusEntry{{BrokerName: "broker-a", QueueID: 0, MinOffset: 0, MaxOffset: 2}}
	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("entries mismatch\nexpected=%#v\nactual=%#v", expected, entries)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientTopicStatusByClusterUsesClusterRouteAndTopicStats(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetTopicStatsInfo {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected topic stats request code %d", request.Code)}
			return
		}
		if request.ExtFields["topic"] != "TopicTest" {
			brokerDone <- &testError{message: fmt.Sprintf("expected real topic TopicTest, got %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(`{"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"TopicTest"}:{"lastUpdateTimestamp":0,"maxOffset":4,"minOffset":1}}}`)
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected route request code %d", request.Code)}
			return
		}
		if request.ExtFields["topic"] != "DefaultCluster" {
			nameServerDone <- &testError{message: fmt.Sprintf("expected cluster route topic DefaultCluster, got %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	entries, err := NewClient(time.Second).TopicStatusByCluster(context.Background(), nameServerListener.Addr().String(), "TopicTest", "DefaultCluster")
	if err != nil {
		t.Fatalf("topic status by cluster: %v", err)
	}
	expected := []topicStatusEntry{{BrokerName: "broker-a", QueueID: 0, MinOffset: 1, MaxOffset: 4}}
	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("entries mismatch\nexpected=%#v\nactual=%#v", expected, entries)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientClusterListUsesClusterInfoAndRuntimeStatsRequests(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	earliest := time.Now().Add(-time.Hour).UnixMilli()
	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerRuntimeInfo {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected runtime request code %d", request.Code)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"table":{"brokerActive":"true","putTps":"1.50 1 1","getTransferredTps":"2.50 1 1","sendThreadPoolQueueSize":"3","sendThreadPoolQueueHeadWaitTimeMills":"4","pullThreadPoolQueueSize":"5","pullThreadPoolQueueHeadWaitTimeMills":"6","pageCacheLockTimeMills":"7","earliestMessageTimeStamp":"%d","commitLogDiskRatio":"0.125","timerReadBehind":"8","timerOffsetBehind":"9","timerCongestNum":"10000","timerEnqueueTps":"1.1","timerDequeueTps":"2.2","brokerVersionDesc":"V5_3_2"}}`, earliest))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected cluster info request code %d", request.Code)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		frame := remotingFrameForTest(t, response, body)
		_, err = conn.Write(frame)
		nameServerDone <- err
	}()

	rows, err := NewClient(time.Second).ClusterList(context.Background(), nameServerListener.Addr().String(), "DefaultCluster")
	if err != nil {
		t.Fatalf("cluster list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %#v", rows)
	}
	row := rows[0]
	if row.ClusterName != "DefaultCluster" || row.BrokerName != "broker-a" || row.BrokerID != "0" || row.Addr != brokerListener.Addr().String() {
		t.Fatalf("unexpected cluster row identity %#v", row)
	}
	if row.Version != "V5_3_2" || row.InTPS != 1.5 || row.OutTPS != 2.5 || row.AckThreadPoolQueueSize != "N" || row.AckThreadPoolQueueHeadWaitMS != "N" || !row.BrokerActive {
		t.Fatalf("unexpected cluster row stats %#v", row)
	}
	if row.Hour < 0.99 || row.Hour > 1.01 {
		t.Fatalf("unexpected hour %.4f", row.Hour)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestClientClusterAclConfigVersionUsesOfficialRequestCodeAndExtFields(t *testing.T) {
	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerDone := make(chan error, 1)
	go func() {
		conn, err := brokerListener.Accept()
		if err != nil {
			brokerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			brokerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterAclInfo {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected acl version request code %d", request.Code)}
			return
		}
		if len(request.ExtFields) != 0 || len(request.Body) != 0 {
			brokerDone <- &testError{message: fmt.Sprintf("unexpected acl version request fields=%#v body=%q", request.ExtFields, request.Body)}
			return
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
			ExtFields: map[string]string{
				"clusterName":       "DefaultCluster",
				"brokerName":        "broker-a",
				"brokerAddr":        brokerListener.Addr().String(),
				"version":           `{"timestamp":1780891116954,"counter":7}`,
				"allAclFileVersion": `{"/home/rocketmq/conf/plain_acl.yml":{"timestamp":1780891116954,"counter":7}}`,
			},
		}
		_, err = conn.Write(remotingFrameForTest(t, response, nil))
		brokerDone <- err
	}()

	nameServerDone := make(chan error, 1)
	go func() {
		conn, err := nameServerListener.Accept()
		if err != nil {
			nameServerDone <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			nameServerDone <- err
			return
		}
		if request.Code != requestCodeGetBrokerClusterInfo {
			nameServerDone <- &testError{message: fmt.Sprintf("unexpected cluster info request code %d", request.Code)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		body := []byte(fmt.Sprintf(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, brokerListener.Addr().String()))
		_, err = conn.Write(remotingFrameForTest(t, response, body))
		nameServerDone <- err
	}()

	rows, err := NewClient(time.Second).ClusterAclConfigVersion(context.Background(), nameServerListener.Addr().String(), "DefaultCluster")
	if err != nil {
		t.Fatalf("cluster acl config version: %v", err)
	}
	expected := []clusterAclConfigVersionRow{{
		ClusterName:    "DefaultCluster",
		BrokerName:     "broker-a",
		BrokerAddr:     brokerListener.Addr().String(),
		AclFilePath:    "/home/rocketmq/conf/plain_acl.yml",
		VersionCounter: 7,
		LastUpdateTime: time.UnixMilli(1780891116954),
	}}
	if !reflect.DeepEqual(rows, expected) {
		t.Fatalf("acl config version rows mismatch\nexpected=%#v\nactual=%#v", expected, rows)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestDecodeClusterAclConfigVersionRequiresAllAclFileVersion(t *testing.T) {
	_, err := decodeClusterAclConfigVersionResponse(map[string]string{
		"clusterName": "DefaultCluster",
		"brokerName":  "broker-a",
		"brokerAddr":  "127.0.0.1:10911",
		"version":     `{"timestamp":1780891116954,"counter":7}`,
	})
	if err == nil {
		t.Fatal("expected missing allAclFileVersion to fail")
	}
}

func TestClientTopicClusterListUsesClusterInfoAndRouteRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		defer close(done)
		seen := map[int]bool{}
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				done <- err
				return
			}
			seen[request.Code] = true
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			var body []byte
			switch request.Code {
			case requestCodeGetBrokerClusterInfo:
				body = []byte(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`)
			case requestCodeGetRouteInfoByTopic:
				if request.ExtFields["topic"] != "TopicTest" {
					_ = conn.Close()
					done <- &testError{message: "unexpected topic route ext field"}
					return
				}
				body = []byte(`{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`)
			default:
				_ = conn.Close()
				done <- &testError{message: "unexpected request code"}
				return
			}
			frame := remotingFrameForTest(t, response, body)
			_, err = conn.Write(frame)
			_ = conn.Close()
			if err != nil {
				done <- err
				return
			}
		}
		if !seen[requestCodeGetBrokerClusterInfo] || !seen[requestCodeGetRouteInfoByTopic] {
			done <- &testError{message: "missing expected nameserver request"}
		}
	}()

	clusters, err := NewClient(time.Second).TopicClusterList(context.Background(), listener.Addr().String(), "TopicTest")
	if err != nil {
		t.Fatalf("topic cluster list: %v", err)
	}
	expected := []string{"DefaultCluster"}
	if !reflect.DeepEqual(clusters, expected) {
		t.Fatalf("clusters mismatch\nexpected=%#v\nactual=%#v", expected, clusters)
	}
	if err := <-done; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
}

func TestClientTopicRouteSendsTopicHeader(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeGetRouteInfoByTopic {
			done <- &testError{message: "unexpected request code"}
			return
		}
		if request.ExtFields["topic"] != "TopicTest" {
			done <- &testError{message: "unexpected topic ext field"}
			return
		}
		response := remotingCommand{
			Code:     responseCodeSuccess,
			Language: "JAVA",
			Version:  0,
			Opaque:   request.Opaque,
			Flag:     1,
		}
		frame := remotingFrameForTest(t, response, []byte(`{"queueDatas":[],"brokerDatas":[]}`))
		_, err = conn.Write(frame)
		done <- err
	}()

	body, err := NewClient(time.Second).TopicRoute(context.Background(), listener.Addr().String(), "TopicTest")
	if err != nil {
		t.Fatalf("topic route: %v", err)
	}
	if string(body) != `{"queueDatas":[],"brokerDatas":[]}` {
		t.Fatalf("unexpected body %s", body)
	}
	if err := <-done; err != nil {
		t.Fatalf("server side: %v", err)
	}
}

func TestClientViewBrokerStatsDataSendsOfficialHeader(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer listener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeViewBrokerStatsData {
			done <- &testError{message: fmt.Sprintf("unexpected request code %d", request.Code)}
			return
		}
		if request.ExtFields["statsName"] != "TOPIC_PUT_NUMS" || request.ExtFields["statsKey"] != "TopicTest" {
			done <- &testError{message: fmt.Sprintf("unexpected stats ext fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		frame := remotingFrameForTest(t, response, []byte(`{"statsMinute":{"sum":1,"tps":1.25,"avgpt":0},"statsHour":{"sum":12,"tps":0,"avgpt":0},"statsDay":{"sum":24,"tps":0,"avgpt":0}}`))
		_, err = conn.Write(frame)
		done <- err
	}()

	stats, err := NewClient(time.Second).viewBrokerStatsData(context.Background(), listener.Addr().String(), "TOPIC_PUT_NUMS", "TopicTest")
	if err != nil {
		t.Fatalf("view broker stats data: %v", err)
	}
	if stats.StatsMinute.Sum != 1 || stats.StatsMinute.TPS != 1.25 || brokerStats24HourSum(stats) != 24 {
		t.Fatalf("unexpected stats %#v", stats)
	}
	if err := <-done; err != nil {
		t.Fatalf("server side: %v", err)
	}
}

func TestClientSumBrokerStatsSkipsFailedBrokers(t *testing.T) {
	failListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed broker: %v", err)
	}
	failAddr := failListener.Addr().String()
	if err := failListener.Close(); err != nil {
		t.Fatalf("close failed broker: %v", err)
	}

	successListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen success broker: %v", err)
	}
	defer successListener.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := successListener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := decodeCommand(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Code != requestCodeViewBrokerStatsData {
			done <- &testError{message: fmt.Sprintf("unexpected request code %d", request.Code)}
			return
		}
		if request.ExtFields["statsName"] != statsNameTopicPutNums || request.ExtFields["statsKey"] != "TopicTest" {
			done <- &testError{message: fmt.Sprintf("unexpected stats ext fields %#v", request.ExtFields)}
			return
		}
		response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
		frame := remotingFrameForTest(t, response, []byte(`{"statsMinute":{"sum":3,"tps":1.25,"avgpt":0},"statsHour":{"sum":12,"tps":0,"avgpt":0},"statsDay":{"sum":24,"tps":0,"avgpt":0}}`))
		_, err = conn.Write(frame)
		done <- err
	}()

	tps, today, err := NewClient(time.Second).sumBrokerStats(context.Background(), []string{failAddr, successListener.Addr().String()}, statsNameTopicPutNums, "TopicTest")
	if err != nil {
		t.Fatalf("sum broker stats: %v", err)
	}
	if tps != 1.25 || today != 24 {
		t.Fatalf("unexpected stats tps=%v today=%d", tps, today)
	}
	if err := <-done; err != nil {
		t.Fatalf("server side: %v", err)
	}
}

func TestClientStatsAllTopicRowsKeepsRowWhenConsumeStatsFails(t *testing.T) {
	nameServerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen nameserver: %v", err)
	}
	defer nameServerListener.Close()

	brokerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker: %v", err)
	}
	defer brokerListener.Close()

	brokerAddr := brokerListener.Addr().String()

	nameServerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := nameServerListener.Accept()
			if err != nil {
				nameServerDone <- err
				return
			}
			defer conn.Close()
			request, err := decodeCommand(conn)
			if err != nil {
				nameServerDone <- err
				return
			}
			if request.Code != requestCodeGetRouteInfoByTopic {
				nameServerDone <- &testError{message: fmt.Sprintf("unexpected nameserver code %d", request.Code)}
				return
			}
			if request.ExtFields["topic"] != "TopicTest" {
				nameServerDone <- &testError{message: fmt.Sprintf("unexpected route topic %q", request.ExtFields["topic"])}
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			body := fmt.Sprintf(`{"brokerDatas":[{"brokerAddrs":{0:%q},"brokerName":"broker-a","cluster":"DefaultCluster"}],"queueDatas":[]}`, brokerAddr)
			frame := remotingFrameForTest(t, response, []byte(body))
			_, err = conn.Write(frame)
			if err != nil {
				nameServerDone <- err
				return
			}
		}
		nameServerDone <- nil
	}()

	brokerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 3; i++ {
			conn, err := brokerListener.Accept()
			if err != nil {
				brokerDone <- err
				return
			}
			request, err := decodeCommand(conn)
			if err != nil {
				_ = conn.Close()
				brokerDone <- err
				return
			}
			response := remotingCommand{Code: responseCodeSuccess, Language: "JAVA", Version: 0, Opaque: request.Opaque, Flag: 1}
			var body []byte
			switch request.Code {
			case requestCodeQueryTopicConsumeByWho:
				body = []byte(`{"groupList":["GroupA"]}`)
			case requestCodeViewBrokerStatsData:
				switch request.ExtFields["statsName"] {
				case statsNameTopicPutNums:
					body = []byte(`{"statsMinute":{"sum":3,"tps":1.25,"avgpt":0},"statsHour":{"sum":12,"tps":0,"avgpt":0},"statsDay":{"sum":24,"tps":0,"avgpt":0}}`)
				case statsNameGroupGetNums:
					body = []byte(`{"statsMinute":{"sum":2,"tps":2.50,"avgpt":0},"statsHour":{"sum":8,"tps":0,"avgpt":0},"statsDay":{"sum":8,"tps":0,"avgpt":0}}`)
				default:
					_ = conn.Close()
					brokerDone <- &testError{message: fmt.Sprintf("unexpected stats name %q", request.ExtFields["statsName"])}
					return
				}
			case requestCodeGetConsumeStats:
				response.Code = 1
				response.Remark = "boom"
			default:
				_ = conn.Close()
				brokerDone <- &testError{message: fmt.Sprintf("unexpected broker code %d", request.Code)}
				return
			}
			frame := remotingFrameForTest(t, response, body)
			_, err = conn.Write(frame)
			_ = conn.Close()
			if err != nil {
				brokerDone <- err
				return
			}
		}
		brokerDone <- nil
	}()

	rows, err := NewClient(time.Second).statsAllTopicRows(context.Background(), nameServerListener.Addr().String(), "TopicTest", false)
	if err != nil {
		t.Fatalf("statsAll topic rows: %v", err)
	}
	expected := []statsAllRow{{
		Topic:         "TopicTest",
		ConsumerGroup: "GroupA",
		Accumulation:  0,
		InTPS:         1.25,
		OutTPS:        2.50,
		InMsg24Hour:   24,
		OutMsg24Hour:  8,
	}}
	if !reflect.DeepEqual(rows, expected) {
		t.Fatalf("statsAll rows mismatch\nexpected=%#v\nactual=%#v", expected, rows)
	}
	if err := <-nameServerDone; err != nil {
		t.Fatalf("nameserver side: %v", err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatalf("broker side: %v", err)
	}
}

func TestDecodeTopicListBodyUsesOfficialSetOrder(t *testing.T) {
	topics, err := decodeTopicListBody([]byte(`{"brokerAddr":null,"topicList":["B","A","C"]}`))
	if err != nil {
		t.Fatalf("decode topic list body: %v", err)
	}
	expected := []string{"A", "B", "C"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("topics mismatch\nexpected=%#v\nactual=%#v", expected, topics)
	}
}

func remotingFrameForTest(t *testing.T, command remotingCommand, body []byte) []byte {
	t.Helper()
	frame, err := encodeCommand(command)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	if len(body) == 0 {
		return frame
	}
	totalLength := int(binary.BigEndian.Uint32(frame[0:4])) + len(body)
	withBody := make([]byte, 0, len(frame)+len(body))
	binaryLength := make([]byte, 4)
	binary.BigEndian.PutUint32(binaryLength, uint32(totalLength))
	withBody = append(withBody, binaryLength...)
	withBody = append(withBody, frame[4:]...)
	withBody = append(withBody, body...)
	return withBody
}

func rocketMQRemotingFrameForTest(t *testing.T, command remotingCommand, body []byte) []byte {
	t.Helper()
	header := rocketMQHeaderForTest(t, command)
	totalLength := 4 + len(header) + len(body)
	frame := make([]byte, 8, 8+len(header)+len(body))
	binary.BigEndian.PutUint32(frame[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(frame[4:8], uint32(serializeTypeRocketMQ)<<24|uint32(len(header)))
	frame = append(frame, header...)
	frame = append(frame, body...)
	return frame
}

func rocketMQHeaderForTest(t *testing.T, command remotingCommand) []byte {
	t.Helper()
	var header bytes.Buffer
	writeUint16ForTest(t, &header, uint16(command.Code))
	header.WriteByte(0)
	writeUint16ForTest(t, &header, uint16(command.Version))
	writeUint32ForTest(t, &header, uint32(command.Opaque))
	writeUint32ForTest(t, &header, uint32(command.Flag))
	writeRocketMQStringForTest(t, &header, false, command.Remark)
	var extFields bytes.Buffer
	for _, key := range sortedKeysAnyString(command.ExtFields) {
		writeRocketMQStringForTest(t, &extFields, true, key)
		writeRocketMQStringForTest(t, &extFields, false, command.ExtFields[key])
	}
	writeUint32ForTest(t, &header, uint32(extFields.Len()))
	header.Write(extFields.Bytes())
	return header.Bytes()
}

func writeRocketMQStringForTest(t *testing.T, buffer *bytes.Buffer, useShortLength bool, value string) {
	t.Helper()
	if useShortLength {
		writeUint16ForTest(t, buffer, uint16(len([]byte(value))))
	} else {
		writeUint32ForTest(t, buffer, uint32(len([]byte(value))))
	}
	buffer.WriteString(value)
}

func writeUint16ForTest(t *testing.T, buffer *bytes.Buffer, value uint16) {
	t.Helper()
	if err := binary.Write(buffer, binary.BigEndian, value); err != nil {
		t.Fatalf("write uint16: %v", err)
	}
}

func writeUint32ForTest(t *testing.T, buffer *bytes.Buffer, value uint32) {
	t.Helper()
	if err := binary.Write(buffer, binary.BigEndian, value); err != nil {
		t.Fatalf("write uint32: %v", err)
	}
}

type nativeClientFunc struct {
	topicList            func(ctx context.Context, nameServer string) ([]string, error)
	topicListCluster     func(ctx context.Context, nameServer string) ([]topicClusterRow, error)
	allocateMQ           func(ctx context.Context, nameServer string, topic string, ipList string) ([]allocateMQAssignment, error)
	clusterList          func(ctx context.Context, nameServer string, clusterName string) ([]clusterListRow, error)
	clusterListMoreStats func(ctx context.Context, nameServer string, clusterName string) ([]clusterListMoreStatsRow, error)
	brokerStatus         func(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerStatusTable, error)
	getBrokerConfig      func(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerConfigSection, error)
	updateBrokerConfig   func(ctx context.Context, nameServer string, options updateBrokerConfigOptions) ([]string, error)
	updateNamesrvConfig  func(ctx context.Context, nameServers string, options updateNamesrvConfigOptions) ([]string, error)
	wipeWritePerm        func(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error)
	addWritePerm         func(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error)
	sendMessage          func(ctx context.Context, nameServer string, options sendMessageOptions) (*sendMessageResult, error)
	sendMsgStatus        func(ctx context.Context, nameServer string, options sendMsgStatusOptions) ([]sendMsgStatusResult, error)
	checkMsgSendRT       func(ctx context.Context, nameServer string, options checkMsgSendRTOptions) (*checkMsgSendRTResult, error)
	clusterRT            func(ctx context.Context, nameServer string, options clusterRTOptions) (*clusterRTResult, error)
	// addBroker/removeBroker 注入 BrokerContainer 变更动作，便于命令层测试锁定参数与 stdout。
	addBroker                        func(ctx context.Context, brokerContainerAddr string, options addBrokerOptions) error
	removeBroker                     func(ctx context.Context, brokerContainerAddr string, options removeBrokerOptions) error
	resetMasterFlushOffset           func(ctx context.Context, brokerAddr string, offset int64) error
	getBrokerEpoch                   func(ctx context.Context, nameServer string, brokerName string) ([]brokerEpochResult, error)
	getBrokerEpochByCluster          func(ctx context.Context, nameServer string, clusterName string) ([]brokerEpochResult, error)
	getControllerMetaData            func(ctx context.Context, controllerAddr string) (*controllerMetaData, error)
	getSyncStateSet                  func(ctx context.Context, controllerAddr string, brokerNames []string) (*syncStateSetResult, error)
	getSyncStateSetByCluster         func(ctx context.Context, nameServer string, controllerAddr string, clusterName string) (*syncStateSetResult, error)
	getControllerConfig              func(ctx context.Context, controllerAddrs string) ([]namesrvConfigSection, error)
	updateControllerConfig           func(ctx context.Context, controllerAddrs string, options updateControllerConfigOptions) ([]string, error)
	cleanBrokerMetadata              func(ctx context.Context, controllerAddr string, options cleanBrokerMetadataOptions) error
	electMaster                      func(ctx context.Context, controllerAddr string, options electMasterOptions) (*electMasterResult, error)
	resetOffsetByTime                func(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error)
	skipAccumulatedMessage           func(ctx context.Context, nameServer string, options skipAccumulatedMessageOptions) ([]skipAccumulatedMessageRow, error)
	exportConfigs                    func(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error)
	exportMetadata                   func(ctx context.Context, nameServer string, options exportMetadataOptions) (*exportMetadataResult, error)
	exportMetrics                    func(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error)
	getNamesrvConfig                 func(ctx context.Context, nameServers string) ([]namesrvConfigSection, error)
	getConsumerConfig                func(ctx context.Context, nameServer string, groupName string) ([]consumerConfigSection, error)
	brokerConsumeStats               func(ctx context.Context, brokerAddr string, isOrder bool, timeout time.Duration) (*brokerConsumeStats, error)
	statsAll                         func(ctx context.Context, nameServer string, topic string, activeOnly bool) ([]statsAllRow, error)
	brokerHAStatus                   func(ctx context.Context, brokerAddr string) (*haStatusResult, error)
	brokerHAStatusByCluster          func(ctx context.Context, nameServer string, clusterName string) ([]haStatusBrokerResult, error)
	clusterAclConfigVersion          func(ctx context.Context, nameServer string, clusterName string) ([]clusterAclConfigVersionRow, error)
	setCommitLogReadAheadMode        func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, mode string) ([]commitLogReadAheadModeSection, error)
	listUser                         func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, filter string) ([]listUserRow, error)
	getUser                          func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, username string) (*listUserRow, error)
	copyUser                         func(ctx context.Context, sourceBroker string, targetBroker string, usernames string) ([]copyUserResult, error)
	listAcl                          func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subjectFilter string, resourceFilter string) ([]aclInfo, error)
	getAcl                           func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subject string) ([]aclInfo, error)
	copyAcl                          func(ctx context.Context, sourceBroker string, targetBroker string, subjects string) ([]copyAclResult, error)
	createUser                       func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error)
	updateUser                       func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error)
	deleteUser                       func(ctx context.Context, nameServer string, options authUserOptions) ([]string, error)
	createAcl                        func(ctx context.Context, nameServer string, options aclOptions) ([]string, error)
	updateAcl                        func(ctx context.Context, nameServer string, options aclOptions) ([]string, error)
	deleteAcl                        func(ctx context.Context, nameServer string, options aclOptions) ([]string, error)
	updateAclConfig                  func(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error)
	deleteAclConfig                  func(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error)
	updateGlobalWhiteAddr            func(ctx context.Context, nameServer string, options globalWhiteAddrOptions) ([]string, error)
	checkRocksdbCqWriteProgress      func(ctx context.Context, nameServer string, clusterName string, topic string, checkStoreTime int64) ([]checkRocksdbCqWriteProgressRow, error)
	rocksDBConfigToJson              func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, configTypes []string) error
	exportPopRecord                  func(ctx context.Context, nameServer string, brokerAddr string, clusterName string, dryRun bool) ([]exportPopRecordRow, error)
	updateKvConfig                   func(ctx context.Context, nameServers string, namespace string, key string, value string) error
	getKvConfig                      func(ctx context.Context, nameServers string, namespace string, key string) (string, error)
	deleteKvConfig                   func(ctx context.Context, nameServers string, namespace string, key string) error
	updateTopicList                  func(ctx context.Context, nameServer string, options updateTopicListOptions) ([]string, error)
	updateTopic                      func(ctx context.Context, nameServer string, options updateTopicOptions) (*updateTopicResult, error)
	updateStaticTopic                func(ctx context.Context, nameServer string, options updateStaticTopicOptions) (*updateStaticTopicResult, error)
	remappingStaticTopic             func(ctx context.Context, nameServer string, options remappingStaticTopicOptions) (*remappingStaticTopicResult, error)
	updateTopicPerm                  func(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error)
	setConsumeMode                   func(ctx context.Context, nameServer string, options setConsumeModeOptions) (*setConsumeModeResult, error)
	deleteTopic                      func(ctx context.Context, nameServer string, clusterName string, topic string) error
	updateSubGroup                   func(ctx context.Context, nameServer string, options updateSubGroupOptions) (*updateSubGroupResult, error)
	updateSubGroupList               func(ctx context.Context, nameServer string, options updateSubGroupListOptions) ([]string, error)
	deleteSubGroup                   func(ctx context.Context, nameServer string, options deleteSubGroupOptions) ([]deleteSubGroupResult, error)
	queryConsumeQueue                func(ctx context.Context, nameServer string, brokerAddr string, topic string, queueID int, index int64, count int, consumerGroup string) (*queryConsumeQueueResult, error)
	topicRoute                       func(ctx context.Context, nameServer string, topic string) ([]byte, error)
	topicStatus                      func(ctx context.Context, nameServer string, topic string) ([]topicStatusEntry, error)
	topicStatusByCluster             func(ctx context.Context, nameServer string, topic string, cluster string) ([]topicStatusEntry, error)
	topicClusterList                 func(ctx context.Context, nameServer string, topic string) ([]string, error)
	consumerConnection               func(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string) (*consumerConnectionDetail, error)
	consumerStatus                   func(ctx context.Context, nameServer string, consumerGroup string, clientID string, brokerAddr string, jstack bool) (string, error)
	consumerStatusList               func(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string, jstack bool) (string, error)
	cloneGroupOffset                 func(ctx context.Context, nameServer string, srcGroup string, destGroup string, topic string) error
	producer                         func(ctx context.Context, brokerAddr string) (*producerTableInfo, error)
	updateColdDataFlowCtrGroupConfig func(ctx context.Context, nameServer string, options coldDataFlowCtrGroupConfigOptions) ([]string, error)
	removeColdDataFlowCtrGroupConfig func(ctx context.Context, nameServer string, options removeColdDataFlowCtrGroupConfigOptions) ([]string, error)
	cleanExpiredCQ                   func(ctx context.Context, nameServer string, options cleanExpiredCQOptions) (bool, error)
	cleanUnusedTopic                 func(ctx context.Context, nameServer string, options cleanUnusedTopicOptions) (bool, error)
	deleteExpiredCommitLog           func(ctx context.Context, nameServer string, options deleteExpiredCommitLogOptions) (bool, error)
	getColdDataFlowCtrInfo           func(ctx context.Context, brokerAddr string) (string, error)
	getColdDataFlowCtrInfoByCluster  func(ctx context.Context, nameServer string, clusterName string) ([]coldDataFlowCtrInfoSection, error)
	producerConnection               func(ctx context.Context, nameServer string, producerGroup string, topic string) (*producerConnectionDetail, error)
	queryMessagesByKey               func(ctx context.Context, nameServer string, topic string, key string, clusterName string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageSearchResult, error)
	queryMessageByOffset             func(ctx context.Context, nameServer string, topic string, brokerName string, queueID int, offset int64) (*messageDetail, error)
	printMessages                    func(ctx context.Context, nameServer string, options printMsgOptions) (*printMsgResult, error)
	printMessagesByQueue             func(ctx context.Context, nameServer string, options printMsgByQueueOptions) (*printMsgByQueueResult, error)
	consumeMessages                  func(ctx context.Context, nameServer string, options consumeMessageOptions) (*consumeMessageResult, error)
	queryMessageByID                 func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error)
	messageTrackDetail               func(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error)
	queryMessageByUniqueKey          func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error)
	queryMessagesByUniqueKey         func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) ([]messageDetail, error)
	consumeMessageDirectly           func(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error)
	consumeMessageDirectlyByID       func(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error)
	resendMessageByID                func(ctx context.Context, nameServer string, topic string, clusterName string, msgID string, unitName string) (*queryMsgByIDResendResult, error)
	queryMessageTraceByID            func(ctx context.Context, nameServer string, traceTopic string, msgID string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageTraceView, error)
	consumerProgress                 func(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error)
	consumerProgressWithClientIP     func(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error)
	consumerConnectionSummary        func(ctx context.Context, nameServer string, consumerGroup string) (*consumerConnectionSummary, error)
	consumerProgressSummary          func(ctx context.Context, nameServer string) ([]consumerProgressSummaryRow, error)
	startMonitoring                  func(ctx context.Context, nameServer string) error
}

func (f nativeClientFunc) TopicList(ctx context.Context, nameServer string) ([]string, error) {
	if f.topicList == nil {
		return nil, nil
	}
	return f.topicList(ctx, nameServer)
}

func (f nativeClientFunc) TopicListCluster(ctx context.Context, nameServer string) ([]topicClusterRow, error) {
	if f.topicListCluster == nil {
		return nil, nil
	}
	return f.topicListCluster(ctx, nameServer)
}

func (f nativeClientFunc) AllocateMQ(ctx context.Context, nameServer string, topic string, ipList string) ([]allocateMQAssignment, error) {
	return f.allocateMQ(ctx, nameServer, topic, ipList)
}

func (f nativeClientFunc) ClusterList(ctx context.Context, nameServer string, clusterName string) ([]clusterListRow, error) {
	if f.clusterList == nil {
		return nil, nil
	}
	return f.clusterList(ctx, nameServer, clusterName)
}

func (f nativeClientFunc) ClusterListMoreStats(ctx context.Context, nameServer string, clusterName string) ([]clusterListMoreStatsRow, error) {
	if f.clusterListMoreStats == nil {
		return nil, nil
	}
	return f.clusterListMoreStats(ctx, nameServer, clusterName)
}

func (f nativeClientFunc) BrokerStatus(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerStatusTable, error) {
	if f.brokerStatus == nil {
		return nil, nil
	}
	return f.brokerStatus(ctx, nameServer, brokerAddr, clusterName)
}

func (f nativeClientFunc) GetBrokerConfig(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerConfigSection, error) {
	if f.getBrokerConfig == nil {
		return nil, nil
	}
	return f.getBrokerConfig(ctx, nameServer, brokerAddr, clusterName)
}

func (f nativeClientFunc) UpdateBrokerConfig(ctx context.Context, nameServer string, options updateBrokerConfigOptions) ([]string, error) {
	if f.updateBrokerConfig == nil {
		return nil, nil
	}
	return f.updateBrokerConfig(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateNamesrvConfig(ctx context.Context, nameServers string, options updateNamesrvConfigOptions) ([]string, error) {
	if f.updateNamesrvConfig == nil {
		return nil, nil
	}
	return f.updateNamesrvConfig(ctx, nameServers, options)
}

func (f nativeClientFunc) WipeWritePerm(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error) {
	if f.wipeWritePerm == nil {
		return nil, nil
	}
	return f.wipeWritePerm(ctx, nameServers, brokerName)
}

func (f nativeClientFunc) AddWritePerm(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error) {
	if f.addWritePerm == nil {
		return nil, nil
	}
	return f.addWritePerm(ctx, nameServers, brokerName)
}

func (f nativeClientFunc) SendMessage(ctx context.Context, nameServer string, options sendMessageOptions) (*sendMessageResult, error) {
	if f.sendMessage == nil {
		return nil, nil
	}
	return f.sendMessage(ctx, nameServer, options)
}

func (f nativeClientFunc) SendMsgStatus(ctx context.Context, nameServer string, options sendMsgStatusOptions) ([]sendMsgStatusResult, error) {
	if f.sendMsgStatus == nil {
		return nil, nil
	}
	return f.sendMsgStatus(ctx, nameServer, options)
}

func (f nativeClientFunc) CheckMsgSendRT(ctx context.Context, nameServer string, options checkMsgSendRTOptions) (*checkMsgSendRTResult, error) {
	if f.checkMsgSendRT == nil {
		return nil, nil
	}
	return f.checkMsgSendRT(ctx, nameServer, options)
}

func (f nativeClientFunc) ClusterRT(ctx context.Context, nameServer string, options clusterRTOptions) (*clusterRTResult, error) {
	if f.clusterRT == nil {
		return nil, nil
	}
	return f.clusterRT(ctx, nameServer, options)
}

func (f nativeClientFunc) AddBroker(ctx context.Context, brokerContainerAddr string, options addBrokerOptions) error {
	if f.addBroker == nil {
		return nil
	}
	return f.addBroker(ctx, brokerContainerAddr, options)
}

func (f nativeClientFunc) RemoveBroker(ctx context.Context, brokerContainerAddr string, options removeBrokerOptions) error {
	if f.removeBroker == nil {
		return nil
	}
	return f.removeBroker(ctx, brokerContainerAddr, options)
}

func (f nativeClientFunc) ResetMasterFlushOffset(ctx context.Context, brokerAddr string, offset int64) error {
	if f.resetMasterFlushOffset == nil {
		return nil
	}
	return f.resetMasterFlushOffset(ctx, brokerAddr, offset)
}

func (f nativeClientFunc) GetBrokerEpoch(ctx context.Context, nameServer string, brokerName string) ([]brokerEpochResult, error) {
	if f.getBrokerEpoch == nil {
		return nil, nil
	}
	return f.getBrokerEpoch(ctx, nameServer, brokerName)
}

func (f nativeClientFunc) GetBrokerEpochByCluster(ctx context.Context, nameServer string, clusterName string) ([]brokerEpochResult, error) {
	if f.getBrokerEpochByCluster == nil {
		return nil, nil
	}
	return f.getBrokerEpochByCluster(ctx, nameServer, clusterName)
}

func (f nativeClientFunc) GetControllerMetaData(ctx context.Context, controllerAddr string) (*controllerMetaData, error) {
	if f.getControllerMetaData == nil {
		return nil, nil
	}
	return f.getControllerMetaData(ctx, controllerAddr)
}

func (f nativeClientFunc) GetSyncStateSet(ctx context.Context, controllerAddr string, brokerNames []string) (*syncStateSetResult, error) {
	if f.getSyncStateSet == nil {
		return nil, nil
	}
	return f.getSyncStateSet(ctx, controllerAddr, brokerNames)
}

func (f nativeClientFunc) GetSyncStateSetByCluster(ctx context.Context, nameServer string, controllerAddr string, clusterName string) (*syncStateSetResult, error) {
	if f.getSyncStateSetByCluster == nil {
		return nil, nil
	}
	return f.getSyncStateSetByCluster(ctx, nameServer, controllerAddr, clusterName)
}

func (f nativeClientFunc) GetControllerConfig(ctx context.Context, controllerAddrs string) ([]namesrvConfigSection, error) {
	if f.getControllerConfig == nil {
		return nil, nil
	}
	return f.getControllerConfig(ctx, controllerAddrs)
}

func (f nativeClientFunc) UpdateControllerConfig(ctx context.Context, controllerAddrs string, options updateControllerConfigOptions) ([]string, error) {
	if f.updateControllerConfig == nil {
		return nil, nil
	}
	return f.updateControllerConfig(ctx, controllerAddrs, options)
}

func (f nativeClientFunc) CleanBrokerMetadata(ctx context.Context, controllerAddr string, options cleanBrokerMetadataOptions) error {
	if f.cleanBrokerMetadata == nil {
		return nil
	}
	return f.cleanBrokerMetadata(ctx, controllerAddr, options)
}

func (f nativeClientFunc) ElectMaster(ctx context.Context, controllerAddr string, options electMasterOptions) (*electMasterResult, error) {
	if f.electMaster == nil {
		return nil, nil
	}
	return f.electMaster(ctx, controllerAddr, options)
}

func (f nativeClientFunc) ResetOffsetByTime(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error) {
	if f.resetOffsetByTime == nil {
		return nil, nil
	}
	return f.resetOffsetByTime(ctx, nameServer, options)
}

func (f nativeClientFunc) SkipAccumulatedMessage(ctx context.Context, nameServer string, options skipAccumulatedMessageOptions) ([]skipAccumulatedMessageRow, error) {
	if f.skipAccumulatedMessage == nil {
		return nil, nil
	}
	return f.skipAccumulatedMessage(ctx, nameServer, options)
}

func (f nativeClientFunc) ExportConfigs(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error) {
	if f.exportConfigs == nil {
		return "", nil
	}
	return f.exportConfigs(ctx, nameServer, clusterName, filePath)
}

func (f nativeClientFunc) ExportMetadata(ctx context.Context, nameServer string, options exportMetadataOptions) (*exportMetadataResult, error) {
	if f.exportMetadata == nil {
		return nil, nil
	}
	return f.exportMetadata(ctx, nameServer, options)
}

func (f nativeClientFunc) ExportMetrics(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error) {
	if f.exportMetrics == nil {
		return "", nil
	}
	return f.exportMetrics(ctx, nameServer, clusterName, filePath)
}

func (f nativeClientFunc) GetNamesrvConfig(ctx context.Context, nameServers string) ([]namesrvConfigSection, error) {
	if f.getNamesrvConfig == nil {
		return nil, nil
	}
	return f.getNamesrvConfig(ctx, nameServers)
}

func (f nativeClientFunc) GetConsumerConfig(ctx context.Context, nameServer string, groupName string) ([]consumerConfigSection, error) {
	if f.getConsumerConfig == nil {
		return nil, nil
	}
	return f.getConsumerConfig(ctx, nameServer, groupName)
}

func (f nativeClientFunc) BrokerConsumeStats(ctx context.Context, brokerAddr string, isOrder bool, timeout time.Duration) (*brokerConsumeStats, error) {
	if f.brokerConsumeStats == nil {
		return nil, nil
	}
	return f.brokerConsumeStats(ctx, brokerAddr, isOrder, timeout)
}

func (f nativeClientFunc) StatsAll(ctx context.Context, nameServer string, topic string, activeOnly bool) ([]statsAllRow, error) {
	if f.statsAll == nil {
		return nil, nil
	}
	return f.statsAll(ctx, nameServer, topic, activeOnly)
}

func (f nativeClientFunc) BrokerHAStatus(ctx context.Context, brokerAddr string) (*haStatusResult, error) {
	if f.brokerHAStatus == nil {
		return nil, nil
	}
	return f.brokerHAStatus(ctx, brokerAddr)
}

func (f nativeClientFunc) BrokerHAStatusByCluster(ctx context.Context, nameServer string, clusterName string) ([]haStatusBrokerResult, error) {
	if f.brokerHAStatusByCluster == nil {
		return nil, nil
	}
	return f.brokerHAStatusByCluster(ctx, nameServer, clusterName)
}

func (f nativeClientFunc) ClusterAclConfigVersion(ctx context.Context, nameServer string, clusterName string) ([]clusterAclConfigVersionRow, error) {
	if f.clusterAclConfigVersion == nil {
		return nil, nil
	}
	return f.clusterAclConfigVersion(ctx, nameServer, clusterName)
}

func (f nativeClientFunc) SetCommitLogReadAheadMode(ctx context.Context, nameServer string, brokerAddr string, clusterName string, mode string) ([]commitLogReadAheadModeSection, error) {
	if f.setCommitLogReadAheadMode == nil {
		return nil, nil
	}
	return f.setCommitLogReadAheadMode(ctx, nameServer, brokerAddr, clusterName, mode)
}

func (f nativeClientFunc) ListUser(ctx context.Context, nameServer string, brokerAddr string, clusterName string, filter string) ([]listUserRow, error) {
	if f.listUser == nil {
		return nil, nil
	}
	return f.listUser(ctx, nameServer, brokerAddr, clusterName, filter)
}

func (f nativeClientFunc) GetUser(ctx context.Context, nameServer string, brokerAddr string, clusterName string, username string) (*listUserRow, error) {
	if f.getUser == nil {
		return nil, nil
	}
	return f.getUser(ctx, nameServer, brokerAddr, clusterName, username)
}

func (f nativeClientFunc) CopyUser(ctx context.Context, sourceBroker string, targetBroker string, usernames string) ([]copyUserResult, error) {
	if f.copyUser == nil {
		return nil, nil
	}
	return f.copyUser(ctx, sourceBroker, targetBroker, usernames)
}

func (f nativeClientFunc) ListAcl(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subjectFilter string, resourceFilter string) ([]aclInfo, error) {
	if f.listAcl == nil {
		return nil, nil
	}
	return f.listAcl(ctx, nameServer, brokerAddr, clusterName, subjectFilter, resourceFilter)
}

func (f nativeClientFunc) GetAcl(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subject string) ([]aclInfo, error) {
	if f.getAcl == nil {
		return nil, nil
	}
	return f.getAcl(ctx, nameServer, brokerAddr, clusterName, subject)
}

func (f nativeClientFunc) CopyAcl(ctx context.Context, sourceBroker string, targetBroker string, subjects string) ([]copyAclResult, error) {
	if f.copyAcl == nil {
		return nil, nil
	}
	return f.copyAcl(ctx, sourceBroker, targetBroker, subjects)
}

func (f nativeClientFunc) CreateUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
	if f.createUser == nil {
		return nil, nil
	}
	return f.createUser(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
	if f.updateUser == nil {
		return nil, nil
	}
	return f.updateUser(ctx, nameServer, options)
}

func (f nativeClientFunc) DeleteUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
	if f.deleteUser == nil {
		return nil, nil
	}
	return f.deleteUser(ctx, nameServer, options)
}

func (f nativeClientFunc) CreateAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
	if f.createAcl == nil {
		return nil, nil
	}
	return f.createAcl(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
	if f.updateAcl == nil {
		return nil, nil
	}
	return f.updateAcl(ctx, nameServer, options)
}

func (f nativeClientFunc) DeleteAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
	if f.deleteAcl == nil {
		return nil, nil
	}
	return f.deleteAcl(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateAclConfig(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
	if f.updateAclConfig == nil {
		return nil, nil
	}
	return f.updateAclConfig(ctx, nameServer, options)
}

func (f nativeClientFunc) DeleteAclConfig(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
	if f.deleteAclConfig == nil {
		return nil, nil
	}
	return f.deleteAclConfig(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateGlobalWhiteAddr(ctx context.Context, nameServer string, options globalWhiteAddrOptions) ([]string, error) {
	if f.updateGlobalWhiteAddr == nil {
		return nil, nil
	}
	return f.updateGlobalWhiteAddr(ctx, nameServer, options)
}

func (f nativeClientFunc) CheckRocksdbCqWriteProgress(ctx context.Context, nameServer string, clusterName string, topic string, checkStoreTime int64) ([]checkRocksdbCqWriteProgressRow, error) {
	if f.checkRocksdbCqWriteProgress == nil {
		return nil, nil
	}
	return f.checkRocksdbCqWriteProgress(ctx, nameServer, clusterName, topic, checkStoreTime)
}

func (f nativeClientFunc) RocksDBConfigToJson(ctx context.Context, nameServer string, brokerAddr string, clusterName string, configTypes []string) error {
	if f.rocksDBConfigToJson == nil {
		return nil
	}
	return f.rocksDBConfigToJson(ctx, nameServer, brokerAddr, clusterName, configTypes)
}

func (f nativeClientFunc) ExportPopRecord(ctx context.Context, nameServer string, brokerAddr string, clusterName string, dryRun bool) ([]exportPopRecordRow, error) {
	if f.exportPopRecord == nil {
		return nil, nil
	}
	return f.exportPopRecord(ctx, nameServer, brokerAddr, clusterName, dryRun)
}

func (f nativeClientFunc) UpdateKvConfig(ctx context.Context, nameServers string, namespace string, key string, value string) error {
	if f.updateKvConfig == nil {
		return nil
	}
	return f.updateKvConfig(ctx, nameServers, namespace, key, value)
}

func (f nativeClientFunc) GetKvConfig(ctx context.Context, nameServers string, namespace string, key string) (string, error) {
	if f.getKvConfig == nil {
		return "", nil
	}
	return f.getKvConfig(ctx, nameServers, namespace, key)
}

func (f nativeClientFunc) DeleteKvConfig(ctx context.Context, nameServers string, namespace string, key string) error {
	if f.deleteKvConfig == nil {
		return nil
	}
	return f.deleteKvConfig(ctx, nameServers, namespace, key)
}

func (f nativeClientFunc) UpdateTopicList(ctx context.Context, nameServer string, options updateTopicListOptions) ([]string, error) {
	if f.updateTopicList == nil {
		return nil, nil
	}
	return f.updateTopicList(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateTopic(ctx context.Context, nameServer string, options updateTopicOptions) (*updateTopicResult, error) {
	if f.updateTopic == nil {
		return nil, nil
	}
	return f.updateTopic(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateStaticTopic(ctx context.Context, nameServer string, options updateStaticTopicOptions) (*updateStaticTopicResult, error) {
	if f.updateStaticTopic == nil {
		return nil, nil
	}
	return f.updateStaticTopic(ctx, nameServer, options)
}

func (f nativeClientFunc) RemappingStaticTopic(ctx context.Context, nameServer string, options remappingStaticTopicOptions) (*remappingStaticTopicResult, error) {
	if f.remappingStaticTopic == nil {
		return nil, nil
	}
	return f.remappingStaticTopic(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateTopicPerm(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error) {
	if f.updateTopicPerm == nil {
		return nil, nil
	}
	return f.updateTopicPerm(ctx, nameServer, options)
}

func (f nativeClientFunc) SetConsumeMode(ctx context.Context, nameServer string, options setConsumeModeOptions) (*setConsumeModeResult, error) {
	if f.setConsumeMode == nil {
		return nil, nil
	}
	return f.setConsumeMode(ctx, nameServer, options)
}

func (f nativeClientFunc) DeleteTopic(ctx context.Context, nameServer string, clusterName string, topic string) error {
	if f.deleteTopic == nil {
		return nil
	}
	return f.deleteTopic(ctx, nameServer, clusterName, topic)
}

func (f nativeClientFunc) UpdateSubGroup(ctx context.Context, nameServer string, options updateSubGroupOptions) (*updateSubGroupResult, error) {
	if f.updateSubGroup == nil {
		return nil, nil
	}
	return f.updateSubGroup(ctx, nameServer, options)
}

func (f nativeClientFunc) UpdateSubGroupList(ctx context.Context, nameServer string, options updateSubGroupListOptions) ([]string, error) {
	if f.updateSubGroupList == nil {
		return nil, nil
	}
	return f.updateSubGroupList(ctx, nameServer, options)
}

func (f nativeClientFunc) DeleteSubGroup(ctx context.Context, nameServer string, options deleteSubGroupOptions) ([]deleteSubGroupResult, error) {
	if f.deleteSubGroup == nil {
		return nil, nil
	}
	return f.deleteSubGroup(ctx, nameServer, options)
}

func (f nativeClientFunc) QueryConsumeQueue(ctx context.Context, nameServer string, brokerAddr string, topic string, queueID int, index int64, count int, consumerGroup string) (*queryConsumeQueueResult, error) {
	if f.queryConsumeQueue == nil {
		return nil, nil
	}
	return f.queryConsumeQueue(ctx, nameServer, brokerAddr, topic, queueID, index, count, consumerGroup)
}

func (f nativeClientFunc) TopicRoute(ctx context.Context, nameServer string, topic string) ([]byte, error) {
	return f.topicRoute(ctx, nameServer, topic)
}

func (f nativeClientFunc) TopicStatus(ctx context.Context, nameServer string, topic string) ([]topicStatusEntry, error) {
	return f.topicStatus(ctx, nameServer, topic)
}

func (f nativeClientFunc) TopicStatusByCluster(ctx context.Context, nameServer string, topic string, cluster string) ([]topicStatusEntry, error) {
	return f.topicStatusByCluster(ctx, nameServer, topic, cluster)
}

func (f nativeClientFunc) TopicClusterList(ctx context.Context, nameServer string, topic string) ([]string, error) {
	return f.topicClusterList(ctx, nameServer, topic)
}

func (f nativeClientFunc) ConsumerConnection(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string) (*consumerConnectionDetail, error) {
	return f.consumerConnection(ctx, nameServer, consumerGroup, brokerAddr)
}

func (f nativeClientFunc) ConsumerStatus(ctx context.Context, nameServer string, consumerGroup string, clientID string, brokerAddr string, jstack bool) (string, error) {
	return f.consumerStatus(ctx, nameServer, consumerGroup, clientID, brokerAddr, jstack)
}

func (f nativeClientFunc) ConsumerStatusList(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string, jstack bool) (string, error) {
	return f.consumerStatusList(ctx, nameServer, consumerGroup, brokerAddr, jstack)
}

func (f nativeClientFunc) CloneGroupOffset(ctx context.Context, nameServer string, srcGroup string, destGroup string, topic string) error {
	if f.cloneGroupOffset == nil {
		return nil
	}
	return f.cloneGroupOffset(ctx, nameServer, srcGroup, destGroup, topic)
}

func (f nativeClientFunc) Producer(ctx context.Context, brokerAddr string) (*producerTableInfo, error) {
	return f.producer(ctx, brokerAddr)
}

func (f nativeClientFunc) UpdateColdDataFlowCtrGroupConfig(ctx context.Context, nameServer string, options coldDataFlowCtrGroupConfigOptions) ([]string, error) {
	if f.updateColdDataFlowCtrGroupConfig == nil {
		return nil, nil
	}
	return f.updateColdDataFlowCtrGroupConfig(ctx, nameServer, options)
}

func (f nativeClientFunc) RemoveColdDataFlowCtrGroupConfig(ctx context.Context, nameServer string, options removeColdDataFlowCtrGroupConfigOptions) ([]string, error) {
	if f.removeColdDataFlowCtrGroupConfig == nil {
		return nil, nil
	}
	return f.removeColdDataFlowCtrGroupConfig(ctx, nameServer, options)
}

func (f nativeClientFunc) CleanExpiredCQ(ctx context.Context, nameServer string, options cleanExpiredCQOptions) (bool, error) {
	if f.cleanExpiredCQ == nil {
		return false, nil
	}
	return f.cleanExpiredCQ(ctx, nameServer, options)
}

func (f nativeClientFunc) CleanUnusedTopic(ctx context.Context, nameServer string, options cleanUnusedTopicOptions) (bool, error) {
	if f.cleanUnusedTopic == nil {
		return false, nil
	}
	return f.cleanUnusedTopic(ctx, nameServer, options)
}

func (f nativeClientFunc) DeleteExpiredCommitLog(ctx context.Context, nameServer string, options deleteExpiredCommitLogOptions) (bool, error) {
	if f.deleteExpiredCommitLog == nil {
		return false, nil
	}
	return f.deleteExpiredCommitLog(ctx, nameServer, options)
}

func (f nativeClientFunc) GetColdDataFlowCtrInfo(ctx context.Context, brokerAddr string) (string, error) {
	if f.getColdDataFlowCtrInfo == nil {
		return "", nil
	}
	return f.getColdDataFlowCtrInfo(ctx, brokerAddr)
}

func (f nativeClientFunc) GetColdDataFlowCtrInfoByCluster(ctx context.Context, nameServer string, clusterName string) ([]coldDataFlowCtrInfoSection, error) {
	if f.getColdDataFlowCtrInfoByCluster == nil {
		return nil, nil
	}
	return f.getColdDataFlowCtrInfoByCluster(ctx, nameServer, clusterName)
}

func (f nativeClientFunc) ProducerConnection(ctx context.Context, nameServer string, producerGroup string, topic string) (*producerConnectionDetail, error) {
	return f.producerConnection(ctx, nameServer, producerGroup, topic)
}

func (f nativeClientFunc) QueryMessagesByKey(ctx context.Context, nameServer string, topic string, key string, clusterName string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageSearchResult, error) {
	return f.queryMessagesByKey(ctx, nameServer, topic, key, clusterName, beginTimestamp, endTimestamp, maxNum)
}

func (f nativeClientFunc) QueryMessageByOffset(ctx context.Context, nameServer string, topic string, brokerName string, queueID int, offset int64) (*messageDetail, error) {
	return f.queryMessageByOffset(ctx, nameServer, topic, brokerName, queueID, offset)
}

func (f nativeClientFunc) PrintMessages(ctx context.Context, nameServer string, options printMsgOptions) (*printMsgResult, error) {
	return f.printMessages(ctx, nameServer, options)
}

func (f nativeClientFunc) PrintMessagesByQueue(ctx context.Context, nameServer string, options printMsgByQueueOptions) (*printMsgByQueueResult, error) {
	return f.printMessagesByQueue(ctx, nameServer, options)
}

func (f nativeClientFunc) ConsumeMessages(ctx context.Context, nameServer string, options consumeMessageOptions) (*consumeMessageResult, error) {
	return f.consumeMessages(ctx, nameServer, options)
}

func (f nativeClientFunc) QueryMessageByID(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
	return f.queryMessageByID(ctx, nameServer, topic, clusterName, msgID)
}

func (f nativeClientFunc) MessageTrackDetail(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error) {
	if f.messageTrackDetail == nil {
		return nil, nil
	}
	return f.messageTrackDetail(ctx, nameServer, detail)
}

func (f nativeClientFunc) QueryMessageByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
	return f.queryMessageByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
}

func (f nativeClientFunc) QueryMessagesByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) ([]messageDetail, error) {
	return f.queryMessagesByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
}

func (f nativeClientFunc) ConsumeMessageDirectly(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error) {
	return f.consumeMessageDirectly(ctx, nameServer, consumerGroup, clientID, topic, clusterName, msgID)
}

func (f nativeClientFunc) ConsumeMessageDirectlyByID(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error) {
	return f.consumeMessageDirectlyByID(ctx, nameServer, consumerGroup, clientID, topic, clusterName, msgID)
}

func (f nativeClientFunc) ResendMessageByID(ctx context.Context, nameServer string, topic string, clusterName string, msgID string, unitName string) (*queryMsgByIDResendResult, error) {
	return f.resendMessageByID(ctx, nameServer, topic, clusterName, msgID, unitName)
}

func (f nativeClientFunc) QueryMessageTraceByID(ctx context.Context, nameServer string, traceTopic string, msgID string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageTraceView, error) {
	return f.queryMessageTraceByID(ctx, nameServer, traceTopic, msgID, beginTimestamp, endTimestamp, maxNum)
}

func (f nativeClientFunc) ConsumerProgress(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
	return f.consumerProgress(ctx, nameServer, consumerGroup, topic, clusterName)
}

func (f nativeClientFunc) ConsumerProgressWithClientIP(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
	return f.consumerProgressWithClientIP(ctx, nameServer, consumerGroup, topic, clusterName)
}

func (f nativeClientFunc) ConsumerProgressSummary(ctx context.Context, nameServer string) ([]consumerProgressSummaryRow, error) {
	return f.consumerProgressSummary(ctx, nameServer)
}

func (f nativeClientFunc) StartMonitoring(ctx context.Context, nameServer string) error {
	return f.startMonitoring(ctx, nameServer)
}

func (f nativeClientFunc) ConsumerConnectionSummary(ctx context.Context, nameServer string, consumerGroup string) (*consumerConnectionSummary, error) {
	if f.consumerConnectionSummary == nil {
		return nil, nil
	}
	return f.consumerConnectionSummary(ctx, nameServer, consumerGroup)
}

func printMsgByQueueDetailForTest() messageDetail {
	properties := messageProperties{}
	properties.Set("MSG_REGION", "DefaultRegion")
	properties.Set("UNIQ_KEY", "UNIQ-1")
	properties.Set("CLUSTER", "DefaultCluster")
	properties.Set("MIN_OFFSET", "0")
	properties.Set("KEYS", "OrderKey")
	properties.Set("WAIT", "true")
	properties.Set("TRACE_ON", "true")
	properties.Set("MAX_OFFSET", "8")
	properties.Set("TAGS", "TagA")
	return messageDetail{
		OffsetMessageID:           "AC18000300002A9F00000000000003E8",
		DisplayMessageID:          "UNIQ-1",
		BrokerName:                "broker-a",
		Topic:                     "TopicTest",
		Tags:                      "TagA",
		Keys:                      "OrderKey",
		QueueID:                   0,
		QueueOffset:               7,
		CommitLogOffset:           1000,
		StoreSize:                 128,
		BodyCRC:                   131628133,
		Flag:                      0,
		ReconsumeTimes:            0,
		PreparedTransactionOffset: 0,
		BornTimestamp:             1780891116911,
		StoreTimestamp:            1780891116954,
		BornHost:                  "172.24.0.4:48298",
		StoreHost:                 "172.24.0.3:10911",
		SysFlag:                   0,
		Properties:                properties,
		Body:                      []byte("hello"),
	}
}

type queryMessageRecordFixture struct {
	Topic           string
	Keys            string
	UniqKey         string
	QueueID         int32
	QueueOffset     int64
	CommitLogOffset int64
	Body            []byte
	BodyCRC         int32
	BornTimestamp   int64
	StoreTimestamp  int64
	BornHostIP      []byte
	BornHostPort    int32
	Flag            int32
	SysFlag         int32
	ReconsumeTimes  int32
	PreparedOffset  int64
	StoreHostIP     []byte
	StoreHostPort   int32
}

func queryMessageRecordForTest(t *testing.T, fixture queryMessageRecordFixture) []byte {
	t.Helper()
	properties := "KEYS" + string(rune(1)) + fixture.Keys + string(rune(2)) +
		"UNIQ_KEY" + string(rune(1)) + fixture.UniqKey + string(rune(2))
	topicBytes := []byte(fixture.Topic)
	propertiesBytes := []byte(properties)
	if len(topicBytes) > 255 {
		t.Fatalf("test topic too long for message magic v1: %d", len(topicBytes))
	}
	body := fixture.Body
	bornTimestamp := fixture.BornTimestamp
	storeTimestamp := fixture.StoreTimestamp
	bornHostIP := fixture.BornHostIP
	if len(bornHostIP) == 0 {
		bornHostIP = []byte{127, 0, 0, 1}
	}
	bornHostPort := fixture.BornHostPort
	if bornHostPort == 0 {
		bornHostPort = 1000
	}
	sysFlag := fixture.SysFlag
	recordSize := 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + len(bornHostIP) + 4 + 8 + len(fixture.StoreHostIP) + 4 + 4 + 8 + 4 + len(body) + 1 + len(topicBytes) + 2 + len(propertiesBytes)
	record := make([]byte, 0, recordSize)
	magicCode := int32(messageMagicCodeV1)
	record = binary.BigEndian.AppendUint32(record, uint32(recordSize))
	record = binary.BigEndian.AppendUint32(record, uint32(magicCode))
	record = binary.BigEndian.AppendUint32(record, uint32(fixture.BodyCRC))
	record = binary.BigEndian.AppendUint32(record, uint32(fixture.QueueID))
	record = binary.BigEndian.AppendUint32(record, uint32(fixture.Flag))
	record = binary.BigEndian.AppendUint64(record, uint64(fixture.QueueOffset))
	record = binary.BigEndian.AppendUint64(record, uint64(fixture.CommitLogOffset))
	record = binary.BigEndian.AppendUint32(record, uint32(sysFlag))
	record = binary.BigEndian.AppendUint64(record, uint64(bornTimestamp))
	record = append(record, bornHostIP...)
	record = binary.BigEndian.AppendUint32(record, uint32(bornHostPort))
	record = binary.BigEndian.AppendUint64(record, uint64(storeTimestamp))
	record = append(record, fixture.StoreHostIP...)
	record = binary.BigEndian.AppendUint32(record, uint32(fixture.StoreHostPort))
	record = binary.BigEndian.AppendUint32(record, uint32(fixture.ReconsumeTimes))
	record = binary.BigEndian.AppendUint64(record, uint64(fixture.PreparedOffset))
	record = binary.BigEndian.AppendUint32(record, uint32(len(body)))
	record = append(record, body...)
	record = append(record, byte(len(topicBytes)))
	record = append(record, topicBytes...)
	record = binary.BigEndian.AppendUint16(record, uint16(len(propertiesBytes)))
	record = append(record, propertiesBytes...)
	if len(record) != recordSize {
		t.Fatalf("test record size mismatch expected=%d actual=%d", recordSize, len(record))
	}
	return record
}

type testError struct {
	message string
}

func (e *testError) Error() string {
	return e.message
}

func TestNativeClientDefaultsTimeout(t *testing.T) {
	client := NewClient(0)
	if client.timeout != 3*time.Second {
		t.Fatalf("expected default timeout, got %s", client.timeout)
	}
}
