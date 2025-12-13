package storage

import (
	"testing"
)

func TestPathBuilder_TableDataPath(t *testing.T) {
	pb := NewPathBuilder()

	path := pb.TableDataPath("users", "00001", "abc123")
	expected := "tables/users/data/part-00001-abc123.parquet"

	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}

func TestPathBuilder_TransactionLogPath(t *testing.T) {
	pb := NewPathBuilder()

	path := pb.TransactionLogPath("users", 42)
	expected := "tables/users/_log/00000000000000000042.json"

	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}

func TestPathBuilder_ShuffleOutputPath(t *testing.T) {
	pb := NewPathBuilder()

	path := pb.ShuffleOutputPath("job123", "stage0", 17, 1, 5)
	expected := "shuffle/job123/stage0/17/1/part-5.parquet"

	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}

func TestPathBuilder_TempDataPath(t *testing.T) {
	pb := NewPathBuilder()

	path := pb.TempDataPath("users", "txn_abc", 2, "00001", "uuid123")
	expected := "tables/users/_tmp/txn_abc/2/part-00001-uuid123.parquet"

	if path != expected {
		t.Errorf("Expected %s, got %s", expected, path)
	}
}

func TestPathBuilder_ParseTableDataPath(t *testing.T) {
	pb := NewPathBuilder()

	path := "tables/users/data/part-00001-abc123.parquet"
	tableName, partID, uuid, err := pb.ParseTableDataPath(path)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if tableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", tableName)
	}

	if partID != "00001" {
		t.Errorf("Expected part ID '00001', got '%s'", partID)
	}

	if uuid != "abc123" {
		t.Errorf("Expected UUID 'abc123', got '%s'", uuid)
	}
}

func TestPathBuilder_ParseTableDataPath_Invalid(t *testing.T) {
	pb := NewPathBuilder()

	invalidPaths := []string{
		"invalid/path",
		"tables/users/wrong/part-00001-abc123.parquet",
		"tables/users/data/invalid-format.parquet",
		"tables/users/data/part-00001.parquet", // missing UUID
	}

	for _, path := range invalidPaths {
		_, _, _, err := pb.ParseTableDataPath(path)
		if err == nil {
			t.Errorf("Expected error for invalid path: %s", path)
		}
	}
}

func TestPathBuilder_ParseTransactionLogPath(t *testing.T) {
	pb := NewPathBuilder()

	path := "tables/users/_log/00000000000000000042.json"
	tableName, version, err := pb.ParseTransactionLogPath(path)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if tableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", tableName)
	}

	if version != 42 {
		t.Errorf("Expected version 42, got %d", version)
	}
}

func TestPathBuilder_ParseTransactionLogPath_Invalid(t *testing.T) {
	pb := NewPathBuilder()

	invalidPaths := []string{
		"invalid/path",
		"tables/users/wrong/00000000000000000042.json",
		"tables/users/_log/invalid.json",
		"tables/users/_log/42.json", // wrong format
	}

	for _, path := range invalidPaths {
		_, _, err := pb.ParseTransactionLogPath(path)
		if err == nil {
			t.Errorf("Expected error for invalid path: %s", path)
		}
	}
}
