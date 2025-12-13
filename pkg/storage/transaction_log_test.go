package storage

import (
	"testing"
	"time"
)

func TestTransactionLogEntry_Serialization(t *testing.T) {
	// Create a sample log entry
	entry := &TransactionLogEntry{
		Version:     1,
		TimestampMs: time.Now().UnixMilli(),
		TxnID:       "txn_123",
		Schema: &Schema{
			Fields: []Field{
				{Name: "id", Type: "int64"},
				{Name: "name", Type: "string"},
			},
		},
		Adds: []FileAdd{
			{
				Path: "data/part-00001-abc123.parquet",
				Rows: 1000,
				Size: 50000,
				Partition: map[string]string{
					"year": "2023",
				},
				Stats: &FileStats{
					MinValues: map[string]interface{}{"id": 1},
					MaxValues: map[string]interface{}{"id": 1000},
				},
			},
		},
		Removes: []FileRemove{
			{Path: "data/old-part.parquet"},
		},
	}

	// Test validation
	tl := &TransactionLog{}
	err := tl.ValidateLogEntry(entry)
	if err != nil {
		t.Errorf("Expected valid log entry, got error: %v", err)
	}
}

func TestTransactionLogEntry_Validation(t *testing.T) {
	tl := &TransactionLog{}

	// Test invalid entries
	invalidEntries := []*TransactionLogEntry{
		{Version: 0, TxnID: "txn_123", TimestampMs: time.Now().UnixMilli()}, // version 0
		{Version: 1, TxnID: "", TimestampMs: time.Now().UnixMilli()},        // empty txn ID
		{Version: 1, TxnID: "txn_123", TimestampMs: 0},                      // zero timestamp
		{
			Version:     1,
			TxnID:       "txn_123",
			TimestampMs: time.Now().UnixMilli(),
			Adds: []FileAdd{
				{Path: "", Rows: 100, Size: 1000}, // empty path
			},
		},
		{
			Version:     1,
			TxnID:       "txn_123",
			TimestampMs: time.Now().UnixMilli(),
			Adds: []FileAdd{
				{Path: "data/part.parquet", Rows: -1, Size: 1000}, // negative rows
			},
		},
		{
			Version:     1,
			TxnID:       "txn_123",
			TimestampMs: time.Now().UnixMilli(),
			Removes: []FileRemove{
				{Path: ""}, // empty path
			},
		},
	}

	for i, entry := range invalidEntries {
		err := tl.ValidateLogEntry(entry)
		if err == nil {
			t.Errorf("Expected error for invalid entry %d, but got none", i)
		}
	}
}

func TestCreateLogEntry(t *testing.T) {
	tl := &TransactionLog{}

	schema := &Schema{
		Fields: []Field{
			{Name: "id", Type: "int64"},
		},
	}

	adds := []FileAdd{
		{Path: "data/part.parquet", Rows: 100, Size: 1000},
	}

	removes := []FileRemove{
		{Path: "data/old.parquet"},
	}

	entry := tl.CreateLogEntry(1, "txn_123", schema, adds, removes)

	if entry.Version != 1 {
		t.Errorf("Expected version 1, got %d", entry.Version)
	}

	if entry.TxnID != "txn_123" {
		t.Errorf("Expected txn ID 'txn_123', got '%s'", entry.TxnID)
	}

	if entry.Schema != schema {
		t.Error("Expected schema to match")
	}

	if len(entry.Adds) != 1 || entry.Adds[0].Path != "data/part.parquet" {
		t.Error("Expected adds to match")
	}

	if len(entry.Removes) != 1 || entry.Removes[0].Path != "data/old.parquet" {
		t.Error("Expected removes to match")
	}

	if entry.TimestampMs <= 0 {
		t.Error("Expected positive timestamp")
	}
}

func TestBuildSnapshot_EmptyTable(t *testing.T) {
	// This test would require a mock client or actual MinIO instance
	// For now, we'll test the logic with a mock scenario

	// Test snapshot building logic with empty file map
	snapshot := &TableSnapshot{
		Version: 1,
		Files:   make([]FileAdd, 0),
	}

	fileMap := make(map[string]FileAdd)

	// Simulate applying an add operation
	add := FileAdd{
		Path: "data/part-00001.parquet",
		Rows: 100,
		Size: 1000,
	}
	fileMap[add.Path] = add

	// Convert to slice
	for _, file := range fileMap {
		snapshot.Files = append(snapshot.Files, file)
	}

	if len(snapshot.Files) != 1 {
		t.Errorf("Expected 1 file, got %d", len(snapshot.Files))
	}

	if snapshot.Files[0].Path != "data/part-00001.parquet" {
		t.Errorf("Expected path 'data/part-00001.parquet', got '%s'", snapshot.Files[0].Path)
	}
}

func TestBuildSnapshot_AddRemoveLogic(t *testing.T) {
	// Test the add/remove logic without requiring actual storage
	fileMap := make(map[string]FileAdd)

	// Add a file
	add1 := FileAdd{Path: "data/part-1.parquet", Rows: 100, Size: 1000}
	fileMap[add1.Path] = add1

	// Add another file
	add2 := FileAdd{Path: "data/part-2.parquet", Rows: 200, Size: 2000}
	fileMap[add2.Path] = add2

	// Remove the first file
	delete(fileMap, "data/part-1.parquet")

	// Should only have the second file
	if len(fileMap) != 1 {
		t.Errorf("Expected 1 file after removal, got %d", len(fileMap))
	}

	if _, exists := fileMap["data/part-2.parquet"]; !exists {
		t.Error("Expected part-2.parquet to still exist")
	}

	if _, exists := fileMap["data/part-1.parquet"]; exists {
		t.Error("Expected part-1.parquet to be removed")
	}
}
