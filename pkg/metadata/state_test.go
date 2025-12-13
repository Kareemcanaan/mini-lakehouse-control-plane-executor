package metadata

import (
	"testing"

	pb "mini-lakehouse/proto/gen"
)

func TestStateMachine_CreateTable(t *testing.T) {
	fsm := NewStateMachine()

	// Test create table
	result := fsm.applyCreateTable(CreateTableCommand{
		TableName: "test_table",
		Schema: &pb.Schema{
			Fields: []*pb.Field{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			},
		},
	})

	if result != nil {
		t.Fatalf("Expected nil result, got: %v", result)
	}

	// Verify table was created
	table, exists := fsm.GetTable("test_table")
	if !exists {
		t.Fatal("Table should exist after creation")
	}

	if table.Name != "test_table" {
		t.Errorf("Expected table name 'test_table', got: %s", table.Name)
	}

	if table.LatestVersion != 0 {
		t.Errorf("Expected latest version 0, got: %d", table.LatestVersion)
	}
}

func TestStateMachine_Commit(t *testing.T) {
	fsm := NewStateMachine()

	// Create table first
	fsm.applyCreateTable(CreateTableCommand{
		TableName: "test_table",
		Schema: &pb.Schema{
			Fields: []*pb.Field{
				{Name: "id", Type: "int64", Nullable: false},
			},
		},
	})

	// Test commit
	result := fsm.applyCommit(CommitCommand{
		TableName:   "test_table",
		BaseVersion: 0,
		TxnID:       "txn_123",
		Adds: []*pb.FileAdd{
			{
				Path: "data/part-001.parquet",
				Rows: 1000,
				Size: 50000,
			},
		},
		Removes: []*pb.FileRemove{},
	})

	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map result, got: %v", result)
	}

	newVersion, exists := resultMap["new_version"]
	if !exists {
		t.Fatal("Expected new_version in result")
	}

	if newVersion != uint64(1) {
		t.Errorf("Expected new version 1, got: %v", newVersion)
	}

	// Verify table version was updated
	table, _ := fsm.GetTable("test_table")
	if table.LatestVersion != 1 {
		t.Errorf("Expected latest version 1, got: %d", table.LatestVersion)
	}
}

func TestStateMachine_OptimisticConcurrency(t *testing.T) {
	fsm := NewStateMachine()

	// Create table first
	fsm.applyCreateTable(CreateTableCommand{
		TableName: "test_table",
		Schema:    &pb.Schema{Fields: []*pb.Field{{Name: "id", Type: "int64"}}},
	})

	// First commit should succeed
	result1 := fsm.applyCommit(CommitCommand{
		TableName:   "test_table",
		BaseVersion: 0,
		TxnID:       "txn_1",
		Adds:        []*pb.FileAdd{{Path: "file1.parquet", Rows: 100, Size: 1000}},
	})

	if _, ok := result1.(map[string]interface{}); !ok {
		t.Fatalf("First commit should succeed, got: %v", result1)
	}

	// Second commit with stale base version should fail
	result2 := fsm.applyCommit(CommitCommand{
		TableName:   "test_table",
		BaseVersion: 0, // Stale version
		TxnID:       "txn_2",
		Adds:        []*pb.FileAdd{{Path: "file2.parquet", Rows: 100, Size: 1000}},
	})

	if err, ok := result2.(error); !ok || err == nil {
		t.Fatalf("Second commit should fail with optimistic concurrency error, got: %v", result2)
	}
}

func TestStateMachine_IdempotentCommit(t *testing.T) {
	fsm := NewStateMachine()

	// Create table first
	fsm.applyCreateTable(CreateTableCommand{
		TableName: "test_table",
		Schema:    &pb.Schema{Fields: []*pb.Field{{Name: "id", Type: "int64"}}},
	})

	// First commit
	result1 := fsm.applyCommit(CommitCommand{
		TableName:   "test_table",
		BaseVersion: 0,
		TxnID:       "txn_123",
		Adds:        []*pb.FileAdd{{Path: "file1.parquet", Rows: 100, Size: 1000}},
	})

	resultMap1, ok := result1.(map[string]interface{})
	if !ok {
		t.Fatalf("First commit should succeed, got: %v", result1)
	}

	// Retry same transaction - should return same version
	result2 := fsm.applyCommit(CommitCommand{
		TableName:   "test_table",
		BaseVersion: 0,         // Same base version
		TxnID:       "txn_123", // Same transaction ID
		Adds:        []*pb.FileAdd{{Path: "file1.parquet", Rows: 100, Size: 1000}},
	})

	resultMap2, ok := result2.(map[string]interface{})
	if !ok {
		t.Fatalf("Retry commit should succeed, got: %v", result2)
	}

	if resultMap1["new_version"] != resultMap2["new_version"] {
		t.Errorf("Retry should return same version: %v vs %v", resultMap1["new_version"], resultMap2["new_version"])
	}

	if !resultMap2["duplicate"].(bool) {
		t.Error("Retry should be marked as duplicate")
	}
}
