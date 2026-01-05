package metadata

import (
	"fmt"
	"testing"

	pb "mini-lakehouse/proto/gen"

	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// **Feature: mini-lakehouse, Property 10: Commit idempotency**
// *For any* commit request with a transaction ID, retrying it should return the same version without creating duplicate changes
// **Validates: Requirements 2.6**
func TestProperty10CommitIdempotency(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("commit retry with same transaction ID returns same version", prop.ForAll(
		func(numRetries int, seed int64) bool {
			// Ensure reasonable number of retries
			if numRetries < 1 {
				numRetries = 1
			}
			if numRetries > 10 {
				numRetries = 10
			}

			// Create a fresh state machine for each test
			fsm := NewStateMachine()

			// Create a test table
			tableName := fmt.Sprintf("test_table_%d", seed)
			createResult := fsm.applyCreateTable(CreateTableCommand{
				TableName: tableName,
				Schema: &pb.Schema{
					Fields: []*pb.Field{
						{Name: "id", Type: "int64", Nullable: false},
						{Name: "name", Type: "string", Nullable: true},
						{Name: "value", Type: "float64", Nullable: true},
					},
				},
			})

			if createResult != nil {
				t.Logf("Failed to create table: %v", createResult)
				return false
			}

			// Get the current version (should be 0 for new table)
			table, exists := fsm.GetTable(tableName)
			if !exists {
				t.Logf("Table does not exist after creation")
				return false
			}
			baseVersion := table.LatestVersion

			// Prepare commit command
			txnID := fmt.Sprintf("txn_%d", seed)
			filePath := fmt.Sprintf("data/part-%d.parquet", seed)

			commitCmd := CommitCommand{
				TableName:   tableName,
				BaseVersion: baseVersion,
				TxnID:       txnID,
				Adds: []*pb.FileAdd{
					{
						Path: filePath,
						Rows: uint64(1000 + seed%1000),
						Size: uint64(50000 + seed%10000),
						Partition: map[string]string{
							"date": fmt.Sprintf("2024-01-%02d", (seed%28)+1),
						},
						Stats: &pb.FileStats{
							MinValues: map[string]string{
								"id":    fmt.Sprintf("%d", seed%1000),
								"value": "0.1",
							},
							MaxValues: map[string]string{
								"id":    fmt.Sprintf("%d", (seed%1000)+999),
								"value": "999.9",
							},
						},
					},
				},
				Removes: []*pb.FileRemove{},
			}

			// Execute the first commit
			firstResult := fsm.applyCommit(commitCmd)
			firstResultMap, ok := firstResult.(map[string]interface{})
			if !ok {
				t.Logf("First commit failed: %v", firstResult)
				return false
			}

			firstVersion, exists := firstResultMap["new_version"]
			if !exists {
				t.Logf("First commit result missing new_version")
				return false
			}

			firstVersionUint, ok := firstVersion.(uint64)
			if !ok {
				t.Logf("First commit new_version is not uint64: %T", firstVersion)
				return false
			}

			// Verify first commit is not marked as duplicate
			if duplicate, exists := firstResultMap["duplicate"]; exists {
				if duplicateBool, ok := duplicate.(bool); ok && duplicateBool {
					t.Logf("First commit should not be marked as duplicate")
					return false
				}
			}

			// Retry the same commit multiple times
			for i := 0; i < numRetries; i++ {
				retryResult := fsm.applyCommit(commitCmd)
				retryResultMap, ok := retryResult.(map[string]interface{})
				if !ok {
					t.Logf("Retry %d failed: %v", i+1, retryResult)
					return false
				}

				// Verify same version is returned
				retryVersion, exists := retryResultMap["new_version"]
				if !exists {
					t.Logf("Retry %d result missing new_version", i+1)
					return false
				}

				retryVersionUint, ok := retryVersion.(uint64)
				if !ok {
					t.Logf("Retry %d new_version is not uint64: %T", i+1, retryVersion)
					return false
				}

				if retryVersionUint != firstVersionUint {
					t.Logf("Retry %d returned different version: %d vs %d", i+1, retryVersionUint, firstVersionUint)
					return false
				}

				// Verify retry is marked as duplicate
				duplicate, exists := retryResultMap["duplicate"]
				if !exists {
					t.Logf("Retry %d result missing duplicate flag", i+1)
					return false
				}

				duplicateBool, ok := duplicate.(bool)
				if !ok {
					t.Logf("Retry %d duplicate flag is not bool: %T", i+1, duplicate)
					return false
				}

				if !duplicateBool {
					t.Logf("Retry %d should be marked as duplicate", i+1)
					return false
				}
			}

			// Verify table version was incremented only once
			finalTable, exists := fsm.GetTable(tableName)
			if !exists {
				t.Logf("Table disappeared after retries")
				return false
			}

			expectedVersion := baseVersion + 1
			if finalTable.LatestVersion != expectedVersion {
				t.Logf("Expected final version %d, got %d", expectedVersion, finalTable.LatestVersion)
				return false
			}

			if firstVersionUint != expectedVersion {
				t.Logf("First commit version %d doesn't match expected %d", firstVersionUint, expectedVersion)
				return false
			}

			// Verify snapshot contains exactly one file
			files, schema, err := fsm.GetSnapshot(tableName, expectedVersion)
			if err != nil {
				t.Logf("Failed to get snapshot: %v", err)
				return false
			}

			if len(files) != 1 {
				t.Logf("Expected exactly 1 file in snapshot, got %d", len(files))
				return false
			}

			if files[0].Path != filePath {
				t.Logf("Expected file path %s, got %s", filePath, files[0].Path)
				return false
			}

			if schema == nil {
				t.Logf("Schema should not be nil")
				return false
			}

			return true
		},
		goptergen.IntRange(1, 10),      // Number of retries
		goptergen.Int64Range(1, 10000), // Seed for deterministic generation
	))

	properties.TestingRun(t)
}

// Additional test for commit idempotency with edge cases
func TestProperty10CommitIdempotencyEdgeCases(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	properties := gopter.NewProperties(parameters)

	properties.Property("commit idempotency with edge cases", prop.ForAll(
		func(seed int64) bool {
			// Test specific scenarios for idempotency
			testCases := []struct {
				name           string
				baseVersion    uint64
				setupCommits   int
				hasFileRemoves bool
				multipleFiles  bool
			}{
				{
					name:           "simple add on fresh table",
					baseVersion:    0,
					setupCommits:   0,
					hasFileRemoves: false,
					multipleFiles:  false,
				},
				{
					name:           "add on versioned table",
					baseVersion:    3,
					setupCommits:   3,
					hasFileRemoves: false,
					multipleFiles:  false,
				},
				{
					name:           "multiple files add",
					baseVersion:    1,
					setupCommits:   1,
					hasFileRemoves: false,
					multipleFiles:  true,
				},
				{
					name:           "add and remove operations",
					baseVersion:    2,
					setupCommits:   2,
					hasFileRemoves: true,
					multipleFiles:  false,
				},
			}

			for _, testCase := range testCases {
				fsm := NewStateMachine()
				tableName := fmt.Sprintf("test_%s_%d", testCase.name, seed)

				// Create table
				createResult := fsm.applyCreateTable(CreateTableCommand{
					TableName: tableName,
					Schema: &pb.Schema{
						Fields: []*pb.Field{
							{Name: "id", Type: "int64", Nullable: false},
							{Name: "data", Type: "string", Nullable: true},
						},
					},
				})

				if createResult != nil {
					t.Logf("Test case '%s': Failed to create table: %v", testCase.name, createResult)
					return false
				}

				// Setup: make commits to reach the desired base version
				setupFiles := make([]string, 0)
				for i := 0; i < testCase.setupCommits; i++ {
					setupFile := fmt.Sprintf("setup/file_%d.parquet", i)
					setupFiles = append(setupFiles, setupFile)

					setupResult := fsm.applyCommit(CommitCommand{
						TableName:   tableName,
						BaseVersion: uint64(i),
						TxnID:       fmt.Sprintf("setup_txn_%d_%d", seed, i),
						Adds: []*pb.FileAdd{
							{
								Path: setupFile,
								Rows: 100,
								Size: 1000,
							},
						},
					})

					if _, ok := setupResult.(map[string]interface{}); !ok {
						t.Logf("Test case '%s': Setup commit %d failed: %v", testCase.name, i, setupResult)
						return false
					}
				}

				// Verify we're at the expected base version
				table, exists := fsm.GetTable(tableName)
				if !exists || table.LatestVersion != testCase.baseVersion {
					t.Logf("Test case '%s': Expected base version %d, got %d", testCase.name, testCase.baseVersion, table.LatestVersion)
					return false
				}

				// Prepare the idempotent commit
				txnID := fmt.Sprintf("idempotent_txn_%s_%d", testCase.name, seed)

				var adds []*pb.FileAdd
				var removes []*pb.FileRemove

				if testCase.multipleFiles {
					// Add multiple files
					for i := 0; i < 3; i++ {
						adds = append(adds, &pb.FileAdd{
							Path: fmt.Sprintf("multi/file_%d.parquet", i),
							Rows: uint64(200 + i*50),
							Size: uint64(2000 + i*500),
						})
					}
				} else {
					// Add single file
					adds = append(adds, &pb.FileAdd{
						Path: fmt.Sprintf("single/file_%d.parquet", seed%1000),
						Rows: uint64(300 + seed%100),
						Size: uint64(3000 + seed%1000),
					})
				}

				if testCase.hasFileRemoves && len(setupFiles) > 0 {
					// Remove one of the setup files
					removes = append(removes, &pb.FileRemove{
						Path: setupFiles[0],
					})
				}

				commitCmd := CommitCommand{
					TableName:   tableName,
					BaseVersion: testCase.baseVersion,
					TxnID:       txnID,
					Adds:        adds,
					Removes:     removes,
				}

				// Execute first commit
				firstResult := fsm.applyCommit(commitCmd)
				firstResultMap, ok := firstResult.(map[string]interface{})
				if !ok {
					t.Logf("Test case '%s': First commit failed: %v", testCase.name, firstResult)
					return false
				}

				firstVersion := firstResultMap["new_version"].(uint64)

				// Retry the commit 3 times
				for retry := 0; retry < 3; retry++ {
					retryResult := fsm.applyCommit(commitCmd)
					retryResultMap, ok := retryResult.(map[string]interface{})
					if !ok {
						t.Logf("Test case '%s': Retry %d failed: %v", testCase.name, retry+1, retryResult)
						return false
					}

					retryVersion := retryResultMap["new_version"].(uint64)
					if retryVersion != firstVersion {
						t.Logf("Test case '%s': Retry %d returned different version: %d vs %d", testCase.name, retry+1, retryVersion, firstVersion)
						return false
					}

					if !retryResultMap["duplicate"].(bool) {
						t.Logf("Test case '%s': Retry %d should be marked as duplicate", testCase.name, retry+1)
						return false
					}
				}

				// Verify final state
				finalTable, exists := fsm.GetTable(tableName)
				if !exists {
					t.Logf("Test case '%s': Table disappeared", testCase.name)
					return false
				}

				expectedFinalVersion := testCase.baseVersion + 1
				if finalTable.LatestVersion != expectedFinalVersion {
					t.Logf("Test case '%s': Expected final version %d, got %d", testCase.name, expectedFinalVersion, finalTable.LatestVersion)
					return false
				}

				// Verify snapshot consistency
				files, _, err := fsm.GetSnapshot(tableName, expectedFinalVersion)
				if err != nil {
					t.Logf("Test case '%s': Failed to get snapshot: %v", testCase.name, err)
					return false
				}

				expectedFileCount := len(setupFiles) + len(adds) - len(removes)
				if len(files) != expectedFileCount {
					t.Logf("Test case '%s': Expected %d files in snapshot, got %d", testCase.name, expectedFileCount, len(files))
					return false
				}
			}

			return true
		},
		goptergen.Int64Range(1, 1000), // Seed for test case generation
	))

	properties.TestingRun(t)
}
