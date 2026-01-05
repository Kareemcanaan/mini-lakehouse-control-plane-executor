package metadata

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	pb "mini-lakehouse/proto/gen"

	"github.com/hashicorp/raft"
	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// **Feature: mini-lakehouse, Property 6: Concurrent commit exclusion**
// *For any* set of concurrent commit attempts with the same base version, exactly one should succeed and the others should be rejected
// **Validates: Requirements 2.2**
func TestProperty6ConcurrentCommitExclusion(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("concurrent commits with same base version have exactly one success", prop.ForAll(
		func(numConcurrentCommits int, seed int64) bool {
			// Ensure we have at least 2 concurrent commits and not too many
			if numConcurrentCommits < 2 {
				numConcurrentCommits = 2
			}
			if numConcurrentCommits > 10 {
				numConcurrentCommits = 10
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
						{Name: "value", Type: "string", Nullable: true},
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

			// Prepare concurrent commit commands with the same base version
			var wg sync.WaitGroup
			results := make([]interface{}, numConcurrentCommits)

			// Execute concurrent commits
			for i := 0; i < numConcurrentCommits; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()

					// Each commit has a unique transaction ID and file
					txnID := fmt.Sprintf("txn_%d_%d", seed, index)
					filePath := fmt.Sprintf("data/part-%d-%d.parquet", seed, index)

					commitCmd := CommitCommand{
						TableName:   tableName,
						BaseVersion: baseVersion, // Same base version for all
						TxnID:       txnID,
						Adds: []*pb.FileAdd{
							{
								Path: filePath,
								Rows: uint64(100 + index),
								Size: uint64(1000 + index*100),
								Partition: map[string]string{
									"batch": fmt.Sprintf("batch_%d", index),
								},
								Stats: &pb.FileStats{
									MinValues: map[string]string{"id": fmt.Sprintf("%d", index*100)},
									MaxValues: map[string]string{"id": fmt.Sprintf("%d", (index+1)*100)},
								},
							},
						},
						Removes: []*pb.FileRemove{},
					}

					// Create the command to apply through Raft
					cmd := Command{
						Type: "commit",
						Data: commitCmd,
					}

					// Serialize the command as Raft would
					cmdBytes, err := json.Marshal(cmd)
					if err != nil {
						results[index] = fmt.Errorf("failed to marshal command: %v", err)
						return
					}

					// Apply through the FSM Apply method (simulating Raft)
					log := &raft.Log{Data: cmdBytes}
					results[index] = fsm.Apply(log)
				}(i)
			}

			// Wait for all commits to complete
			wg.Wait()

			// Analyze results: exactly one should succeed, others should fail
			successCount := 0
			failureCount := 0
			var successVersion uint64

			for i, result := range results {
				if resultMap, ok := result.(map[string]interface{}); ok {
					// Success case
					successCount++
					if newVersion, exists := resultMap["new_version"]; exists {
						if v, ok := newVersion.(uint64); ok {
							if successCount == 1 {
								successVersion = v
							} else if successVersion != v {
								t.Logf("Multiple successes with different versions: %d vs %d", successVersion, v)
								return false
							}
						}
					}
				} else if err, ok := result.(error); ok {
					// Failure case - should be optimistic concurrency error
					if err.Error() == "" {
						t.Logf("Commit %d failed with empty error", i)
						return false
					}
					failureCount++
				} else {
					t.Logf("Commit %d returned unexpected result type: %T", i, result)
					return false
				}
			}

			// Verify exactly one success
			if successCount != 1 {
				t.Logf("Expected exactly 1 success, got %d successes and %d failures", successCount, failureCount)
				return false
			}

			if failureCount != numConcurrentCommits-1 {
				t.Logf("Expected %d failures, got %d", numConcurrentCommits-1, failureCount)
				return false
			}

			// Verify the table version was incremented exactly once
			finalTable, exists := fsm.GetTable(tableName)
			if !exists {
				t.Logf("Table disappeared after commits")
				return false
			}

			expectedVersion := baseVersion + 1
			if finalTable.LatestVersion != expectedVersion {
				t.Logf("Expected final version %d, got %d", expectedVersion, finalTable.LatestVersion)
				return false
			}

			if successVersion != expectedVersion {
				t.Logf("Success version %d doesn't match expected %d", successVersion, expectedVersion)
				return false
			}

			return true
		},
		goptergen.IntRange(2, 10),      // Number of concurrent commits
		goptergen.Int64Range(1, 10000), // Seed for deterministic generation
	))

	properties.TestingRun(t)
}

// Additional test for concurrent commits with different scenarios
func TestProperty6ConcurrentCommitExclusionEdgeCases(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	properties := gopter.NewProperties(parameters)

	properties.Property("concurrent commit exclusion with edge cases", prop.ForAll(
		func(seed int64) bool {
			// Test specific scenarios that might reveal race conditions
			testCases := []struct {
				name         string
				numCommits   int
				baseVersion  uint64
				setupCommits int // Number of commits to make before the concurrent test
			}{
				{
					name:         "two commits on fresh table",
					numCommits:   2,
					baseVersion:  0,
					setupCommits: 0,
				},
				{
					name:         "multiple commits on fresh table",
					numCommits:   5,
					baseVersion:  0,
					setupCommits: 0,
				},
				{
					name:         "commits on table with existing versions",
					numCommits:   3,
					baseVersion:  2,
					setupCommits: 2,
				},
				{
					name:         "many commits on versioned table",
					numCommits:   8,
					baseVersion:  5,
					setupCommits: 5,
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
						},
					},
				})

				if createResult != nil {
					t.Logf("Test case '%s': Failed to create table: %v", testCase.name, createResult)
					return false
				}

				// Setup: make some commits to reach the desired base version
				for i := 0; i < testCase.setupCommits; i++ {
					setupCmd := Command{
						Type: "commit",
						Data: CommitCommand{
							TableName:   tableName,
							BaseVersion: uint64(i),
							TxnID:       fmt.Sprintf("setup_txn_%d_%d", seed, i),
							Adds: []*pb.FileAdd{
								{
									Path: fmt.Sprintf("setup/file_%d.parquet", i),
									Rows: 100,
									Size: 1000,
								},
							},
						},
					}

					// Serialize and apply through FSM Apply method
					cmdBytes, err := json.Marshal(setupCmd)
					if err != nil {
						t.Logf("Test case '%s': Failed to marshal setup command %d: %v", testCase.name, i, err)
						return false
					}

					log := &raft.Log{Data: cmdBytes}
					setupResult := fsm.Apply(log)

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

				// Execute concurrent commits
				var wg sync.WaitGroup
				results := make([]interface{}, testCase.numCommits)

				for i := 0; i < testCase.numCommits; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()

						txnID := fmt.Sprintf("concurrent_txn_%s_%d_%d", testCase.name, seed, index)
						filePath := fmt.Sprintf("concurrent/file_%d.parquet", index)

						cmd := Command{
							Type: "commit",
							Data: CommitCommand{
								TableName:   tableName,
								BaseVersion: testCase.baseVersion,
								TxnID:       txnID,
								Adds: []*pb.FileAdd{
									{
										Path: filePath,
										Rows: uint64(50 + index),
										Size: uint64(500 + index*50),
									},
								},
							},
						}

						// Serialize and apply through FSM Apply method
						cmdBytes, err := json.Marshal(cmd)
						if err != nil {
							results[index] = fmt.Errorf("failed to marshal command: %v", err)
							return
						}

						log := &raft.Log{Data: cmdBytes}
						results[index] = fsm.Apply(log)
					}(i)
				}

				wg.Wait()

				// Count successes and failures
				successCount := 0
				for _, result := range results {
					if _, ok := result.(map[string]interface{}); ok {
						successCount++
					}
				}

				if successCount != 1 {
					t.Logf("Test case '%s': Expected exactly 1 success, got %d", testCase.name, successCount)
					return false
				}

				// Verify final version
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
			}

			return true
		},
		goptergen.Int64Range(1, 1000), // Seed for test case generation
	))

	properties.TestingRun(t)
}
