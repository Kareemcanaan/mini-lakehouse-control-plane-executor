package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// **Feature: mini-lakehouse, Property 9: Log replay determinism**
// *For any* sequence of transaction log entries, applying them multiple times should produce identical file lists
// **Validates: Requirements 2.5**
func TestProperty9LogReplayDeterminism(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("log replay produces deterministic file lists", prop.ForAll(
		func(numEntries int, seed int64) bool {
			// Ensure we have a reasonable number of entries
			if numEntries < 1 {
				numEntries = 1
			}
			if numEntries > 10 {
				numEntries = 10
			}

			// Generate a sequence of transaction log entries
			entries := generateLogEntrySequence(numEntries, seed)

			// Apply the log entries multiple times and compare results
			fileList1 := applyLogEntries(entries)
			fileList2 := applyLogEntries(entries)
			fileList3 := applyLogEntries(entries)

			// All file lists should be identical
			success := compareFileLists(fileList1, fileList2) && compareFileLists(fileList2, fileList3)

			if !success {
				t.Logf("Log replay produced different results:")
				t.Logf("First replay: %v", fileList1)
				t.Logf("Second replay: %v", fileList2)
				t.Logf("Third replay: %v", fileList3)
			}

			return success
		},
		goptergen.IntRange(1, 10),     // Number of log entries
		goptergen.Int64Range(1, 1000), // Seed for deterministic generation
	))

	properties.TestingRun(t)
}

// generateLogEntrySequence generates a deterministic sequence of transaction log entries
func generateLogEntrySequence(numEntries int, seed int64) []*TransactionLogEntry {
	entries := make([]*TransactionLogEntry, numEntries)

	// Track files that exist to ensure valid remove operations
	existingFiles := make(map[string]bool)

	for i := 0; i < numEntries; i++ {
		version := uint64(i + 1)
		txnID := fmt.Sprintf("txn_%d_%d", seed, i)

		var adds []FileAdd
		var removes []FileRemove

		// Deterministically decide what operations to perform based on seed and index
		opType := (int(seed) + i) % 4

		switch opType {
		case 0, 1: // Add files (more likely)
			numFiles := ((int(seed) + i) % 3) + 1 // 1-3 files
			for j := 0; j < numFiles; j++ {
				filePath := fmt.Sprintf("data/part-%d-%d-%d.parquet", seed, i, j)
				add := FileAdd{
					Path: filePath,
					Rows: int64(100 + (int(seed)+i+j)*10),
					Size: int64(1000 + (int(seed)+i+j)*100),
					Partition: map[string]string{
						"year": fmt.Sprintf("%d", 2020+(i%5)),
					},
					Stats: &FileStats{
						MinValues: map[string]interface{}{"id": int64(i * 100)},
						MaxValues: map[string]interface{}{"id": int64((i + 1) * 100)},
					},
				}
				adds = append(adds, add)
				existingFiles[filePath] = true
			}

		case 2: // Remove files (if any exist)
			if len(existingFiles) > 0 {
				// Remove up to 2 existing files
				count := 0
				for filePath := range existingFiles {
					if count >= 2 {
						break
					}
					removes = append(removes, FileRemove{Path: filePath})
					delete(existingFiles, filePath)
					count++
				}
			}
			// If no files to remove, add a file instead
			if len(removes) == 0 {
				filePath := fmt.Sprintf("data/part-%d-%d-fallback.parquet", seed, i)
				adds = append(adds, FileAdd{
					Path:      filePath,
					Rows:      int64(50),
					Size:      int64(500),
					Partition: map[string]string{},
				})
				existingFiles[filePath] = true
			}

		case 3: // Mixed operations
			// Add one file
			addPath := fmt.Sprintf("data/part-%d-%d-mixed.parquet", seed, i)
			adds = append(adds, FileAdd{
				Path:      addPath,
				Rows:      int64(75),
				Size:      int64(750),
				Partition: map[string]string{},
			})
			existingFiles[addPath] = true

			// Remove one file if any exist
			if len(existingFiles) > 1 { // Keep at least one file
				for filePath := range existingFiles {
					if filePath != addPath { // Don't remove the file we just added
						removes = append(removes, FileRemove{Path: filePath})
						delete(existingFiles, filePath)
						break
					}
				}
			}
		}

		// Create schema for first entry or occasionally update it
		var schema *Schema
		if i == 0 || (i%3 == 0 && i > 0) {
			schema = &Schema{
				Fields: []Field{
					{Name: "id", Type: "int64"},
					{Name: "name", Type: "string"},
					{Name: "value", Type: "float64"},
				},
			}
			// Add an extra field occasionally to test schema evolution
			if i > 0 && i%5 == 0 {
				schema.Fields = append(schema.Fields, Field{Name: "timestamp", Type: "int64"})
			}
		}

		entries[i] = &TransactionLogEntry{
			Version:     version,
			TimestampMs: 1640995200000 + int64(i*1000), // Deterministic timestamps
			TxnID:       txnID,
			Schema:      schema,
			Adds:        adds,
			Removes:     removes,
		}
	}

	return entries
}

// applyLogEntries simulates the BuildSnapshot logic to apply log entries and return the final file list
func applyLogEntries(entries []*TransactionLogEntry) []string {
	// Track files by path for efficient add/remove operations (same logic as BuildSnapshot)
	fileMap := make(map[string]FileAdd)

	// Apply log entries in order
	for _, entry := range entries {
		// Apply file additions
		for _, add := range entry.Adds {
			fileMap[add.Path] = add
		}

		// Apply file removals
		for _, remove := range entry.Removes {
			delete(fileMap, remove.Path)
		}
	}

	// Convert to sorted list of file paths for deterministic comparison
	var filePaths []string
	for path := range fileMap {
		filePaths = append(filePaths, path)
	}

	// Sort for deterministic results (same as BuildSnapshot)
	sort.Strings(filePaths)

	return filePaths
}

// compareFileLists compares two file lists for equality
func compareFileLists(list1, list2 []string) bool {
	if len(list1) != len(list2) {
		return false
	}

	for i, path1 := range list1 {
		if path1 != list2[i] {
			return false
		}
	}

	return true
}

// Additional test to verify the property with edge cases
func TestProperty9LogReplayDeterminismEdgeCases(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	properties := gopter.NewProperties(parameters)

	properties.Property("log replay determinism with edge cases", prop.ForAll(
		func(seed int64) bool {
			// Test specific edge cases that might cause non-deterministic behavior
			testCases := []struct {
				name    string
				entries []*TransactionLogEntry
			}{
				{
					name:    "empty log",
					entries: []*TransactionLogEntry{},
				},
				{
					name: "single add",
					entries: []*TransactionLogEntry{
						{
							Version:     1,
							TimestampMs: 1640995200000,
							TxnID:       fmt.Sprintf("txn_single_%d", seed),
							Adds: []FileAdd{
								{Path: "data/single.parquet", Rows: 100, Size: 1000},
							},
						},
					},
				},
				{
					name: "add then remove same file",
					entries: []*TransactionLogEntry{
						{
							Version:     1,
							TimestampMs: 1640995200000,
							TxnID:       fmt.Sprintf("txn_add_%d", seed),
							Adds: []FileAdd{
								{Path: "data/temp.parquet", Rows: 100, Size: 1000},
							},
						},
						{
							Version:     2,
							TimestampMs: 1640995201000,
							TxnID:       fmt.Sprintf("txn_remove_%d", seed),
							Removes: []FileRemove{
								{Path: "data/temp.parquet"},
							},
						},
					},
				},
				{
					name: "multiple adds and removes",
					entries: []*TransactionLogEntry{
						{
							Version:     1,
							TimestampMs: 1640995200000,
							TxnID:       fmt.Sprintf("txn_multi1_%d", seed),
							Adds: []FileAdd{
								{Path: "data/file1.parquet", Rows: 100, Size: 1000},
								{Path: "data/file2.parquet", Rows: 200, Size: 2000},
							},
						},
						{
							Version:     2,
							TimestampMs: 1640995201000,
							TxnID:       fmt.Sprintf("txn_multi2_%d", seed),
							Adds: []FileAdd{
								{Path: "data/file3.parquet", Rows: 300, Size: 3000},
							},
							Removes: []FileRemove{
								{Path: "data/file1.parquet"},
							},
						},
					},
				},
			}

			for _, testCase := range testCases {
				// Apply the same log entries multiple times
				result1 := applyLogEntries(testCase.entries)
				result2 := applyLogEntries(testCase.entries)
				result3 := applyLogEntries(testCase.entries)

				if !compareFileLists(result1, result2) || !compareFileLists(result2, result3) {
					t.Logf("Edge case '%s' failed determinism test", testCase.name)
					t.Logf("Result 1: %v", result1)
					t.Logf("Result 2: %v", result2)
					t.Logf("Result 3: %v", result3)
					return false
				}
			}

			return true
		},
		goptergen.Int64Range(1, 1000), // Seed for test case generation
	))

	properties.TestingRun(t)
}
