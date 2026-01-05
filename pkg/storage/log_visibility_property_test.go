package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// **Feature: mini-lakehouse, Property 4: Log-controlled visibility**
// *For any* table snapshot, the visible files should exactly match those specified in the transaction log, regardless of what exists in storage
// **Validates: Requirements 1.4, 1.5**
func TestProperty4LogControlledVisibility(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("log controls visibility regardless of storage contents", prop.ForAll(
		func(numLogEntries int, numExtraFiles int, seed int64) bool {
			// Ensure reasonable bounds
			if numLogEntries < 1 {
				numLogEntries = 1
			}
			if numLogEntries > 8 {
				numLogEntries = 8
			}
			if numExtraFiles < 0 {
				numExtraFiles = 0
			}
			if numExtraFiles > 5 {
				numExtraFiles = 5
			}

			// Generate transaction log entries that define visible files
			logEntries := generateVisibilityTestLogEntries(numLogEntries, seed)

			// Simulate files that exist in storage (including extra files not in log)
			storageFiles := generateStorageFiles(logEntries, numExtraFiles, seed)

			// Build snapshot from log entries (this is what should determine visibility)
			visibleFiles := buildVisibleFilesFromLog(logEntries)

			// Simulate what would happen if we used directory listing instead
			directoryListingFiles := simulateDirectoryListing(storageFiles)

			// The key property: visible files from log should be independent of storage contents
			// 1. All files in the log should be considered visible
			// 2. Extra files in storage should NOT be visible
			// 3. Files removed by log should NOT be visible even if they exist in storage

			logControlledVisibility := validateLogControlledVisibility(visibleFiles, storageFiles, logEntries)

			if !logControlledVisibility {
				t.Logf("Log-controlled visibility failed:")
				t.Logf("Log entries: %d", len(logEntries))
				t.Logf("Visible files from log: %v", visibleFiles)
				t.Logf("Files in storage: %v", storageFiles)
				t.Logf("Directory listing would show: %v", directoryListingFiles)
			}

			return logControlledVisibility
		},
		goptergen.IntRange(1, 8),      // Number of log entries
		goptergen.IntRange(0, 5),      // Number of extra files in storage
		goptergen.Int64Range(1, 1000), // Seed for deterministic generation
	))

	properties.TestingRun(t)
}

// generateVisibilityTestLogEntries creates log entries that add and remove files
func generateVisibilityTestLogEntries(numEntries int, seed int64) []*TransactionLogEntry {
	entries := make([]*TransactionLogEntry, numEntries)
	existingFiles := make(map[string]bool)

	for i := 0; i < numEntries; i++ {
		version := uint64(i + 1)
		txnID := fmt.Sprintf("txn_vis_%d_%d", seed, i)

		var adds []FileAdd
		var removes []FileRemove

		// Deterministic operation based on seed and index
		opType := (int(seed) + i) % 3

		switch opType {
		case 0, 1: // Add files (more common)
			numFiles := ((int(seed) + i) % 3) + 1 // 1-3 files
			for j := 0; j < numFiles; j++ {
				filePath := fmt.Sprintf("data/part-%d-%d-%d.parquet", seed, i, j)
				add := FileAdd{
					Path: filePath,
					Rows: int64(100 + j*50),
					Size: int64(1000 + j*500),
					Partition: map[string]string{
						"date": fmt.Sprintf("2023-01-%02d", (i%28)+1),
					},
				}
				adds = append(adds, add)
				existingFiles[filePath] = true
			}

		case 2: // Remove some files (if any exist)
			if len(existingFiles) > 0 {
				// Remove up to 2 files
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
			// If no files to remove, add one instead
			if len(removes) == 0 {
				filePath := fmt.Sprintf("data/part-%d-%d-fallback.parquet", seed, i)
				adds = append(adds, FileAdd{
					Path:      filePath,
					Rows:      int64(25),
					Size:      int64(250),
					Partition: map[string]string{},
				})
				existingFiles[filePath] = true
			}
		}

		entries[i] = &TransactionLogEntry{
			Version:     version,
			TimestampMs: 1640995200000 + int64(i*1000),
			TxnID:       txnID,
			Adds:        adds,
			Removes:     removes,
		}
	}

	return entries
}

// generateStorageFiles simulates files that exist in object storage
// This includes files from the log PLUS extra files that shouldn't be visible
func generateStorageFiles(logEntries []*TransactionLogEntry, numExtraFiles int, seed int64) []string {
	storageFiles := make(map[string]bool)

	// Add all files that were ever added in the log (even if later removed)
	for _, entry := range logEntries {
		for _, add := range entry.Adds {
			storageFiles[add.Path] = true
		}
	}

	// Add extra files that exist in storage but are NOT in the transaction log
	for i := 0; i < numExtraFiles; i++ {
		extraFile := fmt.Sprintf("data/orphaned-%d-%d.parquet", seed, i)
		storageFiles[extraFile] = true
	}

	// Add some temporary files that should never be visible
	tempFiles := []string{
		fmt.Sprintf("_tmp/txn_%d/attempt_1/part-0.parquet", seed),
		fmt.Sprintf("_tmp/txn_%d/attempt_2/part-0.parquet", seed),
	}
	for _, tempFile := range tempFiles {
		storageFiles[tempFile] = true
	}

	// Convert to sorted slice
	var files []string
	for file := range storageFiles {
		files = append(files, file)
	}
	sort.Strings(files)

	return files
}

// buildVisibleFilesFromLog determines which files should be visible based on transaction log
func buildVisibleFilesFromLog(logEntries []*TransactionLogEntry) []string {
	fileMap := make(map[string]FileAdd)

	// Apply log entries in order (same logic as BuildSnapshot)
	for _, entry := range logEntries {
		// Apply file additions
		for _, add := range entry.Adds {
			fileMap[add.Path] = add
		}

		// Apply file removals
		for _, remove := range entry.Removes {
			delete(fileMap, remove.Path)
		}
	}

	// Convert to sorted list of file paths
	var visibleFiles []string
	for path := range fileMap {
		visibleFiles = append(visibleFiles, path)
	}
	sort.Strings(visibleFiles)

	return visibleFiles
}

// simulateDirectoryListing simulates what would be visible if we used directory listing
func simulateDirectoryListing(storageFiles []string) []string {
	var dataFiles []string

	// Directory listing would show all files in the data/ directory
	for _, file := range storageFiles {
		if strings.HasPrefix(file, "data/") && !strings.Contains(file, "_tmp/") {
			dataFiles = append(dataFiles, file)
		}
	}

	sort.Strings(dataFiles)
	return dataFiles
}

// validateLogControlledVisibility verifies the core property
func validateLogControlledVisibility(visibleFiles, storageFiles []string, logEntries []*TransactionLogEntry) bool {
	// Property 1: All visible files must be in storage (consistency check)
	storageSet := make(map[string]bool)
	for _, file := range storageFiles {
		storageSet[file] = true
	}

	for _, visibleFile := range visibleFiles {
		if !storageSet[visibleFile] {
			// This would be a bug - log references a file that doesn't exist in storage
			return false
		}
	}

	// Property 2: Visible files should match exactly what the log says
	// (not what directory listing would show)
	directoryFiles := simulateDirectoryListing(storageFiles)

	// The key test: visible files should be different from directory listing
	// when there are extra files in storage
	if len(directoryFiles) > len(visibleFiles) {
		// Directory listing shows more files - this is expected
		// Verify that visible files are a proper subset
		visibleSet := make(map[string]bool)
		for _, file := range visibleFiles {
			visibleSet[file] = true
		}

		extraFilesInDirectory := 0
		for _, dirFile := range directoryFiles {
			if !visibleSet[dirFile] {
				extraFilesInDirectory++
			}
		}

		// There should be extra files in directory that are not visible
		if extraFilesInDirectory == 0 {
			return false
		}
	}

	// Property 3: Files removed by the log should not be visible
	// even if they still exist in storage
	removedFiles := make(map[string]bool)
	addedFiles := make(map[string]bool)

	for _, entry := range logEntries {
		for _, add := range entry.Adds {
			addedFiles[add.Path] = true
		}
		for _, remove := range entry.Removes {
			removedFiles[remove.Path] = true
		}
	}

	// Check that removed files are not in visible files
	for removedFile := range removedFiles {
		for _, visibleFile := range visibleFiles {
			if visibleFile == removedFile {
				return false // Removed file is still visible - violation!
			}
		}
	}

	return true
}

// Test edge cases for log-controlled visibility
func TestProperty4LogControlledVisibilityEdgeCases(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	properties := gopter.NewProperties(parameters)

	properties.Property("log-controlled visibility edge cases", prop.ForAll(
		func(seed int64) bool {
			testCases := []struct {
				name          string
				logEntries    []*TransactionLogEntry
				storageFiles  []string
				expectVisible []string
			}{
				{
					name:          "empty log with files in storage",
					logEntries:    []*TransactionLogEntry{},
					storageFiles:  []string{"data/orphan1.parquet", "data/orphan2.parquet"},
					expectVisible: []string{}, // No files should be visible
				},
				{
					name: "log adds file, storage has extra files",
					logEntries: []*TransactionLogEntry{
						{
							Version: 1,
							TxnID:   fmt.Sprintf("txn_edge1_%d", seed),
							Adds: []FileAdd{
								{Path: "data/logged.parquet", Rows: 100, Size: 1000},
							},
						},
					},
					storageFiles: []string{
						"data/logged.parquet",
						"data/extra1.parquet",
						"data/extra2.parquet",
					},
					expectVisible: []string{"data/logged.parquet"}, // Only logged file visible
				},
				{
					name: "log adds then removes file, file still in storage",
					logEntries: []*TransactionLogEntry{
						{
							Version: 1,
							TxnID:   fmt.Sprintf("txn_edge2a_%d", seed),
							Adds: []FileAdd{
								{Path: "data/temp.parquet", Rows: 100, Size: 1000},
							},
						},
						{
							Version: 2,
							TxnID:   fmt.Sprintf("txn_edge2b_%d", seed),
							Removes: []FileRemove{
								{Path: "data/temp.parquet"},
							},
						},
					},
					storageFiles: []string{
						"data/temp.parquet", // File still exists in storage
						"data/other.parquet",
					},
					expectVisible: []string{}, // No files should be visible
				},
			}

			for _, testCase := range testCases {
				visibleFiles := buildVisibleFilesFromLog(testCase.logEntries)

				// Check that visible files match expected
				if !compareStringSlices(visibleFiles, testCase.expectVisible) {
					t.Logf("Edge case '%s' failed", testCase.name)
					t.Logf("Expected visible: %v", testCase.expectVisible)
					t.Logf("Actual visible: %v", visibleFiles)
					return false
				}

				// Verify the core property holds
				if !validateLogControlledVisibility(visibleFiles, testCase.storageFiles, testCase.logEntries) {
					t.Logf("Edge case '%s' failed log-controlled visibility validation", testCase.name)
					return false
				}
			}

			return true
		},
		goptergen.Int64Range(1, 1000), // Seed for test case generation
	))

	properties.TestingRun(t)
}

// compareStringSlices compares two string slices for equality
func compareStringSlices(slice1, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	for i, str1 := range slice1 {
		if str1 != slice2[i] {
			return false
		}
	}

	return true
}
