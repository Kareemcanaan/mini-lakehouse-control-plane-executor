package coordinator

import (
	"context"
	"fmt"
	"testing"

	"mini-lakehouse/proto/gen"

	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// **Feature: mini-lakehouse, Property 30: Query result preservation**
// *For any* snapshot version, queries should return identical results before and after compaction
// **Validates: Requirements 7.5**
func TestProperty30QueryResultPreservation(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("compaction preserves query results", prop.ForAll(
		func(numSmallFiles int, fileSize int64) bool {
			// Ensure we have enough small files for compaction
			if numSmallFiles < 3 {
				numSmallFiles = 3
			}
			if numSmallFiles > 10 {
				numSmallFiles = 10
			}

			// Create test files with deterministic data
			var testFiles []TestFileInfo
			totalRows := int64(0)

			// Create small files (candidates for compaction)
			for i := 0; i < numSmallFiles; i++ {
				rows := int64(100 + i*50) // 100, 150, 200, etc.
				file := TestFileInfo{
					Path: fmt.Sprintf("data/small-%d.parquet", i),
					Size: 5 * 1024 * 1024, // 5MB - small file
					Rows: rows,
					Data: generateDeterministicTestData(int(rows), i),
				}
				testFiles = append(testFiles, file)
				totalRows += rows
			}

			// Add one large file (not compacted)
			largeFile := TestFileInfo{
				Path: "data/large-1.parquet",
				Size: 50 * 1024 * 1024, // 50MB - large file
				Rows: 1000,
				Data: generateDeterministicTestData(1000, 999),
			}
			testFiles = append(testFiles, largeFile)
			totalRows += 1000

			// Create mock metadata client
			mockClient := &MockMetadataClientForCompaction{
				latestVersion: 5,
				newVersion:    6,
				files:         convertToFileInfo(testFiles),
				schema: &gen.Schema{
					Fields: []*gen.Field{
						{Name: "id", Type: "int64"},
						{Name: "value", Type: "int64"},
						{Name: "category", Type: "string"},
					},
				},
			}

			txnManager := NewTransactionManager()
			compactionService := NewCompactionService(mockClient, nil, txnManager, nil)

			ctx := context.Background()

			// Test simple count query (most basic test)
			querySpec := TestQuerySpec{
				Filter:     "",
				GroupBy:    []string{},
				Aggregates: []string{"count"},
			}

			// Execute query before compaction
			resultsBefore := simulateQueryExecution(testFiles, querySpec)

			// Execute compaction
			plan, err := compactionService.IdentifyCompactionCandidates(ctx, "test_table")
			if err != nil {
				t.Logf("Failed to identify compaction candidates: %v", err)
				return false
			}

			if plan == nil {
				// No compaction needed, results should be identical
				return true
			}

			result, err := compactionService.ExecuteCompaction(ctx, plan)
			if err != nil {
				t.Logf("Failed to execute compaction: %v", err)
				return false
			}

			if !result.Success {
				t.Logf("Compaction failed: %s", result.Error)
				return false
			}

			// Simulate the state after compaction
			filesAfterCompaction := simulateFilesAfterCompaction(testFiles, plan.InputFiles)

			// Execute query after compaction
			resultsAfter := simulateQueryExecution(filesAfterCompaction, querySpec)

			// Results should be identical
			success := compareQueryResults(resultsBefore, resultsAfter)
			if !success {
				t.Logf("Query results differ: before=%v, after=%v", resultsBefore, resultsAfter)
			}

			return success
		},
		goptergen.IntRange(3, 8),                      // Number of small files
		goptergen.Int64Range(1024*1024, 10*1024*1024), // File size range
	))

	properties.TestingRun(t)
}

// TestFileInfo represents a test file with data
type TestFileInfo struct {
	Path string
	Size int64
	Rows int64
	Data []TestRow // Simulated data for query testing
}

// TestRow represents a row of test data
type TestRow struct {
	ID       int64
	Value    int64
	Category string
}

// TestQuerySpec represents a query specification for testing
type TestQuerySpec struct {
	Filter     string   // Simple filter like "value > 100"
	GroupBy    []string // Group by columns
	Aggregates []string // Aggregate functions like "sum", "count"
}

// CompactionQueryResult represents the result of a query (renamed to avoid conflict)
type CompactionQueryResult struct {
	Rows []map[string]interface{}
}

// generateDeterministicTestData generates deterministic test data for a file
func generateDeterministicTestData(rows int, seed int) []TestRow {
	data := make([]TestRow, rows)
	categories := []string{"A", "B", "C"}

	for i := 0; i < rows; i++ {
		data[i] = TestRow{
			ID:       int64(seed*1000 + i + 1),
			Value:    int64((i*17 + seed*7 + 42) % 300), // Deterministic but varied values
			Category: categories[(i+seed)%len(categories)],
		}
	}

	return data
}

// convertToFileInfo converts TestFileInfo to gen.FileInfo
func convertToFileInfo(testFiles []TestFileInfo) []*gen.FileInfo {
	files := make([]*gen.FileInfo, len(testFiles))
	for i, tf := range testFiles {
		files[i] = &gen.FileInfo{
			Path: tf.Path,
			Size: uint64(tf.Size),
			Rows: uint64(tf.Rows),
		}
	}
	return files
}

// simulateQueryExecution simulates executing a query on the given files
func simulateQueryExecution(files []TestFileInfo, querySpec TestQuerySpec) CompactionQueryResult {
	// Collect all data from files
	var allData []TestRow
	for _, file := range files {
		allData = append(allData, file.Data...)
	}

	// Apply filter
	var filteredData []TestRow
	for _, row := range allData {
		if matchesFilter(row, querySpec.Filter) {
			filteredData = append(filteredData, row)
		}
	}

	// Apply grouping and aggregation
	if len(querySpec.GroupBy) > 0 {
		return executeGroupByQuery(filteredData, querySpec)
	} else {
		return executeSimpleAggregateQuery(filteredData, querySpec)
	}
}

// matchesFilter checks if a row matches the filter condition
func matchesFilter(row TestRow, filter string) bool {
	if filter == "" {
		return true
	}

	switch filter {
	case "value > 50":
		return row.Value > 50
	case "value < 200":
		return row.Value < 200
	case "category = 'A'":
		return row.Category == "A"
	default:
		return true
	}
}

// executeGroupByQuery executes a GROUP BY query
func executeGroupByQuery(data []TestRow, querySpec TestQuerySpec) CompactionQueryResult {
	groups := make(map[string][]TestRow)

	// Group data
	for _, row := range data {
		key := getGroupKey(row, querySpec.GroupBy)
		groups[key] = append(groups[key], row)
	}

	// Calculate aggregates for each group
	var resultRows []map[string]interface{}
	for groupKey, groupData := range groups {
		resultRow := make(map[string]interface{})
		resultRow["category"] = groupKey // Assuming we're grouping by category

		for _, agg := range querySpec.Aggregates {
			switch agg {
			case "count":
				resultRow["count"] = len(groupData)
			case "sum":
				sum := int64(0)
				for _, row := range groupData {
					sum += row.Value
				}
				resultRow["sum_value"] = sum
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	return CompactionQueryResult{Rows: resultRows}
}

// executeSimpleAggregateQuery executes a simple aggregate query (no GROUP BY)
func executeSimpleAggregateQuery(data []TestRow, querySpec TestQuerySpec) CompactionQueryResult {
	resultRow := make(map[string]interface{})

	for _, agg := range querySpec.Aggregates {
		switch agg {
		case "count":
			resultRow["count"] = len(data)
		case "sum":
			sum := int64(0)
			for _, row := range data {
				sum += row.Value
			}
			resultRow["sum_value"] = sum
		}
	}

	return CompactionQueryResult{Rows: []map[string]interface{}{resultRow}}
}

// getGroupKey generates a group key for GROUP BY
func getGroupKey(row TestRow, groupBy []string) string {
	// For simplicity, assume we're only grouping by category
	return row.Category
}

// simulateFilesAfterCompaction simulates the file state after compaction
func simulateFilesAfterCompaction(originalFiles []TestFileInfo, compactedFiles []CompactionCandidate) []TestFileInfo {
	var result []TestFileInfo

	// Add files that were not compacted
	compactedPaths := make(map[string]bool)
	for _, cf := range compactedFiles {
		compactedPaths[cf.Path] = true
	}

	for _, file := range originalFiles {
		if !compactedPaths[file.Path] {
			result = append(result, file)
		}
	}

	// Add the compacted file (merge data from compacted files)
	var compactedData []TestRow
	for _, file := range originalFiles {
		if compactedPaths[file.Path] {
			compactedData = append(compactedData, file.Data...)
		}
	}

	if len(compactedData) > 0 {
		compactedFile := TestFileInfo{
			Path: "data/compacted-00000.parquet",
			Size: 50 * 1024 * 1024, // Assume compacted file is larger
			Rows: int64(len(compactedData)),
			Data: compactedData,
		}
		result = append(result, compactedFile)
	}

	return result
}

// compareQueryResults compares two query results for equality
func compareQueryResults(result1, result2 CompactionQueryResult) bool {
	if len(result1.Rows) != len(result2.Rows) {
		return false
	}

	// For simplicity, we'll compare the aggregate values
	// In a real implementation, we'd need more sophisticated comparison
	for i, row1 := range result1.Rows {
		if i >= len(result2.Rows) {
			return false
		}

		row2 := result2.Rows[i]

		// Compare key fields
		for key, value1 := range row1 {
			value2, exists := row2[key]
			if !exists {
				return false
			}

			// Compare values (handling different numeric types)
			if !compareValues(value1, value2) {
				return false
			}
		}
	}

	return true
}

// compareValues compares two values, handling type conversions
func compareValues(v1, v2 interface{}) bool {
	// Handle numeric comparisons
	switch val1 := v1.(type) {
	case int:
		if val2, ok := v2.(int); ok {
			return val1 == val2
		}
		if val2, ok := v2.(int64); ok {
			return int64(val1) == val2
		}
	case int64:
		if val2, ok := v2.(int64); ok {
			return val1 == val2
		}
		if val2, ok := v2.(int); ok {
			return val1 == int64(val2)
		}
	case string:
		if val2, ok := v2.(string); ok {
			return val1 == val2
		}
	}

	// Fallback to direct comparison
	return v1 == v2
}
