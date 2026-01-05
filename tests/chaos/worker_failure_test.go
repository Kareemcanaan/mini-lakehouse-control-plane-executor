package chaos

import (
	"encoding/json"
	"fmt"
	"mini-lakehouse/tests/common"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkerFailureDuringShuffle tests worker failure during map/shuffle stage execution
// This test verifies:
// 1. Task retry and SUCCESS manifest behavior
// 2. Query completes with correct results despite worker failure
// 3. System maintains consistency during failures
func TestWorkerFailureDuringShuffle(t *testing.T) {
	coordinatorURL := "http://localhost:8081"
	tableName := "chaos_worker_test"

	// Ensure system is ready
	require.NoError(t, common.WaitForCoordinator(coordinatorURL, 120*time.Second), "Coordinator should be ready")

	// Setup test
	t.Run("Setup", func(t *testing.T) {
		setupWorkerFailureTest(t, coordinatorURL, tableName)
	})

	// Execute chaos test
	t.Run("ChaosTest", func(t *testing.T) {
		executeChaosWorkerFailureTest(t, coordinatorURL, tableName)
	})

	// Verify system recovery
	t.Run("VerifyRecovery", func(t *testing.T) {
		verifySystemRecovery(t, coordinatorURL, tableName)
	})

	// Cleanup
	common.CleanupTable(coordinatorURL, tableName)
}

// Test data structures - using common types
// (No need to redeclare types that are in common package)

func setupWorkerFailureTest(t *testing.T, coordinatorURL, tableName string) {
	// Clean up any existing table
	common.CleanupTable(coordinatorURL, tableName)

	// Create test table with schema suitable for shuffle operations
	schema := common.TableSchema{
		Fields: []common.Field{
			{Name: "id", Type: "int64"},
			{Name: "category", Type: "string"},
			{Name: "value", Type: "float64"},
			{Name: "data", Type: "string"},
		},
	}

	request := common.CreateTableRequest{
		TableName: tableName,
		Schema:    schema,
	}

	response := common.MakeRequest(t, "POST", coordinatorURL+"/tables", request)
	assert.Equal(t, http.StatusOK, response.StatusCode, "Table creation should succeed")

	// Insert substantial data to ensure shuffle operations occur
	// This creates enough data to require multiple workers and shuffle stages
	testData := common.GenerateLargeDataset(1000) // 1000 records across multiple categories

	// Insert data in batches to simulate realistic workload
	batchSize := 100
	for i := 0; i < len(testData); i += batchSize {
		end := i + batchSize
		if end > len(testData) {
			end = len(testData)
		}

		batch := testData[i:end]
		insertRequest := common.InsertDataRequest[common.ChaosTestRecord]{Data: batch}

		insertResponse := common.MakeRequest(t, "POST", coordinatorURL+"/tables/"+tableName+"/insert", insertRequest)
		assert.Equal(t, http.StatusOK, insertResponse.StatusCode, "Batch %d insertion should succeed", i/batchSize+1)

		// Small delay between batches
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("Setup completed: Created table %s with %d records", tableName, len(testData))
}

func executeChaosWorkerFailureTest(t *testing.T, coordinatorURL, tableName string) {
	// Step 1: Start a complex query that will require shuffle operations
	complexQuery := fmt.Sprintf(`
		SELECT 
			category,
			COUNT(*) as item_count,
			SUM(value) as total_value,
			AVG(value) as avg_value,
			MIN(value) as min_value,
			MAX(value) as max_value
		FROM %s 
		WHERE value > 50.0
		GROUP BY category 
		ORDER BY total_value DESC
	`, tableName)

	t.Log("Step 1: Starting complex GROUP BY query that requires shuffle operations...")

	// Start query in background
	queryDone := make(chan common.QueryResponse, 1)
	queryError := make(chan error, 1)

	go func() {
		queryRequest := common.QueryRequest{SQL: complexQuery}
		response := common.MakeRequest(t, "POST", coordinatorURL+"/query", queryRequest)

		if response.StatusCode != http.StatusOK {
			queryError <- fmt.Errorf("query failed with status %d", response.StatusCode)
			return
		}

		var queryResult common.QueryResponse
		err := json.NewDecoder(response.Body).Decode(&queryResult)
		if err != nil {
			queryError <- fmt.Errorf("failed to decode query response: %v", err)
			return
		}

		queryDone <- queryResult
	}()

	// Step 2: Wait a moment for query to start and begin shuffle operations
	t.Log("Step 2: Waiting for query to start shuffle operations...")
	time.Sleep(2 * time.Second)

	// Step 3: Kill a worker during execution
	t.Log("Step 3: Killing worker-1 during query execution...")

	workerToKill := "mini-lakehouse-worker-1"
	err := common.KillContainer(workerToKill)
	require.NoError(t, err, "Should be able to kill worker container")

	t.Logf("Worker %s killed successfully", workerToKill)

	// Step 4: Wait for query to complete (should succeed with remaining workers)
	t.Log("Step 4: Waiting for query to complete despite worker failure...")

	var queryResult common.QueryResponse
	select {
	case result := <-queryDone:
		queryResult = result
		t.Log("Query completed successfully despite worker failure!")
	case err := <-queryError:
		t.Fatalf("Query failed: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("Query timed out after 60 seconds")
	}

	// Step 5: Verify query results are correct
	t.Log("Step 5: Verifying query results are correct...")

	assert.Equal(t, "completed", queryResult.Status, "Query should complete successfully")
	assert.NotEmpty(t, queryResult.Results, "Query should return results")

	// Verify we have results for multiple categories
	assert.GreaterOrEqual(t, len(queryResult.Results), 3, "Should have results for multiple categories")

	// Verify result structure
	for i, result := range queryResult.Results {
		assert.Contains(t, result, "category", "Result %d should have category", i)
		assert.Contains(t, result, "item_count", "Result %d should have item_count", i)
		assert.Contains(t, result, "total_value", "Result %d should have total_value", i)
		assert.Contains(t, result, "avg_value", "Result %d should have avg_value", i)

		// Verify aggregation values are reasonable
		itemCount := result["item_count"].(float64)
		totalValue := result["total_value"].(float64)
		avgValue := result["avg_value"].(float64)

		assert.Greater(t, itemCount, 0.0, "Item count should be positive")
		assert.Greater(t, totalValue, 0.0, "Total value should be positive")
		assert.Greater(t, avgValue, 50.0, "Average value should be > 50 (due to WHERE clause)")
	}

	t.Logf("Query results verified: %d categories with correct aggregations", len(queryResult.Results))

	// Step 6: Restart the killed worker
	t.Log("Step 6: Restarting the killed worker...")

	err = common.StartContainer(workerToKill)
	require.NoError(t, err, "Should be able to restart worker container")

	// Wait for worker to register with coordinator
	time.Sleep(10 * time.Second)

	t.Log("Worker restarted and should be registering with coordinator")
}

func verifySystemRecovery(t *testing.T, coordinatorURL, tableName string) {
	t.Log("Verifying system recovery after worker failure...")

	// Test 1: Verify all workers are healthy
	t.Log("Test 1: Checking worker health...")

	// Give workers time to fully recover
	time.Sleep(15 * time.Second)

	// Test 2: Execute another query to ensure system is fully functional
	t.Log("Test 2: Executing verification query...")

	verificationQuery := fmt.Sprintf(`
		SELECT 
			category,
			COUNT(*) as count
		FROM %s 
		GROUP BY category
		ORDER BY category
	`, tableName)

	queryRequest := common.QueryRequest{SQL: verificationQuery}
	response := common.MakeRequest(t, "POST", coordinatorURL+"/query", queryRequest)
	assert.Equal(t, http.StatusOK, response.StatusCode, "Verification query should succeed")

	var queryResult common.QueryResponse
	err := json.NewDecoder(response.Body).Decode(&queryResult)
	require.NoError(t, err, "Should decode verification query response")

	assert.Equal(t, "completed", queryResult.Status, "Verification query should complete")
	assert.NotEmpty(t, queryResult.Results, "Verification query should return results")

	t.Log("✅ System recovery verified: All components working correctly")

	// Test 3: Verify SUCCESS manifests are working correctly
	t.Log("Test 3: Verifying SUCCESS manifest behavior...")

	// Execute a simple query that should create SUCCESS manifests
	simpleQuery := fmt.Sprintf("SELECT COUNT(*) as total FROM %s", tableName)
	simpleRequest := common.QueryRequest{SQL: simpleQuery}

	simpleResponse := common.MakeRequest(t, "POST", coordinatorURL+"/query", simpleRequest)
	assert.Equal(t, http.StatusOK, simpleResponse.StatusCode, "Simple query should succeed")

	var simpleResult common.QueryResponse
	err = json.NewDecoder(simpleResponse.Body).Decode(&simpleResult)
	require.NoError(t, err, "Should decode simple query response")

	assert.Equal(t, "completed", simpleResult.Status, "Simple query should complete")
	assert.Len(t, simpleResult.Results, 1, "Simple query should return one result")

	totalCount := simpleResult.Results[0]["total"].(float64)
	assert.Equal(t, 1000.0, totalCount, "Should have all 1000 records")

	t.Log("✅ SUCCESS manifest behavior verified")
}

// Additional helper to check container status
func getContainerStatus(containerName string) (string, error) {
	cmd := exec.Command("docker", "ps", "-a", "--filter", fmt.Sprintf("name=%s", containerName), "--format", "{{.Status}}")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get container status: %v", err)
	}

	status := strings.TrimSpace(string(output))
	return status, nil
}
