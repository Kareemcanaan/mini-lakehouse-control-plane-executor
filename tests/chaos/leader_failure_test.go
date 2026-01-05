package chaos

import (
	"encoding/json"
	"fmt"
	"mini-lakehouse/tests/common"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLeaderFailureDuringCommit tests metadata service leader failure during transaction commit
// This test verifies:
// 1. Exactly-once semantics via transaction ID
// 2. Commit succeeds with new leader
// 3. System maintains consistency during leader changes
func TestLeaderFailureDuringCommit(t *testing.T) {
	coordinatorURL := "http://localhost:8081"
	tableName := "chaos_leader_test"

	// Ensure system is ready
	require.NoError(t, common.WaitForCoordinator(coordinatorURL, 120*time.Second), "Coordinator should be ready")

	// Setup test
	t.Run("Setup", func(t *testing.T) {
		setupLeaderFailureTest(t, coordinatorURL, tableName)
	})

	// Execute chaos test
	t.Run("ChaosTest", func(t *testing.T) {
		executeLeaderFailureChaosTest(t, coordinatorURL, tableName)
	})

	// Verify exactly-once semantics
	t.Run("VerifyExactlyOnce", func(t *testing.T) {
		verifyExactlyOnceSemantics(t, coordinatorURL, tableName)
	})

	// Cleanup
	common.CleanupTable(coordinatorURL, tableName)
}

// Test data structures - using common types
// (No need to redeclare types that are in common package)

func setupLeaderFailureTest(t *testing.T, coordinatorURL, tableName string) {
	// Clean up any existing table
	common.CleanupTable(coordinatorURL, tableName)

	// Create test table
	schema := common.TableSchema{
		Fields: []common.Field{
			{Name: "id", Type: "int64"},
			{Name: "transaction", Type: "string"},
			{Name: "value", Type: "int64"},
			{Name: "timestamp", Type: "string"},
		},
	}

	request := common.CreateTableRequest{
		TableName: tableName,
		Schema:    schema,
	}

	response := common.MakeRequest(t, "POST", coordinatorURL+"/tables", request)
	assert.Equal(t, http.StatusOK, response.StatusCode, "Table creation should succeed")

	// Insert initial data to establish baseline
	initialData := []common.LeaderTestRecord{
		{ID: 1, Transaction: "initial_1", Value: 100, Timestamp: "2024-01-01T00:00:00Z"},
		{ID: 2, Transaction: "initial_2", Value: 200, Timestamp: "2024-01-01T00:01:00Z"},
	}

	insertRequest := common.InsertDataRequest[common.LeaderTestRecord]{Data: initialData}
	insertResponse := common.MakeRequest(t, "POST", coordinatorURL+"/tables/"+tableName+"/insert", insertRequest)
	assert.Equal(t, http.StatusOK, insertResponse.StatusCode, "Initial data insertion should succeed")

	t.Logf("Setup completed: Created table %s with initial data", tableName)
}

func executeLeaderFailureChaosTest(t *testing.T, coordinatorURL, tableName string) {
	// Step 1: Identify current Raft leader
	t.Log("Step 1: Identifying current Raft leader...")

	metadataEndpoints := []common.MetadataEndpoint{
		{Port: 8080, URL: "http://localhost:8080", Container: "mini-lakehouse-meta-1"},
		{Port: 8090, URL: "http://localhost:8090", Container: "mini-lakehouse-meta-2"},
		{Port: 8100, URL: "http://localhost:8100", Container: "mini-lakehouse-meta-3"},
	}

	currentLeader, err := common.FindCurrentLeader(metadataEndpoints)
	require.NoError(t, err, "Should be able to find current leader")
	require.NotNil(t, currentLeader, "Should have a current leader")

	t.Logf("Current leader: %s (port %d)", currentLeader.Container, currentLeader.Port)

	// Step 2: Prepare concurrent commit operations with unique transaction IDs
	t.Log("Step 2: Preparing concurrent commit operations...")

	commitData := []common.LeaderTestRecord{
		{ID: 100, Transaction: "chaos_commit_1", Value: 1000, Timestamp: "2024-01-02T00:00:00Z"},
		{ID: 101, Transaction: "chaos_commit_2", Value: 1001, Timestamp: "2024-01-02T00:01:00Z"},
		{ID: 102, Transaction: "chaos_commit_3", Value: 1002, Timestamp: "2024-01-02T00:02:00Z"},
	}

	// Step 3: Start commit operations in background
	t.Log("Step 3: Starting commit operations...")

	var wg sync.WaitGroup
	commitResults := make(chan common.CommitResult, len(commitData))

	for i, data := range commitData {
		wg.Add(1)
		go func(index int, record common.LeaderTestRecord) {
			defer wg.Done()

			// Add small delay to stagger commits
			time.Sleep(time.Duration(index) * 500 * time.Millisecond)

			result := common.AttemptCommitWithRetry(coordinatorURL, tableName, []common.LeaderTestRecord{record}, fmt.Sprintf("txn_%d_%d", index, time.Now().Unix()))
			commitResults <- result
		}(i, data)
	}

	// Step 4: Kill the leader during commit operations
	t.Log("Step 4: Killing current leader during commit operations...")

	// Wait a moment for commits to start
	time.Sleep(1 * time.Second)

	err = common.KillContainer(currentLeader.Container)
	require.NoError(t, err, "Should be able to kill leader container")

	t.Logf("Leader %s killed during commit operations", currentLeader.Container)

	// Step 5: Wait for all commit operations to complete
	t.Log("Step 5: Waiting for commit operations to complete...")

	go func() {
		wg.Wait()
		close(commitResults)
	}()

	// Collect results
	var results []common.CommitResult
	for result := range commitResults {
		results = append(results, result)
	}

	// Step 6: Verify at least some commits succeeded
	t.Log("Step 6: Verifying commit results...")

	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
			t.Logf("Commit succeeded: TxnID=%s, Attempts=%d", result.TxnID, result.Attempts)
		} else {
			t.Logf("Commit failed: TxnID=%s, Error=%s", result.TxnID, result.Error)
		}
	}

	// At least one commit should succeed (system should recover)
	assert.Greater(t, successCount, 0, "At least one commit should succeed despite leader failure")

	// Step 7: Wait for new leader election
	t.Log("Step 7: Waiting for new leader election...")

	var newLeader *common.MetadataEndpoint
	for i := 0; i < 30; i++ { // Wait up to 60 seconds
		time.Sleep(2 * time.Second)

		leader, err := common.FindCurrentLeader(metadataEndpoints)
		if err == nil && leader != nil && leader.Container != currentLeader.Container {
			newLeader = leader
			break
		}

		t.Logf("Attempt %d/30: Waiting for new leader election...", i+1)
	}

	require.NotNil(t, newLeader, "New leader should be elected")
	t.Logf("New leader elected: %s (port %d)", newLeader.Container, newLeader.Port)

	// Step 8: Verify system is operational with new leader
	t.Log("Step 8: Verifying system operation with new leader...")

	// Attempt a new commit with the new leader
	postFailureData := []common.LeaderTestRecord{
		{ID: 200, Transaction: "post_failure", Value: 2000, Timestamp: "2024-01-02T01:00:00Z"},
	}

	postFailureResult := common.AttemptCommitWithRetry(coordinatorURL, tableName, postFailureData, fmt.Sprintf("post_failure_txn_%d", time.Now().Unix()))
	assert.True(t, postFailureResult.Success, "Commit should succeed with new leader")

	t.Log("✅ System operational with new leader")

	// Step 9: Restart the failed leader
	t.Log("Step 9: Restarting failed leader...")

	err = common.StartContainer(currentLeader.Container)
	require.NoError(t, err, "Should be able to restart failed leader")

	// Wait for it to rejoin cluster
	time.Sleep(15 * time.Second)

	t.Log("Failed leader restarted and rejoining cluster")
}

func verifyExactlyOnceSemantics(t *testing.T, coordinatorURL, tableName string) {
	t.Log("Verifying exactly-once semantics...")

	// Step 1: Query all data to verify consistency
	t.Log("Step 1: Querying all data to verify consistency...")

	queryRequest := common.QueryRequest{SQL: fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName)}
	response := common.MakeRequest(t, "POST", coordinatorURL+"/query", queryRequest)
	assert.Equal(t, http.StatusOK, response.StatusCode, "Query should succeed")

	var queryResult common.QueryResponse
	err := json.NewDecoder(response.Body).Decode(&queryResult)
	require.NoError(t, err, "Should decode query response")

	assert.Equal(t, "completed", queryResult.Status, "Query should complete successfully")

	// Step 2: Verify no duplicate transactions
	t.Log("Step 2: Verifying no duplicate transactions...")

	transactionCounts := make(map[string]int)
	for _, result := range queryResult.Results {
		txn := result["transaction"].(string)
		transactionCounts[txn]++
	}

	for txn, count := range transactionCounts {
		assert.Equal(t, 1, count, "Transaction %s should appear exactly once", txn)
	}

	t.Logf("✅ Exactly-once semantics verified: %d unique transactions", len(transactionCounts))

	// Step 3: Test duplicate transaction ID handling
	t.Log("Step 3: Testing duplicate transaction ID handling...")

	duplicateData := []common.LeaderTestRecord{
		{ID: 300, Transaction: "duplicate_test", Value: 3000, Timestamp: "2024-01-02T02:00:00Z"},
	}

	txnID := fmt.Sprintf("duplicate_txn_%d", time.Now().Unix())

	// First commit
	result1 := common.AttemptCommitWithRetry(coordinatorURL, tableName, duplicateData, txnID)
	assert.True(t, result1.Success, "First commit with txnID should succeed")

	// Second commit with same transaction ID (should be idempotent)
	result2 := common.AttemptCommitWithRetry(coordinatorURL, tableName, duplicateData, txnID)

	// The second commit should either succeed (idempotent) or fail gracefully
	// The key is that we don't get duplicate data
	_ = result2 // Use result2 to avoid unused variable error

	// Verify no duplicates were created
	countQuery := common.QueryRequest{SQL: fmt.Sprintf("SELECT COUNT(*) as count FROM %s WHERE transaction = 'duplicate_test'", tableName)}
	countResponse := common.MakeRequest(t, "POST", coordinatorURL+"/query", countQuery)
	assert.Equal(t, http.StatusOK, countResponse.StatusCode, "Count query should succeed")

	var countResult common.QueryResponse
	err = json.NewDecoder(countResponse.Body).Decode(&countResult)
	require.NoError(t, err, "Should decode count query response")

	count := countResult.Results[0]["count"].(float64)
	assert.Equal(t, 1.0, count, "Should have exactly one record with duplicate transaction")

	t.Log("✅ Duplicate transaction ID handling verified")
}
