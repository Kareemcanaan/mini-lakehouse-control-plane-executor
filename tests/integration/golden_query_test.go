package integration

import (
	"encoding/json"
	"fmt"
	"mini-lakehouse/tests/common"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GoldenQueryTest tests the complete workflow: create table → insert data → execute GROUP BY query → verify exact expected output
func TestGoldenQuery(t *testing.T) {
	// Test configuration
	coordinatorURL := "http://localhost:8081"
	tableName := "golden_test_table"

	// Wait for coordinator to be ready
	require.NoError(t, common.WaitForCoordinator(coordinatorURL, 120*time.Second), "Coordinator should be ready")

	// Clean up any existing table
	common.CleanupTable(coordinatorURL, tableName)

	t.Run("CreateTable", func(t *testing.T) {
		testCreateTable(t, coordinatorURL, tableName)
	})

	t.Run("InsertDeterministicData", func(t *testing.T) {
		testInsertDeterministicData(t, coordinatorURL, tableName)
	})

	t.Run("ExecuteGoldenQuery", func(t *testing.T) {
		testExecuteGoldenQuery(t, coordinatorURL, tableName)
	})

	t.Run("VerifySnapshotIsolation", func(t *testing.T) {
		testVerifySnapshotIsolation(t, coordinatorURL, tableName)
	})

	// Cleanup
	common.CleanupTable(coordinatorURL, tableName)
}

// Test data structures - using common types
// (No need to redeclare types that are in common package)

// Expected results for the golden query
type ExpectedGroupByResult struct {
	Category     string  `json:"category"`
	ItemCount    int64   `json:"item_count"`
	TotalRevenue float64 `json:"total_revenue"`
	AvgPrice     float64 `json:"avg_price"`
}

func testCreateTable(t *testing.T, coordinatorURL, tableName string) {
	schema := common.TableSchema{
		Fields: []common.Field{
			{Name: "id", Type: "int64"},
			{Name: "category", Type: "string"},
			{Name: "product", Type: "string"},
			{Name: "price", Type: "float64"},
			{Name: "quantity", Type: "int64"},
			{Name: "date", Type: "string"},
		},
	}

	request := common.CreateTableRequest{
		TableName: tableName,
		Schema:    schema,
	}

	response := common.MakeRequest(t, "POST", coordinatorURL+"/tables", request)
	assert.Equal(t, http.StatusOK, response.StatusCode, "Table creation should succeed")

	// Verify table exists
	getResponse := common.MakeRequest(t, "GET", coordinatorURL+"/tables/"+tableName, nil)
	assert.Equal(t, http.StatusOK, getResponse.StatusCode, "Table should exist after creation")
}

func testInsertDeterministicData(t *testing.T, coordinatorURL, tableName string) {
	// Deterministic test data for predictable results
	testData := []common.TestRecord{
		// Electronics category - 4 items
		{ID: 1, Category: "Electronics", Product: "Laptop", Price: 1000.00, Quantity: 2, Date: "2024-01-01"},
		{ID: 2, Category: "Electronics", Product: "Mouse", Price: 25.00, Quantity: 5, Date: "2024-01-01"},
		{ID: 3, Category: "Electronics", Product: "Keyboard", Price: 75.00, Quantity: 3, Date: "2024-01-02"},
		{ID: 4, Category: "Electronics", Product: "Monitor", Price: 300.00, Quantity: 1, Date: "2024-01-02"},

		// Furniture category - 3 items
		{ID: 5, Category: "Furniture", Product: "Chair", Price: 150.00, Quantity: 4, Date: "2024-01-03"},
		{ID: 6, Category: "Furniture", Product: "Desk", Price: 400.00, Quantity: 2, Date: "2024-01-03"},
		{ID: 7, Category: "Furniture", Product: "Lamp", Price: 50.00, Quantity: 6, Date: "2024-01-04"},

		// Books category - 2 items
		{ID: 8, Category: "Books", Product: "Novel", Price: 15.00, Quantity: 10, Date: "2024-01-05"},
		{ID: 9, Category: "Books", Product: "Textbook", Price: 80.00, Quantity: 3, Date: "2024-01-05"},
	}

	request := common.InsertDataRequest[common.TestRecord]{Data: testData}

	response := common.MakeRequest(t, "POST", coordinatorURL+"/tables/"+tableName+"/insert", request)
	assert.Equal(t, http.StatusOK, response.StatusCode, "Data insertion should succeed")

	// Verify data was inserted by counting records
	countQuery := common.QueryRequest{SQL: fmt.Sprintf("SELECT COUNT(*) as total FROM %s", tableName)}
	countResponse := common.MakeRequest(t, "POST", coordinatorURL+"/query", countQuery)
	assert.Equal(t, http.StatusOK, countResponse.StatusCode, "Count query should succeed")

	var queryResult common.QueryResponse
	err := json.NewDecoder(countResponse.Body).Decode(&queryResult)
	require.NoError(t, err, "Should decode count query response")

	require.Len(t, queryResult.Results, 1, "Count query should return one row")
	totalCount := queryResult.Results[0]["total"]
	assert.Equal(t, float64(9), totalCount, "Should have inserted 9 records")
}

func testExecuteGoldenQuery(t *testing.T, coordinatorURL, tableName string) {
	// The golden query: GROUP BY with multiple aggregations
	goldenSQL := fmt.Sprintf(`
		SELECT 
			category,
			COUNT(*) as item_count,
			SUM(price * quantity) as total_revenue,
			AVG(price) as avg_price
		FROM %s 
		GROUP BY category 
		ORDER BY category
	`, tableName)

	queryRequest := common.QueryRequest{SQL: goldenSQL}

	response := common.MakeRequest(t, "POST", coordinatorURL+"/query", queryRequest)
	assert.Equal(t, http.StatusOK, response.StatusCode, "Golden query should succeed")

	var queryResult common.QueryResponse
	err := json.NewDecoder(response.Body).Decode(&queryResult)
	require.NoError(t, err, "Should decode golden query response")

	assert.Equal(t, "completed", queryResult.Status, "Query should complete successfully")
	require.Len(t, queryResult.Results, 3, "Should have 3 category groups")

	// Expected results (calculated manually from test data)
	expectedResults := []ExpectedGroupByResult{
		{
			Category:     "Books",
			ItemCount:    2,
			TotalRevenue: 15.00*10 + 80.00*3,  // 150 + 240 = 390
			AvgPrice:     (15.00 + 80.00) / 2, // 47.5
		},
		{
			Category:     "Electronics",
			ItemCount:    4,
			TotalRevenue: 1000.00*2 + 25.00*5 + 75.00*3 + 300.00*1, // 2000 + 125 + 225 + 300 = 2650
			AvgPrice:     (1000.00 + 25.00 + 75.00 + 300.00) / 4,   // 350
		},
		{
			Category:     "Furniture",
			ItemCount:    3,
			TotalRevenue: 150.00*4 + 400.00*2 + 50.00*6, // 600 + 800 + 300 = 1700
			AvgPrice:     (150.00 + 400.00 + 50.00) / 3, // 200
		},
	}

	// Verify each result matches expected values
	for i, expected := range expectedResults {
		result := queryResult.Results[i]

		assert.Equal(t, expected.Category, result["category"],
			"Category should match for result %d", i)
		assert.Equal(t, float64(expected.ItemCount), result["item_count"],
			"Item count should match for category %s", expected.Category)
		assert.InDelta(t, expected.TotalRevenue, result["total_revenue"], 0.01,
			"Total revenue should match for category %s", expected.Category)
		assert.InDelta(t, expected.AvgPrice, result["avg_price"], 0.01,
			"Average price should match for category %s", expected.Category)
	}

	t.Logf("Golden query results verified successfully:")
	for i, result := range queryResult.Results {
		t.Logf("  %d. Category: %s, Count: %.0f, Revenue: %.2f, Avg Price: %.2f",
			i+1, result["category"], result["item_count"], result["total_revenue"], result["avg_price"])
	}
}

func testVerifySnapshotIsolation(t *testing.T, coordinatorURL, tableName string) {
	// Get current version
	versionResponse := common.MakeRequest(t, "GET", coordinatorURL+"/tables/"+tableName+"/versions", nil)
	assert.Equal(t, http.StatusOK, versionResponse.StatusCode, "Should get table versions")

	// Execute the same query multiple times to ensure consistent results
	goldenSQL := fmt.Sprintf(`
		SELECT 
			category,
			COUNT(*) as item_count,
			SUM(price * quantity) as total_revenue
		FROM %s 
		GROUP BY category 
		ORDER BY category
	`, tableName)

	var firstResult common.QueryResponse

	// Execute query multiple times
	for i := 0; i < 3; i++ {
		queryRequest := common.QueryRequest{SQL: goldenSQL}
		response := common.MakeRequest(t, "POST", coordinatorURL+"/query", queryRequest)
		assert.Equal(t, http.StatusOK, response.StatusCode, "Query %d should succeed", i+1)

		var queryResult common.QueryResponse
		err := json.NewDecoder(response.Body).Decode(&queryResult)
		require.NoError(t, err, "Should decode query response %d", i+1)

		if i == 0 {
			firstResult = queryResult
		} else {
			// Verify results are identical (snapshot isolation)
			assert.Equal(t, len(firstResult.Results), len(queryResult.Results),
				"Result count should be consistent across queries")

			for j, firstRow := range firstResult.Results {
				currentRow := queryResult.Results[j]
				assert.Equal(t, firstRow["category"], currentRow["category"],
					"Category should be consistent in query %d, row %d", i+1, j)
				assert.Equal(t, firstRow["item_count"], currentRow["item_count"],
					"Item count should be consistent in query %d, row %d", i+1, j)
				assert.Equal(t, firstRow["total_revenue"], currentRow["total_revenue"],
					"Total revenue should be consistent in query %d, row %d", i+1, j)
			}
		}

		// Small delay between queries
		time.Sleep(100 * time.Millisecond)
	}

	t.Log("Snapshot isolation verified: multiple queries returned identical results")
}
