package common

// Shared test data structures used across integration and chaos tests

// TableSchema represents the schema for a table
type TableSchema struct {
	Fields []Field `json:"fields"`
}

// Field represents a field in a table schema
type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// CreateTableRequest represents a request to create a table
type CreateTableRequest struct {
	TableName string      `json:"table_name"` // Changed from "name" to "table_name"
	Schema    TableSchema `json:"schema"`
}

// QueryRequest represents a query request
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	JobID   string                   `json:"job_id"`
	Results []map[string]interface{} `json:"results"`
	Status  string                   `json:"status"`
	Error   string                   `json:"error,omitempty"`
}

// TestRecord represents a generic test record for integration tests
type TestRecord struct {
	ID       int64   `json:"id"`
	Category string  `json:"category"`
	Product  string  `json:"product"`
	Price    float64 `json:"price"`
	Quantity int64   `json:"quantity"`
	Date     string  `json:"date"`
}

// ChaosTestRecord represents a test record for chaos tests
type ChaosTestRecord struct {
	ID       int64   `json:"id"`
	Category string  `json:"category"`
	Value    float64 `json:"value"`
	Data     string  `json:"data"`
}

// LeaderTestRecord represents a test record for leader failure tests
type LeaderTestRecord struct {
	ID          int64  `json:"id"`
	Transaction string `json:"transaction"`
	Value       int64  `json:"value"`
	Timestamp   string `json:"timestamp"`
}

// InsertDataRequest represents a request to insert data (generic)
type InsertDataRequest[T any] struct {
	Data []T `json:"data"`
}

// LeaderResponse represents a response from the leader endpoint
type LeaderResponse struct {
	NodeID   string `json:"node_id"`
	IsLeader bool   `json:"is_leader"`
	Term     int64  `json:"term"`
}

// MetadataEndpoint represents a metadata service endpoint
type MetadataEndpoint struct {
	Port      int
	URL       string
	Container string
}

// CommitResult represents the result of a commit operation
type CommitResult struct {
	TxnID    string
	Success  bool
	Error    string
	Attempts int
}
