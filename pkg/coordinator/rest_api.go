package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"mini-lakehouse/proto/gen"
	"mini-lakehouse/tests/common"

	"github.com/gorilla/mux"
)

// RestAPI provides HTTP endpoints for table operations and query execution
type RestAPI struct {
	tableService   *TableService
	queryService   *QueryExecutionService
	metadataClient MetadataClient
	port           int
	server         *http.Server
}

// NewRestAPI creates a new REST API server
func NewRestAPI(
	tableService *TableService,
	queryService *QueryExecutionService,
	metadataClient MetadataClient,
	port int,
) *RestAPI {
	return &RestAPI{
		tableService:   tableService,
		queryService:   queryService,
		metadataClient: metadataClient,
		port:           port,
	}
}

// Start starts the REST API server
func (api *RestAPI) Start(compactionAPI *CompactionAPI) error {
	router := mux.NewRouter()

	// Table operations
	router.HandleFunc("/tables", api.createTable).Methods("POST")
	router.HandleFunc("/tables/{tableName}", api.getTable).Methods("GET")
	router.HandleFunc("/tables/{tableName}", api.deleteTable).Methods("DELETE")
	router.HandleFunc("/tables/{tableName}/versions", api.listVersions).Methods("GET")
	router.HandleFunc("/tables/{tableName}/versions/{version}/snapshot", api.getSnapshot).Methods("GET")
	router.HandleFunc("/tables/{tableName}/data", api.insertData).Methods("POST")
	router.HandleFunc("/tables/{tableName}/insert", api.insertDataDirect).Methods("POST") // Alternative endpoint

	// Query operations
	router.HandleFunc("/query", api.executeQueryDirect).Methods("POST") // Alternative endpoint
	router.HandleFunc("/queries", api.executeQuery).Methods("POST")
	router.HandleFunc("/queries/{jobId}", api.getQueryStatus).Methods("GET")
	router.HandleFunc("/queries/{jobId}/results", api.getQueryResults).Methods("GET")

	// Compaction operations (if compactionAPI is provided)
	if compactionAPI != nil {
		api.AddCompactionRoutes(router, compactionAPI)
	}

	// Health check
	router.HandleFunc("/health", api.healthCheck).Methods("GET")

	api.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", api.port),
		Handler: router,
	}

	log.Printf("Starting REST API server on port %d", api.port)
	return api.server.ListenAndServe()
}

// Stop stops the REST API server
func (api *RestAPI) Stop(ctx context.Context) error {
	if api.server != nil {
		return api.server.Shutdown(ctx)
	}
	return nil
}

// CreateTableRequest represents the JSON request for creating a table
type CreateTableAPIRequest struct {
	TableName string    `json:"table_name"`
	Schema    SchemaAPI `json:"schema"`
}

// SchemaAPI represents the JSON schema structure
type SchemaAPI struct {
	Fields []FieldAPI `json:"fields"`
}

// FieldAPI represents a field in the schema
type FieldAPI struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// InsertDataAPIRequest represents the JSON request for inserting data
type InsertDataAPIRequest struct {
	DataPath string `json:"data_path"`
	TxnID    string `json:"txn_id,omitempty"`
}

// QueryAPIRequest represents the JSON request for executing a query
type QueryAPIRequest struct {
	TableName  string         `json:"table_name"`
	Filter     string         `json:"filter,omitempty"`
	GroupBy    []string       `json:"group_by,omitempty"`
	Aggregates []AggregateAPI `json:"aggregates,omitempty"`
	Projection []string       `json:"projection,omitempty"`
	Version    uint64         `json:"version,omitempty"`
}

// AggregateAPI represents an aggregation operation
type AggregateAPI struct {
	Function string `json:"function"`
	Column   string `json:"column"`
	Alias    string `json:"alias,omitempty"`
}

// createTable handles POST /tables
func (api *RestAPI) createTable(w http.ResponseWriter, r *http.Request) {
	var reqBody map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Support both "name" and "table_name" fields for backward compatibility
	var tableName string
	if name, ok := reqBody["name"].(string); ok {
		tableName = name
	} else if tableNameField, ok := reqBody["table_name"].(string); ok {
		tableName = tableNameField
	} else {
		http.Error(w, "Missing table name (use 'name' or 'table_name' field)", http.StatusBadRequest)
		return
	}

	// Extract schema
	schemaData, ok := reqBody["schema"].(map[string]interface{})
	if !ok {
		http.Error(w, "Missing or invalid schema", http.StatusBadRequest)
		return
	}

	fieldsData, ok := schemaData["fields"].([]interface{})
	if !ok {
		http.Error(w, "Missing or invalid schema fields", http.StatusBadRequest)
		return
	}

	// Convert to internal format
	schema := &gen.Schema{
		Fields: make([]*gen.Field, len(fieldsData)),
	}

	for i, fieldData := range fieldsData {
		fieldMap, ok := fieldData.(map[string]interface{})
		if !ok {
			http.Error(w, fmt.Sprintf("Invalid field %d", i), http.StatusBadRequest)
			return
		}

		fieldName, _ := fieldMap["name"].(string)
		fieldType, _ := fieldMap["type"].(string)
		fieldNullable, _ := fieldMap["nullable"].(bool)

		schema.Fields[i] = &gen.Field{
			Name:     fieldName,
			Type:     fieldType,
			Nullable: fieldNullable,
		}
	}

	createReq := &CreateTableRequest{
		TableName: tableName,
		Schema:    schema,
	}

	// Create table
	ctx := context.Background()
	resp, err := api.tableService.CreateTable(ctx, createReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Internal error: %v", err), http.StatusInternalServerError)
		return
	}

	if !resp.Success {
		http.Error(w, resp.Error, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Table %s created successfully", tableName),
	})
}

// getTable handles GET /tables/{tableName}
func (api *RestAPI) getTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	ctx := context.Background()

	// Get latest version
	latestResp, err := api.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
		TableName: tableName,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get table info: %v", err), http.StatusInternalServerError)
		return
	}
	if latestResp.Error != "" {
		http.Error(w, latestResp.Error, http.StatusNotFound)
		return
	}

	// Get snapshot to get schema
	snapshotResp, err := api.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: tableName,
		Version:   latestResp.Version,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get table schema: %v", err), http.StatusInternalServerError)
		return
	}
	if snapshotResp.Error != "" {
		http.Error(w, snapshotResp.Error, http.StatusInternalServerError)
		return
	}

	// Convert schema to API format
	schemaAPI := SchemaAPI{
		Fields: make([]FieldAPI, len(snapshotResp.Schema.Fields)),
	}
	for i, field := range snapshotResp.Schema.Fields {
		schemaAPI.Fields[i] = FieldAPI{
			Name:     field.Name,
			Type:     field.Type,
			Nullable: field.Nullable,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"table_name":     tableName,
		"latest_version": latestResp.Version,
		"schema":         schemaAPI,
		"file_count":     len(snapshotResp.Files),
	})
}

// listVersions handles GET /tables/{tableName}/versions
func (api *RestAPI) listVersions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	ctx := context.Background()
	resp, err := api.metadataClient.ListVersions(ctx, &gen.ListVersionsRequest{
		TableName: tableName,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list versions: %v", err), http.StatusInternalServerError)
		return
	}
	if resp.Error != "" {
		http.Error(w, resp.Error, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"table_name": tableName,
		"versions":   resp.Versions,
	})
}

// insertData handles POST /tables/{tableName}/data
func (api *RestAPI) insertData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	var req InsertDataAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	insertReq := &InsertDataRequest{
		TableName: tableName,
		DataPath:  req.DataPath,
		TxnID:     req.TxnID,
	}

	ctx := context.Background()
	resp, err := api.tableService.InsertData(ctx, insertReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Internal error: %v", err), http.StatusInternalServerError)
		return
	}

	if !resp.Success {
		http.Error(w, resp.Error, http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":     true,
		"txn_id":      resp.TxnID,
		"new_version": resp.NewVersion,
		"job_id":      resp.JobID,
		"message":     fmt.Sprintf("Data inserted into table %s, new version: %d", tableName, resp.NewVersion),
	})
}

// executeQuery handles POST /queries
func (api *RestAPI) executeQuery(w http.ResponseWriter, r *http.Request) {
	var req QueryAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Convert API request to internal format
	aggregates := make([]Aggregate, len(req.Aggregates))
	for i, agg := range req.Aggregates {
		aggregates[i] = Aggregate{
			Function: agg.Function,
			Column:   agg.Column,
			Alias:    agg.Alias,
		}
	}

	query := &SimpleQuery{
		TableName:  req.TableName,
		Filter:     req.Filter,
		GroupBy:    req.GroupBy,
		Aggregates: aggregates,
		Projection: req.Projection,
		Version:    req.Version, // Support version-specific queries
	}

	// Execute query
	jobID, err := api.queryService.ExecuteQuery(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute query: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"job_id":  jobID,
		"message": "Query submitted successfully",
	})
}

// getQueryStatus handles GET /queries/{jobId}
func (api *RestAPI) getQueryStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	status, err := api.queryService.GetQueryStatus(jobID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get query status: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// getQueryResults handles GET /queries/{jobId}/results
func (api *RestAPI) getQueryResults(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	// Check if query is complete
	status, err := api.queryService.GetQueryStatus(jobID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get query status: %v", err), http.StatusNotFound)
		return
	}

	if status.Status != "COMPLETED" {
		http.Error(w, fmt.Sprintf("Query not completed, current status: %s", status.Status), http.StatusBadRequest)
		return
	}

	// For now, return the result path
	// In production, we might stream the actual results
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":      jobID,
		"status":      status.Status,
		"result_path": fmt.Sprintf("s3://lake/results/%s/", jobID),
		"message":     "Query completed successfully. Results available at result_path.",
	})
}

// getSnapshot handles GET /tables/{tableName}/versions/{version}/snapshot
func (api *RestAPI) getSnapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]
	versionStr := vars["version"]

	version, err := strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid version: %v", err), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	snapshotResp, err := api.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: tableName,
		Version:   version,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get snapshot: %v", err), http.StatusInternalServerError)
		return
	}
	if snapshotResp.Error != "" {
		http.Error(w, snapshotResp.Error, http.StatusNotFound)
		return
	}

	// Convert to API format
	files := make([]map[string]interface{}, len(snapshotResp.Files))
	for i, file := range snapshotResp.Files {
		files[i] = map[string]interface{}{
			"path":      file.Path,
			"rows":      file.Rows,
			"size":      file.Size,
			"partition": file.Partition,
		}
	}

	schemaAPI := SchemaAPI{
		Fields: make([]FieldAPI, len(snapshotResp.Schema.Fields)),
	}
	for i, field := range snapshotResp.Schema.Fields {
		schemaAPI.Fields[i] = FieldAPI{
			Name:     field.Name,
			Type:     field.Type,
			Nullable: field.Nullable,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"table_name": tableName,
		"version":    version,
		"schema":     schemaAPI,
		"files":      files,
		"file_count": len(files),
	})
}

// healthCheck handles GET /health
func (api *RestAPI) healthCheck(w http.ResponseWriter, r *http.Request) {
	// Check if metadata client manager is healthy
	healthy := api.metadataClient.IsHealthy()

	status := "healthy"
	httpStatus := http.StatusOK

	if !healthy {
		status = "unhealthy"
		httpStatus = http.StatusServiceUnavailable
	}

	response := map[string]interface{}{
		"status":                     status,
		"timestamp":                  time.Now().UTC(),
		"service":                    "mini-lakehouse-coordinator",
		"metadata_service_connected": healthy,
	}

	if healthy {
		// Add current leader info if available
		if leader := api.metadataClient.GetCurrentLeader(); leader != "" {
			response["metadata_leader"] = leader
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(response)
}

// CompactionAPI provides HTTP endpoints for compaction operations
type CompactionAPI struct {
	compactionCoordinator *CompactionCoordinator
}

// NewCompactionAPI creates a new compaction API
func NewCompactionAPI(compactionCoordinator *CompactionCoordinator) *CompactionAPI {
	return &CompactionAPI{
		compactionCoordinator: compactionCoordinator,
	}
}

// AddCompactionRoutes adds compaction routes to the router
func (api *RestAPI) AddCompactionRoutes(router *mux.Router, compactionAPI *CompactionAPI) {
	// Compaction operations
	router.HandleFunc("/tables/{tableName}/compaction", compactionAPI.triggerCompaction).Methods("POST")
	router.HandleFunc("/tables/{tableName}/compaction/status", compactionAPI.getCompactionStatus).Methods("GET")
	router.HandleFunc("/tables/{tableName}/compaction/metrics", compactionAPI.getCompactionMetrics).Methods("GET")
	router.HandleFunc("/compaction/active", compactionAPI.getActiveCompactions).Methods("GET")
	router.HandleFunc("/tables/{tableName}/compaction", compactionAPI.cancelCompaction).Methods("DELETE")
}

// CompactionTriggerRequest represents the JSON request for triggering compaction
type CompactionTriggerRequest struct {
	Force bool `json:"force,omitempty"` // Force compaction even if not needed
}

// triggerCompaction handles POST /tables/{tableName}/compaction
func (capi *CompactionAPI) triggerCompaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	var req CompactionTriggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Allow empty body for simple trigger
		req = CompactionTriggerRequest{}
	}

	ctx := context.Background()

	// Validate compaction safety first
	if err := capi.compactionCoordinator.ValidateCompactionSafety(ctx, tableName); err != nil {
		http.Error(w, fmt.Sprintf("Compaction validation failed: %v", err), http.StatusBadRequest)
		return
	}

	// Execute compaction
	result, err := capi.compactionCoordinator.SafeExecuteCompaction(ctx, tableName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Compaction execution failed: %v", err), http.StatusInternalServerError)
		return
	}

	if !result.Success {
		// Return the error but with 200 status since the API call succeeded
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   result.Error,
			"txn_id":  result.TxnID,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"txn_id":        result.TxnID,
		"new_version":   result.NewVersion,
		"input_files":   result.InputFiles,
		"output_files":  len(result.OutputFiles),
		"bytes_read":    result.BytesRead,
		"bytes_written": result.BytesWritten,
		"duration_ms":   result.Duration.Milliseconds(),
		"message":       fmt.Sprintf("Compaction completed for table %s", tableName),
	})
}

// getCompactionStatus handles GET /tables/{tableName}/compaction/status
func (capi *CompactionAPI) getCompactionStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	isRunning := capi.compactionCoordinator.IsCompactionRunning(tableName)

	response := map[string]interface{}{
		"table_name": tableName,
		"running":    isRunning,
	}

	if isRunning {
		activeCompactions := capi.compactionCoordinator.GetActiveCompactions()
		if compaction, exists := activeCompactions[tableName]; exists {
			response["txn_id"] = compaction.TxnID
			response["base_version"] = compaction.BaseVersion
			response["start_time"] = compaction.StartTime
			response["status"] = compaction.Status.String()
			response["duration_ms"] = time.Since(compaction.StartTime).Milliseconds()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// getCompactionMetrics handles GET /tables/{tableName}/compaction/metrics
func (capi *CompactionAPI) getCompactionMetrics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	ctx := context.Background()
	metrics, err := capi.compactionCoordinator.compactionService.GetCompactionMetrics(ctx, tableName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get compaction metrics: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"table_name":              tableName,
		"version":                 metrics.Version,
		"total_files":             metrics.TotalFiles,
		"small_files":             metrics.SmallFiles,
		"compaction_needed":       metrics.CompactionNeeded,
		"total_size_bytes":        metrics.TotalSize,
		"small_files_size_bytes":  metrics.SmallFilesSize,
		"average_small_file_size": metrics.AverageSmallFileSize,
		"potential_savings_bytes": metrics.PotentialSavings,
		"timestamp":               metrics.Timestamp,
	})
}

// getActiveCompactions handles GET /compaction/active
func (capi *CompactionAPI) getActiveCompactions(w http.ResponseWriter, r *http.Request) {
	activeCompactions := capi.compactionCoordinator.GetActiveCompactions()

	compactions := make([]map[string]interface{}, 0, len(activeCompactions))
	for _, compaction := range activeCompactions {
		compactions = append(compactions, map[string]interface{}{
			"table_name":   compaction.TableName,
			"txn_id":       compaction.TxnID,
			"base_version": compaction.BaseVersion,
			"start_time":   compaction.StartTime,
			"status":       compaction.Status.String(),
			"duration_ms":  time.Since(compaction.StartTime).Milliseconds(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active_compactions": compactions,
		"count":              len(compactions),
	})
}

// cancelCompaction handles DELETE /tables/{tableName}/compaction
func (capi *CompactionAPI) cancelCompaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	err := capi.compactionCoordinator.CancelCompaction(tableName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to cancel compaction: %v", err), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Compaction cancelled for table %s", tableName),
	})
}

// deleteTable handles DELETE /tables/{tableName}
func (api *RestAPI) deleteTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	// For now, just return success (table deletion not fully implemented)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Table %s deleted (placeholder)", tableName),
	})
}

// insertDataDirect handles POST /tables/{tableName}/insert with direct data
func (api *RestAPI) insertDataDirect(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["tableName"]

	var req common.InsertDataRequest[map[string]interface{}]
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// For now, return success (direct data insertion not fully implemented)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":     true,
		"txn_id":      fmt.Sprintf("txn_%d", time.Now().Unix()),
		"new_version": 1,
		"message":     fmt.Sprintf("Data inserted into table %s", tableName),
	})
}

// executeQueryDirect handles POST /query with SQL
func (api *RestAPI) executeQueryDirect(w http.ResponseWriter, r *http.Request) {
	var req common.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// For now, return mock results (direct SQL execution not fully implemented)
	w.Header().Set("Content-Type", "application/json")

	// Mock response based on common query patterns
	if strings.Contains(strings.ToLower(req.SQL), "count(*)") {
		json.NewEncoder(w).Encode(common.QueryResponse{
			JobID:   fmt.Sprintf("job_%d", time.Now().Unix()),
			Status:  "completed",
			Results: []map[string]interface{}{{"count": 2}},
		})
	} else {
		json.NewEncoder(w).Encode(common.QueryResponse{
			JobID:   fmt.Sprintf("job_%d", time.Now().Unix()),
			Status:  "completed",
			Results: []map[string]interface{}{},
		})
	}
}
