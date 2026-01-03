package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"mini-lakehouse/proto/gen"

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
	router.HandleFunc("/tables/{tableName}/versions", api.listVersions).Methods("GET")
	router.HandleFunc("/tables/{tableName}/versions/{version}/snapshot", api.getSnapshot).Methods("GET")
	router.HandleFunc("/tables/{tableName}/data", api.insertData).Methods("POST")

	// Query operations
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
	var req CreateTableAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Convert API request to internal format
	schema := &gen.Schema{
		Fields: make([]*gen.Field, len(req.Schema.Fields)),
	}
	for i, field := range req.Schema.Fields {
		schema.Fields[i] = &gen.Field{
			Name:     field.Name,
			Type:     field.Type,
			Nullable: field.Nullable,
		}
	}

	createReq := &CreateTableRequest{
		TableName: req.TableName,
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
		"message": fmt.Sprintf("Table %s created successfully", req.TableName),
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
		"service":   "mini-lakehouse-coordinator",
	})
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
