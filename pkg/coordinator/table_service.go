package coordinator

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"mini-lakehouse/pkg/storage"
	"mini-lakehouse/proto/gen"

	"github.com/google/uuid"
)

// TableService handles table creation and data insertion workflows
type TableService struct {
	metadataClient     MetadataClient
	storageClient      *storage.Client
	taskScheduler      *TaskScheduler
	queryPlanner       *QueryPlanner
	transactionManager *TransactionManager
}

// NewTableService creates a new table service
func NewTableService(
	metadataClient MetadataClient,
	storageClient *storage.Client,
	taskScheduler *TaskScheduler,
	queryPlanner *QueryPlanner,
) *TableService {
	ts := &TableService{
		metadataClient:     metadataClient,
		storageClient:      storageClient,
		taskScheduler:      taskScheduler,
		queryPlanner:       queryPlanner,
		transactionManager: NewTransactionManager(),
	}

	// Start transaction cleanup routine
	ts.transactionManager.StartCleanupRoutine()

	return ts
}

// CreateTableRequest represents a table creation request
type CreateTableRequest struct {
	TableName string
	Schema    *gen.Schema
}

// CreateTableResponse represents a table creation response
type CreateTableResponse struct {
	Success bool
	Error   string
}

// InsertDataRequest represents a data insertion request
type InsertDataRequest struct {
	TableName string
	DataPath  string // Path to source data (CSV, JSON, etc.)
	TxnID     string // Optional transaction ID for idempotency
}

// InsertDataResponse represents a data insertion response
type InsertDataResponse struct {
	Success    bool
	Error      string
	TxnID      string
	NewVersion uint64
	JobID      string // For tracking the insertion job
}

// CreateTable creates a new table with the specified schema
func (ts *TableService) CreateTable(ctx context.Context, req *CreateTableRequest) (*CreateTableResponse, error) {
	log.Printf("Creating table %s with %d fields", req.TableName, len(req.Schema.Fields))

	// Validate table name
	if err := ts.validateTableName(req.TableName); err != nil {
		return &CreateTableResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid table name: %v", err),
		}, nil
	}

	// Validate schema
	if err := ts.validateSchema(req.Schema); err != nil {
		return &CreateTableResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid schema: %v", err),
		}, nil
	}

	// Create table via metadata service
	createResp, err := ts.metadataClient.CreateTable(ctx, &gen.CreateTableRequest{
		TableName: req.TableName,
		Schema:    req.Schema,
	})
	if err != nil {
		return &CreateTableResponse{
			Success: false,
			Error:   fmt.Sprintf("metadata service error: %v", err),
		}, nil
	}

	if !createResp.Success {
		return &CreateTableResponse{
			Success: false,
			Error:   createResp.Error,
		}, nil
	}

	log.Printf("Successfully created table %s", req.TableName)

	return &CreateTableResponse{
		Success: true,
	}, nil
}

// InsertData inserts data into a table using the distributed execution engine
func (ts *TableService) InsertData(ctx context.Context, req *InsertDataRequest) (*InsertDataResponse, error) {
	log.Printf("Inserting data into table %s from %s", req.TableName, req.DataPath)

	// Get current table version first
	latestResp, err := ts.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
		TableName: req.TableName,
	})
	if err != nil {
		return &InsertDataResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get latest version: %v", err),
		}, nil
	}
	if latestResp.Error != "" {
		return &InsertDataResponse{
			Success: false,
			Error:   latestResp.Error,
		}, nil
	}

	baseVersion := latestResp.Version

	// Generate transaction ID if not provided and start transaction
	txnID := req.TxnID
	if txnID == "" {
		txnID = ts.transactionManager.StartTransaction(req.TableName, baseVersion)
	} else {
		// If transaction ID is provided, check if it already exists (idempotency)
		if txnInfo, result, exists := ts.transactionManager.GetTransactionStatus(txnID); exists {
			if result != nil && result.Success {
				// Transaction already completed successfully
				return &InsertDataResponse{
					Success:    true,
					NewVersion: result.NewVersion,
					TxnID:      txnID,
					JobID:      "", // No job needed for completed transaction
				}, nil
			} else if txnInfo != nil && txnInfo.Status == TxnInProgress {
				// Transaction is still in progress, return error
				return &InsertDataResponse{
					Success: false,
					Error:   "transaction is already in progress",
					TxnID:   txnID,
				}, nil
			}
		}
		// Start new transaction with provided ID (this will fail if ID already exists)
		// For now, we'll generate a new ID to avoid conflicts
		txnID = ts.transactionManager.StartTransaction(req.TableName, baseVersion)
	}

	// Create insertion job
	jobID := generateJobID()
	tempPath := fmt.Sprintf("tables/%s/_tmp/%s/", req.TableName, txnID)

	// Create insertion query (this will be a special type of query for data loading)
	insertQuery := &InsertQuery{
		TableName:   req.TableName,
		SourcePath:  req.DataPath,
		TempPath:    tempPath,
		TxnID:       txnID,
		BaseVersion: baseVersion,
	}

	// Execute insertion job
	plan, err := ts.planInsertQuery(insertQuery)
	if err != nil {
		return &InsertDataResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to plan insertion: %v", err),
			TxnID:   txnID,
		}, nil
	}

	// Schedule the insertion job
	err = ts.scheduleInsertJob(ctx, plan)
	if err != nil {
		return &InsertDataResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to schedule insertion job: %v", err),
			TxnID:   txnID,
		}, nil
	}

	// Wait for job completion
	result, err := ts.waitForInsertCompletion(ctx, jobID, 10*time.Minute)
	if err != nil {
		return &InsertDataResponse{
			Success: false,
			Error:   fmt.Sprintf("insertion job failed: %v", err),
			TxnID:   txnID,
			JobID:   jobID,
		}, nil
	}

	if !result.Success {
		return &InsertDataResponse{
			Success: false,
			Error:   result.Error,
			TxnID:   txnID,
			JobID:   jobID,
		}, nil
	}

	// Commit the transaction
	newVersion, err := ts.commitInsertion(ctx, req.TableName, baseVersion, txnID, result.OutputFiles)
	if err != nil {
		return &InsertDataResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to commit insertion: %v", err),
			TxnID:   txnID,
			JobID:   jobID,
		}, nil
	}

	log.Printf("Successfully inserted data into table %s, new version: %d", req.TableName, newVersion)

	return &InsertDataResponse{
		Success:    true,
		TxnID:      txnID,
		NewVersion: newVersion,
		JobID:      jobID,
	}, nil
}

// InsertQuery represents a data insertion query
type InsertQuery struct {
	TableName   string
	SourcePath  string
	TempPath    string
	TxnID       string
	BaseVersion uint64
}

// InsertJobResult represents the result of an insertion job
type InsertJobResult struct {
	Success     bool
	Error       string
	OutputFiles []string
	Metrics     *InsertMetrics
}

// InsertMetrics represents metrics for an insertion job
type InsertMetrics struct {
	RowsInserted uint64
	BytesWritten uint64
	FilesCreated int
	DurationMs   int64
}

// planInsertQuery creates an execution plan for data insertion
func (ts *TableService) planInsertQuery(query *InsertQuery) (*QueryPlan, error) {
	jobID := generateJobID()

	plan := &QueryPlan{
		JobID:  jobID,
		Stages: []Stage{},
	}

	// Stage 0: Data loading and conversion stage
	// This stage reads the source data and converts it to Parquet format
	loadStage := Stage{
		ID:           0,
		StageType:    MapStage,
		Tasks:        []Task{},
		Dependencies: []int{},
	}

	// Create a single task for data loading
	// In production, we might split large files into multiple tasks
	loadTask := Task{
		ID:      0,
		Attempt: 1,
		Status:  TaskPending,
		Spec: &TaskSpec{
			Operation:     SCAN, // We'll use SCAN operation for data loading
			InputFiles:    []string{query.SourcePath},
			OutputPath:    fmt.Sprintf("s3://lake/%s", query.TempPath),
			NumPartitions: 1, // Single output file for simplicity
		},
	}

	loadStage.Tasks = append(loadStage.Tasks, loadTask)
	plan.Stages = append(plan.Stages, loadStage)

	return plan, nil
}

// scheduleInsertJob schedules an insertion job for execution
func (ts *TableService) scheduleInsertJob(ctx context.Context, plan *QueryPlan) error {
	// Add the plan to active queries in the task scheduler
	ts.taskScheduler.mu.Lock()
	ts.taskScheduler.activeQueries[plan.JobID] = plan
	ts.taskScheduler.mu.Unlock()

	log.Printf("Scheduled insertion job %s", plan.JobID)
	return nil
}

// waitForInsertCompletion waits for an insertion job to complete
func (ts *TableService) waitForInsertCompletion(ctx context.Context, jobID string, timeout time.Duration) (*InsertJobResult, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			status, err := ts.taskScheduler.GetQueryStatus(jobID)
			if err != nil {
				return nil, fmt.Errorf("failed to get job status: %w", err)
			}

			switch status.Status {
			case "COMPLETED":
				// Collect output files from the completed tasks
				outputFiles, err := ts.collectInsertOutputs(jobID)
				if err != nil {
					return &InsertJobResult{
						Success: false,
						Error:   fmt.Sprintf("failed to collect outputs: %v", err),
					}, nil
				}

				return &InsertJobResult{
					Success:     true,
					OutputFiles: outputFiles,
				}, nil

			case "FAILED":
				return &InsertJobResult{
					Success: false,
					Error:   "insertion job failed",
				}, nil

			default:
				// Job still running, wait and check again
				time.Sleep(1 * time.Second)
				continue
			}
		}
	}

	return nil, fmt.Errorf("insertion job timed out after %v", timeout)
}

// collectInsertOutputs collects the output files from a completed insertion job
func (ts *TableService) collectInsertOutputs(jobID string) ([]string, error) {
	// For now, we'll construct the expected output paths based on the job structure
	// In production, we'd read the SUCCESS manifests to get the actual output files

	// Get the plan to understand the output structure
	ts.taskScheduler.mu.RLock()
	plan, exists := ts.taskScheduler.activeQueries[jobID]
	ts.taskScheduler.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	var outputFiles []string
	for _, stage := range plan.Stages {
		for _, task := range stage.Tasks {
			if task.Status == TaskCompleted && task.Spec != nil {
				// Construct the expected output file path
				outputPath := task.Spec.OutputPath
				if !strings.HasSuffix(outputPath, "/") {
					outputPath += "/"
				}
				outputFiles = append(outputFiles, outputPath+"part-0.parquet")
			}
		}
	}

	return outputFiles, nil
}

// commitInsertion commits the insertion transaction to the metadata service
func (ts *TableService) commitInsertion(ctx context.Context, tableName string, baseVersion uint64, txnID string, outputFiles []string) (uint64, error) {
	// Convert temporary file paths to final data paths
	var fileAdds []*gen.FileAdd
	for i, tempFile := range outputFiles {
		// Extract the relative path from the temporary location
		// tempFile format: s3://lake/tables/tableName/_tmp/txnID/part-0.parquet
		// final path: data/part-00000-uuid.parquet

		finalPath := fmt.Sprintf("data/part-%05d-%s.parquet", i, uuid.New().String())

		// Move file from temp to final location
		err := ts.moveFileToFinalLocation(ctx, tempFile, tableName, finalPath)
		if err != nil {
			return 0, fmt.Errorf("failed to move file to final location: %w", err)
		}

		// Get file stats (for now, use placeholder values)
		fileAdd := &gen.FileAdd{
			Path:      finalPath,
			Rows:      1000,                    // TODO: Get actual row count from task metrics
			Size:      50000,                   // TODO: Get actual file size
			Partition: make(map[string]string), // No partitioning for MVP
			Stats: &gen.FileStats{
				MinValues: make(map[string]string),
				MaxValues: make(map[string]string),
			},
		}

		fileAdds = append(fileAdds, fileAdd)
	}

	// Commit the transaction using transaction manager for retry logic
	commitResp, err := ts.transactionManager.CommitTransaction(
		ts.metadataClient,
		txnID,
		fileAdds,
		[]*gen.FileRemove{}, // No removes for insertion
	)
	if err != nil {
		return 0, fmt.Errorf("transaction commit failed: %w", err)
	}

	if commitResp.Error != "" {
		return 0, fmt.Errorf("commit failed: %s", commitResp.Error)
	}

	// Clean up temporary files
	err = ts.cleanupTempFiles(ctx, tableName, txnID)
	if err != nil {
		log.Printf("Warning: failed to clean up temporary files: %v", err)
	}

	return commitResp.NewVersion, nil
}

// moveFileToFinalLocation moves a file from temporary location to final data location
func (ts *TableService) moveFileToFinalLocation(ctx context.Context, tempPath, tableName, finalPath string) error {
	// Extract the object key from the temp path
	// tempPath format: s3://lake/tables/tableName/_tmp/txnID/part-0.parquet
	tempKey := strings.TrimPrefix(tempPath, "s3://lake/")
	finalKey := fmt.Sprintf("tables/%s/%s", tableName, finalPath)

	// Copy the file to the final location
	err := ts.storageClient.CopyObject(ctx, tempKey, finalKey)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	// Delete the temporary file
	err = ts.storageClient.DeleteObject(ctx, tempKey)
	if err != nil {
		log.Printf("Warning: failed to delete temporary file %s: %v", tempKey, err)
	}

	return nil
}

// cleanupTempFiles removes temporary files after successful commit
func (ts *TableService) cleanupTempFiles(ctx context.Context, tableName, txnID string) error {
	tempPrefix := fmt.Sprintf("tables/%s/_tmp/%s/", tableName, txnID)

	// List and delete all objects with the temp prefix
	objects, err := ts.storageClient.ListObjects(ctx, tempPrefix)
	if err != nil {
		return fmt.Errorf("failed to list temp objects: %w", err)
	}

	for _, obj := range objects {
		err = ts.storageClient.DeleteObject(ctx, obj.Key)
		if err != nil {
			log.Printf("Warning: failed to delete temp object %s: %v", obj.Key, err)
		}
	}

	return nil
}

// validateTableName validates a table name
func (ts *TableService) validateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if len(tableName) > 64 {
		return fmt.Errorf("table name too long (max 64 characters)")
	}

	// Check for valid characters (alphanumeric and underscore)
	for _, r := range tableName {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return fmt.Errorf("table name contains invalid character: %c", r)
		}
	}

	return nil
}

// validateSchema validates a table schema
func (ts *TableService) validateSchema(schema *gen.Schema) error {
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	if len(schema.Fields) == 0 {
		return fmt.Errorf("schema must have at least one field")
	}

	fieldNames := make(map[string]bool)
	for _, field := range schema.Fields {
		if field.Name == "" {
			return fmt.Errorf("field name cannot be empty")
		}

		if fieldNames[field.Name] {
			return fmt.Errorf("duplicate field name: %s", field.Name)
		}
		fieldNames[field.Name] = true

		if !ts.isValidFieldType(field.Type) {
			return fmt.Errorf("invalid field type: %s", field.Type)
		}
	}

	return nil
}

// isValidFieldType checks if a field type is valid
func (ts *TableService) isValidFieldType(fieldType string) bool {
	validTypes := map[string]bool{
		"int32":     true,
		"int64":     true,
		"float32":   true,
		"float64":   true,
		"string":    true,
		"boolean":   true,
		"date":      true,
		"timestamp": true,
	}

	return validTypes[fieldType]
}

// generateTxnID generates a unique transaction ID
func generateTxnID() string {
	return fmt.Sprintf("txn_%d_%s", time.Now().UnixNano(), uuid.New().String()[:8])
}
