package coordinator

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"mini-lakehouse/proto/gen"
)

// DistributedQueryExecutor handles multi-stage query execution with shuffle
type DistributedQueryExecutor struct {
	taskScheduler   *TaskScheduler
	queryPlanner    *QueryPlanner
	metadataClient  gen.MetadataServiceClient
	faultTolerance  *FaultToleranceManager
	snapshotManager *SnapshotIsolationManager

	// Active executions
	mu               sync.RWMutex
	activeExecutions map[string]*QueryExecution // jobID -> execution
}

// QueryExecution tracks the state of a distributed query execution
type QueryExecution struct {
	JobID          string
	Query          *SimpleQuery
	Plan           *QueryPlan
	Status         QueryExecutionStatus
	StartTime      time.Time
	EndTime        *time.Time
	Error          string
	ResultLocation string

	// Stage tracking
	StageResults    map[int][]string // stageID -> output paths
	CurrentStage    int
	CompletedStages map[int]bool
}

// QueryExecutionStatus represents the status of query execution
type QueryExecutionStatus string

const (
	QueryStatusPending   QueryExecutionStatus = "PENDING"
	QueryStatusRunning   QueryExecutionStatus = "RUNNING"
	QueryStatusCompleted QueryExecutionStatus = "COMPLETED"
	QueryStatusFailed    QueryExecutionStatus = "FAILED"
)

// NewDistributedQueryExecutor creates a new distributed query executor
func NewDistributedQueryExecutor(
	taskScheduler *TaskScheduler,
	queryPlanner *QueryPlanner,
	metadataClient gen.MetadataServiceClient,
	faultTolerance *FaultToleranceManager,
) *DistributedQueryExecutor {
	snapshotManager := NewSnapshotIsolationManager(metadataClient)

	return &DistributedQueryExecutor{
		taskScheduler:    taskScheduler,
		queryPlanner:     queryPlanner,
		metadataClient:   metadataClient,
		faultTolerance:   faultTolerance,
		snapshotManager:  snapshotManager,
		activeExecutions: make(map[string]*QueryExecution),
	}
}

// ExecuteQuery executes a distributed query with multi-stage processing
func (dqe *DistributedQueryExecutor) ExecuteQuery(ctx context.Context, query *SimpleQuery) (*QueryExecution, error) {
	log.Printf("Starting distributed execution of query on table %s at version %d", query.TableName, query.Version)

	// Create snapshot-isolated query
	snapshotQuery, snapshot, err := dqe.snapshotManager.CreateSnapshotReadQuery(
		ctx,
		query.TableName,
		query.Version,
		query.Filter,
		query.GroupBy,
		query.Aggregates,
		query.Projection,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot-isolated query: %w", err)
	}

	log.Printf("Query will execute against table %s at version %d with %d files",
		snapshot.TableName, snapshot.Version, len(snapshot.Files))

	// Create query plan using the snapshot-isolated query
	plan, err := dqe.queryPlanner.PlanQuery(snapshotQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to plan query: %w", err)
	}

	// Create query execution
	execution := &QueryExecution{
		JobID:           plan.JobID,
		Query:           query,
		Plan:            plan,
		Status:          QueryStatusPending,
		StartTime:       time.Now(),
		StageResults:    make(map[int][]string),
		CurrentStage:    0,
		CompletedStages: make(map[int]bool),
	}

	// Register execution
	dqe.mu.Lock()
	dqe.activeExecutions[execution.JobID] = execution
	dqe.mu.Unlock()

	// Start execution asynchronously
	go dqe.executeQueryAsync(ctx, execution)

	return execution, nil
}

// StartBackgroundServices starts background services for the distributed query executor
func (dqe *DistributedQueryExecutor) StartBackgroundServices(ctx context.Context) {
	// Start snapshot cache cleanup
	go dqe.snapshotManager.StartCacheCleanup(ctx)

	// Start execution cleanup
	go dqe.startExecutionCleanup(ctx)
}

// startExecutionCleanup periodically cleans up old completed executions
func (dqe *DistributedQueryExecutor) startExecutionCleanup(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dqe.CleanupCompletedExecutions(1 * time.Hour)
		}
	}
}

// executeQueryAsync executes a query asynchronously
func (dqe *DistributedQueryExecutor) executeQueryAsync(ctx context.Context, execution *QueryExecution) {
	execution.Status = QueryStatusRunning

	log.Printf("Starting async execution of query %s with %d stages", execution.JobID, len(execution.Plan.Stages))

	// Execute stages in order
	for stageID, stage := range execution.Plan.Stages {
		log.Printf("Executing stage %d for query %s", stageID, execution.JobID)

		// Wait for dependencies
		if !dqe.waitForStageDependencies(ctx, execution, stageID) {
			dqe.failExecution(execution, fmt.Sprintf("stage %d dependencies failed", stageID))
			return
		}

		// Update stage inputs based on previous stage outputs
		err := dqe.updateStageInputs(execution, stageID)
		if err != nil {
			dqe.failExecution(execution, fmt.Sprintf("failed to update stage inputs: %v", err))
			return
		}

		// Execute stage
		err = dqe.executeStage(ctx, execution, stageID)
		if err != nil {
			dqe.failExecution(execution, fmt.Sprintf("stage %d failed: %v", stageID, err))
			return
		}

		// Mark stage as completed
		execution.CompletedStages[stageID] = true
		execution.CurrentStage = stageID + 1

		log.Printf("Stage %d completed for query %s", stageID, execution.JobID)
	}

	// Collect final results
	err := dqe.collectQueryResults(ctx, execution)
	if err != nil {
		dqe.failExecution(execution, fmt.Sprintf("failed to collect results: %v", err))
		return
	}

	// Mark execution as completed
	execution.Status = QueryStatusCompleted
	endTime := time.Now()
	execution.EndTime = &endTime

	log.Printf("Query %s completed successfully in %v", execution.JobID, endTime.Sub(execution.StartTime))
}

// waitForStageDependencies waits for all dependencies of a stage to complete
func (dqe *DistributedQueryExecutor) waitForStageDependencies(ctx context.Context, execution *QueryExecution, stageID int) bool {
	if stageID >= len(execution.Plan.Stages) {
		return false
	}

	stage := execution.Plan.Stages[stageID]

	// Check if all dependency stages are completed
	for _, depStageID := range stage.Dependencies {
		for {
			select {
			case <-ctx.Done():
				return false
			default:
				if execution.CompletedStages[depStageID] {
					break // This dependency is satisfied
				}

				// Check if execution has failed
				if execution.Status == QueryStatusFailed {
					return false
				}

				// Wait a bit and check again
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return true
}

// updateStageInputs updates the input files for a stage based on previous stage outputs
func (dqe *DistributedQueryExecutor) updateStageInputs(execution *QueryExecution, stageID int) error {
	if stageID >= len(execution.Plan.Stages) {
		return fmt.Errorf("invalid stage ID: %d", stageID)
	}

	stage := &execution.Plan.Stages[stageID]

	// For stages with dependencies, update input files based on previous stage outputs
	if len(stage.Dependencies) > 0 {
		var allInputs []string

		for _, depStageID := range stage.Dependencies {
			if outputs, exists := execution.StageResults[depStageID]; exists {
				allInputs = append(allInputs, outputs...)
			}
		}

		// Update all tasks in this stage with the new inputs
		for i := range stage.Tasks {
			task := &stage.Tasks[i]
			if task.Spec != nil {
				// For reduce stages, we need to read from shuffle outputs
				if stage.StageType == ReduceStage {
					// Construct shuffle input pattern
					shufflePattern := fmt.Sprintf("s3://lake/shuffle/%s/stage_%d/", execution.JobID, stage.Dependencies[0])
					task.Spec.InputFiles = []string{shufflePattern}
				} else {
					task.Spec.InputFiles = allInputs
				}
			}
		}
	}

	return nil
}

// executeStage executes all tasks in a stage
func (dqe *DistributedQueryExecutor) executeStage(ctx context.Context, execution *QueryExecution, stageID int) error {
	if stageID >= len(execution.Plan.Stages) {
		return fmt.Errorf("invalid stage ID: %d", stageID)
	}

	stage := &execution.Plan.Stages[stageID]

	// Add stage tasks to the task scheduler
	dqe.taskScheduler.mu.Lock()
	dqe.taskScheduler.activeQueries[execution.JobID] = execution.Plan
	dqe.taskScheduler.mu.Unlock()

	// Wait for all tasks in the stage to complete
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			allCompleted := true
			anyFailed := false

			for _, task := range stage.Tasks {
				if task.Status == TaskFailed {
					anyFailed = true
					break
				}
				if task.Status != TaskCompleted {
					allCompleted = false
				}
			}

			if anyFailed {
				return fmt.Errorf("one or more tasks in stage %d failed", stageID)
			}

			if allCompleted {
				// Collect stage outputs
				outputs, err := dqe.collectStageOutputs(execution.JobID, stageID)
				if err != nil {
					return fmt.Errorf("failed to collect stage outputs: %w", err)
				}
				execution.StageResults[stageID] = outputs
				return nil
			}

			// Wait a bit and check again
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// collectStageOutputs collects the output files from a completed stage
func (dqe *DistributedQueryExecutor) collectStageOutputs(jobID string, stageID int) ([]string, error) {
	// Read SUCCESS manifests to get the canonical outputs
	var outputs []string

	dqe.taskScheduler.mu.RLock()
	plan, exists := dqe.taskScheduler.activeQueries[jobID]
	dqe.taskScheduler.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("query %s not found", jobID)
	}

	if stageID >= len(plan.Stages) {
		return nil, fmt.Errorf("invalid stage ID: %d", stageID)
	}

	stage := plan.Stages[stageID]
	for _, task := range stage.Tasks {
		if task.Status == TaskCompleted && task.Spec != nil {
			// For shuffle operations, collect all partition outputs
			if task.Spec.Operation == SHUFFLE {
				// Read SUCCESS manifest to get actual outputs
				successPath := fmt.Sprintf("%s/SUCCESS.json", strings.TrimSuffix(task.Spec.OutputPath, "/"))
				// TODO: Read and parse SUCCESS manifest
				// For now, construct expected outputs
				if task.Spec.NumPartitions > 0 {
					for i := uint32(0); i < task.Spec.NumPartitions; i++ {
						partitionPath := fmt.Sprintf("%s/part-%d.parquet", task.Spec.OutputPath, i)
						outputs = append(outputs, partitionPath)
					}
				}
			} else {
				// Single output file
				outputPath := task.Spec.OutputPath
				if !strings.HasSuffix(outputPath, ".parquet") {
					outputPath = fmt.Sprintf("%s/part-0.parquet", strings.TrimSuffix(outputPath, "/"))
				}
				outputs = append(outputs, outputPath)
			}
		}
	}

	return outputs, nil
}

// collectQueryResults collects the final results of a query
func (dqe *DistributedQueryExecutor) collectQueryResults(ctx context.Context, execution *QueryExecution) error {
	// The final stage results are the query results
	finalStageID := len(execution.Plan.Stages) - 1
	if finalOutputs, exists := execution.StageResults[finalStageID]; exists && len(finalOutputs) > 0 {
		// For simplicity, use the first output as the result location
		execution.ResultLocation = finalOutputs[0]

		// In production, we might want to:
		// 1. Combine multiple output files into a single result
		// 2. Move results to a dedicated results location
		// 3. Generate result metadata

		log.Printf("Query %s results available at: %s", execution.JobID, execution.ResultLocation)
		return nil
	}

	return fmt.Errorf("no results found for query %s", execution.JobID)
}

// failExecution marks an execution as failed
func (dqe *DistributedQueryExecutor) failExecution(execution *QueryExecution, reason string) {
	execution.Status = QueryStatusFailed
	execution.Error = reason
	endTime := time.Now()
	execution.EndTime = &endTime

	log.Printf("Query %s failed: %s", execution.JobID, reason)
}

// GetQueryExecution returns the execution status of a query
func (dqe *DistributedQueryExecutor) GetQueryExecution(jobID string) (*QueryExecution, error) {
	dqe.mu.RLock()
	defer dqe.mu.RUnlock()

	execution, exists := dqe.activeExecutions[jobID]
	if !exists {
		return nil, fmt.Errorf("query execution %s not found", jobID)
	}

	return execution, nil
}

// ListActiveExecutions returns all active query executions
func (dqe *DistributedQueryExecutor) ListActiveExecutions() []*QueryExecution {
	dqe.mu.RLock()
	defer dqe.mu.RUnlock()

	var executions []*QueryExecution
	for _, execution := range dqe.activeExecutions {
		executions = append(executions, execution)
	}

	return executions
}

// CleanupCompletedExecutions removes completed executions from memory
func (dqe *DistributedQueryExecutor) CleanupCompletedExecutions(maxAge time.Duration) {
	dqe.mu.Lock()
	defer dqe.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)

	for jobID, execution := range dqe.activeExecutions {
		if execution.Status == QueryStatusCompleted || execution.Status == QueryStatusFailed {
			if execution.EndTime != nil && execution.EndTime.Before(cutoff) {
				delete(dqe.activeExecutions, jobID)
				log.Printf("Cleaned up execution %s", jobID)
			}
		}
	}
}

// CancelQuery cancels a running query
func (dqe *DistributedQueryExecutor) CancelQuery(jobID string) error {
	dqe.mu.Lock()
	defer dqe.mu.Unlock()

	execution, exists := dqe.activeExecutions[jobID]
	if !exists {
		return fmt.Errorf("query execution %s not found", jobID)
	}

	if execution.Status != QueryStatusRunning {
		return fmt.Errorf("query %s is not running (status: %s)", jobID, execution.Status)
	}

	// Mark as failed (cancellation)
	execution.Status = QueryStatusFailed
	execution.Error = "Query cancelled by user"
	endTime := time.Now()
	execution.EndTime = &endTime

	// TODO: Cancel running tasks
	// This would involve:
	// 1. Identifying running tasks for this job
	// 2. Sending cancel requests to workers
	// 3. Cleaning up intermediate outputs

	log.Printf("Query %s cancelled", jobID)
	return nil
}

// GetQueryMetrics returns metrics for a query execution
func (dqe *DistributedQueryExecutor) GetQueryMetrics(jobID string) (*QueryExecutionMetrics, error) {
	execution, err := dqe.GetQueryExecution(jobID)
	if err != nil {
		return nil, err
	}

	metrics := &QueryExecutionMetrics{
		JobID:           execution.JobID,
		Status:          string(execution.Status),
		StartTime:       execution.StartTime,
		EndTime:         execution.EndTime,
		TotalStages:     len(execution.Plan.Stages),
		CompletedStages: len(execution.CompletedStages),
		CurrentStage:    execution.CurrentStage,
	}

	// Calculate duration
	if execution.EndTime != nil {
		duration := execution.EndTime.Sub(execution.StartTime)
		metrics.DurationMs = duration.Milliseconds()
	} else {
		duration := time.Since(execution.StartTime)
		metrics.DurationMs = duration.Milliseconds()
	}

	// Count tasks
	for _, stage := range execution.Plan.Stages {
		metrics.TotalTasks += len(stage.Tasks)
		for _, task := range stage.Tasks {
			switch task.Status {
			case TaskCompleted:
				metrics.CompletedTasks++
			case TaskFailed:
				metrics.FailedTasks++
			case TaskRunning:
				metrics.RunningTasks++
			}
		}
	}

	return metrics, nil
}

// QueryExecutionMetrics represents metrics for a query execution
type QueryExecutionMetrics struct {
	JobID           string     `json:"job_id"`
	Status          string     `json:"status"`
	StartTime       time.Time  `json:"start_time"`
	EndTime         *time.Time `json:"end_time,omitempty"`
	DurationMs      int64      `json:"duration_ms"`
	TotalStages     int        `json:"total_stages"`
	CompletedStages int        `json:"completed_stages"`
	CurrentStage    int        `json:"current_stage"`
	TotalTasks      int        `json:"total_tasks"`
	CompletedTasks  int        `json:"completed_tasks"`
	FailedTasks     int        `json:"failed_tasks"`
	RunningTasks    int        `json:"running_tasks"`
}
