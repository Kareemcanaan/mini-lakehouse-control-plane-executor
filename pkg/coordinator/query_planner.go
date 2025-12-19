package coordinator

import (
	"context"
	"fmt"
	"mini-lakehouse/proto/gen"
	"time"
)

// QueryPlan represents a complete query execution plan
type QueryPlan struct {
	JobID  string
	Stages []Stage
}

// Stage represents a single stage in the query execution plan
type Stage struct {
	ID           int
	StageType    StageType
	Tasks        []Task
	Dependencies []int // Stage IDs this stage depends on
}

// StageType defines the type of stage in the query plan
type StageType int

const (
	MapStage StageType = iota
	ReduceStage
)

// Task represents a single task within a stage
type Task struct {
	ID       int
	Spec     *TaskSpec
	Attempt  int
	WorkerID string
	Status   TaskStatus
}

// TaskSpec represents the specification for a task (mirrors proto definition)
type TaskSpec struct {
	Operation     TaskOperation
	InputFiles    []string
	Filter        string
	GroupBy       []string
	Aggregates    []*Aggregate
	NumPartitions uint32
	OutputPath    string
	Projection    []string
}

// TaskOperation represents the type of operation for a task
type TaskOperation int

const (
	SCAN TaskOperation = iota
	MAP_FILTER
	MAP_PROJECT
	MAP_AGG
	REDUCE_AGG
	SHUFFLE
)

// TaskStatus represents the current status of a task
type TaskStatus int

const (
	TaskPending TaskStatus = iota
	TaskRunning
	TaskCompleted
	TaskFailed
)

// QueryPlanner handles the creation of query execution plans
type QueryPlanner struct {
	metadataClient gen.MetadataServiceClient
}

// NewQueryPlanner creates a new query planner instance
func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{}
}

// NewQueryPlannerWithMetadata creates a new query planner instance with metadata service client
func NewQueryPlannerWithMetadata(metadataClient gen.MetadataServiceClient) *QueryPlanner {
	return &QueryPlanner{
		metadataClient: metadataClient,
	}
}

// SimpleQuery represents a simplified query structure for MVP
type SimpleQuery struct {
	TableName  string
	Filter     string
	GroupBy    []string
	Aggregates []Aggregate
	Projection []string
}

// Aggregate represents an aggregation operation
type Aggregate struct {
	Function string // "sum", "count", "avg", "min", "max"
	Column   string
	Alias    string
}

// PlanQuery creates a stage-based execution plan for the given query
func (qp *QueryPlanner) PlanQuery(query *SimpleQuery) (*QueryPlan, error) {
	return qp.PlanQueryWithVersion(query, 0) // Use latest version by default
}

// PlanQueryWithVersion creates a stage-based execution plan for the given query at a specific version
func (qp *QueryPlanner) PlanQueryWithVersion(query *SimpleQuery, version uint64) (*QueryPlan, error) {
	jobID := generateJobID()

	plan := &QueryPlan{
		JobID:  jobID,
		Stages: []Stage{},
	}

	// Get input files from metadata service
	inputFiles, err := qp.getInputFiles(query.TableName, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get input files: %w", err)
	}

	// For MVP, we'll create a simple two-stage plan:
	// Stage 0: Map stage (scan, filter, partial aggregation)
	// Stage 1: Reduce stage (final aggregation)

	// Stage 0: Map Stage
	mapStage := Stage{
		ID:           0,
		StageType:    MapStage,
		Tasks:        []Task{},
		Dependencies: []int{}, // No dependencies for first stage
	}

	// Create map tasks - for MVP, we'll create one task per input file
	// In production, we might group multiple files per task for efficiency
	for i, inputFile := range inputFiles {
		mapTask := Task{
			ID:      i,
			Attempt: 1,
			Status:  TaskPending,
			Spec: &TaskSpec{
				Operation:     determineMapOperation(query),
				InputFiles:    []string{inputFile},
				Filter:        query.Filter,
				GroupBy:       query.GroupBy,
				Aggregates:    convertAggregates(query.Aggregates),
				NumPartitions: 4, // Default partitioning for shuffle
				OutputPath:    fmt.Sprintf("s3://lake/shuffle/%s/stage_0/task_%d/attempt_1/", jobID, i),
				Projection:    query.Projection,
			},
		}
		mapStage.Tasks = append(mapStage.Tasks, mapTask)
	}

	// If no input files, create a single task with empty input (for metadata-only queries)
	if len(inputFiles) == 0 {
		mapTask := Task{
			ID:      0,
			Attempt: 1,
			Status:  TaskPending,
			Spec: &TaskSpec{
				Operation:     determineMapOperation(query),
				InputFiles:    []string{},
				Filter:        query.Filter,
				GroupBy:       query.GroupBy,
				Aggregates:    convertAggregates(query.Aggregates),
				NumPartitions: 1,
				OutputPath:    fmt.Sprintf("s3://lake/shuffle/%s/stage_0/task_0/attempt_1/", jobID),
				Projection:    query.Projection,
			},
		}
		mapStage.Tasks = append(mapStage.Tasks, mapTask)
	}

	plan.Stages = append(plan.Stages, mapStage)

	// Stage 1: Reduce Stage (only if we have aggregations)
	if len(query.Aggregates) > 0 {
		reduceStage := Stage{
			ID:           1,
			StageType:    ReduceStage,
			Tasks:        []Task{},
			Dependencies: []int{0}, // Depends on stage 0
		}

		reduceTask := Task{
			ID:      0,
			Attempt: 1,
			Status:  TaskPending,
			Spec: &TaskSpec{
				Operation:     REDUCE_AGG,
				InputFiles:    []string{fmt.Sprintf("s3://lake/shuffle/%s/stage_0/", jobID)}, // Will read from stage 0 outputs
				GroupBy:       query.GroupBy,
				Aggregates:    convertAggregates(query.Aggregates),
				NumPartitions: 1, // Final result
				OutputPath:    fmt.Sprintf("s3://lake/results/%s/", jobID),
				Projection:    query.Projection,
			},
		}
		reduceStage.Tasks = append(reduceStage.Tasks, reduceTask)
		plan.Stages = append(plan.Stages, reduceStage)
	}

	return plan, nil
}

// getInputFiles retrieves the list of input files for a table from the metadata service
func (qp *QueryPlanner) getInputFiles(tableName string, version uint64) ([]string, error) {
	// If no metadata client, return placeholder for testing
	if qp.metadataClient == nil {
		return []string{fmt.Sprintf("s3://lake/tables/%s/data/part-00000-placeholder.parquet", tableName)}, nil
	}

	ctx := context.Background()

	// If version is 0, get the latest version
	if version == 0 {
		latestResp, err := qp.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
			TableName: tableName,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get latest version: %w", err)
		}
		if latestResp.Error != "" {
			return nil, fmt.Errorf("metadata service error: %s", latestResp.Error)
		}
		version = latestResp.Version
	}

	// Get snapshot for the specified version
	snapshotResp, err := qp.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: tableName,
		Version:   version,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}
	if snapshotResp.Error != "" {
		return nil, fmt.Errorf("metadata service error: %s", snapshotResp.Error)
	}

	// Convert file info to file paths
	var inputFiles []string
	for _, fileInfo := range snapshotResp.Files {
		inputFiles = append(inputFiles, fmt.Sprintf("s3://lake/tables/%s/%s", tableName, fileInfo.Path))
	}

	return inputFiles, nil
}

// determineMapOperation determines the appropriate map operation based on the query
func determineMapOperation(query *SimpleQuery) TaskOperation {
	if len(query.Aggregates) > 0 {
		return MAP_AGG
	}
	if query.Filter != "" && len(query.Projection) > 0 {
		return MAP_FILTER // Combined filter and project
	}
	if query.Filter != "" {
		return MAP_FILTER
	}
	if len(query.Projection) > 0 {
		return MAP_PROJECT
	}
	return SCAN
}

// GetTaskDependencies returns the tasks that must complete before the given task can run
func (qp *QueryPlanner) GetTaskDependencies(plan *QueryPlan, stageID, taskID int) []TaskRef {
	var dependencies []TaskRef

	if stageID >= len(plan.Stages) {
		return dependencies
	}

	stage := plan.Stages[stageID]

	// Add dependencies from all tasks in prerequisite stages
	for _, depStageID := range stage.Dependencies {
		if depStageID < len(plan.Stages) {
			depStage := plan.Stages[depStageID]
			for _, task := range depStage.Tasks {
				dependencies = append(dependencies, TaskRef{
					StageID: depStageID,
					TaskID:  task.ID,
				})
			}
		}
	}

	return dependencies
}

// TaskRef represents a reference to a specific task
type TaskRef struct {
	StageID int
	TaskID  int
}

// IsStageReady checks if all dependencies for a stage are completed
func (qp *QueryPlanner) IsStageReady(plan *QueryPlan, stageID int) bool {
	if stageID >= len(plan.Stages) {
		return false
	}

	stage := plan.Stages[stageID]

	// Check if all dependency stages are completed
	for _, depStageID := range stage.Dependencies {
		if depStageID >= len(plan.Stages) {
			return false
		}

		depStage := plan.Stages[depStageID]
		for _, task := range depStage.Tasks {
			if task.Status != TaskCompleted {
				return false
			}
		}
	}

	return true
}

// GetReadyTasks returns all tasks that are ready to be scheduled
func (qp *QueryPlanner) GetReadyTasks(plan *QueryPlan) []TaskRef {
	var readyTasks []TaskRef

	for stageID, stage := range plan.Stages {
		if qp.IsStageReady(plan, stageID) {
			for _, task := range stage.Tasks {
				if task.Status == TaskPending {
					readyTasks = append(readyTasks, TaskRef{
						StageID: stageID,
						TaskID:  task.ID,
					})
				}
			}
		}
	}

	return readyTasks
}

// UpdateTaskStatus updates the status of a specific task
func (qp *QueryPlanner) UpdateTaskStatus(plan *QueryPlan, stageID, taskID int, status TaskStatus) error {
	if stageID >= len(plan.Stages) {
		return fmt.Errorf("invalid stage ID: %d", stageID)
	}

	stage := &plan.Stages[stageID]
	for i := range stage.Tasks {
		if stage.Tasks[i].ID == taskID {
			stage.Tasks[i].Status = status
			return nil
		}
	}

	return fmt.Errorf("task not found: stage %d, task %d", stageID, taskID)
}

// IsQueryComplete checks if all tasks in the query plan are completed
func (qp *QueryPlanner) IsQueryComplete(plan *QueryPlan) bool {
	for _, stage := range plan.Stages {
		for _, task := range stage.Tasks {
			if task.Status != TaskCompleted {
				return false
			}
		}
	}
	return true
}

// convertAggregates converts internal Aggregate structs to task spec format
func convertAggregates(aggregates []Aggregate) []*Aggregate {
	var taskAggregates []*Aggregate
	for _, agg := range aggregates {
		taskAggregates = append(taskAggregates, &Aggregate{
			Function: agg.Function,
			Column:   agg.Column,
			Alias:    agg.Alias,
		})
	}
	return taskAggregates
}

// generateJobID creates a unique job identifier
func generateJobID() string {
	return fmt.Sprintf("job_%d", time.Now().UnixNano())
}

// GetTask retrieves a specific task from the plan
func (qp *QueryPlanner) GetTask(plan *QueryPlan, stageID, taskID int) (*Task, error) {
	if stageID >= len(plan.Stages) {
		return nil, fmt.Errorf("invalid stage ID: %d", stageID)
	}

	stage := &plan.Stages[stageID]
	for i := range stage.Tasks {
		if stage.Tasks[i].ID == taskID {
			return &stage.Tasks[i], nil
		}
	}

	return nil, fmt.Errorf("task not found: stage %d, task %d", stageID, taskID)
}

// AssignTaskToWorker assigns a task to a specific worker
func (qp *QueryPlanner) AssignTaskToWorker(plan *QueryPlan, stageID, taskID int, workerID string) error {
	task, err := qp.GetTask(plan, stageID, taskID)
	if err != nil {
		return err
	}

	task.WorkerID = workerID
	task.Status = TaskRunning
	return nil
}

// RetryTask creates a new attempt for a failed task
func (qp *QueryPlanner) RetryTask(plan *QueryPlan, stageID, taskID int) error {
	task, err := qp.GetTask(plan, stageID, taskID)
	if err != nil {
		return err
	}

	// Increment attempt number
	task.Attempt++
	task.Status = TaskPending
	task.WorkerID = ""

	// Update output path to include new attempt
	if task.Spec != nil {
		// Extract base path and update with new attempt
		basePath := fmt.Sprintf("s3://lake/shuffle/%s/stage_%d/task_%d/", plan.JobID, stageID, taskID)
		task.Spec.OutputPath = fmt.Sprintf("%sattempt_%d/", basePath, task.Attempt)
	}

	return nil
}

// GetFailedTasks returns all tasks that have failed and can be retried
func (qp *QueryPlanner) GetFailedTasks(plan *QueryPlan) []TaskRef {
	var failedTasks []TaskRef

	for stageID, stage := range plan.Stages {
		for _, task := range stage.Tasks {
			if task.Status == TaskFailed {
				failedTasks = append(failedTasks, TaskRef{
					StageID: stageID,
					TaskID:  task.ID,
				})
			}
		}
	}

	return failedTasks
}

// GetRunningTasks returns all tasks that are currently running
func (qp *QueryPlanner) GetRunningTasks(plan *QueryPlan) []TaskRef {
	var runningTasks []TaskRef

	for stageID, stage := range plan.Stages {
		for _, task := range stage.Tasks {
			if task.Status == TaskRunning {
				runningTasks = append(runningTasks, TaskRef{
					StageID: stageID,
					TaskID:  task.ID,
				})
			}
		}
	}

	return runningTasks
}

// GetTasksByWorker returns all tasks assigned to a specific worker
func (qp *QueryPlanner) GetTasksByWorker(plan *QueryPlan, workerID string) []TaskRef {
	var workerTasks []TaskRef

	for stageID, stage := range plan.Stages {
		for _, task := range stage.Tasks {
			if task.WorkerID == workerID {
				workerTasks = append(workerTasks, TaskRef{
					StageID: stageID,
					TaskID:  task.ID,
				})
			}
		}
	}

	return workerTasks
}
