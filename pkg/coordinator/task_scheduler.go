package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"mini-lakehouse/proto/gen"
)

// TaskScheduler manages task scheduling and execution
type TaskScheduler struct {
	mu            sync.RWMutex
	workerManager *WorkerManager
	queryPlanner  *QueryPlanner

	// Active query plans
	activeQueries map[string]*QueryPlan // key: jobID

	// Configuration
	taskTimeout time.Duration
	maxRetries  int
}

// NewTaskScheduler creates a new task scheduler
func NewTaskScheduler(workerManager *WorkerManager, queryPlanner *QueryPlanner) *TaskScheduler {
	return &TaskScheduler{
		workerManager: workerManager,
		queryPlanner:  queryPlanner,
		activeQueries: make(map[string]*QueryPlan),
		taskTimeout:   5 * time.Minute,
		maxRetries:    3,
	}
}

// ScheduleQuery schedules a query for execution
func (ts *TaskScheduler) ScheduleQuery(query *SimpleQuery) (*QueryPlan, error) {
	plan, err := ts.queryPlanner.PlanQuery(query)
	if err != nil {
		return nil, fmt.Errorf("failed to plan query: %w", err)
	}

	ts.mu.Lock()
	ts.activeQueries[plan.JobID] = plan
	ts.mu.Unlock()

	log.Printf("Scheduled query %s with %d stages", plan.JobID, len(plan.Stages))

	return plan, nil
}

// StartScheduling starts the main scheduling loop
func (ts *TaskScheduler) StartScheduling(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts.scheduleReadyTasks()
			ts.handleFailedWorkers()
			ts.retryFailedTasks()
		}
	}
}

// scheduleReadyTasks finds and schedules tasks that are ready to run
func (ts *TaskScheduler) scheduleReadyTasks() {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	for jobID, plan := range ts.activeQueries {
		readyTasks := ts.queryPlanner.GetReadyTasks(plan)

		for _, taskRef := range readyTasks {
			err := ts.scheduleTask(jobID, plan, taskRef.StageID, taskRef.TaskID)
			if err != nil {
				log.Printf("Failed to schedule task %s_%d_%d: %v", jobID, taskRef.StageID, taskRef.TaskID, err)
			}
		}
	}
}

// scheduleTask schedules a specific task on an available worker
func (ts *TaskScheduler) scheduleTask(jobID string, plan *QueryPlan, stageID, taskID int) error {
	task, err := ts.queryPlanner.GetTask(plan, stageID, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}

	// Skip if task is already assigned
	if task.Status != TaskPending {
		return nil
	}

	// Select worker for task
	worker, err := ts.workerManager.SelectWorkerForTask(gen.TaskOperation(task.Spec.Operation))
	if err != nil {
		// No workers available, will retry later
		return nil
	}

	// Assign task to worker
	err = ts.queryPlanner.AssignTaskToWorker(plan, stageID, taskID, worker.ID)
	if err != nil {
		return fmt.Errorf("failed to assign task to worker: %w", err)
	}

	// Track task in worker manager
	err = ts.workerManager.AssignTask(worker.ID, jobID, stageID, taskID, task.Attempt)
	if err != nil {
		return fmt.Errorf("failed to track task in worker manager: %w", err)
	}

	// Send task to worker
	err = ts.sendTaskToWorker(worker, jobID, stageID, taskID, task)
	if err != nil {
		// Mark task as failed and remove from worker tracking
		ts.queryPlanner.UpdateTaskStatus(plan, stageID, taskID, TaskFailed)
		ts.workerManager.CompleteTask(worker.ID, jobID, stageID, taskID)
		return fmt.Errorf("failed to send task to worker: %w", err)
	}

	log.Printf("Scheduled task %s_%d_%d on worker %s (attempt %d)",
		jobID, stageID, taskID, worker.ID, task.Attempt)

	return nil
}

// sendTaskToWorker sends a task to a worker via gRPC
func (ts *TaskScheduler) sendTaskToWorker(worker *WorkerInfo, jobID string, stageID, taskID int, task *Task) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Convert internal task spec to proto
	protoSpec := &gen.TaskSpec{
		Operation:     gen.TaskOperation(task.Spec.Operation),
		InputFiles:    task.Spec.InputFiles,
		Filter:        task.Spec.Filter,
		GroupBy:       task.Spec.GroupBy,
		Aggregates:    convertAggregatesToProto(task.Spec.Aggregates),
		NumPartitions: task.Spec.NumPartitions,
		OutputPath:    task.Spec.OutputPath,
		Projection:    task.Spec.Projection,
	}

	request := &gen.RunTaskRequest{
		JobId:   jobID,
		Stage:   uint32(stageID),
		TaskId:  uint32(taskID),
		Attempt: uint32(task.Attempt),
		Spec:    protoSpec,
	}

	response, err := worker.Client.RunTask(ctx, request)
	if err != nil {
		return fmt.Errorf("gRPC call failed: %w", err)
	}

	if !response.Success {
		return fmt.Errorf("worker rejected task: %s", response.Error)
	}

	return nil
}

// handleFailedWorkers handles tasks from workers that have failed
func (ts *TaskScheduler) handleFailedWorkers() {
	failedWorkers := ts.workerManager.GetFailedWorkers()

	for _, worker := range failedWorkers {
		tasks := ts.workerManager.GetTasksForFailedWorker(worker.ID)

		for _, activeTask := range tasks {
			ts.handleFailedTask(activeTask.JobID, activeTask.StageID, activeTask.TaskID,
				fmt.Sprintf("worker %s failed", worker.ID))
		}

		// Remove failed worker
		ts.workerManager.RemoveWorker(worker.ID)
	}
}

// handleFailedTask handles a task that has failed
func (ts *TaskScheduler) handleFailedTask(jobID string, stageID, taskID int, reason string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	plan, exists := ts.activeQueries[jobID]
	if !exists {
		log.Printf("Query %s not found when handling failed task", jobID)
		return
	}

	task, err := ts.queryPlanner.GetTask(plan, stageID, taskID)
	if err != nil {
		log.Printf("Failed to get task %s_%d_%d: %v", jobID, stageID, taskID, err)
		return
	}

	log.Printf("Task %s_%d_%d failed: %s (attempt %d)", jobID, stageID, taskID, reason, task.Attempt)

	// Mark task as failed
	ts.queryPlanner.UpdateTaskStatus(plan, stageID, taskID, TaskFailed)

	// Check if we should retry
	if task.Attempt < ts.maxRetries {
		log.Printf("Will retry task %s_%d_%d (attempt %d/%d)",
			jobID, stageID, taskID, task.Attempt+1, ts.maxRetries)
	} else {
		log.Printf("Task %s_%d_%d exceeded max retries (%d), marking query as failed",
			jobID, stageID, taskID, ts.maxRetries)
		// TODO: Mark entire query as failed
	}
}

// retryFailedTasks retries tasks that have failed but haven't exceeded max retries
func (ts *TaskScheduler) retryFailedTasks() {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	for jobID, plan := range ts.activeQueries {
		failedTasks := ts.queryPlanner.GetFailedTasks(plan)

		for _, taskRef := range failedTasks {
			task, err := ts.queryPlanner.GetTask(plan, taskRef.StageID, taskRef.TaskID)
			if err != nil {
				continue
			}

			// Only retry if we haven't exceeded max retries
			if task.Attempt < ts.maxRetries {
				err = ts.queryPlanner.RetryTask(plan, taskRef.StageID, taskRef.TaskID)
				if err != nil {
					log.Printf("Failed to retry task %s_%d_%d: %v",
						jobID, taskRef.StageID, taskRef.TaskID, err)
				}
			}
		}
	}
}

// HandleTaskCompletion handles a task completion report from a worker
func (ts *TaskScheduler) HandleTaskCompletion(workerID, jobID string, stageID, taskID int, result *gen.TaskResult) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	plan, exists := ts.activeQueries[jobID]
	if !exists {
		return fmt.Errorf("query %s not found", jobID)
	}

	// Remove task from worker tracking
	err := ts.workerManager.CompleteTask(workerID, jobID, stageID, taskID)
	if err != nil {
		log.Printf("Warning: failed to remove task from worker tracking: %v", err)
	}

	if result.Success {
		// Mark task as completed
		err = ts.queryPlanner.UpdateTaskStatus(plan, stageID, taskID, TaskCompleted)
		if err != nil {
			return fmt.Errorf("failed to update task status: %w", err)
		}

		log.Printf("Task %s_%d_%d completed successfully on worker %s",
			jobID, stageID, taskID, workerID)

		// Check if query is complete
		if ts.queryPlanner.IsQueryComplete(plan) {
			log.Printf("Query %s completed successfully", jobID)
			// TODO: Handle query completion (collect results, cleanup, etc.)
		}
	} else {
		// Handle task failure
		ts.handleFailedTask(jobID, stageID, taskID, result.Error)
	}

	return nil
}

// GetQueryStatus returns the status of a query
func (ts *TaskScheduler) GetQueryStatus(jobID string) (*QueryStatus, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	plan, exists := ts.activeQueries[jobID]
	if !exists {
		return nil, fmt.Errorf("query %s not found", jobID)
	}

	status := &QueryStatus{
		JobID:  jobID,
		Stages: make([]StageStatus, len(plan.Stages)),
	}

	totalTasks := 0
	completedTasks := 0
	failedTasks := 0
	runningTasks := 0

	for i, stage := range plan.Stages {
		stageStatus := StageStatus{
			StageID: stage.ID,
			Tasks:   make([]TaskStatusInfo, len(stage.Tasks)),
		}

		for j, task := range stage.Tasks {
			taskStatus := TaskStatusInfo{
				TaskID:   task.ID,
				Status:   task.Status,
				WorkerID: task.WorkerID,
				Attempt:  task.Attempt,
			}

			stageStatus.Tasks[j] = taskStatus
			totalTasks++

			switch task.Status {
			case TaskCompleted:
				completedTasks++
			case TaskFailed:
				failedTasks++
			case TaskRunning:
				runningTasks++
			}
		}

		status.Stages[i] = stageStatus
	}

	status.TotalTasks = totalTasks
	status.CompletedTasks = completedTasks
	status.FailedTasks = failedTasks
	status.RunningTasks = runningTasks

	if ts.queryPlanner.IsQueryComplete(plan) {
		status.Status = "COMPLETED"
	} else if failedTasks > 0 {
		status.Status = "FAILED"
	} else if runningTasks > 0 {
		status.Status = "RUNNING"
	} else {
		status.Status = "PENDING"
	}

	return status, nil
}

// RemoveQuery removes a completed or failed query from active tracking
func (ts *TaskScheduler) RemoveQuery(jobID string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	delete(ts.activeQueries, jobID)
	log.Printf("Removed query %s from active tracking", jobID)
}

// convertAggregatesToProto converts internal aggregates to proto format
func convertAggregatesToProto(aggregates []*Aggregate) []*gen.Aggregate {
	var protoAggregates []*gen.Aggregate
	for _, agg := range aggregates {
		protoAggregates = append(protoAggregates, &gen.Aggregate{
			Function: agg.Function,
			Column:   agg.Column,
			Alias:    agg.Alias,
		})
	}
	return protoAggregates
}

// QueryStatus represents the status of a query execution
type QueryStatus struct {
	JobID          string
	Status         string // PENDING, RUNNING, COMPLETED, FAILED
	Stages         []StageStatus
	TotalTasks     int
	CompletedTasks int
	FailedTasks    int
	RunningTasks   int
}

// StageStatus represents the status of a stage
type StageStatus struct {
	StageID int
	Tasks   []TaskStatusInfo
}

// TaskStatusInfo represents the status of a task (different from internal TaskStatus enum)
type TaskStatusInfo struct {
	TaskID   int
	Status   TaskStatus
	WorkerID string
	Attempt  int
}
