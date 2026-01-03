package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"mini-lakehouse/pkg/storage"
	"mini-lakehouse/proto/gen"
)

// FaultToleranceManager handles fault detection, recovery, and SUCCESS manifest management
type FaultToleranceManager struct {
	storageClient *storage.Client
	taskScheduler *TaskScheduler
	workerManager *WorkerManager

	// Enhanced failure detection and recovery
	failureDetector     *WorkerFailureDetector
	reassignmentManager *TaskReassignmentManager

	// Configuration
	taskTimeout      time.Duration
	heartbeatTimeout time.Duration
	maxRetries       int
}

// NewFaultToleranceManager creates a new fault tolerance manager
func NewFaultToleranceManager(storageClient *storage.Client, taskScheduler *TaskScheduler, workerManager *WorkerManager) *FaultToleranceManager {
	ftm := &FaultToleranceManager{
		storageClient:    storageClient,
		taskScheduler:    taskScheduler,
		workerManager:    workerManager,
		taskTimeout:      5 * time.Minute,
		heartbeatTimeout: 30 * time.Second,
		maxRetries:       3,
	}

	// Initialize enhanced failure detection and recovery
	ftm.failureDetector = NewWorkerFailureDetector(workerManager, taskScheduler)
	ftm.reassignmentManager = NewTaskReassignmentManager(workerManager, taskScheduler, ftm.failureDetector, ftm)

	return ftm
}

// SuccessManifest represents the SUCCESS manifest for a completed task
type SuccessManifest struct {
	TaskID      int              `json:"task_id"`
	Attempt     int              `json:"attempt"`
	TimestampMs int64            `json:"timestamp_ms"`
	Outputs     []TaskOutputInfo `json:"outputs"`
	Metrics     *TaskMetricsInfo `json:"metrics,omitempty"`
}

// TaskOutputInfo represents output information in the SUCCESS manifest
type TaskOutputInfo struct {
	Partition int    `json:"partition"`
	Path      string `json:"path"`
	Rows      uint64 `json:"rows"`
	Size      uint64 `json:"size"`
}

// TaskMetricsInfo represents task metrics in the SUCCESS manifest
type TaskMetricsInfo struct {
	DurationMs    uint64 `json:"duration_ms"`
	RowsProcessed uint64 `json:"rows_processed"`
	BytesRead     uint64 `json:"bytes_read"`
	BytesWritten  uint64 `json:"bytes_written"`
}

// StartFaultDetection starts background fault detection and recovery
func (ftm *FaultToleranceManager) StartFaultDetection(ctx context.Context) {
	// Start enhanced failure detection
	ftm.failureDetector.Start()
	ftm.reassignmentManager.Start()

	// Start legacy fault detection loop for compatibility
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ftm.failureDetector.Stop()
			ftm.reassignmentManager.Stop()
			return
		case <-ticker.C:
			ftm.detectAndHandleFaults()
		}
	}
}

// detectAndHandleFaults detects various types of faults and handles them
func (ftm *FaultToleranceManager) detectAndHandleFaults() {
	// Detect worker failures
	ftm.handleWorkerFailures()

	// Detect task timeouts
	ftm.handleTaskTimeouts()
}

// handleWorkerFailures detects and handles worker failures
func (ftm *FaultToleranceManager) handleWorkerFailures() {
	failedWorkers := ftm.workerManager.GetFailedWorkers()

	for _, worker := range failedWorkers {
		log.Printf("Detected failed worker: %s (last seen: %v)",
			worker.ID, worker.LastSeen)

		// Get all active tasks for this worker
		activeTasks := ftm.workerManager.GetTasksForFailedWorker(worker.ID)

		// Reschedule all active tasks
		for _, task := range activeTasks {
			ftm.rescheduleTask(task.JobID, task.StageID, task.TaskID,
				fmt.Sprintf("worker %s failed", worker.ID))
		}

		// Remove the failed worker
		ftm.workerManager.RemoveWorker(worker.ID)
	}
}

// handleTaskTimeouts detects and handles task timeouts
func (ftm *FaultToleranceManager) handleTaskTimeouts() {
	// Get all running tasks and check for timeouts
	allWorkers := ftm.workerManager.GetAllWorkers()
	now := time.Now()

	for _, worker := range allWorkers {
		for _, task := range worker.ActiveTasks {
			if now.Sub(task.StartTime) > ftm.taskTimeout {
				log.Printf("Task %s_%d_%d timed out on worker %s (running for %v)",
					task.JobID, task.StageID, task.TaskID, worker.ID, now.Sub(task.StartTime))

				// Cancel the task on the worker
				ftm.cancelTaskOnWorker(worker, task.JobID, task.TaskID)

				// Reschedule the task
				ftm.rescheduleTask(task.JobID, task.StageID, task.TaskID, "task timeout")
			}
		}
	}
}

// cancelTaskOnWorker sends a cancel request to a worker
func (ftm *FaultToleranceManager) cancelTaskOnWorker(worker *WorkerInfo, jobID string, taskID int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	request := &gen.CancelTaskRequest{
		JobId:  jobID,
		TaskId: uint32(taskID),
	}

	_, err := worker.Client.CancelTask(ctx, request)
	if err != nil {
		log.Printf("Failed to cancel task %s_%d on worker %s: %v",
			jobID, taskID, worker.ID, err)
	} else {
		log.Printf("Cancelled task %s_%d on worker %s", jobID, taskID, worker.ID)
	}
}

// rescheduleTask reschedules a failed task
func (ftm *FaultToleranceManager) rescheduleTask(jobID string, stageID, taskID int, reason string) {
	log.Printf("Rescheduling task %s_%d_%d due to: %s", jobID, stageID, taskID, reason)

	// The task scheduler will handle the actual rescheduling logic
	ftm.taskScheduler.handleFailedTask(jobID, stageID, taskID, reason)
}

// ValidateTaskOutputs validates that all task outputs exist before writing SUCCESS manifest
func (ftm *FaultToleranceManager) ValidateTaskOutputs(ctx context.Context, outputs []*gen.TaskOutput) error {
	for _, output := range outputs {
		exists, err := ftm.storageClient.ObjectExists(ctx, output.Path)
		if err != nil {
			return fmt.Errorf("failed to check if output exists %s: %w", output.Path, err)
		}

		if !exists {
			return fmt.Errorf("task output does not exist: %s", output.Path)
		}

		log.Printf("Validated task output exists: %s (%d rows, %d bytes)",
			output.Path, output.Rows, output.Size)
	}

	return nil
}

// WriteSuccessManifest writes a SUCCESS manifest for a completed task
func (ftm *FaultToleranceManager) WriteSuccessManifest(ctx context.Context, jobID string, stageID, taskID, attempt int, result *gen.TaskResult) error {
	// First validate that all outputs exist
	err := ftm.ValidateTaskOutputs(ctx, result.Outputs)
	if err != nil {
		return fmt.Errorf("output validation failed: %w", err)
	}

	// Create SUCCESS manifest
	manifest := SuccessManifest{
		TaskID:      taskID,
		Attempt:     attempt,
		TimestampMs: time.Now().UnixMilli(),
		Outputs:     make([]TaskOutputInfo, len(result.Outputs)),
	}

	// Convert outputs
	for i, output := range result.Outputs {
		manifest.Outputs[i] = TaskOutputInfo{
			Partition: int(output.Partition),
			Path:      output.Path,
			Rows:      output.Rows,
			Size:      output.Size,
		}
	}

	// Convert metrics if present
	if result.Metrics != nil {
		manifest.Metrics = &TaskMetricsInfo{
			DurationMs:    result.Metrics.DurationMs,
			RowsProcessed: result.Metrics.RowsProcessed,
			BytesRead:     result.Metrics.BytesRead,
			BytesWritten:  result.Metrics.BytesWritten,
		}
	}

	// Serialize manifest
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to serialize SUCCESS manifest: %w", err)
	}

	// Write SUCCESS manifest to object storage
	successPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/SUCCESS.json", jobID, stageID, taskID)

	err = ftm.storageClient.PutObject(ctx, successPath, strings.NewReader(string(manifestData)), int64(len(manifestData)), "application/json")
	if err != nil {
		return fmt.Errorf("failed to write SUCCESS manifest: %w", err)
	}

	log.Printf("Wrote SUCCESS manifest for task %s_%d_%d at %s",
		jobID, stageID, taskID, successPath)

	return nil
}

// ReadSuccessManifest reads a SUCCESS manifest for a task
func (ftm *FaultToleranceManager) ReadSuccessManifest(ctx context.Context, jobID string, stageID, taskID int) (*SuccessManifest, error) {
	successPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/SUCCESS.json", jobID, stageID, taskID)

	obj, err := ftm.storageClient.GetObject(ctx, successPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read SUCCESS manifest: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read SUCCESS manifest data: %w", err)
	}

	var manifest SuccessManifest
	err = json.Unmarshal(data, &manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SUCCESS manifest: %w", err)
	}

	return &manifest, nil
}

// GetSuccessfulTaskOutputs returns the outputs for a successfully completed task
func (ftm *FaultToleranceManager) GetSuccessfulTaskOutputs(ctx context.Context, jobID string, stageID, taskID int) ([]TaskOutputInfo, error) {
	manifest, err := ftm.ReadSuccessManifest(ctx, jobID, stageID, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to read SUCCESS manifest: %w", err)
	}

	return manifest.Outputs, nil
}

// ListSuccessfulTasks returns all tasks in a stage that have SUCCESS manifests
func (ftm *FaultToleranceManager) ListSuccessfulTasks(ctx context.Context, jobID string, stageID int) ([]int, error) {
	// List all objects in the stage directory
	prefix := fmt.Sprintf("shuffle/%s/stage_%d/", jobID, stageID)
	objects, err := ftm.storageClient.ListObjects(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list stage objects: %w", err)
	}

	var successfulTasks []int

	// Look for SUCCESS.json files
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, "/SUCCESS.json") {
			// Extract task ID from path: shuffle/job/stage_X/task_Y/SUCCESS.json
			parts := strings.Split(obj.Key, "/")
			if len(parts) >= 4 {
				taskDir := parts[len(parts)-2] // task_Y
				if strings.HasPrefix(taskDir, "task_") {
					var taskID int
					_, err := fmt.Sscanf(taskDir, "task_%d", &taskID)
					if err == nil {
						successfulTasks = append(successfulTasks, taskID)
					}
				}
			}
		}
	}

	return successfulTasks, nil
}

// CleanupFailedAttempts removes outputs from failed task attempts
func (ftm *FaultToleranceManager) CleanupFailedAttempts(ctx context.Context, jobID string, stageID, taskID int, successfulAttempt int) error {
	// List all objects for this task
	prefix := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/", jobID, stageID, taskID)
	objects, err := ftm.storageClient.ListObjects(ctx, prefix)
	if err != nil {
		return fmt.Errorf("failed to list task objects: %w", err)
	}

	// Delete objects from failed attempts
	for _, obj := range objects {
		// Check if this object is from a failed attempt
		if strings.Contains(obj.Key, fmt.Sprintf("/attempt_%d/", successfulAttempt)) {
			continue // Keep successful attempt
		}

		if strings.Contains(obj.Key, "/attempt_") {
			// This is from a failed attempt, delete it
			err := ftm.storageClient.DeleteObject(ctx, obj.Key)
			if err != nil {
				log.Printf("Warning: failed to delete failed attempt object %s: %v", obj.Key, err)
			} else {
				log.Printf("Cleaned up failed attempt object: %s", obj.Key)
			}
		}
	}

	return nil
}

// ValidateStageCompletion validates that all tasks in a stage have completed successfully
func (ftm *FaultToleranceManager) ValidateStageCompletion(ctx context.Context, jobID string, stageID int, expectedTasks []int) error {
	successfulTasks, err := ftm.ListSuccessfulTasks(ctx, jobID, stageID)
	if err != nil {
		return fmt.Errorf("failed to list successful tasks: %w", err)
	}

	// Check that all expected tasks are successful
	successfulSet := make(map[int]bool)
	for _, taskID := range successfulTasks {
		successfulSet[taskID] = true
	}

	var missingTasks []int
	for _, expectedTask := range expectedTasks {
		if !successfulSet[expectedTask] {
			missingTasks = append(missingTasks, expectedTask)
		}
	}

	if len(missingTasks) > 0 {
		return fmt.Errorf("stage %d incomplete, missing tasks: %v", stageID, missingTasks)
	}

	log.Printf("Stage %d validation passed: all %d tasks completed successfully",
		stageID, len(expectedTasks))

	return nil
}

// GetRetryableFailures returns tasks that can be retried
func (ftm *FaultToleranceManager) GetRetryableFailures(plan *QueryPlan) []TaskRef {
	var retryable []TaskRef

	for stageID, stage := range plan.Stages {
		for _, task := range stage.Tasks {
			if task.Status == TaskFailed && task.Attempt < ftm.maxRetries {
				retryable = append(retryable, TaskRef{
					StageID: stageID,
					TaskID:  task.ID,
				})
			}
		}
	}

	return retryable
}

// IsTaskRetryable checks if a task can be retried
func (ftm *FaultToleranceManager) IsTaskRetryable(task *Task) bool {
	return task.Status == TaskFailed && task.Attempt < ftm.maxRetries
}

// GetFailureStatistics returns statistics about task failures
func (ftm *FaultToleranceManager) GetFailureStatistics(plan *QueryPlan) FailureStats {
	stats := FailureStats{}

	for _, stage := range plan.Stages {
		for _, task := range stage.Tasks {
			stats.TotalTasks++

			switch task.Status {
			case TaskFailed:
				stats.FailedTasks++
				if task.Attempt >= ftm.maxRetries {
					stats.PermanentFailures++
				} else {
					stats.RetryableFailures++
				}
			case TaskCompleted:
				stats.CompletedTasks++
			case TaskRunning:
				stats.RunningTasks++
			case TaskPending:
				stats.PendingTasks++
			}

			if task.Attempt > 1 {
				stats.RetriedTasks++
			}
		}
	}

	return stats
}

// GetWorkerHealth returns health information for a worker
func (ftm *FaultToleranceManager) GetWorkerHealth(workerID string) (*WorkerHealthMetrics, bool) {
	return ftm.failureDetector.GetWorkerHealth(workerID)
}

// GetReassignmentStats returns statistics about task reassignments
func (ftm *FaultToleranceManager) GetReassignmentStats() ReassignmentStats {
	return ftm.reassignmentManager.GetReassignmentStats()
}

// GetFailureHistory returns recent worker failure events
func (ftm *FaultToleranceManager) GetFailureHistory(limit int) []WorkerFailureEvent {
	return ftm.failureDetector.GetFailureHistory(limit)
}

// IsWorkerHealthy returns true if a worker is considered healthy
func (ftm *FaultToleranceManager) IsWorkerHealthy(workerID string) bool {
	return ftm.failureDetector.IsWorkerHealthy(workerID)
}

// GetHealthyWorkerCount returns the number of healthy workers
func (ftm *FaultToleranceManager) GetHealthyWorkerCount() int {
	return ftm.failureDetector.GetHealthyWorkerCount()
}

// FailureStats represents statistics about task failures
type FailureStats struct {
	TotalTasks        int
	CompletedTasks    int
	FailedTasks       int
	RunningTasks      int
	PendingTasks      int
	RetryableFailures int
	PermanentFailures int
	RetriedTasks      int
}
