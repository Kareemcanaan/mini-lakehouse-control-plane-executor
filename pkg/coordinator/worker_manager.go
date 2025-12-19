package coordinator

import (
	"context"
	"fmt"
	"log"
	"mini-lakehouse/proto/gen"
	"sync"
	"time"
)

// WorkerInfo represents information about a registered worker
type WorkerInfo struct {
	ID           string
	Address      string
	Capabilities *gen.WorkerCapabilities
	Status       *gen.WorkerStatus
	LastSeen     time.Time
	Client       gen.WorkerServiceClient
	ActiveTasks  map[string]*ActiveTask // key: taskKey (jobID_stageID_taskID)
}

// ActiveTask represents a task currently running on a worker
type ActiveTask struct {
	JobID     string
	StageID   int
	TaskID    int
	Attempt   int
	StartTime time.Time
}

// WorkerManager manages worker registration, heartbeats, and task assignment
type WorkerManager struct {
	mu      sync.RWMutex
	workers map[string]*WorkerInfo // key: worker ID

	// Configuration
	heartbeatTimeout time.Duration
	maxRetries       int
}

// NewWorkerManager creates a new worker manager
func NewWorkerManager() *WorkerManager {
	return &WorkerManager{
		workers:          make(map[string]*WorkerInfo),
		heartbeatTimeout: 30 * time.Second,
		maxRetries:       3,
	}
}

// RegisterWorker registers a new worker
func (wm *WorkerManager) RegisterWorker(workerID, address string, capabilities *gen.WorkerCapabilities, client gen.WorkerServiceClient) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	log.Printf("Registering worker %s at %s", workerID, address)

	wm.workers[workerID] = &WorkerInfo{
		ID:           workerID,
		Address:      address,
		Capabilities: capabilities,
		Status: &gen.WorkerStatus{
			ActiveTasks: 0,
			CpuUsage:    0.0,
			MemoryUsage: 0.0,
		},
		LastSeen:    time.Now(),
		Client:      client,
		ActiveTasks: make(map[string]*ActiveTask),
	}

	return nil
}

// UpdateHeartbeat updates worker heartbeat and status
func (wm *WorkerManager) UpdateHeartbeat(workerID string, status *gen.WorkerStatus) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	worker, exists := wm.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not registered", workerID)
	}

	worker.Status = status
	worker.LastSeen = time.Now()

	log.Printf("Heartbeat from worker %s: %d active tasks, CPU: %.1f%%, Memory: %.1f%%",
		workerID, status.ActiveTasks, status.CpuUsage*100, status.MemoryUsage*100)

	return nil
}

// GetHealthyWorkers returns all workers that are currently healthy
func (wm *WorkerManager) GetHealthyWorkers() []*WorkerInfo {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var healthy []*WorkerInfo
	now := time.Now()

	for _, worker := range wm.workers {
		if now.Sub(worker.LastSeen) <= wm.heartbeatTimeout {
			healthy = append(healthy, worker)
		}
	}

	return healthy
}

// GetFailedWorkers returns workers that have missed heartbeats
func (wm *WorkerManager) GetFailedWorkers() []*WorkerInfo {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	var failed []*WorkerInfo
	now := time.Now()

	for _, worker := range wm.workers {
		if now.Sub(worker.LastSeen) > wm.heartbeatTimeout {
			failed = append(failed, worker)
		}
	}

	return failed
}

// SelectWorkerForTask selects the best available worker for a task using load balancing
func (wm *WorkerManager) SelectWorkerForTask(operation gen.TaskOperation) (*WorkerInfo, error) {
	healthyWorkers := wm.GetHealthyWorkers()
	if len(healthyWorkers) == 0 {
		return nil, fmt.Errorf("no healthy workers available")
	}

	// Filter workers that support the operation
	var eligibleWorkers []*WorkerInfo
	operationStr := operation.String()

	for _, worker := range healthyWorkers {
		if wm.supportsOperation(worker, operationStr) {
			eligibleWorkers = append(eligibleWorkers, worker)
		}
	}

	if len(eligibleWorkers) == 0 {
		return nil, fmt.Errorf("no workers support operation %s", operationStr)
	}

	// Select worker with lowest load (fewest active tasks relative to capacity)
	var bestWorker *WorkerInfo
	bestLoad := float64(1000000) // Large number

	for _, worker := range eligibleWorkers {
		maxTasks := float64(worker.Capabilities.MaxConcurrentTasks)
		if maxTasks == 0 {
			maxTasks = 1 // Default to 1 if not specified
		}

		currentLoad := float64(len(worker.ActiveTasks)) / maxTasks

		if currentLoad < bestLoad {
			bestLoad = currentLoad
			bestWorker = worker
		}
	}

	if bestWorker == nil {
		return nil, fmt.Errorf("no suitable worker found")
	}

	return bestWorker, nil
}

// supportsOperation checks if a worker supports a specific operation
func (wm *WorkerManager) supportsOperation(worker *WorkerInfo, operation string) bool {
	// If no supported operations specified, assume it supports all
	if len(worker.Capabilities.SupportedOperations) == 0 {
		return true
	}

	for _, supportedOp := range worker.Capabilities.SupportedOperations {
		if supportedOp == operation {
			return true
		}
	}

	return false
}

// AssignTask assigns a task to a worker and tracks it
func (wm *WorkerManager) AssignTask(workerID string, jobID string, stageID, taskID, attempt int) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	worker, exists := wm.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	taskKey := fmt.Sprintf("%s_%d_%d", jobID, stageID, taskID)

	worker.ActiveTasks[taskKey] = &ActiveTask{
		JobID:     jobID,
		StageID:   stageID,
		TaskID:    taskID,
		Attempt:   attempt,
		StartTime: time.Now(),
	}

	log.Printf("Assigned task %s to worker %s (attempt %d)", taskKey, workerID, attempt)

	return nil
}

// CompleteTask removes a task from worker's active tasks
func (wm *WorkerManager) CompleteTask(workerID string, jobID string, stageID, taskID int) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	worker, exists := wm.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	taskKey := fmt.Sprintf("%s_%d_%d", jobID, stageID, taskID)

	if _, exists := worker.ActiveTasks[taskKey]; !exists {
		return fmt.Errorf("task %s not found on worker %s", taskKey, workerID)
	}

	delete(worker.ActiveTasks, taskKey)

	log.Printf("Completed task %s on worker %s", taskKey, workerID)

	return nil
}

// GetTasksForFailedWorker returns all active tasks for a failed worker
func (wm *WorkerManager) GetTasksForFailedWorker(workerID string) []*ActiveTask {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	worker, exists := wm.workers[workerID]
	if !exists {
		return nil
	}

	var tasks []*ActiveTask
	for _, task := range worker.ActiveTasks {
		tasks = append(tasks, task)
	}

	return tasks
}

// RemoveWorker removes a worker from the manager (for cleanup)
func (wm *WorkerManager) RemoveWorker(workerID string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	delete(wm.workers, workerID)
	log.Printf("Removed worker %s", workerID)
}

// GetWorkerInfo returns information about a specific worker
func (wm *WorkerManager) GetWorkerInfo(workerID string) (*WorkerInfo, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	worker, exists := wm.workers[workerID]
	if !exists {
		return nil, fmt.Errorf("worker %s not found", workerID)
	}

	return worker, nil
}

// GetAllWorkers returns all registered workers
func (wm *WorkerManager) GetAllWorkers() map[string]*WorkerInfo {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	// Return a copy to avoid race conditions
	workers := make(map[string]*WorkerInfo)
	for id, worker := range wm.workers {
		workers[id] = worker
	}

	return workers
}

// StartHealthCheck starts a background goroutine to monitor worker health
func (wm *WorkerManager) StartHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			wm.checkWorkerHealth()
		}
	}
}

// checkWorkerHealth checks for failed workers and logs warnings
func (wm *WorkerManager) checkWorkerHealth() {
	failedWorkers := wm.GetFailedWorkers()

	for _, worker := range failedWorkers {
		timeSinceLastSeen := time.Since(worker.LastSeen)
		log.Printf("WARNING: Worker %s has not sent heartbeat for %v (active tasks: %d)",
			worker.ID, timeSinceLastSeen, len(worker.ActiveTasks))
	}
}

// GetWorkerStats returns statistics about workers
func (wm *WorkerManager) GetWorkerStats() WorkerStats {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	stats := WorkerStats{}
	now := time.Now()

	for _, worker := range wm.workers {
		stats.Total++

		if now.Sub(worker.LastSeen) <= wm.heartbeatTimeout {
			stats.Healthy++
		} else {
			stats.Failed++
		}

		stats.TotalActiveTasks += len(worker.ActiveTasks)
	}

	return stats
}

// WorkerStats represents statistics about workers
type WorkerStats struct {
	Total            int
	Healthy          int
	Failed           int
	TotalActiveTasks int
}
