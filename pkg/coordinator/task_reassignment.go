package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"mini-lakehouse/proto/gen"
)

// TaskReassignmentStrategy defines how tasks should be reassigned
type TaskReassignmentStrategy int

const (
	ImmediateReassignment TaskReassignmentStrategy = iota
	DelayedReassignment
	BatchReassignment
	AdaptiveReassignment
)

// TaskReassignmentManager handles intelligent task reassignment
type TaskReassignmentManager struct {
	mu sync.RWMutex

	workerManager   *WorkerManager
	taskScheduler   *TaskScheduler
	failureDetector *WorkerFailureDetector
	faultTolerance  *FaultToleranceManager

	// Configuration
	maxRetries           int
	retryDelay           time.Duration
	batchSize            int
	adaptiveThreshold    int
	reassignmentStrategy TaskReassignmentStrategy

	// State tracking
	pendingReassignments map[string]*TaskReassignment // taskKey -> reassignment
	reassignmentHistory  []TaskReassignmentEvent
	workerBlacklist      map[string]time.Time // workerID -> blacklist expiry

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
}

// TaskReassignment represents a task that needs to be reassigned
type TaskReassignment struct {
	JobID         string
	StageID       int
	TaskID        int
	Attempt       int
	FailedWorker  string
	FailureReason string
	Timestamp     time.Time
	Retries       int
	Priority      int // Higher number = higher priority
}

// TaskReassignmentEvent represents a completed reassignment
type TaskReassignmentEvent struct {
	TaskKey    string
	FromWorker string
	ToWorker   string
	Timestamp  time.Time
	Reason     string
	Success    bool
	Duration   time.Duration
}

// NewTaskReassignmentManager creates a new task reassignment manager
func NewTaskReassignmentManager(
	workerManager *WorkerManager,
	taskScheduler *TaskScheduler,
	failureDetector *WorkerFailureDetector,
	faultTolerance *FaultToleranceManager,
) *TaskReassignmentManager {
	ctx, cancel := context.WithCancel(context.Background())

	trm := &TaskReassignmentManager{
		workerManager:        workerManager,
		taskScheduler:        taskScheduler,
		failureDetector:      failureDetector,
		faultTolerance:       faultTolerance,
		maxRetries:           3,
		retryDelay:           5 * time.Second,
		batchSize:            10,
		adaptiveThreshold:    5,
		reassignmentStrategy: AdaptiveReassignment,
		pendingReassignments: make(map[string]*TaskReassignment),
		reassignmentHistory:  make([]TaskReassignmentEvent, 0),
		workerBlacklist:      make(map[string]time.Time),
		ctx:                  ctx,
		cancel:               cancel,
	}

	// Register for failure events
	failureDetector.RegisterFailureCallback(trm.handleWorkerFailure)

	return trm
}

// Start begins the task reassignment process
func (trm *TaskReassignmentManager) Start() {
	go trm.runReassignmentLoop()
	log.Println("Task reassignment manager started")
}

// Stop stops the task reassignment process
func (trm *TaskReassignmentManager) Stop() {
	trm.cancel()
	log.Println("Task reassignment manager stopped")
}

// handleWorkerFailure handles worker failure events
func (trm *TaskReassignmentManager) handleWorkerFailure(event WorkerFailureEvent) {
	log.Printf("Handling worker failure: %s - %s", event.WorkerID, event.FailureType.String())

	// Blacklist the worker temporarily based on failure type
	trm.blacklistWorker(event.WorkerID, event.FailureType, event.Severity)

	// Get all active tasks for the failed worker
	activeTasks := trm.workerManager.GetTasksForFailedWorker(event.WorkerID)

	// Create reassignment requests for all active tasks
	for _, task := range activeTasks {
		trm.requestTaskReassignment(
			task.JobID,
			task.StageID,
			task.TaskID,
			task.Attempt,
			event.WorkerID,
			fmt.Sprintf("%s: %s", event.FailureType.String(), event.Details),
		)
	}
}

// blacklistWorker temporarily blacklists a worker based on failure type
func (trm *TaskReassignmentManager) blacklistWorker(workerID string, failureType WorkerFailureType, severity int) {
	trm.mu.Lock()
	defer trm.mu.Unlock()

	// Calculate blacklist duration based on failure type and severity
	var duration time.Duration
	switch failureType {
	case HeartbeatTimeout:
		duration = time.Duration(severity) * 2 * time.Minute
	case TaskTimeout:
		duration = time.Duration(severity) * 1 * time.Minute
	case ConnectionFailure:
		duration = time.Duration(severity) * 3 * time.Minute
	case HighErrorRate:
		duration = time.Duration(severity) * 5 * time.Minute
	case ResourceExhaustion:
		duration = time.Duration(severity) * 2 * time.Minute
	default:
		duration = 5 * time.Minute
	}

	expiry := time.Now().Add(duration)
	trm.workerBlacklist[workerID] = expiry

	log.Printf("Blacklisted worker %s for %v due to %s", workerID, duration, failureType.String())
}

// isWorkerBlacklisted checks if a worker is currently blacklisted
func (trm *TaskReassignmentManager) isWorkerBlacklisted(workerID string) bool {
	trm.mu.RLock()
	defer trm.mu.RUnlock()

	expiry, exists := trm.workerBlacklist[workerID]
	if !exists {
		return false
	}

	if time.Now().After(expiry) {
		// Blacklist expired, remove it
		delete(trm.workerBlacklist, workerID)
		return false
	}

	return true
}

// requestTaskReassignment requests reassignment of a task
func (trm *TaskReassignmentManager) requestTaskReassignment(
	jobID string,
	stageID, taskID, attempt int,
	failedWorker, reason string,
) {
	trm.mu.Lock()
	defer trm.mu.Unlock()

	taskKey := fmt.Sprintf("%s_%d_%d", jobID, stageID, taskID)

	// Check if already pending
	if existing, exists := trm.pendingReassignments[taskKey]; exists {
		existing.Retries++
		log.Printf("Task %s already pending reassignment, incrementing retries to %d", taskKey, existing.Retries)
		return
	}

	// Calculate priority based on various factors
	priority := trm.calculateTaskPriority(jobID, stageID, taskID, attempt)

	reassignment := &TaskReassignment{
		JobID:         jobID,
		StageID:       stageID,
		TaskID:        taskID,
		Attempt:       attempt,
		FailedWorker:  failedWorker,
		FailureReason: reason,
		Timestamp:     time.Now(),
		Retries:       0,
		Priority:      priority,
	}

	trm.pendingReassignments[taskKey] = reassignment

	log.Printf("Requested reassignment for task %s (priority: %d, reason: %s)",
		taskKey, priority, reason)
}

// calculateTaskPriority calculates the priority of a task for reassignment
func (trm *TaskReassignmentManager) calculateTaskPriority(jobID string, stageID, taskID, attempt int) int {
	priority := 100 // Base priority

	// Higher priority for tasks that have failed multiple times
	priority += attempt * 20

	// Higher priority for earlier stages (they block later stages)
	priority += (10 - stageID) * 10

	// Higher priority for tasks in smaller stages (critical path)
	// This would require query plan analysis, simplified for now
	priority += 10

	return priority
}

// runReassignmentLoop runs the main reassignment processing loop
func (trm *TaskReassignmentManager) runReassignmentLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-trm.ctx.Done():
			return
		case <-ticker.C:
			trm.processReassignments()
			trm.cleanupExpiredBlacklists()
		}
	}
}

// processReassignments processes pending task reassignments
func (trm *TaskReassignmentManager) processReassignments() {
	trm.mu.Lock()
	pending := make([]*TaskReassignment, 0, len(trm.pendingReassignments))
	for _, reassignment := range trm.pendingReassignments {
		pending = append(pending, reassignment)
	}
	trm.mu.Unlock()

	if len(pending) == 0 {
		return
	}

	// Sort by priority (highest first)
	for i := 0; i < len(pending)-1; i++ {
		for j := i + 1; j < len(pending); j++ {
			if pending[i].Priority < pending[j].Priority {
				pending[i], pending[j] = pending[j], pending[i]
			}
		}
	}

	// Process based on strategy
	switch trm.reassignmentStrategy {
	case ImmediateReassignment:
		trm.processImmediateReassignments(pending)
	case DelayedReassignment:
		trm.processDelayedReassignments(pending)
	case BatchReassignment:
		trm.processBatchReassignments(pending)
	case AdaptiveReassignment:
		trm.processAdaptiveReassignments(pending)
	}
}

// processImmediateReassignments processes all reassignments immediately
func (trm *TaskReassignmentManager) processImmediateReassignments(pending []*TaskReassignment) {
	for _, reassignment := range pending {
		trm.attemptReassignment(reassignment)
	}
}

// processDelayedReassignments processes reassignments with delays
func (trm *TaskReassignmentManager) processDelayedReassignments(pending []*TaskReassignment) {
	now := time.Now()

	for _, reassignment := range pending {
		// Wait for retry delay before attempting reassignment
		if now.Sub(reassignment.Timestamp) >= trm.retryDelay {
			trm.attemptReassignment(reassignment)
		}
	}
}

// processBatchReassignments processes reassignments in batches
func (trm *TaskReassignmentManager) processBatchReassignments(pending []*TaskReassignment) {
	batchCount := 0
	for _, reassignment := range pending {
		if batchCount >= trm.batchSize {
			break
		}
		trm.attemptReassignment(reassignment)
		batchCount++
	}
}

// processAdaptiveReassignments uses adaptive strategy based on system load
func (trm *TaskReassignmentManager) processAdaptiveReassignments(pending []*TaskReassignment) {
	healthyWorkerCount := trm.failureDetector.GetHealthyWorkerCount()

	if healthyWorkerCount == 0 {
		log.Printf("No healthy workers available, deferring %d reassignments", len(pending))
		return
	}

	// Adapt batch size based on healthy worker count
	adaptiveBatchSize := trm.batchSize
	if healthyWorkerCount < trm.adaptiveThreshold {
		adaptiveBatchSize = healthyWorkerCount / 2
		if adaptiveBatchSize < 1 {
			adaptiveBatchSize = 1
		}
	}

	batchCount := 0
	for _, reassignment := range pending {
		if batchCount >= adaptiveBatchSize {
			break
		}
		trm.attemptReassignment(reassignment)
		batchCount++
	}
}

// attemptReassignment attempts to reassign a task to a healthy worker
func (trm *TaskReassignmentManager) attemptReassignment(reassignment *TaskReassignment) {
	startTime := time.Now()
	taskKey := fmt.Sprintf("%s_%d_%d", reassignment.JobID, reassignment.StageID, reassignment.TaskID)

	// Check if task has exceeded max retries
	if reassignment.Retries >= trm.maxRetries {
		log.Printf("Task %s exceeded max retries (%d), marking as permanently failed",
			taskKey, trm.maxRetries)
		trm.recordReassignmentEvent(taskKey, reassignment.FailedWorker, "",
			"max_retries_exceeded", false, time.Since(startTime))
		trm.removePendingReassignment(taskKey)
		return
	}

	// Find a suitable worker
	newWorker, err := trm.selectWorkerForReassignment(reassignment)
	if err != nil {
		log.Printf("No suitable worker found for task %s: %v", taskKey, err)
		reassignment.Retries++
		return
	}

	// Cancel the task on the failed worker (if still connected)
	trm.cancelTaskOnFailedWorker(reassignment)

	// Assign task to new worker
	err = trm.assignTaskToNewWorker(reassignment, newWorker)
	if err != nil {
		log.Printf("Failed to assign task %s to worker %s: %v", taskKey, newWorker.ID, err)
		reassignment.Retries++
		return
	}

	// Record successful reassignment
	trm.recordReassignmentEvent(taskKey, reassignment.FailedWorker, newWorker.ID,
		reassignment.FailureReason, true, time.Since(startTime))

	// Remove from pending
	trm.removePendingReassignment(taskKey)

	log.Printf("Successfully reassigned task %s from worker %s to worker %s",
		taskKey, reassignment.FailedWorker, newWorker.ID)
}

// selectWorkerForReassignment selects the best worker for task reassignment
func (trm *TaskReassignmentManager) selectWorkerForReassignment(reassignment *TaskReassignment) (*WorkerInfo, error) {
	healthyWorkers := trm.workerManager.GetHealthyWorkers()

	// Filter out blacklisted workers and the failed worker
	var eligibleWorkers []*WorkerInfo
	for _, worker := range healthyWorkers {
		if worker.ID == reassignment.FailedWorker {
			continue
		}
		if trm.isWorkerBlacklisted(worker.ID) {
			continue
		}
		if !trm.failureDetector.IsWorkerHealthy(worker.ID) {
			continue
		}
		eligibleWorkers = append(eligibleWorkers, worker)
	}

	if len(eligibleWorkers) == 0 {
		return nil, fmt.Errorf("no eligible workers available")
	}

	// Select worker with lowest load and best health metrics
	var bestWorker *WorkerInfo
	bestScore := float64(-1)

	for _, worker := range eligibleWorkers {
		score := trm.calculateWorkerScore(worker)
		if score > bestScore {
			bestScore = score
			bestWorker = worker
		}
	}

	return bestWorker, nil
}

// calculateWorkerScore calculates a score for worker selection
func (trm *TaskReassignmentManager) calculateWorkerScore(worker *WorkerInfo) float64 {
	score := 100.0

	// Factor in current load (lower is better)
	maxTasks := float64(worker.Capabilities.MaxConcurrentTasks)
	if maxTasks == 0 {
		maxTasks = 1
	}
	currentLoad := float64(len(worker.ActiveTasks)) / maxTasks
	score -= currentLoad * 50

	// Factor in resource usage (lower is better)
	if worker.Status != nil {
		score -= worker.Status.CpuUsage * 30
		score -= worker.Status.MemoryUsage * 20
	}

	// Factor in health metrics
	if health, exists := trm.failureDetector.GetWorkerHealth(worker.ID); exists {
		score += health.TaskSuccessRate * 20
		score -= float64(health.ConsecutiveFailures) * 10
	}

	return score
}

// cancelTaskOnFailedWorker attempts to cancel the task on the failed worker
func (trm *TaskReassignmentManager) cancelTaskOnFailedWorker(reassignment *TaskReassignment) {
	worker, err := trm.workerManager.GetWorkerInfo(reassignment.FailedWorker)
	if err != nil {
		// Worker already removed, nothing to cancel
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	request := &gen.CancelTaskRequest{
		JobId:  reassignment.JobID,
		TaskId: uint32(reassignment.TaskID),
	}

	_, err = worker.Client.CancelTask(ctx, request)
	if err != nil {
		log.Printf("Failed to cancel task on failed worker %s: %v", reassignment.FailedWorker, err)
	}
}

// assignTaskToNewWorker assigns the task to a new worker
func (trm *TaskReassignmentManager) assignTaskToNewWorker(reassignment *TaskReassignment, newWorker *WorkerInfo) error {
	// Remove task from old worker tracking
	trm.workerManager.CompleteTask(reassignment.FailedWorker, reassignment.JobID, reassignment.StageID, reassignment.TaskID)

	// Increment attempt number for retry
	newAttempt := reassignment.Attempt + 1

	// Assign to new worker
	err := trm.workerManager.AssignTask(newWorker.ID, reassignment.JobID, reassignment.StageID, reassignment.TaskID, newAttempt)
	if err != nil {
		return fmt.Errorf("failed to assign task to worker: %w", err)
	}

	// Update task scheduler with new assignment
	// This would require access to the query plan and task spec
	// For now, we'll delegate to the task scheduler's retry mechanism
	trm.taskScheduler.handleFailedTask(reassignment.JobID, reassignment.StageID, reassignment.TaskID, reassignment.FailureReason)

	return nil
}

// recordReassignmentEvent records a reassignment event
func (trm *TaskReassignmentManager) recordReassignmentEvent(taskKey, fromWorker, toWorker, reason string, success bool, duration time.Duration) {
	trm.mu.Lock()
	defer trm.mu.Unlock()

	event := TaskReassignmentEvent{
		TaskKey:    taskKey,
		FromWorker: fromWorker,
		ToWorker:   toWorker,
		Timestamp:  time.Now(),
		Reason:     reason,
		Success:    success,
		Duration:   duration,
	}

	trm.reassignmentHistory = append(trm.reassignmentHistory, event)

	// Keep only recent events (last 100)
	if len(trm.reassignmentHistory) > 100 {
		trm.reassignmentHistory = trm.reassignmentHistory[1:]
	}
}

// removePendingReassignment removes a reassignment from pending list
func (trm *TaskReassignmentManager) removePendingReassignment(taskKey string) {
	trm.mu.Lock()
	defer trm.mu.Unlock()
	delete(trm.pendingReassignments, taskKey)
}

// cleanupExpiredBlacklists removes expired worker blacklists
func (trm *TaskReassignmentManager) cleanupExpiredBlacklists() {
	trm.mu.Lock()
	defer trm.mu.Unlock()

	now := time.Now()
	for workerID, expiry := range trm.workerBlacklist {
		if now.After(expiry) {
			delete(trm.workerBlacklist, workerID)
			log.Printf("Removed worker %s from blacklist", workerID)
		}
	}
}

// GetReassignmentStats returns statistics about task reassignments
func (trm *TaskReassignmentManager) GetReassignmentStats() ReassignmentStats {
	trm.mu.RLock()
	defer trm.mu.RUnlock()

	stats := ReassignmentStats{
		PendingReassignments: len(trm.pendingReassignments),
		BlacklistedWorkers:   len(trm.workerBlacklist),
		TotalReassignments:   len(trm.reassignmentHistory),
	}

	// Calculate success rate
	successful := 0
	for _, event := range trm.reassignmentHistory {
		if event.Success {
			successful++
		}
	}

	if len(trm.reassignmentHistory) > 0 {
		stats.SuccessRate = float64(successful) / float64(len(trm.reassignmentHistory))
	}

	return stats
}

// ReassignmentStats represents statistics about task reassignments
type ReassignmentStats struct {
	PendingReassignments int
	BlacklistedWorkers   int
	TotalReassignments   int
	SuccessRate          float64
}
