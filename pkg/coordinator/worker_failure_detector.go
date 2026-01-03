package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"mini-lakehouse/proto/gen"
)

// WorkerFailureType represents different types of worker failures
type WorkerFailureType int

const (
	HeartbeatTimeout WorkerFailureType = iota
	TaskTimeout
	ConnectionFailure
	HighErrorRate
	ResourceExhaustion
)

func (wft WorkerFailureType) String() string {
	switch wft {
	case HeartbeatTimeout:
		return "heartbeat_timeout"
	case TaskTimeout:
		return "task_timeout"
	case ConnectionFailure:
		return "connection_failure"
	case HighErrorRate:
		return "high_error_rate"
	case ResourceExhaustion:
		return "resource_exhaustion"
	default:
		return "unknown"
	}
}

// WorkerFailureEvent represents a detected worker failure
type WorkerFailureEvent struct {
	WorkerID    string
	FailureType WorkerFailureType
	Timestamp   time.Time
	Details     string
	Severity    int // 1=low, 2=medium, 3=high
}

// WorkerHealthMetrics tracks health metrics for a worker
type WorkerHealthMetrics struct {
	WorkerID            string
	LastHeartbeat       time.Time
	ConsecutiveFailures int
	TaskSuccessRate     float64
	AvgTaskDuration     time.Duration
	ResourceUsage       *gen.WorkerStatus
	ConnectionErrors    int
	LastConnectionTest  time.Time
}

// WorkerFailureDetector provides enhanced worker failure detection
type WorkerFailureDetector struct {
	mu sync.RWMutex

	workerManager *WorkerManager
	taskScheduler *TaskScheduler

	// Configuration
	heartbeatTimeout     time.Duration
	taskTimeout          time.Duration
	connectionTestPeriod time.Duration
	failureThreshold     int
	errorRateThreshold   float64

	// State tracking
	workerMetrics    map[string]*WorkerHealthMetrics // workerID -> metrics
	failureHistory   []WorkerFailureEvent
	failureCallbacks []func(WorkerFailureEvent)

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
}

// NewWorkerFailureDetector creates a new enhanced worker failure detector
func NewWorkerFailureDetector(workerManager *WorkerManager, taskScheduler *TaskScheduler) *WorkerFailureDetector {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerFailureDetector{
		workerManager:        workerManager,
		taskScheduler:        taskScheduler,
		heartbeatTimeout:     30 * time.Second,
		taskTimeout:          5 * time.Minute,
		connectionTestPeriod: 60 * time.Second,
		failureThreshold:     3,
		errorRateThreshold:   0.5, // 50% error rate
		workerMetrics:        make(map[string]*WorkerHealthMetrics),
		failureHistory:       make([]WorkerFailureEvent, 0),
		failureCallbacks:     make([]func(WorkerFailureEvent), 0),
		ctx:                  ctx,
		cancel:               cancel,
	}
}

// Start begins the failure detection process
func (wfd *WorkerFailureDetector) Start() {
	go wfd.runDetectionLoop()
	go wfd.runConnectionTests()
	log.Println("Worker failure detector started")
}

// Stop stops the failure detection process
func (wfd *WorkerFailureDetector) Stop() {
	wfd.cancel()
	log.Println("Worker failure detector stopped")
}

// RegisterFailureCallback registers a callback for failure events
func (wfd *WorkerFailureDetector) RegisterFailureCallback(callback func(WorkerFailureEvent)) {
	wfd.mu.Lock()
	defer wfd.mu.Unlock()
	wfd.failureCallbacks = append(wfd.failureCallbacks, callback)
}

// runDetectionLoop runs the main failure detection loop
func (wfd *WorkerFailureDetector) runDetectionLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-wfd.ctx.Done():
			return
		case <-ticker.C:
			wfd.detectFailures()
		}
	}
}

// runConnectionTests periodically tests worker connections
func (wfd *WorkerFailureDetector) runConnectionTests() {
	ticker := time.NewTicker(wfd.connectionTestPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-wfd.ctx.Done():
			return
		case <-ticker.C:
			wfd.testWorkerConnections()
		}
	}
}

// detectFailures detects various types of worker failures
func (wfd *WorkerFailureDetector) detectFailures() {
	wfd.updateWorkerMetrics()
	wfd.detectHeartbeatTimeouts()
	wfd.detectTaskTimeouts()
	wfd.detectHighErrorRates()
	wfd.detectResourceExhaustion()
}

// updateWorkerMetrics updates health metrics for all workers
func (wfd *WorkerFailureDetector) updateWorkerMetrics() {
	wfd.mu.Lock()
	defer wfd.mu.Unlock()

	allWorkers := wfd.workerManager.GetAllWorkers()
	now := time.Now()

	for workerID, worker := range allWorkers {
		metrics, exists := wfd.workerMetrics[workerID]
		if !exists {
			metrics = &WorkerHealthMetrics{
				WorkerID:            workerID,
				LastHeartbeat:       worker.LastSeen,
				ConsecutiveFailures: 0,
				TaskSuccessRate:     1.0,
				ConnectionErrors:    0,
				LastConnectionTest:  now,
			}
			wfd.workerMetrics[workerID] = metrics
		}

		// Update metrics
		metrics.LastHeartbeat = worker.LastSeen
		metrics.ResourceUsage = worker.Status

		// Calculate task success rate (simplified for now)
		// In a real implementation, you'd track task outcomes
		if len(worker.ActiveTasks) > 0 {
			// Assume some success rate based on active tasks
			metrics.TaskSuccessRate = 0.9 // Placeholder
		}
	}

	// Clean up metrics for removed workers
	for workerID := range wfd.workerMetrics {
		if _, exists := allWorkers[workerID]; !exists {
			delete(wfd.workerMetrics, workerID)
		}
	}
}

// detectHeartbeatTimeouts detects workers that have missed heartbeats
func (wfd *WorkerFailureDetector) detectHeartbeatTimeouts() {
	wfd.mu.RLock()
	defer wfd.mu.RUnlock()

	now := time.Now()

	for workerID, metrics := range wfd.workerMetrics {
		if now.Sub(metrics.LastHeartbeat) > wfd.heartbeatTimeout {
			event := WorkerFailureEvent{
				WorkerID:    workerID,
				FailureType: HeartbeatTimeout,
				Timestamp:   now,
				Details:     fmt.Sprintf("No heartbeat for %v", now.Sub(metrics.LastHeartbeat)),
				Severity:    3, // High severity
			}

			wfd.reportFailure(event)
		}
	}
}

// detectTaskTimeouts detects tasks that have been running too long
func (wfd *WorkerFailureDetector) detectTaskTimeouts() {
	allWorkers := wfd.workerManager.GetAllWorkers()
	now := time.Now()

	for workerID, worker := range allWorkers {
		for _, task := range worker.ActiveTasks {
			if now.Sub(task.StartTime) > wfd.taskTimeout {
				event := WorkerFailureEvent{
					WorkerID:    workerID,
					FailureType: TaskTimeout,
					Timestamp:   now,
					Details:     fmt.Sprintf("Task %s_%d_%d running for %v", task.JobID, task.StageID, task.TaskID, now.Sub(task.StartTime)),
					Severity:    2, // Medium severity
				}

				wfd.reportFailure(event)
			}
		}
	}
}

// detectHighErrorRates detects workers with high error rates
func (wfd *WorkerFailureDetector) detectHighErrorRates() {
	wfd.mu.RLock()
	defer wfd.mu.RUnlock()

	for workerID, metrics := range wfd.workerMetrics {
		if metrics.TaskSuccessRate < wfd.errorRateThreshold {
			event := WorkerFailureEvent{
				WorkerID:    workerID,
				FailureType: HighErrorRate,
				Timestamp:   time.Now(),
				Details:     fmt.Sprintf("Success rate: %.2f%%", metrics.TaskSuccessRate*100),
				Severity:    2, // Medium severity
			}

			wfd.reportFailure(event)
		}
	}
}

// detectResourceExhaustion detects workers with resource exhaustion
func (wfd *WorkerFailureDetector) detectResourceExhaustion() {
	wfd.mu.RLock()
	defer wfd.mu.RUnlock()

	for workerID, metrics := range wfd.workerMetrics {
		if metrics.ResourceUsage != nil {
			// Check CPU and memory usage
			if metrics.ResourceUsage.CpuUsage > 0.95 || metrics.ResourceUsage.MemoryUsage > 0.95 {
				event := WorkerFailureEvent{
					WorkerID:    workerID,
					FailureType: ResourceExhaustion,
					Timestamp:   time.Now(),
					Details:     fmt.Sprintf("CPU: %.1f%%, Memory: %.1f%%", metrics.ResourceUsage.CpuUsage*100, metrics.ResourceUsage.MemoryUsage*100),
					Severity:    2, // Medium severity
				}

				wfd.reportFailure(event)
			}
		}
	}
}

// testWorkerConnections tests connectivity to all workers
func (wfd *WorkerFailureDetector) testWorkerConnections() {
	allWorkers := wfd.workerManager.GetAllWorkers()

	for workerID, worker := range allWorkers {
		go wfd.testWorkerConnection(workerID, worker)
	}
}

// testWorkerConnection tests connectivity to a specific worker
func (wfd *WorkerFailureDetector) testWorkerConnection(workerID string, worker *WorkerInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try a simple connection test by creating a minimal task request
	// This will test connectivity without actually executing a task
	_, err := worker.Client.RunTask(ctx, &gen.RunTaskRequest{
		JobId:  "connection_test",
		TaskId: 0,
		Spec:   &gen.TaskSpec{Operation: gen.TaskOperation_SCAN}, // Use SCAN as a basic operation
	})

	wfd.mu.Lock()
	defer wfd.mu.Unlock()

	metrics, exists := wfd.workerMetrics[workerID]
	if !exists {
		return
	}

	metrics.LastConnectionTest = time.Now()

	if err != nil {
		metrics.ConnectionErrors++

		if metrics.ConnectionErrors >= wfd.failureThreshold {
			event := WorkerFailureEvent{
				WorkerID:    workerID,
				FailureType: ConnectionFailure,
				Timestamp:   time.Now(),
				Details:     fmt.Sprintf("Connection test failed: %v", err),
				Severity:    3, // High severity
			}

			wfd.reportFailure(event)
		}
	} else {
		// Reset connection error count on successful connection
		metrics.ConnectionErrors = 0
	}
}

// reportFailure reports a detected failure and triggers callbacks
func (wfd *WorkerFailureDetector) reportFailure(event WorkerFailureEvent) {
	wfd.mu.Lock()
	defer wfd.mu.Unlock()

	// Add to failure history
	wfd.failureHistory = append(wfd.failureHistory, event)

	// Keep only recent failures (last 100)
	if len(wfd.failureHistory) > 100 {
		wfd.failureHistory = wfd.failureHistory[1:]
	}

	log.Printf("Worker failure detected: %s - %s (%s) - %s",
		event.WorkerID, event.FailureType.String(), getSeverityString(event.Severity), event.Details)

	// Update worker metrics
	if metrics, exists := wfd.workerMetrics[event.WorkerID]; exists {
		metrics.ConsecutiveFailures++
	}

	// Trigger callbacks
	for _, callback := range wfd.failureCallbacks {
		go callback(event)
	}
}

// getSeverityString returns a string representation of severity
func getSeverityString(severity int) string {
	switch severity {
	case 1:
		return "LOW"
	case 2:
		return "MEDIUM"
	case 3:
		return "HIGH"
	default:
		return "UNKNOWN"
	}
}

// GetWorkerHealth returns health metrics for a worker
func (wfd *WorkerFailureDetector) GetWorkerHealth(workerID string) (*WorkerHealthMetrics, bool) {
	wfd.mu.RLock()
	defer wfd.mu.RUnlock()

	metrics, exists := wfd.workerMetrics[workerID]
	return metrics, exists
}

// GetFailureHistory returns recent failure events
func (wfd *WorkerFailureDetector) GetFailureHistory(limit int) []WorkerFailureEvent {
	wfd.mu.RLock()
	defer wfd.mu.RUnlock()

	if limit <= 0 || limit > len(wfd.failureHistory) {
		limit = len(wfd.failureHistory)
	}

	start := len(wfd.failureHistory) - limit
	result := make([]WorkerFailureEvent, limit)
	copy(result, wfd.failureHistory[start:])

	return result
}

// IsWorkerHealthy returns true if a worker is considered healthy
func (wfd *WorkerFailureDetector) IsWorkerHealthy(workerID string) bool {
	wfd.mu.RLock()
	defer wfd.mu.RUnlock()

	metrics, exists := wfd.workerMetrics[workerID]
	if !exists {
		return false
	}

	now := time.Now()

	// Check heartbeat
	if now.Sub(metrics.LastHeartbeat) > wfd.heartbeatTimeout {
		return false
	}

	// Check consecutive failures
	if metrics.ConsecutiveFailures >= wfd.failureThreshold {
		return false
	}

	// Check error rate
	if metrics.TaskSuccessRate < wfd.errorRateThreshold {
		return false
	}

	return true
}

// GetHealthyWorkerCount returns the number of healthy workers
func (wfd *WorkerFailureDetector) GetHealthyWorkerCount() int {
	wfd.mu.RLock()
	defer wfd.mu.RUnlock()

	count := 0
	for workerID := range wfd.workerMetrics {
		if wfd.IsWorkerHealthy(workerID) {
			count++
		}
	}

	return count
}
