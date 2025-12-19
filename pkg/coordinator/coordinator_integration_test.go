package coordinator

import (
	"testing"
)

func TestCoordinatorComponentsCreation(t *testing.T) {
	// Test that coordinator components can be instantiated without external dependencies

	// Initialize components
	workerManager := NewWorkerManager()
	queryPlanner := NewQueryPlanner()
	taskScheduler := NewTaskScheduler(workerManager, queryPlanner)

	// Verify all components are created
	if workerManager == nil {
		t.Error("WorkerManager should not be nil")
	}
	if queryPlanner == nil {
		t.Error("QueryPlanner should not be nil")
	}
	if taskScheduler == nil {
		t.Error("TaskScheduler should not be nil")
	}

	// Test basic functionality
	stats := workerManager.GetWorkerStats()
	if stats.Total != 0 {
		t.Errorf("Expected 0 total workers, got %d", stats.Total)
	}

	// Test query planning
	query := &SimpleQuery{
		TableName: "test_table",
		Filter:    "value > 100",
	}

	plan, err := queryPlanner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	if plan.JobID == "" {
		t.Error("Job ID should not be empty")
	}

	if len(plan.Stages) == 0 {
		t.Error("Plan should have at least one stage")
	}
}

func TestWorkerManagerBasicOperations(t *testing.T) {
	wm := NewWorkerManager()

	// Test initial state
	stats := wm.GetWorkerStats()
	if stats.Total != 0 {
		t.Errorf("Expected 0 total workers initially, got %d", stats.Total)
	}

	healthyWorkers := wm.GetHealthyWorkers()
	if len(healthyWorkers) != 0 {
		t.Errorf("Expected 0 healthy workers initially, got %d", len(healthyWorkers))
	}

	failedWorkers := wm.GetFailedWorkers()
	if len(failedWorkers) != 0 {
		t.Errorf("Expected 0 failed workers initially, got %d", len(failedWorkers))
	}
}

func TestTaskSchedulerBasicOperations(t *testing.T) {
	wm := NewWorkerManager()
	qp := NewQueryPlanner()
	ts := NewTaskScheduler(wm, qp)

	// Test query scheduling
	query := &SimpleQuery{
		TableName: "test_table",
		Filter:    "value > 100",
	}

	plan, err := ts.ScheduleQuery(query)
	if err != nil {
		t.Fatalf("Failed to schedule query: %v", err)
	}

	if plan.JobID == "" {
		t.Error("Job ID should not be empty")
	}

	// Test query status
	status, err := ts.GetQueryStatus(plan.JobID)
	if err != nil {
		t.Fatalf("Failed to get query status: %v", err)
	}

	if status.JobID != plan.JobID {
		t.Errorf("Expected job ID %s, got %s", plan.JobID, status.JobID)
	}

	if status.Status != "PENDING" {
		t.Errorf("Expected status PENDING, got %s", status.Status)
	}
}
