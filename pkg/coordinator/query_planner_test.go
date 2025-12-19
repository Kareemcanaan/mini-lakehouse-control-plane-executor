package coordinator

import (
	"testing"
)

func TestQueryPlanner_PlanQuery(t *testing.T) {
	planner := NewQueryPlanner()

	// Test simple query with aggregation
	query := &SimpleQuery{
		TableName: "test_table",
		Filter:    "value > 100",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "sum", Column: "amount", Alias: "total_amount"},
			{Function: "count", Column: "*", Alias: "count"},
		},
		Projection: []string{"category", "total_amount", "count"},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Verify plan structure
	if len(plan.Stages) != 2 {
		t.Errorf("Expected 2 stages, got %d", len(plan.Stages))
	}

	// Verify map stage
	mapStage := plan.Stages[0]
	if mapStage.StageType != MapStage {
		t.Errorf("Expected MapStage, got %v", mapStage.StageType)
	}
	if len(mapStage.Tasks) != 1 {
		t.Errorf("Expected 1 map task, got %d", len(mapStage.Tasks))
	}
	if mapStage.Tasks[0].Spec.Operation != MAP_AGG {
		t.Errorf("Expected MAP_AGG operation, got %v", mapStage.Tasks[0].Spec.Operation)
	}

	// Verify reduce stage
	reduceStage := plan.Stages[1]
	if reduceStage.StageType != ReduceStage {
		t.Errorf("Expected ReduceStage, got %v", reduceStage.StageType)
	}
	if len(reduceStage.Dependencies) != 1 || reduceStage.Dependencies[0] != 0 {
		t.Errorf("Expected reduce stage to depend on stage 0, got %v", reduceStage.Dependencies)
	}
}

func TestQueryPlanner_PlanQueryWithoutAggregation(t *testing.T) {
	planner := NewQueryPlanner()

	// Test simple query without aggregation (scan + filter only)
	query := &SimpleQuery{
		TableName:  "test_table",
		Filter:     "value > 100",
		Projection: []string{"id", "name", "value"},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Should only have one stage for non-aggregation queries
	if len(plan.Stages) != 1 {
		t.Errorf("Expected 1 stage for non-aggregation query, got %d", len(plan.Stages))
	}

	mapStage := plan.Stages[0]
	if mapStage.StageType != MapStage {
		t.Errorf("Expected MapStage, got %v", mapStage.StageType)
	}
}

func TestQueryPlanner_TaskDependencies(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "count", Column: "*", Alias: "count"},
		},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Test dependencies for reduce stage task
	deps := planner.GetTaskDependencies(plan, 1, 0)
	if len(deps) != 1 {
		t.Errorf("Expected 1 dependency for reduce task, got %d", len(deps))
	}
	if deps[0].StageID != 0 || deps[0].TaskID != 0 {
		t.Errorf("Expected dependency on stage 0, task 0, got stage %d, task %d", deps[0].StageID, deps[0].TaskID)
	}

	// Test dependencies for map stage task (should have none)
	deps = planner.GetTaskDependencies(plan, 0, 0)
	if len(deps) != 0 {
		t.Errorf("Expected 0 dependencies for map task, got %d", len(deps))
	}
}

func TestQueryPlanner_StageReadiness(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "count", Column: "*", Alias: "count"},
		},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Initially, only stage 0 should be ready
	if !planner.IsStageReady(plan, 0) {
		t.Error("Stage 0 should be ready initially")
	}
	if planner.IsStageReady(plan, 1) {
		t.Error("Stage 1 should not be ready initially")
	}

	// Complete stage 0 task
	err = planner.UpdateTaskStatus(plan, 0, 0, TaskCompleted)
	if err != nil {
		t.Fatalf("Failed to update task status: %v", err)
	}

	// Now stage 1 should be ready
	if !planner.IsStageReady(plan, 1) {
		t.Error("Stage 1 should be ready after stage 0 completes")
	}
}

func TestQueryPlanner_GetReadyTasks(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "count", Column: "*", Alias: "count"},
		},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Initially, only stage 0 task should be ready
	readyTasks := planner.GetReadyTasks(plan)
	if len(readyTasks) != 1 {
		t.Errorf("Expected 1 ready task initially, got %d", len(readyTasks))
	}
	if readyTasks[0].StageID != 0 || readyTasks[0].TaskID != 0 {
		t.Errorf("Expected stage 0, task 0 to be ready, got stage %d, task %d", readyTasks[0].StageID, readyTasks[0].TaskID)
	}

	// Mark stage 0 task as running
	err = planner.UpdateTaskStatus(plan, 0, 0, TaskRunning)
	if err != nil {
		t.Fatalf("Failed to update task status: %v", err)
	}

	// No tasks should be ready now
	readyTasks = planner.GetReadyTasks(plan)
	if len(readyTasks) != 0 {
		t.Errorf("Expected 0 ready tasks when stage 0 is running, got %d", len(readyTasks))
	}

	// Complete stage 0 task
	err = planner.UpdateTaskStatus(plan, 0, 0, TaskCompleted)
	if err != nil {
		t.Fatalf("Failed to update task status: %v", err)
	}

	// Now stage 1 task should be ready
	readyTasks = planner.GetReadyTasks(plan)
	if len(readyTasks) != 1 {
		t.Errorf("Expected 1 ready task after stage 0 completes, got %d", len(readyTasks))
	}
	if readyTasks[0].StageID != 1 || readyTasks[0].TaskID != 0 {
		t.Errorf("Expected stage 1, task 0 to be ready, got stage %d, task %d", readyTasks[0].StageID, readyTasks[0].TaskID)
	}
}

func TestQueryPlanner_IsQueryComplete(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "count", Column: "*", Alias: "count"},
		},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Query should not be complete initially
	if planner.IsQueryComplete(plan) {
		t.Error("Query should not be complete initially")
	}

	// Complete stage 0 task
	err = planner.UpdateTaskStatus(plan, 0, 0, TaskCompleted)
	if err != nil {
		t.Fatalf("Failed to update task status: %v", err)
	}

	// Query should still not be complete
	if planner.IsQueryComplete(plan) {
		t.Error("Query should not be complete with only stage 0 done")
	}

	// Complete stage 1 task
	err = planner.UpdateTaskStatus(plan, 1, 0, TaskCompleted)
	if err != nil {
		t.Fatalf("Failed to update task status: %v", err)
	}

	// Now query should be complete
	if !planner.IsQueryComplete(plan) {
		t.Error("Query should be complete when all tasks are done")
	}
}

func TestQueryPlanner_GetTask(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "count", Column: "*", Alias: "count"},
		},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Test getting existing task
	task, err := planner.GetTask(plan, 0, 0)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}
	if task.ID != 0 {
		t.Errorf("Expected task ID 0, got %d", task.ID)
	}

	// Test getting non-existent task
	_, err = planner.GetTask(plan, 0, 999)
	if err == nil {
		t.Error("Expected error for non-existent task")
	}

	// Test invalid stage ID
	_, err = planner.GetTask(plan, 999, 0)
	if err == nil {
		t.Error("Expected error for invalid stage ID")
	}
}

func TestQueryPlanner_AssignTaskToWorker(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		Filter:    "value > 100",
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Assign task to worker
	err = planner.AssignTaskToWorker(plan, 0, 0, "worker-1")
	if err != nil {
		t.Fatalf("Failed to assign task to worker: %v", err)
	}

	// Verify assignment
	task, err := planner.GetTask(plan, 0, 0)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}
	if task.WorkerID != "worker-1" {
		t.Errorf("Expected worker ID 'worker-1', got '%s'", task.WorkerID)
	}
	if task.Status != TaskRunning {
		t.Errorf("Expected task status TaskRunning, got %v", task.Status)
	}
}

func TestQueryPlanner_RetryTask(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		Filter:    "value > 100",
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Mark task as failed
	err = planner.UpdateTaskStatus(plan, 0, 0, TaskFailed)
	if err != nil {
		t.Fatalf("Failed to update task status: %v", err)
	}

	// Get original attempt number
	task, err := planner.GetTask(plan, 0, 0)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}
	originalAttempt := task.Attempt

	// Retry task
	err = planner.RetryTask(plan, 0, 0)
	if err != nil {
		t.Fatalf("Failed to retry task: %v", err)
	}

	// Verify retry
	task, err = planner.GetTask(plan, 0, 0)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}
	if task.Attempt != originalAttempt+1 {
		t.Errorf("Expected attempt %d, got %d", originalAttempt+1, task.Attempt)
	}
	if task.Status != TaskPending {
		t.Errorf("Expected task status TaskPending, got %v", task.Status)
	}
	if task.WorkerID != "" {
		t.Errorf("Expected empty worker ID after retry, got '%s'", task.WorkerID)
	}
}

func TestQueryPlanner_GetFailedTasks(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "count", Column: "*", Alias: "count"},
		},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Initially no failed tasks
	failedTasks := planner.GetFailedTasks(plan)
	if len(failedTasks) != 0 {
		t.Errorf("Expected 0 failed tasks initially, got %d", len(failedTasks))
	}

	// Mark a task as failed
	err = planner.UpdateTaskStatus(plan, 0, 0, TaskFailed)
	if err != nil {
		t.Fatalf("Failed to update task status: %v", err)
	}

	// Should now have one failed task
	failedTasks = planner.GetFailedTasks(plan)
	if len(failedTasks) != 1 {
		t.Errorf("Expected 1 failed task, got %d", len(failedTasks))
	}
	if failedTasks[0].StageID != 0 || failedTasks[0].TaskID != 0 {
		t.Errorf("Expected stage 0, task 0 to be failed, got stage %d, task %d", failedTasks[0].StageID, failedTasks[0].TaskID)
	}
}

func TestQueryPlanner_GetTasksByWorker(t *testing.T) {
	planner := NewQueryPlanner()

	query := &SimpleQuery{
		TableName: "test_table",
		GroupBy:   []string{"category"},
		Aggregates: []Aggregate{
			{Function: "count", Column: "*", Alias: "count"},
		},
	}

	plan, err := planner.PlanQuery(query)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Initially no tasks assigned to worker
	workerTasks := planner.GetTasksByWorker(plan, "worker-1")
	if len(workerTasks) != 0 {
		t.Errorf("Expected 0 tasks for worker-1 initially, got %d", len(workerTasks))
	}

	// Assign task to worker
	err = planner.AssignTaskToWorker(plan, 0, 0, "worker-1")
	if err != nil {
		t.Fatalf("Failed to assign task to worker: %v", err)
	}

	// Should now have one task for worker
	workerTasks = planner.GetTasksByWorker(plan, "worker-1")
	if len(workerTasks) != 1 {
		t.Errorf("Expected 1 task for worker-1, got %d", len(workerTasks))
	}
	if workerTasks[0].StageID != 0 || workerTasks[0].TaskID != 0 {
		t.Errorf("Expected stage 0, task 0 for worker-1, got stage %d, task %d", workerTasks[0].StageID, workerTasks[0].TaskID)
	}
}

func TestQueryPlanner_DetermineMapOperation(t *testing.T) {
	tests := []struct {
		name     string
		query    *SimpleQuery
		expected TaskOperation
	}{
		{
			name: "aggregation query",
			query: &SimpleQuery{
				TableName: "test",
				Aggregates: []Aggregate{
					{Function: "count", Column: "*"},
				},
			},
			expected: MAP_AGG,
		},
		{
			name: "filter and projection",
			query: &SimpleQuery{
				TableName:  "test",
				Filter:     "value > 100",
				Projection: []string{"id", "name"},
			},
			expected: MAP_FILTER,
		},
		{
			name: "filter only",
			query: &SimpleQuery{
				TableName: "test",
				Filter:    "value > 100",
			},
			expected: MAP_FILTER,
		},
		{
			name: "projection only",
			query: &SimpleQuery{
				TableName:  "test",
				Projection: []string{"id", "name"},
			},
			expected: MAP_PROJECT,
		},
		{
			name: "scan only",
			query: &SimpleQuery{
				TableName: "test",
			},
			expected: SCAN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineMapOperation(tt.query)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
