package coordinator

import (
	"context"
	"fmt"
	"log"
	"time"

	"mini-lakehouse/proto/gen"

	"google.golang.org/grpc"
)

// CoordinatorGRPCService implements the CoordinatorService gRPC interface
type CoordinatorGRPCService struct {
	gen.UnimplementedCoordinatorServiceServer

	workerManager         *WorkerManager
	taskScheduler         *TaskScheduler
	faultToleranceManager *FaultToleranceManager
}

// NewCoordinatorGRPCService creates a new coordinator gRPC service
func NewCoordinatorGRPCService(
	workerManager *WorkerManager,
	taskScheduler *TaskScheduler,
	faultToleranceManager *FaultToleranceManager,
) *CoordinatorGRPCService {
	return &CoordinatorGRPCService{
		workerManager:         workerManager,
		taskScheduler:         taskScheduler,
		faultToleranceManager: faultToleranceManager,
	}
}

// RegisterWorker handles worker registration requests
func (s *CoordinatorGRPCService) RegisterWorker(ctx context.Context, req *gen.RegisterWorkerRequest) (*gen.RegisterWorkerResponse, error) {
	log.Printf("Worker registration request from %s at %s", req.WorkerId, req.Address)

	// Create gRPC client connection to the worker
	conn, err := grpc.Dial(req.Address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to worker %s: %v", req.WorkerId, err)
		return &gen.RegisterWorkerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to connect to worker: %v", err),
		}, nil
	}

	workerClient := gen.NewWorkerServiceClient(conn)

	// Register the worker
	err = s.workerManager.RegisterWorker(req.WorkerId, req.Address, req.Capabilities, workerClient)
	if err != nil {
		log.Printf("Failed to register worker %s: %v", req.WorkerId, err)
		conn.Close()
		return &gen.RegisterWorkerResponse{
			Success: false,
			Error:   fmt.Sprintf("Registration failed: %v", err),
		}, nil
	}

	log.Printf("Successfully registered worker %s with %d max concurrent tasks",
		req.WorkerId, req.Capabilities.MaxConcurrentTasks)

	return &gen.RegisterWorkerResponse{
		Success: true,
	}, nil
}

// Heartbeat handles worker heartbeat requests
func (s *CoordinatorGRPCService) Heartbeat(ctx context.Context, req *gen.HeartbeatRequest) (*gen.HeartbeatResponse, error) {
	// Update worker heartbeat
	err := s.workerManager.UpdateHeartbeat(req.WorkerId, req.Status)
	if err != nil {
		log.Printf("Failed to update heartbeat for worker %s: %v", req.WorkerId, err)
		return &gen.HeartbeatResponse{
			Success: false,
		}, nil
	}

	// TODO: Implement task cancellation logic
	// For now, return empty cancel list
	return &gen.HeartbeatResponse{
		Success:     true,
		CancelTasks: []string{},
	}, nil
}

// ReportTaskComplete handles task completion reports from workers
func (s *CoordinatorGRPCService) ReportTaskComplete(ctx context.Context, req *gen.ReportTaskCompleteRequest) (*gen.ReportTaskCompleteResponse, error) {
	log.Printf("Task completion report from worker %s: job %s, stage %d, task %d, attempt %d, success: %t",
		req.WorkerId, req.JobId, req.Stage, req.TaskId, req.Attempt, req.Result.Success)

	// Handle task completion in the scheduler
	err := s.taskScheduler.HandleTaskCompletion(
		req.WorkerId,
		req.JobId,
		int(req.Stage),
		int(req.TaskId),
		req.Result,
	)
	if err != nil {
		log.Printf("Failed to handle task completion: %v", err)
		return &gen.ReportTaskCompleteResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to handle task completion: %v", err),
		}, nil
	}

	// If task was successful, write SUCCESS manifest
	if req.Result.Success {
		err = s.faultToleranceManager.WriteSuccessManifest(
			ctx,
			req.JobId,
			int(req.Stage),
			int(req.TaskId),
			int(req.Attempt),
			req.Result,
		)
		if err != nil {
			log.Printf("Failed to write SUCCESS manifest: %v", err)
			// Don't fail the request, but log the error
			// The task is still considered complete
		}
	}

	return &gen.ReportTaskCompleteResponse{
		Success: true,
	}, nil
}

// QueryExecutionService provides query execution orchestration
type QueryExecutionService struct {
	taskScheduler *TaskScheduler
	queryPlanner  *QueryPlanner
}

// NewQueryExecutionService creates a new query execution service
func NewQueryExecutionService(taskScheduler *TaskScheduler, queryPlanner *QueryPlanner) *QueryExecutionService {
	return &QueryExecutionService{
		taskScheduler: taskScheduler,
		queryPlanner:  queryPlanner,
	}
}

// ExecuteQuery executes a query and returns the job ID
func (qes *QueryExecutionService) ExecuteQuery(query *SimpleQuery) (string, error) {
	log.Printf("Executing query on table %s", query.TableName)

	// Schedule the query
	plan, err := qes.taskScheduler.ScheduleQuery(query)
	if err != nil {
		return "", fmt.Errorf("failed to schedule query: %w", err)
	}

	log.Printf("Query scheduled with job ID %s", plan.JobID)
	return plan.JobID, nil
}

// GetQueryStatus returns the status of a query
func (qes *QueryExecutionService) GetQueryStatus(jobID string) (*QueryStatus, error) {
	return qes.taskScheduler.GetQueryStatus(jobID)
}

// CancelQuery cancels a running query
func (qes *QueryExecutionService) CancelQuery(jobID string) error {
	// TODO: Implement query cancellation
	// This would involve:
	// 1. Marking the query as cancelled
	// 2. Cancelling all running tasks
	// 3. Cleaning up resources

	log.Printf("Query cancellation requested for job %s (not yet implemented)", jobID)
	return fmt.Errorf("query cancellation not yet implemented")
}

// WaitForQueryCompletion waits for a query to complete and returns the result
func (qes *QueryExecutionService) WaitForQueryCompletion(ctx context.Context, jobID string) (*QueryResult, error) {
	// Poll for query completion
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			status, err := qes.GetQueryStatus(jobID)
			if err != nil {
				return nil, fmt.Errorf("failed to get query status: %w", err)
			}

			switch status.Status {
			case "COMPLETED":
				// Query completed successfully
				result := &QueryResult{
					JobID:   jobID,
					Success: true,
					// TODO: Collect actual results from object storage
					ResultPath: fmt.Sprintf("s3://lake/results/%s/", jobID),
				}

				// Clean up the query from active tracking
				qes.taskScheduler.RemoveQuery(jobID)

				return result, nil

			case "FAILED":
				// Query failed
				result := &QueryResult{
					JobID:   jobID,
					Success: false,
					Error:   "Query execution failed",
				}

				// Clean up the query from active tracking
				qes.taskScheduler.RemoveQuery(jobID)

				return result, nil

			default:
				// Query still running, wait and check again
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(1 * time.Second):
					continue
				}
			}
		}
	}
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	JobID      string
	Success    bool
	Error      string
	ResultPath string
	Metrics    *QueryMetrics
}

// QueryMetrics represents metrics for a completed query
type QueryMetrics struct {
	DurationMs     int64
	TotalTasks     int
	FailedTasks    int
	RetriedTasks   int
	BytesProcessed uint64
	RowsProcessed  uint64
}
