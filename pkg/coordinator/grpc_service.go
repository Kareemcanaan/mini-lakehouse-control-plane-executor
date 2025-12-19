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
	taskScheduler       *TaskScheduler
	queryPlanner        *QueryPlanner
	distributedExecutor *DistributedQueryExecutor
}

// NewQueryExecutionService creates a new query execution service
func NewQueryExecutionService(
	taskScheduler *TaskScheduler,
	queryPlanner *QueryPlanner,
	distributedExecutor *DistributedQueryExecutor,
) *QueryExecutionService {
	return &QueryExecutionService{
		taskScheduler:       taskScheduler,
		queryPlanner:        queryPlanner,
		distributedExecutor: distributedExecutor,
	}
}

// ExecuteQuery executes a query and returns the job ID
func (qes *QueryExecutionService) ExecuteQuery(query *SimpleQuery) (string, error) {
	log.Printf("Executing distributed query on table %s", query.TableName)

	// Execute the query using the distributed executor
	ctx := context.Background()
	execution, err := qes.distributedExecutor.ExecuteQuery(ctx, query)
	if err != nil {
		return "", fmt.Errorf("failed to execute query: %w", err)
	}

	log.Printf("Distributed query started with job ID %s", execution.JobID)
	return execution.JobID, nil
}

// GetQueryStatus returns the status of a query
func (qes *QueryExecutionService) GetQueryStatus(jobID string) (*QueryStatus, error) {
	// Get execution from distributed executor
	execution, err := qes.distributedExecutor.GetQueryExecution(jobID)
	if err != nil {
		return nil, err
	}

	// Convert to QueryStatus format
	status := &QueryStatus{
		JobID:  execution.JobID,
		Status: string(execution.Status),
	}

	// Get detailed metrics
	metrics, err := qes.distributedExecutor.GetQueryMetrics(jobID)
	if err == nil {
		status.TotalTasks = metrics.TotalTasks
		status.CompletedTasks = metrics.CompletedTasks
		status.FailedTasks = metrics.FailedTasks
		status.RunningTasks = metrics.RunningTasks
	}

	return status, nil
}

// CancelQuery cancels a running query
func (qes *QueryExecutionService) CancelQuery(jobID string) error {
	return qes.distributedExecutor.CancelQuery(jobID)
}

// WaitForQueryCompletion waits for a query to complete and returns the result
func (qes *QueryExecutionService) WaitForQueryCompletion(ctx context.Context, jobID string) (*QueryResult, error) {
	// Poll for query completion
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			execution, err := qes.distributedExecutor.GetQueryExecution(jobID)
			if err != nil {
				return nil, fmt.Errorf("failed to get query execution: %w", err)
			}

			switch execution.Status {
			case QueryStatusCompleted:
				// Query completed successfully
				result := &QueryResult{
					JobID:      jobID,
					Success:    true,
					ResultPath: execution.ResultLocation,
				}

				// Get metrics
				if metrics, err := qes.distributedExecutor.GetQueryMetrics(jobID); err == nil {
					result.Metrics = &QueryMetrics{
						DurationMs:     metrics.DurationMs,
						TotalTasks:     metrics.TotalTasks,
						FailedTasks:    metrics.FailedTasks,
						RetriedTasks:   0, // TODO: Track retries
						BytesProcessed: 0, // TODO: Aggregate from task metrics
						RowsProcessed:  0, // TODO: Aggregate from task metrics
					}
				}

				return result, nil

			case QueryStatusFailed:
				// Query failed
				result := &QueryResult{
					JobID:   jobID,
					Success: false,
					Error:   execution.Error,
				}

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
