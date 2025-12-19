use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::proto::worker::{
    worker_service_server::WorkerService, RunTaskRequest, RunTaskResponse, CancelTaskRequest,
    CancelTaskResponse, TaskSpec as ProtoTaskSpec, TaskOperation as ProtoTaskOperation,
};
use crate::proto::common::{
    TaskOutput as ProtoTaskOutput, TaskMetrics as ProtoTaskMetrics,
};
use crate::proto::coordinator::{
    coordinator_service_client::CoordinatorServiceClient, RegisterWorkerRequest,
    HeartbeatRequest, ReportTaskCompleteRequest,
    WorkerCapabilities, WorkerStatus, TaskResult as ProtoTaskResult,
};
use crate::task_executor::{
    TaskExecutor, TaskSpec, TaskOperation, Aggregate, AggregateFunction, TaskResult,
    TaskMetrics,
};

/// Worker service implementation
pub struct WorkerServiceImpl {
    worker_id: String,
    coordinator_client: Arc<Mutex<Option<CoordinatorServiceClient<tonic::transport::Channel>>>>,
    task_executor: TaskExecutor,
    active_tasks: Arc<Mutex<HashMap<String, TaskHandle>>>,
    heartbeat_sender: Option<mpsc::UnboundedSender<()>>,
}

/// Handle for managing running tasks
#[derive(Debug)]
struct TaskHandle {
    job_id: String,
    task_id: u32,
    attempt: u32,
    start_time: Instant,
    cancel_sender: Option<mpsc::UnboundedSender<()>>,
}

impl WorkerServiceImpl {
    /// Create a new worker service
    pub fn new(worker_id: String) -> Self {
        Self {
            worker_id,
            coordinator_client: Arc::new(Mutex::new(None)),
            task_executor: TaskExecutor::new(),
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_sender: None,
        }
    }

    /// Register with coordinator
    pub async fn register_with_coordinator(
        &mut self,
        coordinator_endpoint: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Registering with coordinator at {}", coordinator_endpoint);

        // Connect to coordinator
        let channel = tonic::transport::Channel::from_shared(coordinator_endpoint)?
            .connect()
            .await?;
        let mut client = CoordinatorServiceClient::new(channel);

        // Register worker
        let request = Request::new(RegisterWorkerRequest {
            worker_id: self.worker_id.clone(),
            address: "0.0.0.0:50051".to_string(), // TODO: Make configurable
            capabilities: Some(WorkerCapabilities {
                max_concurrent_tasks: 4,
                supported_operations: vec![
                    "SCAN".to_string(),
                    "MAP_FILTER".to_string(),
                    "MAP_PROJECT".to_string(),
                    "MAP_AGG".to_string(),
                    "REDUCE_AGG".to_string(),
                    "SHUFFLE".to_string(),
                ],
            }),
        });

        let response = client.register_worker(request).await?;
        let register_response = response.into_inner();

        if !register_response.success {
            return Err(format!("Failed to register with coordinator: {}", 
                register_response.error).into());
        }

        // Store client for later use
        *self.coordinator_client.lock().unwrap() = Some(client);

        info!("Successfully registered with coordinator");
        Ok(())
    }

    /// Start heartbeat loop
    pub async fn start_heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        self.heartbeat_sender = Some(sender);

        let worker_id = self.worker_id.clone();
        let coordinator_client = self.coordinator_client.clone();
        let active_tasks = self.active_tasks.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Send heartbeat
                        let client_opt = {
                            coordinator_client.lock().unwrap().clone()
                        };
                        if let Some(mut client) = client_opt {
                            let active_task_count = active_tasks.lock().unwrap().len() as u32;
                            
                            let request = Request::new(HeartbeatRequest {
                                worker_id: worker_id.clone(),
                                status: Some(WorkerStatus {
                                    active_tasks: active_task_count,
                                    cpu_usage: 0.0, // TODO: Implement actual metrics
                                    memory_usage: 0.0, // TODO: Implement actual metrics
                                }),
                            });

                            match client.heartbeat(request).await {
                                Ok(response) => {
                                    let heartbeat_response = response.into_inner();
                                    if !heartbeat_response.success {
                                        warn!("Heartbeat failed");
                                    }
                                    
                                    // Handle cancel requests
                                    for task_to_cancel in heartbeat_response.cancel_tasks {
                                        info!("Received cancel request for task: {}", task_to_cancel);
                                        // TODO: Implement task cancellation
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to send heartbeat: {}", e);
                                }
                            }
                        }
                    }
                    _ = receiver.recv() => {
                        // Shutdown signal received
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Report task completion to coordinator
    async fn report_task_complete(
        &self,
        job_id: String,
        stage: u32,
        task_id: u32,
        attempt: u32,
        result: TaskResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client_opt = {
            self.coordinator_client.lock().unwrap().clone()
        };
        if let Some(mut client) = client_opt {
            let proto_result = ProtoTaskResult {
                success: result.success,
                error: result.error.unwrap_or_default(),
                outputs: result.outputs.into_iter().map(|o| crate::proto::common::TaskOutput {
                    partition: o.partition as u32,
                    path: o.path,
                    rows: o.rows,
                    size: o.size,
                }).collect(),
                metrics: Some(crate::proto::common::TaskMetrics {
                    duration_ms: result.metrics.duration_ms,
                    rows_processed: result.metrics.rows_processed,
                    bytes_read: result.metrics.bytes_read,
                    bytes_written: result.metrics.bytes_written,
                }),
            };

            let request = Request::new(ReportTaskCompleteRequest {
                worker_id: self.worker_id.clone(),
                job_id,
                stage,
                task_id,
                attempt,
                result: Some(proto_result),
            });

            let response = client.report_task_complete(request).await?;
            let report_response = response.into_inner();

            if !report_response.success {
                warn!("Failed to report task completion: {}", 
                    report_response.error);
            }
        }

        Ok(())
    }

    /// Convert proto task spec to internal task spec
    fn convert_task_spec(&self, proto_spec: ProtoTaskSpec) -> Result<TaskSpec, Status> {
        let operation = match ProtoTaskOperation::try_from(proto_spec.operation) {
            Ok(op) => match op {
                ProtoTaskOperation::Scan => TaskOperation::Scan,
                ProtoTaskOperation::MapFilter => TaskOperation::MapFilter,
                ProtoTaskOperation::MapProject => TaskOperation::MapProject,
                ProtoTaskOperation::MapAgg => TaskOperation::MapAgg,
                ProtoTaskOperation::ReduceAgg => TaskOperation::ReduceAgg,
                ProtoTaskOperation::Shuffle => TaskOperation::Shuffle,
            },
            Err(_) => return Err(Status::invalid_argument("Invalid task operation")),
        };

        let aggregates: Result<Vec<Aggregate>, Status> = proto_spec.aggregates.into_iter().map(|agg| {
            let function = agg.function.parse::<AggregateFunction>()
                .map_err(|_| Status::invalid_argument(format!("Invalid aggregate function: {}", agg.function)))?;
            
            Ok(Aggregate {
                function,
                column: agg.column,
                alias: if agg.alias.is_empty() { None } else { Some(agg.alias) },
            })
        }).collect();

        Ok(TaskSpec {
            operation,
            input_files: proto_spec.input_files,
            filter: if proto_spec.filter.is_empty() { None } else { Some(proto_spec.filter) },
            group_by: proto_spec.group_by,
            aggregates: aggregates?,
            num_partitions: if proto_spec.num_partitions == 0 { None } else { Some(proto_spec.num_partitions as usize) },
            output_path: proto_spec.output_path,
            projection: proto_spec.projection,
        })
    }

    /// Convert internal task result to proto task result
    fn convert_task_result(&self, result: TaskResult) -> (Vec<ProtoTaskOutput>, ProtoTaskMetrics) {
        let outputs = result.outputs.into_iter().map(|o| ProtoTaskOutput {
            partition: o.partition as u32,
            path: o.path,
            rows: o.rows,
            size: o.size,
        }).collect();

        let metrics = ProtoTaskMetrics {
            duration_ms: result.metrics.duration_ms,
            rows_processed: result.metrics.rows_processed,
            bytes_read: result.metrics.bytes_read,
            bytes_written: result.metrics.bytes_written,
        };

        (outputs, metrics)
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    /// Run a task
    async fn run_task(
        &self,
        request: Request<RunTaskRequest>,
    ) -> Result<Response<RunTaskResponse>, Status> {
        let req = request.into_inner();
        let task_key = format!("{}_{}", req.job_id, req.task_id);
        
        info!("Received task: job_id={}, stage={}, task_id={}, attempt={}", 
              req.job_id, req.stage, req.task_id, req.attempt);

        // Check if task is already running
        if self.active_tasks.lock().unwrap().contains_key(&task_key) {
            return Err(Status::already_exists("Task is already running"));
        }

        // Convert proto spec to internal spec
        let spec = match req.spec {
            Some(proto_spec) => self.convert_task_spec(proto_spec)?,
            None => return Err(Status::invalid_argument("Missing task specification")),
        };

        // Create task handle
        let task_handle = TaskHandle {
            job_id: req.job_id.clone(),
            task_id: req.task_id,
            attempt: req.attempt,
            start_time: Instant::now(),
            cancel_sender: None, // TODO: Implement cancellation
        };

        // Store start time for later use
        let start_time = task_handle.start_time;
        
        // Add to active tasks
        self.active_tasks.lock().unwrap().insert(task_key.clone(), task_handle);

        // Execute task
        let result = self.task_executor.execute_task(&spec).await;

        // Remove from active tasks
        self.active_tasks.lock().unwrap().remove(&task_key);

        match result {
            Ok(task_result) => {
                info!("Task completed successfully: job_id={}, task_id={}, duration={}ms", 
                      req.job_id, req.task_id, task_result.metrics.duration_ms);

                // Report completion to coordinator
                if let Err(e) = self.report_task_complete(
                    req.job_id,
                    req.stage,
                    req.task_id,
                    req.attempt,
                    task_result.clone(),
                ).await {
                    warn!("Failed to report task completion: {}", e);
                }

                let (outputs, metrics) = self.convert_task_result(task_result);

                Ok(Response::new(RunTaskResponse {
                    success: true,
                    error: String::new(),
                    outputs,
                    metrics: Some(metrics),
                }))
            }
            Err(e) => {
                error!("Task failed: job_id={}, task_id={}, error={}", 
                       req.job_id, req.task_id, e);

                // Report failure to coordinator
                let failed_result = TaskResult {
                    success: false,
                    error: Some(e.to_string()),
                    outputs: vec![],
                    metrics: TaskMetrics {
                        duration_ms: start_time.elapsed().as_millis() as u64,
                        rows_processed: 0,
                        bytes_read: 0,
                        bytes_written: 0,
                    },
                };

                if let Err(report_err) = self.report_task_complete(
                    req.job_id,
                    req.stage,
                    req.task_id,
                    req.attempt,
                    failed_result,
                ).await {
                    warn!("Failed to report task failure: {}", report_err);
                }

                Ok(Response::new(RunTaskResponse {
                    success: false,
                    error: e.to_string(),
                    outputs: vec![],
                    metrics: Some(ProtoTaskMetrics {
                        duration_ms: start_time.elapsed().as_millis() as u64,
                        rows_processed: 0,
                        bytes_read: 0,
                        bytes_written: 0,
                    }),
                }))
            }
        }
    }

    /// Cancel a task
    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();
        let task_key = format!("{}_{}", req.job_id, req.task_id);

        info!("Received cancel request: job_id={}, task_id={}", req.job_id, req.task_id);

        // Check if task is running
        if let Some(_task_handle) = self.active_tasks.lock().unwrap().get(&task_key) {
            // TODO: Implement actual task cancellation
            // For now, just log the cancellation request
            warn!("Task cancellation requested but not yet implemented: job_id={}, task_id={}", 
                  req.job_id, req.task_id);
            
            Ok(Response::new(CancelTaskResponse {
                success: true,
            }))
        } else {
            // Task not found
            Ok(Response::new(CancelTaskResponse {
                success: false,
            }))
        }
    }
}