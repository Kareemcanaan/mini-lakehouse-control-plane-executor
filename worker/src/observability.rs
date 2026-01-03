use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Registry, Opts, HistogramOpts,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, error, warn, debug, span, Level, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Metrics collector for the worker service
pub struct WorkerMetrics {
    // Task execution metrics
    pub task_duration: HistogramVec,
    pub task_failures: CounterVec,
    pub tasks_completed: CounterVec,
    pub tasks_active: GaugeVec,

    // Parquet operation metrics
    pub parquet_read_duration: HistogramVec,
    pub parquet_write_duration: HistogramVec,
    pub parquet_bytes_read: CounterVec,
    pub parquet_bytes_written: CounterVec,
    pub parquet_rows_read: CounterVec,
    pub parquet_rows_written: CounterVec,

    // Object store metrics
    pub object_store_operations: CounterVec,
    pub object_store_latency: HistogramVec,
    pub object_store_bytes: CounterVec,

    // Worker health metrics
    pub heartbeat_sent: Counter,
    pub heartbeat_failures: Counter,
    pub worker_uptime: Gauge,

    registry: Registry,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let task_duration = HistogramVec::new(
            HistogramOpts::new(
                "worker_task_duration_seconds",
                "Duration of task execution in seconds"
            ).buckets(vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0, 300.0]),
            &["task_type", "status"]
        ).unwrap();

        let task_failures = CounterVec::new(
            Opts::new("worker_task_failures_total", "Total number of task failures"),
            &["task_type", "error_type"]
        ).unwrap();

        let tasks_completed = CounterVec::new(
            Opts::new("worker_tasks_completed_total", "Total number of completed tasks"),
            &["task_type"]
        ).unwrap();

        let tasks_active = GaugeVec::new(
            Opts::new("worker_tasks_active", "Number of currently active tasks"),
            &["task_type"]
        ).unwrap();

        let parquet_read_duration = HistogramVec::new(
            HistogramOpts::new(
                "worker_parquet_read_duration_seconds",
                "Duration of Parquet read operations in seconds"
            ).buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
            &["table_name"]
        ).unwrap();

        let parquet_write_duration = HistogramVec::new(
            HistogramOpts::new(
                "worker_parquet_write_duration_seconds",
                "Duration of Parquet write operations in seconds"
            ).buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
            &["table_name"]
        ).unwrap();

        let parquet_bytes_read = CounterVec::new(
            Opts::new("worker_parquet_bytes_read_total", "Total bytes read from Parquet files"),
            &["table_name"]
        ).unwrap();

        let parquet_bytes_written = CounterVec::new(
            Opts::new("worker_parquet_bytes_written_total", "Total bytes written to Parquet files"),
            &["table_name"]
        ).unwrap();

        let parquet_rows_read = CounterVec::new(
            Opts::new("worker_parquet_rows_read_total", "Total rows read from Parquet files"),
            &["table_name"]
        ).unwrap();

        let parquet_rows_written = CounterVec::new(
            Opts::new("worker_parquet_rows_written_total", "Total rows written to Parquet files"),
            &["table_name"]
        ).unwrap();

        let object_store_operations = CounterVec::new(
            Opts::new("worker_object_store_operations_total", "Total object store operations"),
            &["operation_type", "status"]
        ).unwrap();

        let object_store_latency = HistogramVec::new(
            HistogramOpts::new(
                "worker_object_store_latency_seconds",
                "Latency of object store operations in seconds"
            ).buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["operation_type"]
        ).unwrap();

        let object_store_bytes = CounterVec::new(
            Opts::new("worker_object_store_bytes_total", "Total bytes transferred to/from object store"),
            &["operation_type", "direction"]
        ).unwrap();

        let heartbeat_sent = Counter::new(
            "worker_heartbeat_sent_total",
            "Total number of heartbeats sent to coordinator"
        ).unwrap();

        let heartbeat_failures = Counter::new(
            "worker_heartbeat_failures_total",
            "Total number of heartbeat failures"
        ).unwrap();

        let worker_uptime = Gauge::new(
            "worker_uptime_seconds",
            "Worker uptime in seconds"
        ).unwrap();

        // Register all metrics
        registry.register(Box::new(task_duration.clone())).unwrap();
        registry.register(Box::new(task_failures.clone())).unwrap();
        registry.register(Box::new(tasks_completed.clone())).unwrap();
        registry.register(Box::new(tasks_active.clone())).unwrap();
        registry.register(Box::new(parquet_read_duration.clone())).unwrap();
        registry.register(Box::new(parquet_write_duration.clone())).unwrap();
        registry.register(Box::new(parquet_bytes_read.clone())).unwrap();
        registry.register(Box::new(parquet_bytes_written.clone())).unwrap();
        registry.register(Box::new(parquet_rows_read.clone())).unwrap();
        registry.register(Box::new(parquet_rows_written.clone())).unwrap();
        registry.register(Box::new(object_store_operations.clone())).unwrap();
        registry.register(Box::new(object_store_latency.clone())).unwrap();
        registry.register(Box::new(object_store_bytes.clone())).unwrap();
        registry.register(Box::new(heartbeat_sent.clone())).unwrap();
        registry.register(Box::new(heartbeat_failures.clone())).unwrap();
        registry.register(Box::new(worker_uptime.clone())).unwrap();

        Self {
            task_duration,
            task_failures,
            tasks_completed,
            tasks_active,
            parquet_read_duration,
            parquet_write_duration,
            parquet_bytes_read,
            parquet_bytes_written,
            parquet_rows_read,
            parquet_rows_written,
            object_store_operations,
            object_store_latency,
            object_store_bytes,
            heartbeat_sent,
            heartbeat_failures,
            worker_uptime,
            registry,
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    // Task metrics methods
    pub fn record_task_duration(&self, task_type: &str, status: &str, duration: Duration) {
        self.task_duration
            .with_label_values(&[task_type, status])
            .observe(duration.as_secs_f64());
    }

    pub fn record_task_failure(&self, task_type: &str, error_type: &str) {
        self.task_failures
            .with_label_values(&[task_type, error_type])
            .inc();
    }

    pub fn record_task_completed(&self, task_type: &str) {
        self.tasks_completed
            .with_label_values(&[task_type])
            .inc();
    }

    pub fn set_tasks_active(&self, task_type: &str, count: f64) {
        self.tasks_active
            .with_label_values(&[task_type])
            .set(count);
    }

    // Parquet metrics methods
    pub fn record_parquet_read(&self, table_name: &str, duration: Duration, bytes: u64, rows: u64) {
        self.parquet_read_duration
            .with_label_values(&[table_name])
            .observe(duration.as_secs_f64());
        self.parquet_bytes_read
            .with_label_values(&[table_name])
            .inc_by(bytes as f64);
        self.parquet_rows_read
            .with_label_values(&[table_name])
            .inc_by(rows as f64);
    }

    pub fn record_parquet_write(&self, table_name: &str, duration: Duration, bytes: u64, rows: u64) {
        self.parquet_write_duration
            .with_label_values(&[table_name])
            .observe(duration.as_secs_f64());
        self.parquet_bytes_written
            .with_label_values(&[table_name])
            .inc_by(bytes as f64);
        self.parquet_rows_written
            .with_label_values(&[table_name])
            .inc_by(rows as f64);
    }

    // Object store metrics methods
    pub fn record_object_store_operation(&self, operation_type: &str, status: &str, duration: Duration, bytes: u64, direction: &str) {
        self.object_store_operations
            .with_label_values(&[operation_type, status])
            .inc();
        self.object_store_latency
            .with_label_values(&[operation_type])
            .observe(duration.as_secs_f64());
        self.object_store_bytes
            .with_label_values(&[operation_type, direction])
            .inc_by(bytes as f64);
    }

    // Heartbeat metrics methods
    pub fn record_heartbeat_sent(&self) {
        self.heartbeat_sent.inc();
    }

    pub fn record_heartbeat_failure(&self) {
        self.heartbeat_failures.inc();
    }

    pub fn update_uptime(&self, start_time: Instant) {
        self.worker_uptime.set(start_time.elapsed().as_secs_f64());
    }
}

/// Timer for measuring operation duration
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Structured logging utilities for the worker
pub struct WorkerLogger;

impl WorkerLogger {
    pub fn init() {
        tracing_subscriber::registry()
            .with(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "worker=info,tower_http=debug".into()),
            )
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    pub fn task_span(task_id: &str, task_type: &str, worker_id: &str) -> Span {
        span!(
            Level::INFO,
            "task_execution",
            task_id = task_id,
            task_type = task_type,
            worker_id = worker_id,
            otel.name = "task_execution"
        )
    }

    pub fn parquet_span(operation: &str, table_name: &str, file_path: &str) -> Span {
        span!(
            Level::INFO,
            "parquet_operation",
            operation = operation,
            table_name = table_name,
            file_path = file_path,
            otel.name = format!("parquet_{}", operation).as_str()
        )
    }

    pub fn object_store_span(operation: &str, bucket: &str, key: &str) -> Span {
        span!(
            Level::INFO,
            "object_store_operation",
            operation = operation,
            bucket = bucket,
            key = key,
            otel.name = format!("object_store_{}", operation).as_str()
        )
    }

    pub fn log_task_start(task_id: &str, task_type: &str, worker_id: &str) {
        info!(
            task_id = task_id,
            task_type = task_type,
            worker_id = worker_id,
            event = "task_start",
            "Task execution started"
        );
    }

    pub fn log_task_complete(task_id: &str, task_type: &str, worker_id: &str, duration: Duration) {
        info!(
            task_id = task_id,
            task_type = task_type,
            worker_id = worker_id,
            duration_ms = duration.as_millis(),
            event = "task_complete",
            "Task execution completed"
        );
    }

    pub fn log_task_failed(task_id: &str, task_type: &str, worker_id: &str, error: &str, duration: Duration) {
        error!(
            task_id = task_id,
            task_type = task_type,
            worker_id = worker_id,
            error = error,
            duration_ms = duration.as_millis(),
            event = "task_failed",
            "Task execution failed"
        );
    }

    pub fn log_parquet_operation(operation: &str, table_name: &str, file_path: &str, bytes: u64, rows: u64, duration: Duration) {
        info!(
            operation = operation,
            table_name = table_name,
            file_path = file_path,
            bytes = bytes,
            rows = rows,
            duration_ms = duration.as_millis(),
            event = "parquet_operation",
            "Parquet operation completed"
        );
    }

    pub fn log_object_store_operation(operation: &str, bucket: &str, key: &str, bytes: u64, duration: Duration) {
        info!(
            operation = operation,
            bucket = bucket,
            key = key,
            bytes = bytes,
            duration_ms = duration.as_millis(),
            event = "object_store_operation",
            "Object store operation completed"
        );
    }

    pub fn log_heartbeat(worker_id: &str, coordinator_endpoint: &str) {
        debug!(
            worker_id = worker_id,
            coordinator_endpoint = coordinator_endpoint,
            event = "heartbeat_sent",
            "Heartbeat sent to coordinator"
        );
    }

    pub fn log_heartbeat_failure(worker_id: &str, coordinator_endpoint: &str, error: &str) {
        warn!(
            worker_id = worker_id,
            coordinator_endpoint = coordinator_endpoint,
            error = error,
            event = "heartbeat_failed",
            "Failed to send heartbeat to coordinator"
        );
    }
}

/// Get current timestamp in milliseconds since Unix epoch
pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// OpenTelemetry tracing utilities
use opentelemetry::global;

/// Configuration for OpenTelemetry tracing
pub struct TracingConfig {
    pub service_name: String,
    pub service_version: String,
    pub jaeger_endpoint: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "mini-lakehouse-worker".to_string(),
            service_version: "1.0.0".to_string(),
            jaeger_endpoint: "http://localhost:14268/api/traces".to_string(),
        }
    }
}

/// Initialize OpenTelemetry tracing with Jaeger
pub fn init_tracing(config: TracingConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // For now, just initialize basic tracing without Jaeger
    // TODO: Add proper Jaeger integration when dependencies are resolved
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "worker=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    Ok(())
}

/// Shutdown OpenTelemetry tracing
pub fn shutdown_tracing() {
    global::shutdown_tracer_provider();
}

// Common tracing attribute constants
pub const TASK_ID: &str = "lakehouse.task.id";
pub const TASK_TYPE: &str = "lakehouse.task.type";
pub const WORKER_ID: &str = "lakehouse.worker.id";
pub const JOB_ID: &str = "lakehouse.job.id";
pub const STAGE_ID: &str = "lakehouse.stage.id";
pub const TABLE_NAME: &str = "lakehouse.table.name";
pub const FILE_PATH: &str = "lakehouse.file.path";
pub const OPERATION: &str = "lakehouse.operation";
pub const BUCKET: &str = "lakehouse.bucket";
pub const OBJECT_KEY: &str = "lakehouse.object.key";
pub const BYTES_PROCESSED: &str = "lakehouse.bytes.processed";
pub const ROWS_PROCESSED: &str = "lakehouse.rows.processed";
pub const ERROR_TYPE: &str = "lakehouse.error.type";
pub const ERROR_MESSAGE: &str = "lakehouse.error.message";