package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the Mini Lakehouse system
type Metrics struct {
	// Task metrics
	TaskDuration   *prometheus.HistogramVec
	TaskFailures   *prometheus.CounterVec
	TaskRetries    *prometheus.CounterVec
	TasksCompleted *prometheus.CounterVec
	TasksActive    *prometheus.GaugeVec

	// Query metrics
	QueryDuration *prometheus.HistogramVec
	QueriesTotal  *prometheus.CounterVec
	QueryFailures *prometheus.CounterVec

	// Commit metrics
	CommitDuration *prometheus.HistogramVec
	CommitsTotal   *prometheus.CounterVec
	CommitFailures *prometheus.CounterVec

	// Raft metrics
	RaftLeaderStatus    *prometheus.GaugeVec
	RaftApplyLatency    *prometheus.HistogramVec
	RaftLeaderElections *prometheus.CounterVec

	// Worker metrics
	WorkerHeartbeat *prometheus.GaugeVec
	WorkersActive   prometheus.Gauge

	// Object store metrics
	ObjectStoreBytesRead    *prometheus.CounterVec
	ObjectStoreBytesWritten *prometheus.CounterVec
	ObjectStoreOperations   *prometheus.CounterVec
	ObjectStoreLatency      *prometheus.HistogramVec

	// Compaction metrics
	CompactionDuration     *prometheus.HistogramVec
	CompactionsTotal       *prometheus.CounterVec
	CompactionBytesRead    *prometheus.CounterVec
	CompactionBytesWritten *prometheus.CounterVec
	CompactionFilesInput   *prometheus.HistogramVec
	CompactionFilesOutput  *prometheus.HistogramVec

	registry *prometheus.Registry
}

// NewMetrics creates and registers all Prometheus metrics
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		// Task metrics
		TaskDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_task_duration_seconds",
				Help:    "Duration of task execution in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"task_type", "worker_id", "status"},
		),
		TaskFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_task_failures_total",
				Help: "Total number of task failures",
			},
			[]string{"task_type", "worker_id", "error_type"},
		),
		TaskRetries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_task_retries_total",
				Help: "Total number of task retries",
			},
			[]string{"task_type", "reason"},
		),
		TasksCompleted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_tasks_completed_total",
				Help: "Total number of completed tasks",
			},
			[]string{"task_type", "worker_id"},
		),
		TasksActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lakehouse_tasks_active",
				Help: "Number of currently active tasks",
			},
			[]string{"task_type", "worker_id"},
		),

		// Query metrics
		QueryDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_query_duration_seconds",
				Help:    "Duration of query execution in seconds",
				Buckets: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0, 300.0},
			},
			[]string{"table_name", "query_type", "status"},
		),
		QueriesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_queries_total",
				Help: "Total number of queries executed",
			},
			[]string{"table_name", "query_type"},
		),
		QueryFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_query_failures_total",
				Help: "Total number of query failures",
			},
			[]string{"table_name", "query_type", "error_type"},
		),

		// Commit metrics
		CommitDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_commit_duration_seconds",
				Help:    "Duration of commit operations in seconds",
				Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
			},
			[]string{"table_name", "operation_type"},
		),
		CommitsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_commits_total",
				Help: "Total number of commits",
			},
			[]string{"table_name", "operation_type"},
		),
		CommitFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_commit_failures_total",
				Help: "Total number of commit failures",
			},
			[]string{"table_name", "operation_type", "error_type"},
		),

		// Raft metrics
		RaftLeaderStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lakehouse_metadata_leader_status",
				Help: "Current Raft leader status (1 if leader, 0 if follower)",
			},
			[]string{"node_id"},
		),
		RaftApplyLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_raft_apply_latency_seconds",
				Help:    "Latency of Raft log apply operations in seconds",
				Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
			},
			[]string{"node_id"},
		),
		RaftLeaderElections: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_raft_leader_elections_total",
				Help: "Total number of Raft leader elections",
			},
			[]string{"node_id"},
		),

		// Worker metrics
		WorkerHeartbeat: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lakehouse_worker_heartbeat_last_seen",
				Help: "Timestamp of last worker heartbeat",
			},
			[]string{"worker_id"},
		),
		WorkersActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "lakehouse_workers_active",
				Help: "Number of active workers",
			},
		),

		// Object store metrics
		ObjectStoreBytesRead: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_object_store_bytes_read_total",
				Help: "Total bytes read from object store",
			},
			[]string{"operation_type", "bucket"},
		),
		ObjectStoreBytesWritten: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_object_store_bytes_written_total",
				Help: "Total bytes written to object store",
			},
			[]string{"operation_type", "bucket"},
		),
		ObjectStoreOperations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_object_store_operations_total",
				Help: "Total object store operations",
			},
			[]string{"operation_type", "bucket", "status"},
		),
		ObjectStoreLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_object_store_latency_seconds",
				Help:    "Latency of object store operations in seconds",
				Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			},
			[]string{"operation_type", "bucket"},
		),

		// Compaction metrics
		CompactionDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_compaction_duration_seconds",
				Help:    "Duration of compaction operations in seconds",
				Buckets: []float64{1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0},
			},
			[]string{"table_name", "status"},
		),
		CompactionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_compactions_total",
				Help: "Total number of compaction operations",
			},
			[]string{"table_name", "status"},
		),
		CompactionBytesRead: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_compaction_bytes_read_total",
				Help: "Total bytes read during compaction",
			},
			[]string{"table_name"},
		),
		CompactionBytesWritten: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lakehouse_compaction_bytes_written_total",
				Help: "Total bytes written during compaction",
			},
			[]string{"table_name"},
		),
		CompactionFilesInput: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_compaction_files_input",
				Help:    "Number of input files in compaction operations",
				Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200},
			},
			[]string{"table_name"},
		),
		CompactionFilesOutput: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lakehouse_compaction_files_output",
				Help:    "Number of output files in compaction operations",
				Buckets: []float64{1, 2, 5, 10, 20, 50, 100},
			},
			[]string{"table_name"},
		),

		registry: registry,
	}

	// Register all metrics
	registry.MustRegister(
		m.TaskDuration,
		m.TaskFailures,
		m.TaskRetries,
		m.TasksCompleted,
		m.TasksActive,
		m.QueryDuration,
		m.QueriesTotal,
		m.QueryFailures,
		m.CommitDuration,
		m.CommitsTotal,
		m.CommitFailures,
		m.RaftLeaderStatus,
		m.RaftApplyLatency,
		m.RaftLeaderElections,
		m.WorkerHeartbeat,
		m.WorkersActive,
		m.ObjectStoreBytesRead,
		m.ObjectStoreBytesWritten,
		m.ObjectStoreOperations,
		m.ObjectStoreLatency,
		m.CompactionDuration,
		m.CompactionsTotal,
		m.CompactionBytesRead,
		m.CompactionBytesWritten,
		m.CompactionFilesInput,
		m.CompactionFilesOutput,
	)

	return m
}

// Handler returns the HTTP handler for Prometheus metrics
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RecordTaskDuration records the duration of a task execution
func (m *Metrics) RecordTaskDuration(taskType, workerID, status string, duration time.Duration) {
	m.TaskDuration.WithLabelValues(taskType, workerID, status).Observe(duration.Seconds())
}

// RecordTaskFailure records a task failure
func (m *Metrics) RecordTaskFailure(taskType, workerID, errorType string) {
	m.TaskFailures.WithLabelValues(taskType, workerID, errorType).Inc()
}

// RecordTaskRetry records a task retry
func (m *Metrics) RecordTaskRetry(taskType, reason string) {
	m.TaskRetries.WithLabelValues(taskType, reason).Inc()
}

// RecordTaskCompleted records a completed task
func (m *Metrics) RecordTaskCompleted(taskType, workerID string) {
	m.TasksCompleted.WithLabelValues(taskType, workerID).Inc()
}

// SetTasksActive sets the number of active tasks
func (m *Metrics) SetTasksActive(taskType, workerID string, count float64) {
	m.TasksActive.WithLabelValues(taskType, workerID).Set(count)
}

// RecordQueryDuration records the duration of a query execution
func (m *Metrics) RecordQueryDuration(tableName, queryType, status string, duration time.Duration) {
	m.QueryDuration.WithLabelValues(tableName, queryType, status).Observe(duration.Seconds())
}

// RecordQuery records a query execution
func (m *Metrics) RecordQuery(tableName, queryType string) {
	m.QueriesTotal.WithLabelValues(tableName, queryType).Inc()
}

// RecordQueryFailure records a query failure
func (m *Metrics) RecordQueryFailure(tableName, queryType, errorType string) {
	m.QueryFailures.WithLabelValues(tableName, queryType, errorType).Inc()
}

// RecordCommitDuration records the duration of a commit operation
func (m *Metrics) RecordCommitDuration(tableName, operationType string, duration time.Duration) {
	m.CommitDuration.WithLabelValues(tableName, operationType).Observe(duration.Seconds())
}

// RecordCommit records a commit operation
func (m *Metrics) RecordCommit(tableName, operationType string) {
	m.CommitsTotal.WithLabelValues(tableName, operationType).Inc()
}

// RecordCommitFailure records a commit failure
func (m *Metrics) RecordCommitFailure(tableName, operationType, errorType string) {
	m.CommitFailures.WithLabelValues(tableName, operationType, errorType).Inc()
}

// SetRaftLeaderStatus sets the Raft leader status
func (m *Metrics) SetRaftLeaderStatus(nodeID string, isLeader bool) {
	value := 0.0
	if isLeader {
		value = 1.0
	}
	m.RaftLeaderStatus.WithLabelValues(nodeID).Set(value)
}

// RecordRaftApplyLatency records Raft apply latency
func (m *Metrics) RecordRaftApplyLatency(nodeID string, duration time.Duration) {
	m.RaftApplyLatency.WithLabelValues(nodeID).Observe(duration.Seconds())
}

// RecordRaftLeaderElection records a Raft leader election
func (m *Metrics) RecordRaftLeaderElection(nodeID string) {
	m.RaftLeaderElections.WithLabelValues(nodeID).Inc()
}

// SetWorkerHeartbeat sets the last seen timestamp for a worker
func (m *Metrics) SetWorkerHeartbeat(workerID string, timestamp float64) {
	m.WorkerHeartbeat.WithLabelValues(workerID).Set(timestamp)
}

// SetWorkersActive sets the number of active workers
func (m *Metrics) SetWorkersActive(count float64) {
	m.WorkersActive.Set(count)
}

// RecordObjectStoreBytes records bytes read/written from object store
func (m *Metrics) RecordObjectStoreBytesRead(operationType, bucket string, bytes int64) {
	m.ObjectStoreBytesRead.WithLabelValues(operationType, bucket).Add(float64(bytes))
}

func (m *Metrics) RecordObjectStoreBytesWritten(operationType, bucket string, bytes int64) {
	m.ObjectStoreBytesWritten.WithLabelValues(operationType, bucket).Add(float64(bytes))
}

// RecordObjectStoreOperation records an object store operation
func (m *Metrics) RecordObjectStoreOperation(operationType, bucket, status string) {
	m.ObjectStoreOperations.WithLabelValues(operationType, bucket, status).Inc()
}

// RecordObjectStoreLatency records object store operation latency
func (m *Metrics) RecordObjectStoreLatency(operationType, bucket string, duration time.Duration) {
	m.ObjectStoreLatency.WithLabelValues(operationType, bucket).Observe(duration.Seconds())
}

// RecordCompactionDuration records compaction duration
func (m *Metrics) RecordCompactionDuration(tableName, status string, duration time.Duration) {
	m.CompactionDuration.WithLabelValues(tableName, status).Observe(duration.Seconds())
}

// RecordCompaction records a compaction operation
func (m *Metrics) RecordCompaction(tableName, status string) {
	m.CompactionsTotal.WithLabelValues(tableName, status).Inc()
}

// RecordCompactionBytes records bytes processed during compaction
func (m *Metrics) RecordCompactionBytesRead(tableName string, bytes int64) {
	m.CompactionBytesRead.WithLabelValues(tableName).Add(float64(bytes))
}

func (m *Metrics) RecordCompactionBytesWritten(tableName string, bytes int64) {
	m.CompactionBytesWritten.WithLabelValues(tableName).Add(float64(bytes))
}

// RecordCompactionFiles records file counts during compaction
func (m *Metrics) RecordCompactionFilesInput(tableName string, count int) {
	m.CompactionFilesInput.WithLabelValues(tableName).Observe(float64(count))
}

func (m *Metrics) RecordCompactionFilesOutput(tableName string, count int) {
	m.CompactionFilesOutput.WithLabelValues(tableName).Observe(float64(count))
}
