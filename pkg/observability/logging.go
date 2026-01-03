package observability

import (
	"context"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger wraps zap.Logger with structured logging capabilities
type Logger struct {
	*zap.Logger
}

// LoggingConfig holds configuration for structured logging
type LoggingConfig struct {
	Level       string // debug, info, warn, error
	Development bool   // enables development mode with console encoder
	OutputPaths []string
}

// NewLogger creates a new structured logger
func NewLogger(config LoggingConfig) (*Logger, error) {
	var zapConfig zap.Config

	if config.Development {
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		zapConfig = zap.NewProductionConfig()
		zapConfig.EncoderConfig.TimeKey = "timestamp"
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	// Set log level
	level, err := zapcore.ParseLevel(config.Level)
	if err != nil {
		return nil, err
	}
	zapConfig.Level = zap.NewAtomicLevelAt(level)

	// Set output paths
	if len(config.OutputPaths) > 0 {
		zapConfig.OutputPaths = config.OutputPaths
	}

	// Add common fields
	zapConfig.InitialFields = map[string]interface{}{
		"service": "mini-lakehouse",
	}

	logger, err := zapConfig.Build()
	if err != nil {
		return nil, err
	}

	return &Logger{Logger: logger}, nil
}

// WithContext adds trace information to the logger if available
func (l *Logger) WithContext(ctx context.Context) *Logger {
	span := oteltrace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return l
	}

	spanCtx := span.SpanContext()
	return &Logger{
		Logger: l.Logger.With(
			zap.String("trace_id", spanCtx.TraceID().String()),
			zap.String("span_id", spanCtx.SpanID().String()),
		),
	}
}

// WithJobID adds job ID to the logger
func (l *Logger) WithJobID(jobID string) *Logger {
	return &Logger{Logger: l.Logger.With(zap.String("job_id", jobID))}
}

// WithTaskID adds task ID to the logger
func (l *Logger) WithTaskID(taskID string) *Logger {
	return &Logger{Logger: l.Logger.With(zap.String("task_id", taskID))}
}

// WithWorkerID adds worker ID to the logger
func (l *Logger) WithWorkerID(workerID string) *Logger {
	return &Logger{Logger: l.Logger.With(zap.String("worker_id", workerID))}
}

// WithTableName adds table name to the logger
func (l *Logger) WithTableName(tableName string) *Logger {
	return &Logger{Logger: l.Logger.With(zap.String("table_name", tableName))}
}

// WithTxnID adds transaction ID to the logger
func (l *Logger) WithTxnID(txnID string) *Logger {
	return &Logger{Logger: l.Logger.With(zap.String("txn_id", txnID))}
}

// WithVersion adds version to the logger
func (l *Logger) WithVersion(version uint64) *Logger {
	return &Logger{Logger: l.Logger.With(zap.Uint64("version", version))}
}

// WithError adds error information to the logger
func (l *Logger) WithError(err error) *Logger {
	return &Logger{Logger: l.Logger.With(zap.Error(err))}
}

// WithErrorCode adds error code to the logger
func (l *Logger) WithErrorCode(code string) *Logger {
	return &Logger{Logger: l.Logger.With(zap.String("error_code", code))}
}

// WithDuration adds duration to the logger
func (l *Logger) WithDuration(duration time.Duration) *Logger {
	return &Logger{Logger: l.Logger.With(zap.Duration("duration", duration))}
}

// WithObjectStore adds object store operation fields
func (l *Logger) WithObjectStore(operation, bucket, path string, size int64) *Logger {
	return &Logger{
		Logger: l.Logger.With(
			zap.String("operation", operation),
			zap.String("bucket", bucket),
			zap.String("object_path", path),
			zap.Int64("object_size", size),
		),
	}
}

// WithRaft adds Raft-specific fields
func (l *Logger) WithRaft(nodeID string, isLeader bool, term, logIndex uint64) *Logger {
	return &Logger{
		Logger: l.Logger.With(
			zap.String("raft_node_id", nodeID),
			zap.Bool("raft_is_leader", isLeader),
			zap.Uint64("raft_term", term),
			zap.Uint64("raft_log_index", logIndex),
		),
	}
}

// LogTaskStart logs the start of a task
func (l *Logger) LogTaskStart(taskID, taskType, workerID string) {
	l.WithTaskID(taskID).WithWorkerID(workerID).Info("Task started",
		zap.String("task_type", taskType),
		zap.String("event", "task_start"),
	)
}

// LogTaskComplete logs the completion of a task
func (l *Logger) LogTaskComplete(taskID, taskType, workerID string, duration time.Duration) {
	l.WithTaskID(taskID).WithWorkerID(workerID).WithDuration(duration).Info("Task completed",
		zap.String("task_type", taskType),
		zap.String("event", "task_complete"),
	)
}

// LogTaskFailed logs a task failure
func (l *Logger) LogTaskFailed(taskID, taskType, workerID string, err error, duration time.Duration) {
	l.WithTaskID(taskID).WithWorkerID(workerID).WithError(err).WithDuration(duration).Error("Task failed",
		zap.String("task_type", taskType),
		zap.String("event", "task_failed"),
	)
}

// LogQueryStart logs the start of a query
func (l *Logger) LogQueryStart(jobID, tableName, queryType string) {
	l.WithJobID(jobID).WithTableName(tableName).Info("Query started",
		zap.String("query_type", queryType),
		zap.String("event", "query_start"),
	)
}

// LogQueryComplete logs the completion of a query
func (l *Logger) LogQueryComplete(jobID, tableName, queryType string, duration time.Duration) {
	l.WithJobID(jobID).WithTableName(tableName).WithDuration(duration).Info("Query completed",
		zap.String("query_type", queryType),
		zap.String("event", "query_complete"),
	)
}

// LogQueryFailed logs a query failure
func (l *Logger) LogQueryFailed(jobID, tableName, queryType string, err error, duration time.Duration) {
	l.WithJobID(jobID).WithTableName(tableName).WithError(err).WithDuration(duration).Error("Query failed",
		zap.String("query_type", queryType),
		zap.String("event", "query_failed"),
	)
}

// LogCommitStart logs the start of a commit
func (l *Logger) LogCommitStart(txnID, tableName string, baseVersion uint64) {
	l.WithTxnID(txnID).WithTableName(tableName).WithVersion(baseVersion).Info("Commit started",
		zap.String("event", "commit_start"),
	)
}

// LogCommitComplete logs the completion of a commit
func (l *Logger) LogCommitComplete(txnID, tableName string, baseVersion, newVersion uint64, duration time.Duration) {
	l.WithTxnID(txnID).WithTableName(tableName).WithDuration(duration).Info("Commit completed",
		zap.Uint64("base_version", baseVersion),
		zap.Uint64("new_version", newVersion),
		zap.String("event", "commit_complete"),
	)
}

// LogCommitFailed logs a commit failure
func (l *Logger) LogCommitFailed(txnID, tableName string, baseVersion uint64, err error, duration time.Duration) {
	l.WithTxnID(txnID).WithTableName(tableName).WithVersion(baseVersion).WithError(err).WithDuration(duration).Error("Commit failed",
		zap.String("event", "commit_failed"),
	)
}

// LogObjectStoreOperation logs an object store operation
func (l *Logger) LogObjectStoreOperation(operation, bucket, path string, size int64, duration time.Duration, err error) {
	logger := l.WithObjectStore(operation, bucket, path, size).WithDuration(duration)

	if err != nil {
		logger.WithError(err).Error("Object store operation failed",
			zap.String("event", "object_store_failed"),
		)
	} else {
		logger.Info("Object store operation completed",
			zap.String("event", "object_store_complete"),
		)
	}
}

// LogRaftEvent logs a Raft-related event
func (l *Logger) LogRaftEvent(event, nodeID string, isLeader bool, term, logIndex uint64) {
	l.WithRaft(nodeID, isLeader, term, logIndex).Info("Raft event",
		zap.String("event", event),
	)
}

// LogWorkerHeartbeat logs a worker heartbeat
func (l *Logger) LogWorkerHeartbeat(workerID string, lastSeen time.Time) {
	l.WithWorkerID(workerID).Debug("Worker heartbeat",
		zap.Time("last_seen", lastSeen),
		zap.String("event", "worker_heartbeat"),
	)
}

// LogCompactionStart logs the start of compaction
func (l *Logger) LogCompactionStart(txnID, tableName string, inputFiles int) {
	l.WithTxnID(txnID).WithTableName(tableName).Info("Compaction started",
		zap.Int("input_files", inputFiles),
		zap.String("event", "compaction_start"),
	)
}

// LogCompactionComplete logs the completion of compaction
func (l *Logger) LogCompactionComplete(txnID, tableName string, inputFiles, outputFiles int, bytesRead, bytesWritten int64, duration time.Duration) {
	l.WithTxnID(txnID).WithTableName(tableName).WithDuration(duration).Info("Compaction completed",
		zap.Int("input_files", inputFiles),
		zap.Int("output_files", outputFiles),
		zap.Int64("bytes_read", bytesRead),
		zap.Int64("bytes_written", bytesWritten),
		zap.String("event", "compaction_complete"),
	)
}

// LogCompactionFailed logs a compaction failure
func (l *Logger) LogCompactionFailed(txnID, tableName string, err error, duration time.Duration) {
	l.WithTxnID(txnID).WithTableName(tableName).WithError(err).WithDuration(duration).Error("Compaction failed",
		zap.String("event", "compaction_failed"),
	)
}
