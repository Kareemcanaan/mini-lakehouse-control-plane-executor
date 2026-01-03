package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// TracingConfig holds configuration for OpenTelemetry tracing
type TracingConfig struct {
	ServiceName    string
	ServiceVersion string
	JaegerEndpoint string
	SamplingRate   float64
}

// InitTracing initializes OpenTelemetry tracing with Jaeger exporter
func InitTracing(config TracingConfig) (func(context.Context) error, error) {
	// Create Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(config.JaegerEndpoint)))
	if err != nil {
		return nil, fmt.Errorf("failed to create Jaeger exporter: %w", err)
	}

	// Create resource with service information
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(config.ServiceName),
			semconv.ServiceVersion(config.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(res),
		trace.WithSampler(trace.TraceIDRatioBased(config.SamplingRate)),
	)

	// Set global trace provider
	otel.SetTracerProvider(tp)

	// Return shutdown function
	return tp.Shutdown, nil
}

// Tracer returns a tracer for the given name
func Tracer(name string) oteltrace.Tracer {
	return otel.Tracer(name)
}

// StartSpan starts a new span with the given name and options
func StartSpan(ctx context.Context, tracer oteltrace.Tracer, name string, opts ...oteltrace.SpanStartOption) (context.Context, oteltrace.Span) {
	return tracer.Start(ctx, name, opts...)
}

// SpanFromContext returns the span from the context
func SpanFromContext(ctx context.Context) oteltrace.Span {
	return oteltrace.SpanFromContext(ctx)
}

// SetSpanAttributes sets attributes on the span from context
func SetSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}

// SetSpanStatus sets the status of the span from context
func SetSpanStatus(ctx context.Context, code codes.Code, description string) {
	span := SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(code, description)
	}
}

// RecordError records an error on the span from context
func RecordError(ctx context.Context, err error) {
	span := SpanFromContext(ctx)
	if span.IsRecording() {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// Common span attribute keys for Mini Lakehouse
const (
	// Query attributes
	AttrQueryID   = "lakehouse.query.id"
	AttrTableName = "lakehouse.table.name"
	AttrVersion   = "lakehouse.version"
	AttrQueryType = "lakehouse.query.type"

	// Task attributes
	AttrTaskID      = "lakehouse.task.id"
	AttrTaskType    = "lakehouse.task.type"
	AttrTaskAttempt = "lakehouse.task.attempt"
	AttrWorkerID    = "lakehouse.worker.id"
	AttrJobID       = "lakehouse.job.id"
	AttrStageID     = "lakehouse.stage.id"

	// Transaction attributes
	AttrTxnID       = "lakehouse.txn.id"
	AttrBaseVersion = "lakehouse.base_version"
	AttrNewVersion  = "lakehouse.new_version"

	// Object store attributes
	AttrBucket     = "lakehouse.bucket"
	AttrObjectPath = "lakehouse.object.path"
	AttrObjectSize = "lakehouse.object.size"
	AttrOperation  = "lakehouse.operation"

	// Raft attributes
	AttrNodeID   = "lakehouse.raft.node_id"
	AttrIsLeader = "lakehouse.raft.is_leader"
	AttrTerm     = "lakehouse.raft.term"
	AttrLogIndex = "lakehouse.raft.log_index"

	// Error attributes
	AttrErrorType = "lakehouse.error.type"
	AttrErrorCode = "lakehouse.error.code"
	AttrErrorMsg  = "lakehouse.error.message"
)

// Helper functions to create common attributes
func QueryAttributes(queryID, tableName, queryType string, version uint64) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrQueryID, queryID),
		attribute.String(AttrTableName, tableName),
		attribute.String(AttrQueryType, queryType),
		attribute.Int64(AttrVersion, int64(version)),
	}
}

func TaskAttributes(taskID, taskType, workerID, jobID string, stageID int, attempt int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrTaskID, taskID),
		attribute.String(AttrTaskType, taskType),
		attribute.String(AttrWorkerID, workerID),
		attribute.String(AttrJobID, jobID),
		attribute.Int(AttrStageID, stageID),
		attribute.Int(AttrTaskAttempt, attempt),
	}
}

func TransactionAttributes(txnID string, baseVersion, newVersion uint64) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrTxnID, txnID),
		attribute.Int64(AttrBaseVersion, int64(baseVersion)),
		attribute.Int64(AttrNewVersion, int64(newVersion)),
	}
}

func ObjectStoreAttributes(bucket, path, operation string, size int64) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrBucket, bucket),
		attribute.String(AttrObjectPath, path),
		attribute.String(AttrOperation, operation),
		attribute.Int64(AttrObjectSize, size),
	}
}

func RaftAttributes(nodeID string, isLeader bool, term, logIndex uint64) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrNodeID, nodeID),
		attribute.Bool(AttrIsLeader, isLeader),
		attribute.Int64(AttrTerm, int64(term)),
		attribute.Int64(AttrLogIndex, int64(logIndex)),
	}
}

func ErrorAttributes(errorType, errorCode, errorMsg string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrErrorType, errorType),
		attribute.String(AttrErrorCode, errorCode),
		attribute.String(AttrErrorMsg, errorMsg),
	}
}
