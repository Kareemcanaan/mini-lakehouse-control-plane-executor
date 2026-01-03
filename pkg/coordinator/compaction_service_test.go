package coordinator

import (
	"context"
	"testing"
	"time"

	"mini-lakehouse/proto/gen"

	"google.golang.org/grpc"
)

// MockMetadataClient for testing compaction functionality
type MockMetadataClientForCompaction struct {
	latestVersion uint64
	files         []*gen.FileInfo
	schema        *gen.Schema
	commitError   string
	newVersion    uint64
}

func (m *MockMetadataClientForCompaction) GetLatestVersion(ctx context.Context, req *gen.GetLatestVersionRequest, opts ...grpc.CallOption) (*gen.GetLatestVersionResponse, error) {
	return &gen.GetLatestVersionResponse{
		Version: m.latestVersion,
	}, nil
}

func (m *MockMetadataClientForCompaction) GetSnapshot(ctx context.Context, req *gen.GetSnapshotRequest, opts ...grpc.CallOption) (*gen.GetSnapshotResponse, error) {
	return &gen.GetSnapshotResponse{
		Files:  m.files,
		Schema: m.schema,
	}, nil
}

func (m *MockMetadataClientForCompaction) Commit(ctx context.Context, req *gen.CommitRequest, opts ...grpc.CallOption) (*gen.CommitResponse, error) {
	if m.commitError != "" {
		return &gen.CommitResponse{
			Error: m.commitError,
		}, nil
	}
	return &gen.CommitResponse{
		NewVersion: m.newVersion,
	}, nil
}

func (m *MockMetadataClientForCompaction) CreateTable(ctx context.Context, req *gen.CreateTableRequest, opts ...grpc.CallOption) (*gen.CreateTableResponse, error) {
	return &gen.CreateTableResponse{Success: true}, nil
}

func (m *MockMetadataClientForCompaction) ListVersions(ctx context.Context, req *gen.ListVersionsRequest, opts ...grpc.CallOption) (*gen.ListVersionsResponse, error) {
	return &gen.ListVersionsResponse{}, nil
}

func (m *MockMetadataClientForCompaction) Leader(ctx context.Context, req *gen.LeaderRequest, opts ...grpc.CallOption) (*gen.LeaderResponse, error) {
	return &gen.LeaderResponse{}, nil
}

func (m *MockMetadataClientForCompaction) Health(ctx context.Context, req *gen.HealthRequest, opts ...grpc.CallOption) (*gen.HealthResponse, error) {
	return &gen.HealthResponse{Healthy: true}, nil
}

func TestCompactionCandidateIdentification(t *testing.T) {
	// Create mock metadata client with small files
	mockClient := &MockMetadataClientForCompaction{
		latestVersion: 5,
		files: []*gen.FileInfo{
			{Path: "data/small1.parquet", Size: 5 * 1024 * 1024, Rows: 1000},   // 5MB - small
			{Path: "data/small2.parquet", Size: 8 * 1024 * 1024, Rows: 1500},   // 8MB - small
			{Path: "data/small3.parquet", Size: 6 * 1024 * 1024, Rows: 1200},   // 6MB - small
			{Path: "data/large1.parquet", Size: 50 * 1024 * 1024, Rows: 10000}, // 50MB - large
			{Path: "data/large2.parquet", Size: 80 * 1024 * 1024, Rows: 15000}, // 80MB - large
		},
		schema: &gen.Schema{
			Fields: []*gen.Field{
				{Name: "id", Type: "int64"},
				{Name: "name", Type: "string"},
			},
		},
	}

	txnManager := NewTransactionManager()
	compactionService := NewCompactionService(mockClient, nil, txnManager, nil)

	ctx := context.Background()
	plan, err := compactionService.IdentifyCompactionCandidates(ctx, "test_table")

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if plan == nil {
		t.Fatal("Expected compaction plan, got nil")
	}

	// Should identify 3 small files as candidates
	if len(plan.InputFiles) != 3 {
		t.Errorf("Expected 3 input files, got %d", len(plan.InputFiles))
	}

	// Check that only small files are included
	for _, file := range plan.InputFiles {
		if file.Size >= 10*1024*1024 { // 10MB threshold
			t.Errorf("Large file %s included in compaction plan", file.Path)
		}
	}

	// Check estimated metrics
	if plan.EstimatedSize <= 0 {
		t.Error("Expected positive estimated size")
	}

	if plan.EstimatedRows <= 0 {
		t.Error("Expected positive estimated rows")
	}
}

func TestCompactionNotNeeded(t *testing.T) {
	// Create mock metadata client with only large files
	mockClient := &MockMetadataClientForCompaction{
		latestVersion: 3,
		files: []*gen.FileInfo{
			{Path: "data/large1.parquet", Size: 50 * 1024 * 1024, Rows: 10000}, // 50MB - large
			{Path: "data/large2.parquet", Size: 80 * 1024 * 1024, Rows: 15000}, // 80MB - large
		},
		schema: &gen.Schema{
			Fields: []*gen.Field{
				{Name: "id", Type: "int64"},
			},
		},
	}

	txnManager := NewTransactionManager()
	compactionService := NewCompactionService(mockClient, nil, txnManager, nil)

	ctx := context.Background()
	plan, err := compactionService.IdentifyCompactionCandidates(ctx, "test_table")

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if plan != nil {
		t.Error("Expected no compaction plan for large files only")
	}
}

func TestCompactionMetrics(t *testing.T) {
	// Create mock metadata client with mixed file sizes
	mockClient := &MockMetadataClientForCompaction{
		latestVersion: 7,
		files: []*gen.FileInfo{
			{Path: "data/small1.parquet", Size: 5 * 1024 * 1024, Rows: 1000},   // 5MB - small
			{Path: "data/small2.parquet", Size: 8 * 1024 * 1024, Rows: 1500},   // 8MB - small
			{Path: "data/large1.parquet", Size: 50 * 1024 * 1024, Rows: 10000}, // 50MB - large
		},
		schema: &gen.Schema{
			Fields: []*gen.Field{
				{Name: "id", Type: "int64"},
			},
		},
	}

	txnManager := NewTransactionManager()
	compactionService := NewCompactionService(mockClient, nil, txnManager, nil)

	ctx := context.Background()
	metrics, err := compactionService.GetCompactionMetrics(ctx, "test_table")

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if metrics.TotalFiles != 3 {
		t.Errorf("Expected 3 total files, got %d", metrics.TotalFiles)
	}

	if metrics.SmallFiles != 2 {
		t.Errorf("Expected 2 small files, got %d", metrics.SmallFiles)
	}

	expectedTotalSize := int64(63 * 1024 * 1024) // 5+8+50 MB
	if metrics.TotalSize != expectedTotalSize {
		t.Errorf("Expected total size %d, got %d", expectedTotalSize, metrics.TotalSize)
	}

	expectedSmallSize := int64(13 * 1024 * 1024) // 5+8 MB
	if metrics.SmallFilesSize != expectedSmallSize {
		t.Errorf("Expected small files size %d, got %d", expectedSmallSize, metrics.SmallFilesSize)
	}

	// Should not need compaction (only 2 small files, minimum is 3)
	if metrics.CompactionNeeded {
		t.Error("Expected compaction not needed with only 2 small files")
	}
}

func TestCompactionExecution(t *testing.T) {
	// Create mock metadata client
	mockClient := &MockMetadataClientForCompaction{
		latestVersion: 5,
		newVersion:    6,
		files: []*gen.FileInfo{
			{Path: "data/small1.parquet", Size: 5 * 1024 * 1024, Rows: 1000},
			{Path: "data/small2.parquet", Size: 8 * 1024 * 1024, Rows: 1500},
			{Path: "data/small3.parquet", Size: 6 * 1024 * 1024, Rows: 1200},
		},
		schema: &gen.Schema{
			Fields: []*gen.Field{
				{Name: "id", Type: "int64"},
			},
		},
	}

	txnManager := NewTransactionManager()
	compactionService := NewCompactionService(mockClient, nil, txnManager, nil)

	// Create a compaction plan
	plan := &CompactionPlan{
		TableName:   "test_table",
		BaseVersion: 5,
		InputFiles: []CompactionCandidate{
			{Path: "data/small1.parquet", Size: 5 * 1024 * 1024, Rows: 1000},
			{Path: "data/small2.parquet", Size: 8 * 1024 * 1024, Rows: 1500},
			{Path: "data/small3.parquet", Size: 6 * 1024 * 1024, Rows: 1200},
		},
		EstimatedSize: 17 * 1024 * 1024, // Slightly less due to compression
		EstimatedRows: 3700,
	}

	ctx := context.Background()
	result, err := compactionService.ExecuteCompaction(ctx, plan)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful compaction, got error: %s", result.Error)
	}

	if result.NewVersion != 6 {
		t.Errorf("Expected new version 6, got %d", result.NewVersion)
	}

	if result.InputFiles != 3 {
		t.Errorf("Expected 3 input files, got %d", result.InputFiles)
	}

	if len(result.OutputFiles) != 1 {
		t.Errorf("Expected 1 output file, got %d", len(result.OutputFiles))
	}

	if result.BytesRead <= 0 {
		t.Error("Expected positive bytes read")
	}

	if result.BytesWritten <= 0 {
		t.Error("Expected positive bytes written")
	}

	if result.Duration <= 0 {
		t.Error("Expected positive duration")
	}
}

func TestCompactionCoordinatorSafety(t *testing.T) {
	// Create mock metadata client
	mockClient := &MockMetadataClientForCompaction{
		latestVersion: 5,
		newVersion:    6,
		files: []*gen.FileInfo{
			{Path: "data/small1.parquet", Size: 5 * 1024 * 1024, Rows: 1000},
			{Path: "data/small2.parquet", Size: 8 * 1024 * 1024, Rows: 1500},
			{Path: "data/small3.parquet", Size: 6 * 1024 * 1024, Rows: 1200},
		},
		schema: &gen.Schema{
			Fields: []*gen.Field{
				{Name: "id", Type: "int64"},
			},
		},
	}

	txnManager := NewTransactionManager()
	compactionService := NewCompactionService(mockClient, nil, txnManager, nil)
	coordinator := NewCompactionCoordinator(compactionService)

	ctx := context.Background()

	// Test concurrent compaction prevention
	go func() {
		coordinator.SafeExecuteCompaction(ctx, "test_table")
	}()

	// Give first compaction time to start
	time.Sleep(10 * time.Millisecond)

	// Second compaction should be rejected
	result, err := coordinator.SafeExecuteCompaction(ctx, "test_table")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if result.Success {
		t.Error("Expected second compaction to be rejected")
	}

	if !contains(result.Error, "already running") {
		t.Errorf("Expected 'already running' error, got: %s", result.Error)
	}
}

func TestCompactionVersionConflict(t *testing.T) {
	// Create mock metadata client that simulates version conflict
	mockClient := &MockMetadataClientForCompaction{
		latestVersion: 5,
		commitError:   "optimistic concurrency failure: base version 5 does not match current latest version 6",
		files: []*gen.FileInfo{
			{Path: "data/small1.parquet", Size: 5 * 1024 * 1024, Rows: 1000},
			{Path: "data/small2.parquet", Size: 8 * 1024 * 1024, Rows: 1500},
			{Path: "data/small3.parquet", Size: 6 * 1024 * 1024, Rows: 1200},
		},
		schema: &gen.Schema{
			Fields: []*gen.Field{
				{Name: "id", Type: "int64"},
			},
		},
	}

	txnManager := NewTransactionManager()
	compactionService := NewCompactionService(mockClient, nil, txnManager, nil)

	// Create a compaction plan
	plan := &CompactionPlan{
		TableName:   "test_table",
		BaseVersion: 5,
		InputFiles: []CompactionCandidate{
			{Path: "data/small1.parquet", Size: 5 * 1024 * 1024, Rows: 1000},
			{Path: "data/small2.parquet", Size: 8 * 1024 * 1024, Rows: 1500},
			{Path: "data/small3.parquet", Size: 6 * 1024 * 1024, Rows: 1200},
		},
	}

	ctx := context.Background()
	result, err := compactionService.ExecuteCompaction(ctx, plan)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if result.Success {
		t.Error("Expected compaction to fail due to version conflict")
	}

	if !contains(result.Error, "commit") {
		t.Errorf("Expected commit error, got: %s", result.Error)
	}
}
