package coordinator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "mini-lakehouse/proto/gen"

	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"google.golang.org/grpc"
)

// **Feature: mini-lakehouse, Property 8: Snapshot isolation**
// *For any* version number, reading the table at that version should return deterministic results regardless of concurrent operations
// **Validates: Requirements 2.4**
func TestProperty8SnapshotIsolation(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("snapshot reads return deterministic results regardless of concurrent operations", prop.ForAll(
		func(targetVersion uint64, numConcurrentReads int, numConcurrentWrites int, seed int64) bool {
			// Ensure reasonable bounds
			if targetVersion == 0 {
				targetVersion = 1
			}
			if targetVersion > 10 {
				targetVersion = 10
			}
			if numConcurrentReads < 2 {
				numConcurrentReads = 2
			}
			if numConcurrentReads > 10 {
				numConcurrentReads = 10
			}
			if numConcurrentWrites < 0 {
				numConcurrentWrites = 0
			}
			if numConcurrentWrites > 5 {
				numConcurrentWrites = 5
			}

			// Create mock metadata client with deterministic responses
			mockClient := NewMockMetadataClient(seed)
			tableName := fmt.Sprintf("test_table_%d", seed)

			// Setup table with versions up to targetVersion
			mockClient.SetupTableWithVersions(tableName, targetVersion)

			// Create snapshot isolation manager
			sim := NewSnapshotIsolationManager(mockClient)

			// Channel to collect all read results
			readResults := make(chan *SnapshotReadResponse, numConcurrentReads)
			writeResults := make(chan error, numConcurrentWrites)

			var wg sync.WaitGroup

			// Start concurrent reads of the same version
			for i := 0; i < numConcurrentReads; i++ {
				wg.Add(1)
				go func(readerID int) {
					defer wg.Done()

					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					// Add some jitter to make timing more realistic
					time.Sleep(time.Duration(readerID*10) * time.Millisecond)

					result, err := sim.GetSnapshot(ctx, &SnapshotReadRequest{
						TableName: tableName,
						Version:   targetVersion,
					})

					if err != nil {
						t.Logf("Reader %d failed: %v", readerID, err)
						return
					}

					readResults <- result
				}(i)
			}

			// Start concurrent writes to create newer versions (should not affect reads of targetVersion)
			for i := 0; i < numConcurrentWrites; i++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()

					// Add some jitter
					time.Sleep(time.Duration(writerID*15) * time.Millisecond)

					// Simulate concurrent writes by updating the mock client's state
					newVersion := targetVersion + uint64(writerID) + 1
					err := mockClient.AddNewVersion(tableName, newVersion, fmt.Sprintf("concurrent_write_%d", writerID))
					writeResults <- err
				}(i)
			}

			// Wait for all operations to complete
			wg.Wait()
			close(readResults)
			close(writeResults)

			// Collect all read results
			var snapshots []*SnapshotReadResponse
			for result := range readResults {
				snapshots = append(snapshots, result)
			}

			// Verify we got the expected number of results
			if len(snapshots) != numConcurrentReads {
				t.Logf("Expected %d read results, got %d", numConcurrentReads, len(snapshots))
				return false
			}

			// Verify all snapshots are identical (deterministic)
			if len(snapshots) == 0 {
				t.Logf("No snapshots received")
				return false
			}

			firstSnapshot := snapshots[0]
			for i, snapshot := range snapshots[1:] {
				if !snapshotsEqual(firstSnapshot, snapshot) {
					t.Logf("Snapshot %d differs from first snapshot", i+1)
					t.Logf("First: version=%d, files=%d", firstSnapshot.Version, len(firstSnapshot.Files))
					t.Logf("Other: version=%d, files=%d", snapshot.Version, len(snapshot.Files))
					return false
				}
			}

			// Verify the snapshot version matches what was requested
			if firstSnapshot.Version != targetVersion {
				t.Logf("Expected version %d, got %d", targetVersion, firstSnapshot.Version)
				return false
			}

			// Verify the snapshot contains expected data for the version
			expectedFiles := mockClient.GetExpectedFilesForVersion(tableName, targetVersion)
			if len(firstSnapshot.Files) != len(expectedFiles) {
				t.Logf("Expected %d files for version %d, got %d", len(expectedFiles), targetVersion, len(firstSnapshot.Files))
				return false
			}

			// Verify file contents match expected
			for _, expectedFile := range expectedFiles {
				found := false
				for _, actualFile := range firstSnapshot.Files {
					if actualFile.Path == expectedFile.Path {
						if actualFile.Size != expectedFile.Size || actualFile.Rows != expectedFile.Rows {
							t.Logf("File %s metadata mismatch: expected size=%d rows=%d, got size=%d rows=%d",
								expectedFile.Path, expectedFile.Size, expectedFile.Rows, actualFile.Size, actualFile.Rows)
							return false
						}
						found = true
						break
					}
				}
				if !found {
					t.Logf("Expected file %s not found in snapshot", expectedFile.Path)
					return false
				}
			}

			return true
		},
		goptergen.UInt64Range(1, 10),   // targetVersion
		goptergen.IntRange(2, 10),      // numConcurrentReads
		goptergen.IntRange(0, 5),       // numConcurrentWrites
		goptergen.Int64Range(1, 10000), // seed
	))

	properties.TestingRun(t)
}

// snapshotsEqual compares two snapshots for equality
func snapshotsEqual(a, b *SnapshotReadResponse) bool {
	if a.TableName != b.TableName || a.Version != b.Version {
		return false
	}

	if len(a.Files) != len(b.Files) {
		return false
	}

	// Create maps for comparison
	aFiles := make(map[string]*pb.FileInfo)
	bFiles := make(map[string]*pb.FileInfo)

	for _, file := range a.Files {
		aFiles[file.Path] = file
	}

	for _, file := range b.Files {
		bFiles[file.Path] = file
	}

	// Compare files
	for path, aFile := range aFiles {
		bFile, exists := bFiles[path]
		if !exists {
			return false
		}

		if aFile.Size != bFile.Size || aFile.Rows != bFile.Rows {
			return false
		}
	}

	return true
}

// MockMetadataClient is a test implementation of MetadataClient for property testing
type MockMetadataClient struct {
	mu     sync.RWMutex
	tables map[string]*MockTable
	seed   int64
}

type MockTable struct {
	Name          string
	LatestVersion uint64
	Versions      map[uint64]*MockVersion
	Schema        *pb.Schema
	ConcurrentOps int // Track concurrent operations for testing
}

type MockVersion struct {
	Version uint64
	Files   []*pb.FileInfo
	TxnID   string
}

func NewMockMetadataClient(seed int64) *MockMetadataClient {
	return &MockMetadataClient{
		tables: make(map[string]*MockTable),
		seed:   seed,
	}
}

// SetupTableWithVersions creates a table with versions from 1 to maxVersion
func (m *MockMetadataClient) SetupTableWithVersions(tableName string, maxVersion uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema := &pb.Schema{
		Fields: []*pb.Field{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "value", Type: "string", Nullable: true},
		},
	}

	table := &MockTable{
		Name:          tableName,
		LatestVersion: maxVersion,
		Versions:      make(map[uint64]*MockVersion),
		Schema:        schema,
	}

	// Create versions 1 through maxVersion
	for v := uint64(1); v <= maxVersion; v++ {
		files := make([]*pb.FileInfo, int(v)) // Each version has 'v' number of files
		for i := 0; i < int(v); i++ {
			files[i] = &pb.FileInfo{
				Path: fmt.Sprintf("data/part-%d-%d-%d.parquet", m.seed, v, i),
				Size: uint64(1000 + int(v)*100 + i*10), // Deterministic size
				Rows: uint64(100 + int(v)*10 + i),      // Deterministic row count
				Partition: map[string]string{
					"version": fmt.Sprintf("%d", v),
					"part":    fmt.Sprintf("%d", i),
				},
				Stats: &pb.FileStats{
					MinValues: map[string]string{"id": fmt.Sprintf("%d", i*100)},
					MaxValues: map[string]string{"id": fmt.Sprintf("%d", (i+1)*100-1)},
				},
			}
		}

		table.Versions[v] = &MockVersion{
			Version: v,
			Files:   files,
			TxnID:   fmt.Sprintf("txn_%d_%d", m.seed, v),
		}
	}

	m.tables[tableName] = table
}

// AddNewVersion adds a new version to simulate concurrent writes
func (m *MockMetadataClient) AddNewVersion(tableName string, version uint64, txnID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	table, exists := m.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Simulate concurrent operation
	table.ConcurrentOps++

	// Add new version (this should not affect reads of older versions)
	if version > table.LatestVersion {
		files := []*pb.FileInfo{
			{
				Path: fmt.Sprintf("data/concurrent-%s-%d.parquet", txnID, version),
				Size: uint64(2000 + int(version)*50),
				Rows: uint64(200 + int(version)*5),
			},
		}

		table.Versions[version] = &MockVersion{
			Version: version,
			Files:   files,
			TxnID:   txnID,
		}

		table.LatestVersion = version
	}

	return nil
}

// GetExpectedFilesForVersion returns the expected files for a specific version
func (m *MockMetadataClient) GetExpectedFilesForVersion(tableName string, version uint64) []*pb.FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.tables[tableName]
	if !exists {
		return nil
	}

	mockVersion, exists := table.Versions[version]
	if !exists {
		return nil
	}

	return mockVersion.Files
}

// Implement MetadataClient interface
func (m *MockMetadataClient) CreateTable(ctx context.Context, req *pb.CreateTableRequest, opts ...grpc.CallOption) (*pb.CreateTableResponse, error) {
	return &pb.CreateTableResponse{}, nil
}

func (m *MockMetadataClient) GetLatestVersion(ctx context.Context, req *pb.GetLatestVersionRequest, opts ...grpc.CallOption) (*pb.GetLatestVersionResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.tables[req.TableName]
	if !exists {
		return &pb.GetLatestVersionResponse{
			Error: "table not found",
		}, nil
	}

	return &pb.GetLatestVersionResponse{
		Version: table.LatestVersion,
	}, nil
}

func (m *MockMetadataClient) GetSnapshot(ctx context.Context, req *pb.GetSnapshotRequest, opts ...grpc.CallOption) (*pb.GetSnapshotResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.tables[req.TableName]
	if !exists {
		return &pb.GetSnapshotResponse{
			Error: "table not found",
		}, nil
	}

	version, exists := table.Versions[req.Version]
	if !exists {
		return &pb.GetSnapshotResponse{
			Error: fmt.Sprintf("version %d not found", req.Version),
		}, nil
	}

	// Add small delay to simulate network latency and increase chance of race conditions
	time.Sleep(time.Duration(m.seed%10) * time.Millisecond)

	return &pb.GetSnapshotResponse{
		Files:  version.Files,
		Schema: table.Schema,
	}, nil
}

func (m *MockMetadataClient) Commit(ctx context.Context, req *pb.CommitRequest, opts ...grpc.CallOption) (*pb.CommitResponse, error) {
	return &pb.CommitResponse{}, nil
}

func (m *MockMetadataClient) ListVersions(ctx context.Context, req *pb.ListVersionsRequest, opts ...grpc.CallOption) (*pb.ListVersionsResponse, error) {
	return &pb.ListVersionsResponse{}, nil
}

// IsHealthy returns true for mock client
func (m *MockMetadataClient) IsHealthy() bool {
	return true
}

// GetCurrentLeader returns a mock leader for testing
func (m *MockMetadataClient) GetCurrentLeader() string {
	return "mock-leader"
}
