package coordinator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"mini-lakehouse/proto/gen"

	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// **Feature: mini-lakehouse, Property 22: Leader failure retry**
// *For any* commit operation during leader failure, the coordinator should retry with the new leader
// **Validates: Requirements 6.2**
func TestProperty22LeaderFailureRetry(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("coordinator retries commit operations with new leader after leader failure", prop.ForAll(
		func(baseVersion uint64, numAdds int, numRemoves int, failureAttempt int) bool {
			// Ensure valid ranges
			if baseVersion < 1 {
				baseVersion = 1
			}
			if baseVersion > 1000 {
				baseVersion = 1000
			}
			if numAdds < 1 {
				numAdds = 1
			}
			if numAdds > 5 {
				numAdds = 5
			}
			if numRemoves < 0 {
				numRemoves = 0
			}
			if numRemoves > 3 {
				numRemoves = 3
			}
			if failureAttempt < 1 {
				failureAttempt = 1
			}
			if failureAttempt > 2 {
				failureAttempt = 2
			}

			// Create test context
			tableName := fmt.Sprintf("test_table_%d", baseVersion)

			// Create mock metadata client that simulates leader failure
			mockClient := NewMockMetadataClientWithLeaderFailure(failureAttempt)

			// Create transaction manager
			txnManager := NewTransactionManager()

			// Start a transaction
			txnID := txnManager.StartTransaction(tableName, baseVersion)

			// Generate file adds
			var adds []*gen.FileAdd
			for i := 0; i < numAdds; i++ {
				add := &gen.FileAdd{
					Path: fmt.Sprintf("data/part-%d-%d.parquet", baseVersion, i),
					Rows: uint64(100 + i*50),
					Size: uint64(1024 * (i + 1)),
					Stats: &gen.FileStats{
						MinValues: map[string]string{"id": fmt.Sprintf("%d", i*100)},
						MaxValues: map[string]string{"id": fmt.Sprintf("%d", (i+1)*100-1)},
					},
				}
				adds = append(adds, add)
			}

			// Generate file removes
			var removes []*gen.FileRemove
			for i := 0; i < numRemoves; i++ {
				remove := &gen.FileRemove{
					Path: fmt.Sprintf("data/old-part-%d-%d.parquet", baseVersion-1, i),
				}
				removes = append(removes, remove)
			}

			// Test: Attempt commit with leader failure simulation
			startTime := time.Now()
			resp, err := txnManager.CommitTransaction(mockClient, txnID, adds, removes)
			duration := time.Since(startTime)

			// The commit should eventually succeed after retries
			if err != nil {
				t.Logf("Commit failed after retries: %v", err)
				return false
			}

			if resp == nil {
				t.Logf("Commit response is nil")
				return false
			}

			// Verify response contains new version
			if resp.NewVersion <= baseVersion {
				t.Logf("New version should be greater than base version: got %d, base %d", resp.NewVersion, baseVersion)
				return false
			}

			// Verify the mock client was called the expected number of times
			expectedCalls := failureAttempt + 1 // failures + final success
			if mockClient.GetCallCount() != expectedCalls {
				t.Logf("Expected %d calls to metadata client, got %d", expectedCalls, mockClient.GetCallCount())
				return false
			}

			// Verify leader changes were detected
			if mockClient.GetLeaderChangeCount() != failureAttempt {
				t.Logf("Expected %d leader changes, got %d", failureAttempt, mockClient.GetLeaderChangeCount())
				return false
			}

			// Verify retry took reasonable time (should have some delay due to retries)
			minExpectedDuration := time.Duration(failureAttempt) * time.Second
			if duration < minExpectedDuration {
				t.Logf("Retry duration too short: expected at least %v, got %v", minExpectedDuration, duration)
				return false
			}

			// Verify transaction was marked as completed successfully
			_, result, exists := txnManager.GetTransactionStatus(txnID)
			if !exists {
				t.Logf("Transaction status not found")
				return false
			}

			if result == nil {
				t.Logf("Transaction result is nil")
				return false
			}

			if !result.Success {
				t.Logf("Transaction should be marked as successful")
				return false
			}

			if result.NewVersion != resp.NewVersion {
				t.Logf("Transaction result version mismatch: expected %d, got %d", resp.NewVersion, result.NewVersion)
				return false
			}

			// Verify idempotency is handled by the mock metadata service
			// The mock client should return the same version for the same transaction ID
			if !mockClient.HasCommittedTransaction(txnID) {
				t.Logf("Transaction should be recorded as committed in metadata service")
				return false
			}

			return true
		},
		goptergen.UInt64Range(1, 100), // baseVersion range
		goptergen.IntRange(1, 3),      // numAdds range
		goptergen.IntRange(0, 2),      // numRemoves range
		goptergen.IntRange(1, 2),      // failureAttempt range (1 or 2 failures before success)
	))

	properties.TestingRun(t)
}

// MockMetadataClientWithLeaderFailure simulates leader failures during commit operations
type MockMetadataClientWithLeaderFailure struct {
	mu                sync.RWMutex
	callCount         int
	leaderChangeCount int
	failuresRemaining int
	currentLeader     string
	nextVersion       uint64
	committedTxns     map[string]uint64 // txnID -> version (for idempotency)
}

func NewMockMetadataClientWithLeaderFailure(failureAttempts int) *MockMetadataClientWithLeaderFailure {
	return &MockMetadataClientWithLeaderFailure{
		failuresRemaining: failureAttempts,
		currentLeader:     "leader-1",
		nextVersion:       1,
		committedTxns:     make(map[string]uint64),
	}
}

func (m *MockMetadataClientWithLeaderFailure) Commit(ctx context.Context, req *gen.CommitRequest, opts ...grpc.CallOption) (*gen.CommitResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCount++

	// Check for idempotency first
	if committedVersion, exists := m.committedTxns[req.TxnId]; exists {
		return &gen.CommitResponse{
			NewVersion: committedVersion,
		}, nil
	}

	// Simulate leader failure for the first few attempts
	if m.failuresRemaining > 0 {
		m.failuresRemaining--
		m.leaderChangeCount++

		// Simulate leader change
		if m.currentLeader == "leader-1" {
			m.currentLeader = "leader-2"
		} else {
			m.currentLeader = "leader-1"
		}

		// Return "not leader" error
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}

	// Success case - simulate successful commit
	newVersion := req.BaseVersion + 1
	m.nextVersion = newVersion + 1

	// Record transaction for idempotency
	m.committedTxns[req.TxnId] = newVersion

	return &gen.CommitResponse{
		NewVersion: newVersion,
	}, nil
}

func (m *MockMetadataClientWithLeaderFailure) GetCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.callCount
}

func (m *MockMetadataClientWithLeaderFailure) GetLeaderChangeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leaderChangeCount
}

func (m *MockMetadataClientWithLeaderFailure) GetCurrentLeader() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentLeader
}

func (m *MockMetadataClientWithLeaderFailure) HasCommittedTransaction(txnID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.committedTxns[txnID]
	return exists
}

// Implement other MetadataClient methods (not used in this test)
func (m *MockMetadataClientWithLeaderFailure) CreateTable(ctx context.Context, req *gen.CreateTableRequest, opts ...grpc.CallOption) (*gen.CreateTableResponse, error) {
	return &gen.CreateTableResponse{}, nil
}

func (m *MockMetadataClientWithLeaderFailure) GetLatestVersion(ctx context.Context, req *gen.GetLatestVersionRequest, opts ...grpc.CallOption) (*gen.GetLatestVersionResponse, error) {
	return &gen.GetLatestVersionResponse{Version: 1}, nil
}

func (m *MockMetadataClientWithLeaderFailure) GetSnapshot(ctx context.Context, req *gen.GetSnapshotRequest, opts ...grpc.CallOption) (*gen.GetSnapshotResponse, error) {
	return &gen.GetSnapshotResponse{}, nil
}

func (m *MockMetadataClientWithLeaderFailure) ListVersions(ctx context.Context, req *gen.ListVersionsRequest, opts ...grpc.CallOption) (*gen.ListVersionsResponse, error) {
	return &gen.ListVersionsResponse{}, nil
}

// IsHealthy returns true for mock client
func (m *MockMetadataClientWithLeaderFailure) IsHealthy() bool {
	return true
}
