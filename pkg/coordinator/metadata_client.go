package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"mini-lakehouse/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetadataClientManager manages connections to the metadata service with leader discovery
type MetadataClientManager struct {
	mu sync.RWMutex

	// Configuration
	endpoints      []string
	dialTimeout    time.Duration
	requestTimeout time.Duration
	retryAttempts  int
	retryDelay     time.Duration

	// Current connections
	clients       map[string]gen.MetadataServiceClient // endpoint -> client
	connections   map[string]*grpc.ClientConn          // endpoint -> connection
	currentLeader string                               // current known leader endpoint

	// Background discovery
	ctx    context.Context
	cancel context.CancelFunc
}

// NewMetadataClientManager creates a new metadata client manager
func NewMetadataClientManager(endpoints []string) *MetadataClientManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &MetadataClientManager{
		endpoints:      endpoints,
		dialTimeout:    10 * time.Second,
		requestTimeout: 30 * time.Second,
		retryAttempts:  3,
		retryDelay:     1 * time.Second,
		clients:        make(map[string]gen.MetadataServiceClient),
		connections:    make(map[string]*grpc.ClientConn),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start initializes connections and starts leader discovery
func (mcm *MetadataClientManager) Start() error {
	// Initialize connections to all endpoints
	for _, endpoint := range mcm.endpoints {
		err := mcm.connectToEndpoint(endpoint)
		if err != nil {
			log.Printf("Warning: failed to connect to metadata endpoint %s: %v", endpoint, err)
		}
	}

	// Start leader discovery
	go mcm.startLeaderDiscovery()

	return nil
}

// Stop closes all connections and stops background processes
func (mcm *MetadataClientManager) Stop() error {
	mcm.cancel()

	mcm.mu.Lock()
	defer mcm.mu.Unlock()

	// Close all connections
	for endpoint, conn := range mcm.connections {
		if err := conn.Close(); err != nil {
			log.Printf("Warning: failed to close connection to %s: %v", endpoint, err)
		}
	}

	mcm.connections = make(map[string]*grpc.ClientConn)
	mcm.clients = make(map[string]gen.MetadataServiceClient)
	mcm.currentLeader = ""

	return nil
}

// connectToEndpoint establishes a connection to a metadata service endpoint
func (mcm *MetadataClientManager) connectToEndpoint(endpoint string) error {
	ctx, cancel := context.WithTimeout(context.Background(), mcm.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", endpoint, err)
	}

	client := gen.NewMetadataServiceClient(conn)

	mcm.mu.Lock()
	mcm.connections[endpoint] = conn
	mcm.clients[endpoint] = client
	mcm.mu.Unlock()

	log.Printf("Connected to metadata service at %s", endpoint)
	return nil
}

// startLeaderDiscovery runs leader discovery in the background
func (mcm *MetadataClientManager) startLeaderDiscovery() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Initial leader discovery
	mcm.discoverLeader()

	for {
		select {
		case <-mcm.ctx.Done():
			return
		case <-ticker.C:
			mcm.discoverLeader()
		}
	}
}

// discoverLeader discovers the current leader by querying all endpoints
func (mcm *MetadataClientManager) discoverLeader() {
	mcm.mu.RLock()
	endpoints := make([]string, 0, len(mcm.clients))
	for endpoint := range mcm.clients {
		endpoints = append(endpoints, endpoint)
	}
	mcm.mu.RUnlock()

	for _, endpoint := range endpoints {
		if mcm.checkIfLeader(endpoint) {
			mcm.mu.Lock()
			if mcm.currentLeader != endpoint {
				log.Printf("Discovered new metadata service leader: %s", endpoint)
				mcm.currentLeader = endpoint
			}
			mcm.mu.Unlock()
			return
		}
	}

	// No leader found
	mcm.mu.Lock()
	if mcm.currentLeader != "" {
		log.Printf("Lost metadata service leader, no leader currently available")
		mcm.currentLeader = ""
	}
	mcm.mu.Unlock()
}

// checkIfLeader checks if an endpoint is the current leader
func (mcm *MetadataClientManager) checkIfLeader(endpoint string) bool {
	mcm.mu.RLock()
	client, exists := mcm.clients[endpoint]
	mcm.mu.RUnlock()

	if !exists {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Leader(ctx, &gen.LeaderRequest{})
	if err != nil {
		return false
	}

	// Check if this endpoint is the leader
	return resp.LeaderAddress == endpoint
}

// GetLeaderClient returns the client for the current leader
func (mcm *MetadataClientManager) GetLeaderClient() (gen.MetadataServiceClient, error) {
	mcm.mu.RLock()
	leader := mcm.currentLeader
	client, exists := mcm.clients[leader]
	mcm.mu.RUnlock()

	if leader == "" {
		return nil, fmt.Errorf("no metadata service leader available")
	}

	if !exists {
		return nil, fmt.Errorf("no client for leader %s", leader)
	}

	return client, nil
}

// ExecuteWithRetry executes a metadata operation with automatic retry on leader changes
func (mcm *MetadataClientManager) ExecuteWithRetry(operation func(gen.MetadataServiceClient) error) error {
	var lastErr error

	for attempt := 0; attempt < mcm.retryAttempts; attempt++ {
		client, err := mcm.GetLeaderClient()
		if err != nil {
			lastErr = err
			// Wait for leader discovery
			time.Sleep(mcm.retryDelay)
			mcm.discoverLeader()
			continue
		}

		err = operation(client)
		if err == nil {
			return nil
		}

		// Check if this is a "not leader" error
		if mcm.isNotLeaderError(err) {
			log.Printf("Metadata operation failed due to leader change, retrying (attempt %d/%d)",
				attempt+1, mcm.retryAttempts)
			lastErr = err
			// Force leader rediscovery
			mcm.discoverLeader()
			time.Sleep(mcm.retryDelay)
			continue
		}

		// Other errors are not retryable
		return err
	}

	return fmt.Errorf("metadata operation failed after %d attempts, last error: %w",
		mcm.retryAttempts, lastErr)
}

// isNotLeaderError checks if an error indicates the node is not the leader
func (mcm *MetadataClientManager) isNotLeaderError(err error) bool {
	if err == nil {
		return false
	}

	// Check for gRPC status codes that indicate leadership issues
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.FailedPrecondition || st.Code() == codes.Unavailable {
			return true
		}
	}

	// Check for specific error messages
	errMsg := err.Error()
	return contains(errMsg, "not leader") ||
		contains(errMsg, "no leader") ||
		contains(errMsg, "leader election")
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// CreateTable creates a table with retry logic
func (mcm *MetadataClientManager) CreateTable(ctx context.Context, req *gen.CreateTableRequest, opts ...grpc.CallOption) (*gen.CreateTableResponse, error) {
	var response *gen.CreateTableResponse

	err := mcm.ExecuteWithRetry(func(client gen.MetadataServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, mcm.requestTimeout)
		defer cancel()

		resp, err := client.CreateTable(reqCtx, req, opts...)
		if err != nil {
			return err
		}

		response = resp
		return nil
	})

	return response, err
}

// GetLatestVersion gets the latest version with retry logic
func (mcm *MetadataClientManager) GetLatestVersion(ctx context.Context, req *gen.GetLatestVersionRequest, opts ...grpc.CallOption) (*gen.GetLatestVersionResponse, error) {
	var response *gen.GetLatestVersionResponse

	err := mcm.ExecuteWithRetry(func(client gen.MetadataServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, mcm.requestTimeout)
		defer cancel()

		resp, err := client.GetLatestVersion(reqCtx, req, opts...)
		if err != nil {
			return err
		}

		response = resp
		return nil
	})

	return response, err
}

// GetSnapshot gets a snapshot with retry logic
func (mcm *MetadataClientManager) GetSnapshot(ctx context.Context, req *gen.GetSnapshotRequest, opts ...grpc.CallOption) (*gen.GetSnapshotResponse, error) {
	var response *gen.GetSnapshotResponse

	err := mcm.ExecuteWithRetry(func(client gen.MetadataServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, mcm.requestTimeout)
		defer cancel()

		resp, err := client.GetSnapshot(reqCtx, req, opts...)
		if err != nil {
			return err
		}

		response = resp
		return nil
	})

	return response, err
}

// Commit commits a transaction with retry logic
func (mcm *MetadataClientManager) Commit(ctx context.Context, req *gen.CommitRequest, opts ...grpc.CallOption) (*gen.CommitResponse, error) {
	var response *gen.CommitResponse

	err := mcm.ExecuteWithRetry(func(client gen.MetadataServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, mcm.requestTimeout)
		defer cancel()

		resp, err := client.Commit(reqCtx, req, opts...)
		if err != nil {
			return err
		}

		response = resp
		return nil
	})

	return response, err
}

// ListVersions lists versions with retry logic
func (mcm *MetadataClientManager) ListVersions(ctx context.Context, req *gen.ListVersionsRequest, opts ...grpc.CallOption) (*gen.ListVersionsResponse, error) {
	var response *gen.ListVersionsResponse

	err := mcm.ExecuteWithRetry(func(client gen.MetadataServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, mcm.requestTimeout)
		defer cancel()

		resp, err := client.ListVersions(reqCtx, req, opts...)
		if err != nil {
			return err
		}

		response = resp
		return nil
	})

	return response, err
}

// GetCurrentLeader returns the current leader endpoint
func (mcm *MetadataClientManager) GetCurrentLeader() string {
	mcm.mu.RLock()
	defer mcm.mu.RUnlock()
	return mcm.currentLeader
}

// IsHealthy returns true if we have a healthy connection to the leader
func (mcm *MetadataClientManager) IsHealthy() bool {
	mcm.mu.RLock()
	leader := mcm.currentLeader
	mcm.mu.RUnlock()

	return leader != ""
}
