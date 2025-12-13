package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	pb "mini-lakehouse/proto/gen"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
)

// Service implements the MetadataService gRPC interface
type Service struct {
	pb.UnimplementedMetadataServiceServer
	raft     *raft.Raft
	fsm      *StateMachine
	nodeID   string
	bindAddr string
	dataDir  string
}

// NewService creates a new metadata service instance
func NewService(nodeID, bindAddr, dataDir string) *Service {
	return &Service{
		nodeID:   nodeID,
		bindAddr: bindAddr,
		dataDir:  dataDir,
		fsm:      NewStateMachine(),
	}
}

// Start initializes and starts the Raft cluster
func (s *Service) Start(peers []string) error {
	// Create data directory
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// Setup Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.nodeID)

	// Setup Raft address
	addr, err := net.ResolveTCPAddr("tcp", s.bindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve bind address: %v", err)
	}

	// Create transport
	transport, err := raft.NewTCPTransport(s.bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create transport: %v", err)
	}

	// Create snapshot store
	snapshots, err := raft.NewFileSnapshotStore(s.dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %v", err)
	}

	// Create log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.dataDir, "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create log store: %v", err)
	}

	// Create stable store
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(s.dataDir, "raft-stable.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create stable store: %v", err)
	}

	// Create Raft instance
	s.raft, err = raft.NewRaft(config, s.fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft instance: %v", err)
	}

	// Bootstrap cluster if this is the first node
	if len(peers) > 0 {
		configuration := raft.Configuration{
			Servers: make([]raft.Server, len(peers)),
		}
		for i, peer := range peers {
			configuration.Servers[i] = raft.Server{
				ID:      raft.ServerID(fmt.Sprintf("node-%d", i+1)),
				Address: raft.ServerAddress(peer),
			}
		}
		s.raft.BootstrapCluster(configuration)
	}

	return nil
}

// CreateTable implements the CreateTable gRPC method
func (s *Service) CreateTable(ctx context.Context, req *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	// Check if we're the leader
	if s.raft.State() != raft.Leader {
		leader := s.raft.Leader()
		return &pb.CreateTableResponse{
			Success: false,
			Error:   fmt.Sprintf("not leader, current leader: %s", leader),
		}, nil
	}

	// Create command
	cmd := Command{
		Type: "create_table",
		Data: CreateTableCommand{
			TableName: req.TableName,
			Schema:    req.Schema,
		},
	}

	// Marshal command
	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return &pb.CreateTableResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to marshal command: %v", err),
		}, nil
	}

	// Apply command through Raft
	future := s.raft.Apply(cmdData, 10*time.Second)
	if err := future.Error(); err != nil {
		return &pb.CreateTableResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to apply command: %v", err),
		}, nil
	}

	// Check result
	result := future.Response()
	if result != nil {
		if errResult, ok := result.(error); ok {
			return &pb.CreateTableResponse{
				Success: false,
				Error:   errResult.Error(),
			}, nil
		}
	}

	return &pb.CreateTableResponse{
		Success: true,
	}, nil
}

// GetLatestVersion implements the GetLatestVersion gRPC method
func (s *Service) GetLatestVersion(ctx context.Context, req *pb.GetLatestVersionRequest) (*pb.GetLatestVersionResponse, error) {
	// Ensure linearizable read by checking leadership
	if s.raft.State() != raft.Leader {
		leader := s.raft.Leader()
		return &pb.GetLatestVersionResponse{
			Error: fmt.Sprintf("not leader, current leader: %s", leader),
		}, nil
	}

	// Perform a linearizable read by applying a no-op
	future := s.raft.Barrier(5 * time.Second)
	if err := future.Error(); err != nil {
		return &pb.GetLatestVersionResponse{
			Error: fmt.Sprintf("failed to ensure linearizable read: %v", err),
		}, nil
	}

	// Get table metadata
	table, exists := s.fsm.GetTable(req.TableName)
	if !exists {
		return &pb.GetLatestVersionResponse{
			Error: fmt.Sprintf("table %s does not exist", req.TableName),
		}, nil
	}

	return &pb.GetLatestVersionResponse{
		Version: table.LatestVersion,
	}, nil
}

// GetSnapshot implements the GetSnapshot gRPC method
func (s *Service) GetSnapshot(ctx context.Context, req *pb.GetSnapshotRequest) (*pb.GetSnapshotResponse, error) {
	// Ensure linearizable read by checking leadership
	if s.raft.State() != raft.Leader {
		leader := s.raft.Leader()
		return &pb.GetSnapshotResponse{
			Error: fmt.Sprintf("not leader, current leader: %s", leader),
		}, nil
	}

	// Perform a linearizable read by applying a no-op
	future := s.raft.Barrier(5 * time.Second)
	if err := future.Error(); err != nil {
		return &pb.GetSnapshotResponse{
			Error: fmt.Sprintf("failed to ensure linearizable read: %v", err),
		}, nil
	}

	// Get snapshot
	files, schema, err := s.fsm.GetSnapshot(req.TableName, req.Version)
	if err != nil {
		return &pb.GetSnapshotResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.GetSnapshotResponse{
		Files:  files,
		Schema: schema,
	}, nil
}

// Commit implements the Commit gRPC method
func (s *Service) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	// Check if we're the leader
	if s.raft.State() != raft.Leader {
		leader := s.raft.Leader()
		return &pb.CommitResponse{
			Error: fmt.Sprintf("not leader, current leader: %s", leader),
		}, nil
	}

	// Create command
	cmd := Command{
		Type: "commit",
		Data: CommitCommand{
			TableName:   req.TableName,
			BaseVersion: req.BaseVersion,
			TxnID:       req.TxnId,
			Adds:        req.Adds,
			Removes:     req.Removes,
		},
	}

	// Marshal command
	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return &pb.CommitResponse{
			Error: fmt.Sprintf("failed to marshal command: %v", err),
		}, nil
	}

	// Apply command through Raft
	future := s.raft.Apply(cmdData, 10*time.Second)
	if err := future.Error(); err != nil {
		return &pb.CommitResponse{
			Error: fmt.Sprintf("failed to apply command: %v", err),
		}, nil
	}

	// Check result
	result := future.Response()
	if result == nil {
		return &pb.CommitResponse{
			Error: "no response from state machine",
		}, nil
	}

	if errResult, ok := result.(error); ok {
		return &pb.CommitResponse{
			Error: errResult.Error(),
		}, nil
	}

	if mapResult, ok := result.(map[string]interface{}); ok {
		if newVersion, exists := mapResult["new_version"]; exists {
			if version, ok := newVersion.(uint64); ok {
				return &pb.CommitResponse{
					NewVersion: version,
				}, nil
			}
		}
	}

	return &pb.CommitResponse{
		Error: "unexpected response format from state machine",
	}, nil
}

// ListVersions implements the ListVersions gRPC method
func (s *Service) ListVersions(ctx context.Context, req *pb.ListVersionsRequest) (*pb.ListVersionsResponse, error) {
	// Ensure linearizable read by checking leadership
	if s.raft.State() != raft.Leader {
		leader := s.raft.Leader()
		return &pb.ListVersionsResponse{
			Error: fmt.Sprintf("not leader, current leader: %s", leader),
		}, nil
	}

	// Perform a linearizable read by applying a no-op
	future := s.raft.Barrier(5 * time.Second)
	if err := future.Error(); err != nil {
		return &pb.ListVersionsResponse{
			Error: fmt.Sprintf("failed to ensure linearizable read: %v", err),
		}, nil
	}

	// Get table metadata
	table, exists := s.fsm.GetTable(req.TableName)
	if !exists {
		return &pb.ListVersionsResponse{
			Error: fmt.Sprintf("table %s does not exist", req.TableName),
		}, nil
	}

	// Generate version list
	versions := make([]uint64, table.LatestVersion)
	for i := uint64(1); i <= table.LatestVersion; i++ {
		versions[i-1] = i
	}

	return &pb.ListVersionsResponse{
		Versions: versions,
	}, nil
}

// Leader implements the Leader gRPC method
func (s *Service) Leader(ctx context.Context, req *pb.LeaderRequest) (*pb.LeaderResponse, error) {
	leader := s.raft.Leader()
	leaderID := ""

	// Try to determine leader ID from address
	if leader != "" {
		// For simplicity, extract node ID from address
		// In production, you'd maintain a mapping
		leaderID = string(leader)
	}

	return &pb.LeaderResponse{
		LeaderId:      leaderID,
		LeaderAddress: string(leader),
	}, nil
}

// Health implements the Health gRPC method
func (s *Service) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	state := s.raft.State()
	healthy := state == raft.Leader || state == raft.Follower

	return &pb.HealthResponse{
		Healthy: healthy,
		Status:  state.String(),
	}, nil
}

// StartGRPCServer starts the gRPC server
func (s *Service) StartGRPCServer(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(grpcServer, s)

	return grpcServer.Serve(lis)
}

// Stop gracefully shuts down the service
func (s *Service) Stop() error {
	if s.raft != nil {
		return s.raft.Shutdown().Error()
	}
	return nil
}

// IsLeader returns true if this node is the current Raft leader
func (s *Service) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// GetLeaderAddress returns the current leader's address
func (s *Service) GetLeaderAddress() string {
	return string(s.raft.Leader())
}

// WaitForLeader waits for a leader to be elected
func (s *Service) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.raft.Leader() != "" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("no leader elected within timeout")
}
