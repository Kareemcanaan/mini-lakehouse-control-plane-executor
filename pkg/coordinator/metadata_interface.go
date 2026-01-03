package coordinator

import (
	"context"
	"mini-lakehouse/proto/gen"

	"google.golang.org/grpc"
)

// MetadataClient defines the interface for metadata service operations
type MetadataClient interface {
	CreateTable(ctx context.Context, req *gen.CreateTableRequest, opts ...grpc.CallOption) (*gen.CreateTableResponse, error)
	GetLatestVersion(ctx context.Context, req *gen.GetLatestVersionRequest, opts ...grpc.CallOption) (*gen.GetLatestVersionResponse, error)
	GetSnapshot(ctx context.Context, req *gen.GetSnapshotRequest, opts ...grpc.CallOption) (*gen.GetSnapshotResponse, error)
	Commit(ctx context.Context, req *gen.CommitRequest, opts ...grpc.CallOption) (*gen.CommitResponse, error)
	ListVersions(ctx context.Context, req *gen.ListVersionsRequest, opts ...grpc.CallOption) (*gen.ListVersionsResponse, error)
}

// Ensure MetadataClientManager implements MetadataClient
var _ MetadataClient = (*MetadataClientManager)(nil)

// Ensure the gRPC client implements MetadataClient (it does by default)
var _ MetadataClient = (gen.MetadataServiceClient)(nil)
