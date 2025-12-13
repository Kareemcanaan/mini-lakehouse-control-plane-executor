# Mini Lakehouse

A distributed data processing system that implements core lakehouse architecture patterns with ACID transactions, distributed query processing, and fault tolerance.

## Architecture

The Mini Lakehouse consists of three main planes:

- **Storage Plane**: Transactional data lake using Parquet files and commit logs in object storage (MinIO)
- **Control Plane**: Raft-replicated metadata service providing linearizable consistency (Go)
- **Compute Plane**: Distributed query executor with fault-tolerant task scheduling (Go coordinator + Rust workers)

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.21+
- Rust 1.75+
- Protocol Buffers compiler

### Development Setup

1. Clone the repository
2. Generate protobuf code and build:
   ```bash
   make all
   ```

3. Start the development environment:
   ```bash
   make dev-up
   ```

4. Access services:
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (admin/admin)

### Building

```bash
# Generate protobuf code
make proto

# Build all components
make build

# Build specific components
make build-go    # Coordinator and metadata service
make build-rust  # Workers

# Run tests
make test
```

### Docker

```bash
# Build Docker images
make docker

# Start services
docker-compose up -d

# Stop services
docker-compose down
```

## Services

### Metadata Service (Port 8080-8100)
- Raft-replicated metadata management
- Transaction commit processing
- Table schema and version management

### Coordinator (Port 8070)
- Query planning and execution
- Task scheduling and worker management
- Fault tolerance and retry logic

### Workers (Port 8060-8061)
- Task execution (scan, filter, aggregate)
- Parquet file processing
- Shuffle operations

### MinIO (Port 9000-9001)
- S3-compatible object storage
- Data files, transaction logs, shuffle outputs

## Development

### Project Structure

```
├── cmd/                    # Go service entry points
│   ├── coordinator/        # Coordinator service
│   └── metad/             # Metadata service
├── worker/                # Rust worker implementation
├── proto/                 # gRPC protobuf definitions
├── docker/               # Dockerfiles
├── monitoring/           # Prometheus and Grafana config
└── .github/workflows/    # CI configuration
```

### Testing

The project uses both unit tests and property-based tests:

- **Go**: Standard testing with gopter for property-based tests
- **Rust**: Standard testing with proptest for property-based tests

### Contributing

1. Ensure protobuf code generates successfully: `make verify-proto`
2. Run all tests: `make test`
3. Build all components: `make build`
4. Test with Docker Compose: `make dev-up`

## License

MIT License