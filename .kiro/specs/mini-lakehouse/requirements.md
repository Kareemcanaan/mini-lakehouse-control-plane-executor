# Requirements Document

## Introduction

The Mini Lakehouse is a distributed data processing system that provides storage, compute, and control plane capabilities similar to Databricks. The system stores tables as Parquet files in object storage, maintains transactional consistency through a Raft-replicated metadata service, and executes distributed queries with fault tolerance. The goal is to demonstrate core lakehouse architecture patterns in a minimal but complete implementation.

### Implementation Constraints

The system implements a clear separation between control plane and data plane using different languages optimized for each purpose: Go services for control plane operations (coordination, metadata management) and Rust workers for high-performance data processing, with gRPC providing stable interfaces between components.

## Glossary

- **Mini_Lakehouse**: The complete distributed data processing system
- **Object_Store**: S3-compatible storage (MinIO) for data files, transaction logs, and shuffle outputs
- **Metadata_Service**: Raft-replicated service managing table state and transaction commits
- **Coordinator**: Service that plans queries, schedules tasks, and manages job execution
- **Worker**: Service that executes tasks (scan, filter, aggregate) and reports results
- **Transaction_Log**: Versioned log of table changes stored as JSON files in object storage
- **Snapshot**: Point-in-time view of table state at a specific version
- **Shuffle**: Intermediate data exchange between query stages via object storage
- **Compaction**: Process of merging small files into larger ones for storage efficiency

## Requirements

### Requirement 0

**User Story:** As a developer, I want a clear separation between control plane and data plane implementations, so that the system can be implemented as Go services and Rust workers with stable interfaces.

#### Acceptance Criteria

1. WHEN implementing services, THE Mini_Lakehouse SHALL implement Coordinator and Metadata_Service in Go
2. WHEN implementing execution, THE Mini_Lakehouse SHALL implement Workers (task execution) in Rust
3. WHEN services communicate, THE Mini_Lakehouse SHALL use gRPC with versioned protobufs as the stable interface between Go and Rust components

### Requirement 1

**User Story:** As a data engineer, I want to store table data in Parquet format in object storage, so that I can achieve columnar storage benefits and S3 compatibility.

#### Acceptance Criteria

1. WHEN data is written to a table, THE Mini_Lakehouse SHALL store it as Parquet files in the object store
2. WHEN storing Parquet files, THE Mini_Lakehouse SHALL organize them under a consistent table path structure
3. WHEN writing data files, THE Mini_Lakehouse SHALL use temporary paths until commit succeeds
4. WHEN a transaction commits, THE Mini_Lakehouse SHALL make data files visible through the transaction log
5. WHEN reading table data, THE Mini_Lakehouse SHALL determine file visibility from the transaction log rather than directory listing

### Requirement 2

**User Story:** As a data engineer, I want transactional consistency for table operations, so that I can ensure data integrity across concurrent operations.

#### Acceptance Criteria

1. WHEN a write operation commits, THE Mini_Lakehouse SHALL create a new version in the transaction log
2. WHEN multiple writers attempt concurrent commits, THE Mini_Lakehouse SHALL ensure exactly one succeeds per base version
3. WHEN a commit request specifies a base version, THE Metadata_Service SHALL reject it if the base version is not current
4. WHEN reading table data, THE Mini_Lakehouse SHALL provide snapshot isolation at a specified version
5. WHEN applying transaction log entries, THE Mini_Lakehouse SHALL produce deterministic file lists for any given version
6. WHEN the same commit request is retried with the same transaction ID, THE Metadata_Service SHALL return the same committed version without duplicating table changes

### Requirement 3

**User Story:** As a data engineer, I want a distributed metadata service, so that I can ensure high availability and consistency of table metadata.

#### Acceptance Criteria

1. WHEN the Metadata_Service starts, THE Mini_Lakehouse SHALL run it with three Raft nodes for fault tolerance
2. WHEN a commit request is received, THE Metadata_Service SHALL process it through the Raft leader
3. WHEN the Raft leader fails, THE Mini_Lakehouse SHALL elect a new leader and continue processing requests
4. WHEN storing table metadata, THE Metadata_Service SHALL replicate it across all Raft nodes
5. WHEN a client queries metadata, THE Mini_Lakehouse SHALL provide linearizable read semantics for latestVersion and committed snapshots
6. WHEN servicing metadata reads, THE Mini_Lakehouse SHALL either route reads to the Raft leader or use a linearizable read mechanism, such that reads reflect the latest committed version

### Requirement 4

**User Story:** As a data analyst, I want to execute distributed queries on table data, so that I can perform analytics on large datasets.

#### Acceptance Criteria

1. WHEN a query is submitted, THE Coordinator SHALL create a stage-based execution plan
2. WHEN executing query stages, THE Mini_Lakehouse SHALL support scan, filter, projection, and aggregation operations
3. WHEN processing data across multiple workers, THE Mini_Lakehouse SHALL use object storage for shuffle operations
4. WHEN a query completes, THE Mini_Lakehouse SHALL return results or store them in object storage
5. WHEN query stages execute, THE Mini_Lakehouse SHALL support GROUP BY operations with count and sum aggregates

### Requirement 5

**User Story:** As a system operator, I want fault tolerance for worker failures, so that queries can complete despite individual worker crashes.

#### Acceptance Criteria

1. WHEN a worker fails during task execution, THE Coordinator SHALL detect the failure through missed heartbeats
2. WHEN a task fails, THE Coordinator SHALL retry it on a different worker
3. WHEN writing task outputs, THE Mini_Lakehouse SHALL scope them by attempt number to enable safe retries
4. WHEN a task completes successfully, THE Coordinator SHALL mark its outputs as official through a SUCCESS manifest
5. WHEN reading shuffle data, THE Mini_Lakehouse SHALL only consume outputs from successful attempts
6. WHEN a task reports completion, THE Coordinator SHALL validate output existence and then write the SUCCESS manifest that defines the canonical outputs for that task
7. WHEN tasks are retried, THE Coordinator SHALL treat worker task execution as stateless and idempotent beyond attempt-scoped outputs, relying only on gRPC results and object store artifacts

### Requirement 6

**User Story:** As a system operator, I want the system to survive metadata service leader changes, so that operations can continue during control plane failures.

#### Acceptance Criteria

1. WHEN the metadata service leader fails, THE Mini_Lakehouse SHALL elect a new leader automatically
2. WHEN a coordinator detects leader failure, THE Mini_Lakehouse SHALL retry commit operations with the new leader
3. WHEN processing duplicate commit requests, THE Metadata_Service SHALL detect them using transaction IDs
4. WHEN a new leader takes over, THE Mini_Lakehouse SHALL maintain all existing table state and versions
5. WHEN leader election completes, THE Mini_Lakehouse SHALL resume normal operations without data loss

### Requirement 7

**User Story:** As a data engineer, I want compaction capabilities, so that I can optimize storage by merging small files into larger ones.

#### Acceptance Criteria

1. WHEN small files accumulate, THE Mini_Lakehouse SHALL identify them as compaction candidates based on size thresholds
2. WHEN performing compaction, THE Mini_Lakehouse SHALL read multiple small files and write fewer large files
3. WHEN committing compaction results, THE Mini_Lakehouse SHALL atomically remove old files and add new files
4. WHEN compaction runs concurrently with other operations, THE Mini_Lakehouse SHALL ensure transaction safety
5. WHEN compaction completes, THE Mini_Lakehouse SHALL maintain identical query results for the same snapshot version
6. WHEN committing compaction results, THE Metadata_Service SHALL reject the commit if the compaction base version is not the current latest version

### Requirement 8

**User Story:** As a system operator, I want comprehensive observability, so that I can monitor system health and debug issues.

#### Acceptance Criteria

1. WHEN services run, THE Mini_Lakehouse SHALL expose Prometheus metrics for task duration, failure rates, and throughput
2. WHEN processing requests, THE Mini_Lakehouse SHALL generate OpenTelemetry traces with job, stage, and task spans
3. WHEN operations occur, THE Mini_Lakehouse SHALL write structured logs with job IDs, task IDs, and error codes
4. WHEN Raft operations execute, THE Mini_Lakehouse SHALL track leader status and apply latency metrics
5. WHEN object store operations occur, THE Mini_Lakehouse SHALL measure bytes read and written

### Requirement 9

**User Story:** As a developer, I want the system to be deployable with Docker Compose, so that I can easily test and demonstrate the complete system.

#### Acceptance Criteria

1. WHEN running docker compose up, THE Mini_Lakehouse SHALL start all required services (MinIO, metadata nodes (Go), coordinator (Go), workers (Rust))
2. WHEN the system starts, THE Mini_Lakehouse SHALL automatically configure service discovery and networking
3. WHEN demonstrating the system, THE Mini_Lakehouse SHALL support a complete workflow of table creation, data insertion, and querying
4. WHEN testing fault tolerance, THE Mini_Lakehouse SHALL allow killing individual containers and observing recovery
5. WHEN monitoring the system, THE Mini_Lakehouse SHALL provide Grafana dashboards for key metrics

### Requirement 10

**User Story:** As a developer, I want comprehensive testing capabilities, so that I can verify system correctness under various failure scenarios.

#### Acceptance Criteria

1. WHEN running unit tests, THE Mini_Lakehouse SHALL validate transaction log operations, snapshot building, and commit logic
2. WHEN running integration tests, THE Mini_Lakehouse SHALL verify end-to-end workflows including concurrent operations
3. WHEN running chaos tests, THE Mini_Lakehouse SHALL inject failures and verify system invariants are maintained
4. WHEN testing worker failures, THE Mini_Lakehouse SHALL demonstrate job completion despite worker container kills
5. WHEN testing leader failures, THE Mini_Lakehouse SHALL verify commits succeed after metadata service leader changes