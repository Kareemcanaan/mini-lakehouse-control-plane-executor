# Implementation Plan

- [x] 1. Set up project structure and shared interfaces





  - Create directory structure for Go services (coordinator, metadata) and Rust workers
  - Define gRPC protobuf schemas for service communication
  - Generate protos for Go + Rust and make CI fail if protos don't compile
  - Set up build configuration for both Go and Rust components
  - Configure Docker Compose for development environment
  - _Requirements: 0.1, 0.2, 0.3, 9.1, 9.2_



- [x] 2. Implement object storage foundation





  - [x] 2.1 Create object store client wrapper for MinIO


    - Implement S3-compatible client with retry logic
    - Add path utilities for table, transaction log, and shuffle paths
    - _Requirements: 1.1, 1.2_

  - [x] 2.2 Implement transaction log operations


    - Create transaction log entry serialization/deserialization
    - Implement log reading and writing operations
    - Add snapshot building from log entries
    - _Requirements: 2.1, 2.5_

  - [ ]* 2.3 Write property test for log replay determinism
    - **Property 9: Log replay determinism**
    - **Validates: Requirements 2.5**

  - [ ]* 2.4 Write property test for log-controlled visibility
    - **Property 4: Log-controlled visibility**
    - **Validates: Requirements 1.4, 1.5**



- [x] 3. Implement Raft-based metadata service





  - [x] 3.1 Create Raft state machine for table metadata



    - Implement state machine with tables, versions, and transactions maps
    - Add commit processing through Raft log
    - _Requirements: 3.1, 3.2, 3.4_

  - [x] 3.2 Implement metadata service gRPC endpoints


    - Create CreateTable, GetLatestVersion, GetSnapshot, Commit endpoints
    - Add leader routing and linearizable read semantics
    - _Requirements: 3.5, 3.6_

  - [x] 3.3 Add transaction commit logic with optimistic concurrency


    - Implement base version validation
    - Add transaction ID-based idempotency
    - _Requirements: 2.2, 2.3, 2.6_

  - [ ]* 3.4 Write property test for concurrent commit exclusion
    - **Property 6: Concurrent commit exclusion**
    - **Validates: Requirements 2.2**

  - [ ]* 3.5 Write property test for commit idempotency
    - **Property 10: Commit idempotency**
    - **Validates: Requirements 2.6**



- [x] 4. Checkpoint - Ensure all tests pass





  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement Parquet data operations (Rust)





  - [x] 5.1 Create Parquet file reader with filtering


    - Implement columnar data reading with predicate pushdown
    - Add schema validation and type conversion
    - _Requirements: 1.1, 4.2_

  - [x] 5.2 Create Parquet file writer for data and shuffle outputs


    - Implement efficient columnar data writing
    - Add partitioning support for shuffle operations
    - _Requirements: 1.1, 4.3_

  - [ ]* 5.3 Write property test for Parquet format consistency
    - **Property 1: Parquet format consistency**
    - **Validates: Requirements 1.1**



- [x] 6. Implement worker task execution engine (Rust)





  - [x] 6.1 Create task execution framework


    - Implement scan, filter, projection, and aggregation operations
    - Add GROUP BY support with count and sum aggregates
    - _Requirements: 4.2, 4.5_

  - [x] 6.2 Implement shuffle operations


    - Create data partitioning for reduce stages
    - Add shuffle output writing with attempt isolation
    - _Requirements: 4.3, 5.3_

  - [x] 6.3 Create worker gRPC service


    - Implement RunTask and CancelTask endpoints
    - Add heartbeat and task completion reporting
    - _Requirements: 5.1, 5.6_

  - [ ]* 6.4 Write property test for GROUP BY correctness
    - **Property 18: GROUP BY correctness**
    - **Validates: Requirements 4.5**



- [x] 7. Implement coordinator service (Go)










  - [x] 7.1 Create query planning engine







    - Implement stage-based query plan generation
    - Add task creation and dependency management
    - _Requirements: 4.1_

  - [x] 7.2 Implement task scheduling and worker management


    - Create worker registration and heartbeat monitoring
    - Add task assignment with load balancing
    - _Requirements: 5.1, 5.2_

  - [x] 7.3 Add fault tolerance and retry logic


    - Implement task failure detection and retry
    - Create SUCCESS manifest validation and writing
    - _Requirements: 5.2, 5.4, 5.6_

  - [x] 7.4 Create coordinator gRPC services


    - Implement RegisterWorker, Heartbeat, ReportTaskComplete endpoints
    - Add query execution orchestration
    - _Requirements: 4.4_



  - [ ]* 7.6 Write property test for SUCCESS manifest authority
    - **Property 20: SUCCESS manifest authority**
    - **Validates: Requirements 5.4, 5.6**

- [x] 8. Implement end-to-end query execution










  - [x] 8.1 Create table creation and data insertion workflow


    - Implement CREATE TABLE and INSERT operations
    - Add temporary file management and commit coordination
    - _Requirements: 1.3, 1.4, 2.1_

  - [x] 8.2 Implement distributed query execution


    - Create multi-stage query execution with shuffle
    - Add result collection and return
    - _Requirements: 4.3, 4.4_

  - [x] 8.3 Add snapshot isolation for reads


    - Implement version-specific table reads
    - Ensure deterministic results across concurrent operations
    - _Requirements: 2.4_

  - [ ]* 8.4 Write property test for snapshot isolation
    - **Property 8: Snapshot isolation**
    - **Validates: Requirements 2.4**



- [x] 9. Checkpoint - Ensure all tests pass





  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement fault tolerance mechanisms
  - [x] 10.1 Add leader election handling in coordinator
    - Implement metadata service leader discovery
    - Add commit retry logic during leader changes
    - _Requirements: 6.1, 6.2_

  - [x] 10.2 Implement worker failure detection and recovery
    - Create heartbeat timeout detection
    - Add task reassignment to healthy workers
    - _Requirements: 5.1, 5.2_

  - [x] 10.3 Add duplicate request handling
    - Implement transaction ID-based deduplication
    - Ensure state consistency across retries
    - _Requirements: 6.3_

  - [ ]* 10.4 Write property test for leader failure retry
    - **Property 22: Leader failure retry**
    - **Validates: Requirements 6.2**



- [x] 11. Implement compaction operations (v2 - optional)

  - [x] 11.1 Create compaction candidate identification

    - Implement file size analysis and threshold checking
    - Add compaction planning logic
    - _Requirements: 7.1_

  - [x] 11.2 Implement compaction execution

    - Create multi-file reading and consolidation
    - Add atomic commit with file adds/removes
    - _Requirements: 7.2, 7.3_

  - [x] 11.3 Add compaction safety with concurrent operations

    - Implement version validation for compaction commits
    - Ensure transaction safety during compaction
    - _Requirements: 7.4, 7.6_

  - [x] 11.4 Write property test for query result preservation

    - **Property 30: Query result preservation**
    - **Validates: Requirements 7.5**



- [x] 12. Implement observability and monitoring
  - [x] 12.1 Add Prometheus metrics collection
    - Implement task duration, failure rates, and throughput metrics
    - Add Raft leader status and apply latency tracking
    - _Requirements: 8.1, 8.4_

  - [x] 12.2 Implement OpenTelemetry distributed tracing
    - Create job, stage, and task span hierarchy
    - Add trace context propagation across services
    - _Requirements: 8.2_

  - [x] 12.3 Add structured logging
    - Implement structured logs with job IDs, task IDs, and error codes
    - Add object store operation metrics
    - _Requirements: 8.3, 8.5_



- [-] 13. Create deployment and testing infrastructure
  - [x] 13.1 Complete Docker Compose configuration
    - Add all service definitions with proper networking
    - Configure environment variables and service discovery
    - _Requirements: 9.1, 9.2_

  - [-] 13.2 Create demonstration workflow
    - Implement complete table creation, insertion, and querying demo
    - Add fault tolerance demonstration scripts
    - _Requirements: 9.3, 9.4_



  - [ ] 13.3 Write "golden query" end-to-end integration test
    - Create table → insert data → execute GROUP BY query → verify exact expected output
    - Test complete workflow with deterministic data and results
    - _Requirements: 9.3, 10.2, 4.4, 4.5_

  - [ ] 13.4 Write chaos test for worker failure during shuffle
    - Kill worker during map/shuffle stage execution
    - Verify task retry and SUCCESS manifest behavior
    - Ensure query completes with correct results
    - _Requirements: 10.4, 5.1, 5.2, 5.4_

  - [ ] 13.5 Write chaos test for leader failure during commit
    - Kill metadata service leader during transaction commit
    - Verify exactly-once semantics via transaction ID
    - Ensure commit succeeds with new leader
    - _Requirements: 10.5, 6.1, 6.2, 6.3_

- [ ] 14. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.