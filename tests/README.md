# Mini Lakehouse Testing Infrastructure

This directory contains comprehensive testing infrastructure for the Mini Lakehouse system, including integration tests, chaos engineering tests, and test automation scripts.

## Test Structure

```
tests/
├── integration/           # End-to-end integration tests
│   └── golden_query_test.go
├── chaos/                # Chaos engineering tests
│   ├── worker_failure_test.go
│   └── leader_failure_test.go
├── run_integration_tests.sh  # Integration test runner
├── run_chaos_tests.sh        # Chaos test runner
└── README.md                 # This file
```

## Prerequisites

Before running tests, ensure you have:

1. **System Requirements:**
   - Go 1.21+ installed
   - Docker and Docker Compose installed
   - curl and jq installed
   - Mini Lakehouse system running (`docker compose up -d`)

2. **System Health:**
   - All services should be healthy and responsive
   - At least 2 metadata services running (for chaos tests)
   - At least 2 workers running (for chaos tests)

## Test Categories

### 1. Integration Tests

**Purpose:** Verify end-to-end system functionality with deterministic workflows.

**Key Test: Golden Query Test**
- Creates table with known schema
- Inserts deterministic test data
- Executes complex GROUP BY query
- Verifies exact expected results
- Tests snapshot isolation

**Run Integration Tests:**
```bash
# Run all integration tests
./tests/run_integration_tests.sh

# Or run Go tests directly
go test -v ./tests/integration/... -timeout 5m
```

**Expected Results:**
- Table creation and data insertion succeed
- Query returns exact expected aggregation results
- Snapshot isolation maintains consistency
- All system components work together correctly

### 2. Chaos Engineering Tests

**Purpose:** Verify fault tolerance and system resilience under failure conditions.

#### Worker Failure Test

**Scenario:** Kill worker during map/shuffle stage execution

**Verifies:**
- Task retry mechanisms work correctly
- SUCCESS manifest behavior during failures
- Query completes with correct results despite worker failure
- System recovers when failed worker restarts

**Test Flow:**
1. Setup table with large dataset (1000+ records)
2. Start complex GROUP BY query requiring shuffle operations
3. Kill worker-1 during query execution
4. Verify query completes successfully with remaining workers
5. Verify results are correct and complete
6. Restart failed worker and verify system recovery

#### Leader Failure Test

**Scenario:** Kill metadata service leader during transaction commit

**Verifies:**
- Exactly-once semantics via transaction IDs
- Commit succeeds with new leader after election
- System maintains consistency during leader changes
- Duplicate transaction handling works correctly

**Test Flow:**
1. Identify current Raft leader
2. Start multiple concurrent commit operations
3. Kill current leader during commits
4. Verify new leader is elected
5. Verify commits succeed with new leader
6. Test exactly-once semantics with duplicate transaction IDs
7. Restart failed leader and verify cluster recovery

**Run Chaos Tests:**
```bash
# Run all chaos tests
./tests/run_chaos_tests.sh

# Run specific chaos test
./tests/run_chaos_tests.sh worker    # Worker failure only
./tests/run_chaos_tests.sh leader    # Leader failure only
./tests/run_chaos_tests.sh status    # Show system status

# Or run Go tests directly
go test -v ./tests/chaos/... -run TestWorkerFailureDuringShuffle -timeout 10m
go test -v ./tests/chaos/... -run TestLeaderFailureDuringCommit -timeout 10m
```

## Test Data and Scenarios

### Integration Test Data

**Golden Query Dataset:**
- 9 deterministic records across 3 categories
- Electronics: 4 items (Laptop, Mouse, Keyboard, Monitor)
- Furniture: 3 items (Chair, Desk, Lamp)  
- Books: 2 items (Novel, Textbook)
- Predictable aggregation results for verification

**Expected GROUP BY Results:**
```
Books:       2 items, $390 revenue,  $47.50 avg price
Electronics: 4 items, $2650 revenue, $350.00 avg price  
Furniture:   3 items, $1700 revenue, $200.00 avg price
```

### Chaos Test Data

**Worker Failure Dataset:**
- 1000 records across 5 categories
- Large enough to require multiple workers
- Triggers shuffle operations for GROUP BY queries
- Tests system under realistic load

**Leader Failure Dataset:**
- Multiple concurrent transactions with unique IDs
- Tests transaction commit durability
- Verifies exactly-once semantics
- Tests duplicate transaction handling

## Test Automation

### Integration Test Runner (`run_integration_tests.sh`)

**Features:**
- Automatic system health checks
- Go module management
- Basic functionality verification
- Concurrent operation testing
- Comprehensive test reporting

**Usage:**
```bash
./tests/run_integration_tests.sh
```

### Chaos Test Runner (`run_chaos_tests.sh`)

**Features:**
- Pre-test system validation
- Automated container management (kill/restart)
- System recovery verification
- Detailed fault tolerance reporting
- Flexible test selection

**Usage:**
```bash
./tests/run_chaos_tests.sh [worker|leader|all|status]
```

## Test Validation

### Success Criteria

**Integration Tests:**
- ✅ Table creation succeeds
- ✅ Data insertion completes without errors
- ✅ Queries return exact expected results
- ✅ Snapshot isolation maintains consistency
- ✅ System handles concurrent operations correctly

**Chaos Tests:**
- ✅ Queries complete despite worker failures
- ✅ Task retry mechanisms work correctly
- ✅ SUCCESS manifests ensure output consistency
- ✅ New leader election succeeds within 60 seconds
- ✅ Commits succeed with new leader
- ✅ Exactly-once semantics preserved
- ✅ System recovers when failed components restart

### Failure Scenarios

**Common Issues and Solutions:**

1. **Services Not Ready:**
   ```bash
   # Check service status
   docker compose ps
   docker compose logs
   
   # Restart services
   docker compose down
   docker compose up -d
   ```

2. **Test Timeouts:**
   ```bash
   # Increase test timeout
   go test -timeout 15m ./tests/...
   ```

3. **Container Kill Failures:**
   ```bash
   # Check Docker permissions
   sudo usermod -aG docker $USER
   # Logout and login again
   ```

4. **Port Conflicts:**
   ```bash
   # Check port usage
   netstat -tulpn | grep :8080
   
   # Stop conflicting services
   ```

## Monitoring During Tests

### Real-time Monitoring

**Grafana Dashboards:** http://localhost:3000
- System metrics during test execution
- Query performance and error rates
- Resource utilization

**Jaeger Tracing:** http://localhost:16686
- Distributed traces for test queries
- Request flow visualization during failures

**Prometheus Metrics:** http://localhost:9090
- Raw metrics and custom queries
- Alerting rule evaluation

### Log Analysis

**Container Logs:**
```bash
# View all service logs
docker compose logs -f

# View specific service logs
docker compose logs -f coordinator
docker compose logs -f meta-1
docker compose logs -f worker-1
```

**Test Logs:**
```bash
# Verbose test output
go test -v ./tests/... -timeout 10m

# Test with race detection
go test -race ./tests/...
```

## Performance Expectations

### Test Execution Times

**Integration Tests:**
- Golden Query Test: 30-60 seconds
- Basic functionality tests: 10-20 seconds
- Total integration suite: 2-3 minutes

**Chaos Tests:**
- Worker failure test: 3-5 minutes
- Leader failure test: 4-6 minutes
- Total chaos suite: 8-12 minutes

### System Recovery Times

**Expected Recovery Metrics:**
- Worker failure detection: 5-10 seconds
- Task retry initiation: 2-5 seconds
- Leader election: 10-30 seconds
- Service restart and registration: 15-30 seconds
- Full system recovery: 30-60 seconds

## Extending Tests

### Adding New Integration Tests

1. Create test file in `tests/integration/`
2. Follow existing patterns for setup/cleanup
3. Use deterministic test data
4. Verify exact expected results
5. Add to integration test runner

### Adding New Chaos Tests

1. Create test file in `tests/chaos/`
2. Implement failure injection mechanisms
3. Verify system recovery and consistency
4. Add appropriate timeouts and retries
5. Add to chaos test runner

### Test Data Generation

```go
// Example: Generate test data
func generateTestData(size int, categories []string) []TestRecord {
    data := make([]TestRecord, size)
    for i := 0; i < size; i++ {
        data[i] = TestRecord{
            ID:       int64(i + 1),
            Category: categories[i%len(categories)],
            Value:    float64(rand.Intn(1000) + 1),
            // ... other fields
        }
    }
    return data
}
```

## Troubleshooting

### Common Test Failures

1. **System Not Ready:**
   - Wait longer for services to start
   - Check Docker Compose logs
   - Verify network connectivity

2. **Inconsistent Results:**
   - Check for race conditions
   - Verify test data determinism
   - Ensure proper cleanup between tests

3. **Container Management Issues:**
   - Verify Docker permissions
   - Check container names match expectations
   - Ensure containers can be killed/restarted

4. **Network Issues:**
   - Verify port accessibility
   - Check firewall settings
   - Ensure Docker networking is working

### Debug Commands

```bash
# Check system health
curl http://localhost:8081/health
curl http://localhost:8080/leader

# Verify test data
curl -X POST http://localhost:8081/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM test_table"}'

# Check container status
docker ps -a | grep mini-lakehouse

# View recent logs
docker compose logs --tail=50 coordinator
```

## Best Practices

### Test Development

1. **Deterministic Data:** Use predictable test data for consistent results
2. **Proper Cleanup:** Always clean up test resources
3. **Timeout Handling:** Set appropriate timeouts for operations
4. **Error Handling:** Check all error conditions and edge cases
5. **Documentation:** Document test scenarios and expected outcomes

### Test Execution

1. **System Health:** Verify system is healthy before running tests
2. **Isolation:** Run tests in isolated environments when possible
3. **Monitoring:** Monitor system metrics during test execution
4. **Recovery:** Verify system recovery after chaos tests
5. **Reporting:** Generate comprehensive test reports

This testing infrastructure provides comprehensive validation of the Mini Lakehouse system's functionality, fault tolerance, and consistency guarantees under both normal and failure conditions.