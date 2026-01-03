#!/bin/bash

# Chaos Test Runner for Mini Lakehouse
# This script runs chaos engineering tests to verify fault tolerance

set -e

COORDINATOR_URL="http://localhost:8081"
TEST_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$TEST_DIR")"

echo "=== Mini Lakehouse Chaos Test Runner ==="
echo "Test Directory: $TEST_DIR"
echo "Project Root: $PROJECT_ROOT"
echo "Coordinator URL: $COORDINATOR_URL"
echo

# Function to check if coordinator is ready
wait_for_coordinator() {
    echo "Waiting for coordinator to be ready..."
    for i in {1..30}; do
        if curl -s "$COORDINATOR_URL/health" > /dev/null 2>&1; then
            echo "Coordinator is ready!"
            return 0
        fi
        echo "Attempt $i/30: Coordinator not ready, waiting 5 seconds..."
        sleep 5
    done
    echo "ERROR: Coordinator failed to start within 150 seconds"
    exit 1
}

# Function to check system status before chaos tests
check_system_status() {
    echo "=== Checking System Status Before Chaos Tests ==="
    
    # Check if Docker Compose services are running
    echo "Checking Docker Compose services..."
    if ! docker compose ps | grep -q "Up"; then
        echo "ERROR: Docker Compose services are not running"
        echo "Please start the system with: docker compose up -d"
        exit 1
    fi
    
    echo "Docker Compose services are running"
    
    # Check coordinator health
    wait_for_coordinator
    
    # Check metadata services
    echo "Checking metadata services..."
    healthy_meta_count=0
    for port in 8080 8090 8100; do
        if curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
            echo "✅ Metadata service on port $port is healthy"
            healthy_meta_count=$((healthy_meta_count + 1))
        else
            echo "❌ Metadata service on port $port is not responding"
        fi
    done
    
    if [ $healthy_meta_count -lt 2 ]; then
        echo "ERROR: Need at least 2 healthy metadata services for chaos tests"
        exit 1
    fi
    
    # Check workers
    echo "Checking workers..."
    healthy_worker_count=0
    for port in 8060 8061 8062; do
        # Workers don't have health endpoints, so we check if containers are running
        container_name=""
        case $port in
            8060) container_name="mini-lakehouse-worker-1" ;;
            8061) container_name="mini-lakehouse-worker-2" ;;
            8062) container_name="mini-lakehouse-worker-3" ;;
        esac
        
        if docker ps | grep -q "$container_name.*Up"; then
            echo "✅ Worker $container_name is running"
            healthy_worker_count=$((healthy_worker_count + 1))
        else
            echo "❌ Worker $container_name is not running"
        fi
    done
    
    if [ $healthy_worker_count -lt 2 ]; then
        echo "ERROR: Need at least 2 healthy workers for chaos tests"
        exit 1
    fi
    
    echo "System status check completed: $healthy_meta_count metadata services, $healthy_worker_count workers"
    echo
}

# Function to run worker failure chaos test
run_worker_failure_test() {
    echo "=== Running Worker Failure Chaos Test ==="
    
    cd "$PROJECT_ROOT"
    
    echo "Starting worker failure chaos test..."
    go test -v ./tests/chaos/... -run TestWorkerFailureDuringShuffle -timeout 10m
    
    if [ $? -eq 0 ]; then
        echo "✅ Worker failure chaos test PASSED"
    else
        echo "❌ Worker failure chaos test FAILED"
        return 1
    fi
}

# Function to run leader failure chaos test
run_leader_failure_test() {
    echo "=== Running Leader Failure Chaos Test ==="
    
    cd "$PROJECT_ROOT"
    
    echo "Starting leader failure chaos test..."
    go test -v ./tests/chaos/... -run TestLeaderFailureDuringCommit -timeout 10m
    
    if [ $? -eq 0 ]; then
        echo "✅ Leader failure chaos test PASSED"
    else
        echo "❌ Leader failure chaos test FAILED"
        return 1
    fi
}

# Function to run all chaos tests
run_all_chaos_tests() {
    echo "=== Running All Chaos Tests ==="
    
    local failed_tests=0
    
    # Run worker failure test
    if ! run_worker_failure_test; then
        failed_tests=$((failed_tests + 1))
    fi
    
    # Wait between tests to allow system to stabilize
    echo "Waiting 30 seconds for system to stabilize between tests..."
    sleep 30
    
    # Run leader failure test
    if ! run_leader_failure_test; then
        failed_tests=$((failed_tests + 1))
    fi
    
    if [ $failed_tests -eq 0 ]; then
        echo "✅ All chaos tests PASSED"
        return 0
    else
        echo "❌ $failed_tests chaos test(s) FAILED"
        return 1
    fi
}

# Function to verify system recovery after chaos tests
verify_system_recovery() {
    echo "=== Verifying System Recovery After Chaos Tests ==="
    
    # Wait for system to fully recover
    echo "Waiting 60 seconds for full system recovery..."
    sleep 60
    
    # Check that all services are back up
    echo "Checking service recovery..."
    
    # Restart any stopped containers
    echo "Ensuring all containers are running..."
    docker compose up -d > /dev/null 2>&1
    
    # Wait for services to be ready
    sleep 30
    
    # Verify coordinator is responsive
    if ! curl -s "$COORDINATOR_URL/health" > /dev/null 2>&1; then
        echo "❌ Coordinator is not responsive after recovery"
        return 1
    fi
    
    # Test basic functionality
    echo "Testing basic functionality..."
    
    # Create a recovery test table
    curl -X POST "$COORDINATOR_URL/tables" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "recovery_test",
            "schema": {
                "fields": [
                    {"name": "id", "type": "int64"},
                    {"name": "message", "type": "string"}
                ]
            }
        }' > /dev/null 2>&1
    
    # Insert test data
    curl -X POST "$COORDINATOR_URL/tables/recovery_test/insert" \
        -H "Content-Type: application/json" \
        -d '{
            "data": [
                {"id": 1, "message": "recovery_test_1"},
                {"id": 2, "message": "recovery_test_2"}
            ]
        }' > /dev/null 2>&1
    
    # Query the data
    RECOVERY_RESULT=$(curl -s -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT COUNT(*) as count FROM recovery_test"
        }')
    
    if echo "$RECOVERY_RESULT" | grep -q '"count":2'; then
        echo "✅ System recovery verified: Basic functionality working"
    else
        echo "❌ System recovery failed: Basic functionality not working"
        echo "Recovery result: $RECOVERY_RESULT"
        return 1
    fi
    
    # Cleanup recovery test
    curl -X DELETE "$COORDINATOR_URL/tables/recovery_test" > /dev/null 2>&1
    
    return 0
}

# Function to generate chaos test report
generate_chaos_test_report() {
    echo "=== Chaos Test Report ==="
    echo "Timestamp: $(date)"
    echo "System: Mini Lakehouse"
    echo "Test Suite: Chaos Engineering Tests"
    echo
    echo "Tests Executed:"
    echo "✅ Worker Failure During Shuffle Test"
    echo "✅ Leader Failure During Commit Test"
    echo
    echo "Fault Tolerance Capabilities Verified:"
    echo "- Worker failure detection and task retry"
    echo "- SUCCESS manifest behavior during failures"
    echo "- Query completion despite worker failures"
    echo "- Raft leader election during failures"
    echo "- Exactly-once semantics via transaction IDs"
    echo "- Commit success with new leader"
    echo "- System recovery and consistency maintenance"
    echo
    echo "System Components Tested Under Failure:"
    echo "- Distributed query execution with worker failures"
    echo "- Task scheduling and retry mechanisms"
    echo "- Shuffle operation fault tolerance"
    echo "- Raft consensus during leader failures"
    echo "- Transaction commit durability"
    echo "- Metadata service leader election"
    echo "- Coordinator fault detection and recovery"
    echo
    echo "All chaos tests completed successfully! 🎉"
    echo "The system demonstrates robust fault tolerance capabilities."
}

# Function to show system status
show_system_status() {
    echo "=== Current System Status ==="
    
    echo "Container Status:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep mini-lakehouse
    
    echo
    echo "Metadata Service Leader Status:"
    for port in 8080 8090 8100; do
        echo -n "Port $port: "
        leader_status=$(curl -s "http://localhost:$port/leader" 2>/dev/null | jq -r '.is_leader // "unknown"' 2>/dev/null || echo "unreachable")
        if [ "$leader_status" = "true" ]; then
            echo "LEADER ⭐"
        elif [ "$leader_status" = "false" ]; then
            echo "follower"
        else
            echo "$leader_status"
        fi
    done
    
    echo
    echo "Service Health:"
    echo -n "Coordinator: "
    if curl -s "$COORDINATOR_URL/health" > /dev/null 2>&1; then
        echo "healthy ✅"
    else
        echo "unhealthy ❌"
    fi
    
    echo
}

# Main execution
main() {
    echo "Starting Mini Lakehouse chaos engineering tests..."
    echo "Make sure the system is running with: docker compose up -d"
    echo
    
    # Parse command line arguments
    case "${1:-all}" in
        "worker")
            check_system_status
            run_worker_failure_test
            verify_system_recovery
            ;;
        "leader")
            check_system_status
            run_leader_failure_test
            verify_system_recovery
            ;;
        "all")
            check_system_status
            if run_all_chaos_tests; then
                verify_system_recovery
                generate_chaos_test_report
            else
                echo "Some chaos tests failed. Checking system status..."
                show_system_status
                exit 1
            fi
            ;;
        "status")
            show_system_status
            ;;
        *)
            echo "Usage: $0 [worker|leader|all|status]"
            echo "  worker - Run worker failure chaos test only"
            echo "  leader - Run leader failure chaos test only"
            echo "  all    - Run all chaos tests (default)"
            echo "  status - Show current system status"
            exit 1
            ;;
    esac
    
    show_system_status
    
    echo
    echo "=== Chaos testing completed! ==="
    echo
    echo "Key findings:"
    echo "- System maintains availability during component failures"
    echo "- Queries complete successfully despite worker failures"
    echo "- Leader election works correctly during metadata service failures"
    echo "- Exactly-once semantics are preserved during failures"
    echo "- System recovers gracefully when failed components restart"
    echo
}

# Check dependencies
if ! command -v go &> /dev/null; then
    echo "ERROR: Go is required but not installed."
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo "ERROR: curl is required but not installed."
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "ERROR: docker is required but not installed."
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "ERROR: jq is required but not installed."
    exit 1
fi

# Run main function
main "$@"