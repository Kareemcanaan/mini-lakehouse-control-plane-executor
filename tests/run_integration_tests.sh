#!/bin/bash

# Integration Test Runner for Mini Lakehouse
# This script runs the golden query end-to-end integration test

set -e

COORDINATOR_URL="http://localhost:8081"
TEST_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$TEST_DIR")"

echo "=== Mini Lakehouse Integration Test Runner ==="
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

# Function to check if system is running
check_system_status() {
    echo "=== Checking System Status ==="
    
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
    for port in 8080 8090 8100; do
        if curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
            echo "Metadata service on port $port is healthy"
        else
            echo "WARNING: Metadata service on port $port is not responding"
        fi
    done
    
    echo "System status check completed"
    echo
}

# Function to run Go integration tests
run_go_tests() {
    echo "=== Running Go Integration Tests ==="
    
    cd "$PROJECT_ROOT"
    
    # Ensure Go modules are up to date
    echo "Updating Go modules..."
    go mod tidy
    
    # Run the golden query test
    echo "Running golden query integration test..."
    go test -v ./tests/integration/... -run TestGoldenQuery -timeout 5m
    
    if [ $? -eq 0 ]; then
        echo "✅ Golden query integration test PASSED"
    else
        echo "❌ Golden query integration test FAILED"
        exit 1
    fi
}

# Function to run additional verification tests
run_verification_tests() {
    echo "=== Running Additional Verification Tests ==="
    
    # Test 1: Basic table operations
    echo "Test 1: Basic table operations..."
    
    # Create a test table
    curl -X POST "$COORDINATOR_URL/tables" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "verification_test",
            "schema": {
                "fields": [
                    {"name": "id", "type": "int64"},
                    {"name": "value", "type": "string"}
                ]
            }
        }' > /dev/null 2>&1
    
    # Insert test data
    curl -X POST "$COORDINATOR_URL/tables/verification_test/insert" \
        -H "Content-Type: application/json" \
        -d '{
            "data": [
                {"id": 1, "value": "test1"},
                {"id": 2, "value": "test2"}
            ]
        }' > /dev/null 2>&1
    
    # Query the data
    QUERY_RESULT=$(curl -s -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT COUNT(*) as count FROM verification_test"
        }')
    
    if echo "$QUERY_RESULT" | grep -q '"count":2'; then
        echo "✅ Basic table operations test PASSED"
    else
        echo "❌ Basic table operations test FAILED"
        echo "Query result: $QUERY_RESULT"
        exit 1
    fi
    
    # Cleanup
    curl -X DELETE "$COORDINATOR_URL/tables/verification_test" > /dev/null 2>&1
    
    # Test 2: Concurrent operations
    echo "Test 2: Concurrent operations..."
    
    # Create test table
    curl -X POST "$COORDINATOR_URL/tables" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "concurrent_test",
            "schema": {
                "fields": [
                    {"name": "id", "type": "int64"},
                    {"name": "thread", "type": "string"}
                ]
            }
        }' > /dev/null 2>&1
    
    # Run concurrent inserts
    for i in {1..3}; do
        (
            curl -s -X POST "$COORDINATOR_URL/tables/concurrent_test/insert" \
                -H "Content-Type: application/json" \
                -d '{
                    "data": [
                        {"id": '$i', "thread": "thread_'$i'"}
                    ]
                }' > /dev/null 2>&1
        ) &
    done
    
    # Wait for all background jobs
    wait
    
    # Verify all inserts succeeded
    CONCURRENT_RESULT=$(curl -s -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT COUNT(*) as count FROM concurrent_test"
        }')
    
    if echo "$CONCURRENT_RESULT" | grep -q '"count":3'; then
        echo "✅ Concurrent operations test PASSED"
    else
        echo "❌ Concurrent operations test FAILED"
        echo "Query result: $CONCURRENT_RESULT"
        exit 1
    fi
    
    # Cleanup
    curl -X DELETE "$COORDINATOR_URL/tables/concurrent_test" > /dev/null 2>&1
    
    echo "All verification tests completed successfully"
}

# Function to generate test report
generate_test_report() {
    echo "=== Test Report ==="
    echo "Timestamp: $(date)"
    echo "System: Mini Lakehouse"
    echo "Test Suite: Integration Tests"
    echo
    echo "Tests Executed:"
    echo "✅ Golden Query End-to-End Test"
    echo "✅ Basic Table Operations Test"
    echo "✅ Concurrent Operations Test"
    echo
    echo "System Components Tested:"
    echo "- Table creation and schema validation"
    echo "- Data insertion and transaction commits"
    echo "- Query execution (scan, filter, GROUP BY)"
    echo "- Snapshot isolation"
    echo "- Concurrent operations"
    echo "- Metadata service integration"
    echo "- Coordinator service integration"
    echo "- Worker service integration"
    echo
    echo "All tests completed successfully! 🎉"
}

# Main execution
main() {
    echo "Starting Mini Lakehouse integration tests..."
    echo "Make sure the system is running with: docker compose up -d"
    echo
    
    # Check system status
    check_system_status
    
    # Run the main integration test
    run_go_tests
    
    # Run additional verification tests
    run_verification_tests
    
    # Generate report
    generate_test_report
    
    echo
    echo "=== Integration tests completed successfully! ==="
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

# Run main function
main "$@"