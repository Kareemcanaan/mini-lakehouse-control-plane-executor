#!/bin/bash

# Mini Lakehouse Fault Tolerance Demonstration Script
# This script demonstrates fault tolerance capabilities by simulating failures

set -e

COORDINATOR_URL="http://localhost:8081"
DEMO_TABLE="fault_test_table"

echo "=== Mini Lakehouse Fault Tolerance Demonstration ==="
echo "Coordinator URL: $COORDINATOR_URL"
echo "Demo Table: $DEMO_TABLE"
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

# Function to setup test table
setup_test_table() {
    echo "=== Setting up test table ==="
    
    # Create table
    curl -X POST "$COORDINATOR_URL/tables" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "'$DEMO_TABLE'",
            "schema": {
                "fields": [
                    {"name": "id", "type": "int64"},
                    {"name": "data", "type": "string"},
                    {"name": "value", "type": "float64"}
                ]
            }
        }' > /dev/null 2>&1 || echo "Table may already exist"
    
    # Insert initial data
    curl -X POST "$COORDINATOR_URL/tables/$DEMO_TABLE/insert" \
        -H "Content-Type: application/json" \
        -d '{
            "data": [
                {"id": 1, "data": "initial_data_1", "value": 100.0},
                {"id": 2, "data": "initial_data_2", "value": 200.0},
                {"id": 3, "data": "initial_data_3", "value": 300.0}
            ]
        }' > /dev/null 2>&1
    
    echo "Test table setup completed!"
    echo
}

# Function to demonstrate worker failure tolerance
demo_worker_failure() {
    echo "=== Demonstrating Worker Failure Tolerance ==="
    
    echo "1. Starting a long-running query..."
    
    # Start a query in background that should take some time
    curl -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT * FROM '$DEMO_TABLE' WHERE value > 50"
        }' > /tmp/query_result_before_failure.json 2>&1 &
    QUERY_PID=$!
    
    echo "Query started (PID: $QUERY_PID)"
    sleep 2
    
    echo "2. Killing a worker during query execution..."
    
    # Kill worker-1
    docker kill mini-lakehouse-worker-1 > /dev/null 2>&1 || echo "Worker-1 may already be stopped"
    echo "Worker-1 killed!"
    
    echo "3. Waiting for query to complete (should succeed with remaining workers)..."
    wait $QUERY_PID
    echo "Query completed despite worker failure!"
    
    echo "4. Restarting the failed worker..."
    docker start mini-lakehouse-worker-1 > /dev/null 2>&1
    
    # Wait for worker to register
    sleep 10
    
    echo "5. Verifying system still works with restarted worker..."
    curl -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT COUNT(*) as total_count FROM '$DEMO_TABLE'"
        }' | jq '.' || {
        echo "ERROR: Query failed after worker restart"
        exit 1
    }
    
    echo "Worker failure tolerance demonstrated successfully!"
    echo
}

# Function to demonstrate metadata service leader failure
demo_leader_failure() {
    echo "=== Demonstrating Metadata Service Leader Failure ==="
    
    echo "1. Identifying current Raft leader..."
    
    # Check which metadata service is the leader
    LEADER_FOUND=false
    for port in 8080 8090 8100; do
        if curl -s "http://localhost:$port/leader" | grep -q '"is_leader":true'; then
            echo "Current leader is on port $port"
            LEADER_PORT=$port
            LEADER_FOUND=true
            break
        fi
    done
    
    if [ "$LEADER_FOUND" = false ]; then
        echo "No leader found, using port 8080 as default"
        LEADER_PORT=8080
    fi
    
    # Map port to container name
    case $LEADER_PORT in
        8080) LEADER_CONTAINER="mini-lakehouse-meta-1" ;;
        8090) LEADER_CONTAINER="mini-lakehouse-meta-2" ;;
        8100) LEADER_CONTAINER="mini-lakehouse-meta-3" ;;
    esac
    
    echo "2. Performing a commit operation..."
    
    # Insert data before leader failure
    curl -X POST "$COORDINATOR_URL/tables/$DEMO_TABLE/insert" \
        -H "Content-Type: application/json" \
        -d '{
            "data": [
                {"id": 100, "data": "before_leader_failure", "value": 1000.0}
            ]
        }' > /dev/null 2>&1
    
    echo "Data inserted successfully before leader failure"
    
    echo "3. Killing the current Raft leader ($LEADER_CONTAINER)..."
    docker kill "$LEADER_CONTAINER" > /dev/null 2>&1
    echo "Leader killed!"
    
    echo "4. Waiting for new leader election (30 seconds)..."
    sleep 30
    
    echo "5. Attempting commit operation with new leader..."
    
    # Try to insert data after leader failure
    for attempt in {1..5}; do
        echo "Attempt $attempt/5: Inserting data after leader failure..."
        if curl -X POST "$COORDINATOR_URL/tables/$DEMO_TABLE/insert" \
            -H "Content-Type: application/json" \
            -d '{
                "data": [
                    {"id": 200, "data": "after_leader_failure", "value": 2000.0}
                ]
            }' > /dev/null 2>&1; then
            echo "Data inserted successfully after leader failure!"
            break
        else
            echo "Attempt $attempt failed, retrying in 5 seconds..."
            sleep 5
        fi
    done
    
    echo "6. Verifying data consistency..."
    curl -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT * FROM '$DEMO_TABLE' WHERE id >= 100 ORDER BY id"
        }' | jq '.' || {
        echo "ERROR: Query failed after leader failure"
        exit 1
    }
    
    echo "7. Restarting the failed leader..."
    docker start "$LEADER_CONTAINER" > /dev/null 2>&1
    
    # Wait for service to rejoin cluster
    sleep 15
    
    echo "Leader failure tolerance demonstrated successfully!"
    echo
}

# Function to demonstrate concurrent operations
demo_concurrent_operations() {
    echo "=== Demonstrating Concurrent Operations ==="
    
    echo "1. Starting multiple concurrent insert operations..."
    
    # Start multiple concurrent inserts
    for i in {1..3}; do
        (
            curl -X POST "$COORDINATOR_URL/tables/$DEMO_TABLE/insert" \
                -H "Content-Type: application/json" \
                -d '{
                    "data": [
                        {"id": '$((300 + i))', "data": "concurrent_'$i'", "value": '$((i * 100))'.0}
                    ]
                }' > /tmp/concurrent_insert_$i.json 2>&1
        ) &
    done
    
    echo "Waiting for concurrent operations to complete..."
    wait
    
    echo "2. Verifying all concurrent operations succeeded..."
    curl -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT COUNT(*) as total_count FROM '$DEMO_TABLE' WHERE id >= 300"
        }' | jq '.' || {
        echo "ERROR: Concurrent operations verification failed"
        exit 1
    }
    
    echo "Concurrent operations demonstrated successfully!"
    echo
}

# Function to show system status
show_system_status() {
    echo "=== System Status ==="
    
    echo "Container Status:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep mini-lakehouse
    
    echo
    echo "Metadata Service Leader Status:"
    for port in 8080 8090 8100; do
        echo -n "Port $port: "
        curl -s "http://localhost:$port/leader" 2>/dev/null | jq -r '.is_leader // "unknown"' || echo "unreachable"
    done
    
    echo
}

# Main execution
main() {
    echo "Starting Mini Lakehouse fault tolerance demonstration..."
    echo "Make sure the system is running with: docker compose up -d"
    echo
    
    wait_for_coordinator
    setup_test_table
    
    echo "Choose demonstration type:"
    echo "1. Worker failure tolerance"
    echo "2. Leader failure tolerance"
    echo "3. Concurrent operations"
    echo "4. All demonstrations"
    echo "5. Show system status only"
    
    read -p "Enter choice (1-5): " choice
    
    case $choice in
        1)
            demo_worker_failure
            ;;
        2)
            demo_leader_failure
            ;;
        3)
            demo_concurrent_operations
            ;;
        4)
            demo_worker_failure
            demo_leader_failure
            demo_concurrent_operations
            ;;
        5)
            show_system_status
            ;;
        *)
            echo "Invalid choice. Running all demonstrations..."
            demo_worker_failure
            demo_leader_failure
            demo_concurrent_operations
            ;;
    esac
    
    show_system_status
    
    echo "=== Fault tolerance demonstration completed! ==="
    echo
    echo "Key observations:"
    echo "- Queries continue to work despite worker failures"
    echo "- New leader is elected automatically when current leader fails"
    echo "- Concurrent operations are handled safely"
    echo "- System recovers gracefully when failed components restart"
    echo
}

# Check dependencies
if ! command -v jq &> /dev/null; then
    echo "ERROR: jq is required but not installed."
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