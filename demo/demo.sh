#!/bin/bash

# Mini Lakehouse Demonstration Script
# This script demonstrates the complete workflow of table creation, data insertion, and querying

set -e

COORDINATOR_URL="http://localhost:8081"
DEMO_TABLE="sales_data"

echo "=== Mini Lakehouse Demonstration ==="
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

# Function to create table
create_table() {
    echo "=== Step 1: Creating table '$DEMO_TABLE' ==="
    
    curl -X POST "$COORDINATOR_URL/tables" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "'$DEMO_TABLE'",
            "schema": {
                "fields": [
                    {"name": "id", "type": "int64"},
                    {"name": "product", "type": "string"},
                    {"name": "category", "type": "string"},
                    {"name": "amount", "type": "float64"},
                    {"name": "quantity", "type": "int64"},
                    {"name": "date", "type": "string"}
                ]
            }
        }' || {
        echo "ERROR: Failed to create table"
        exit 1
    }
    
    echo
    echo "Table '$DEMO_TABLE' created successfully!"
    echo
}

# Function to insert sample data
insert_data() {
    echo "=== Step 2: Inserting sample data ==="
    
    # Insert batch 1
    echo "Inserting batch 1..."
    curl -X POST "$COORDINATOR_URL/tables/$DEMO_TABLE/insert" \
        -H "Content-Type: application/json" \
        -d '{
            "data": [
                {"id": 1, "product": "Laptop", "category": "Electronics", "amount": 999.99, "quantity": 2, "date": "2024-01-15"},
                {"id": 2, "product": "Mouse", "category": "Electronics", "amount": 29.99, "quantity": 5, "date": "2024-01-15"},
                {"id": 3, "product": "Keyboard", "category": "Electronics", "amount": 79.99, "quantity": 3, "date": "2024-01-16"},
                {"id": 4, "product": "Monitor", "category": "Electronics", "amount": 299.99, "quantity": 1, "date": "2024-01-16"},
                {"id": 5, "product": "Chair", "category": "Furniture", "amount": 199.99, "quantity": 2, "date": "2024-01-17"}
            ]
        }' || {
        echo "ERROR: Failed to insert batch 1"
        exit 1
    }
    
    echo
    echo "Batch 1 inserted successfully!"
    
    # Insert batch 2
    echo "Inserting batch 2..."
    curl -X POST "$COORDINATOR_URL/tables/$DEMO_TABLE/insert" \
        -H "Content-Type: application/json" \
        -d '{
            "data": [
                {"id": 6, "product": "Desk", "category": "Furniture", "amount": 399.99, "quantity": 1, "date": "2024-01-17"},
                {"id": 7, "product": "Phone", "category": "Electronics", "amount": 699.99, "quantity": 1, "date": "2024-01-18"},
                {"id": 8, "product": "Tablet", "category": "Electronics", "amount": 499.99, "quantity": 2, "date": "2024-01-18"},
                {"id": 9, "product": "Lamp", "category": "Furniture", "amount": 89.99, "quantity": 3, "date": "2024-01-19"},
                {"id": 10, "product": "Headphones", "category": "Electronics", "amount": 149.99, "quantity": 4, "date": "2024-01-19"}
            ]
        }' || {
        echo "ERROR: Failed to insert batch 2"
        exit 1
    }
    
    echo
    echo "Batch 2 inserted successfully!"
    echo "Total records inserted: 10"
    echo
}

# Function to execute queries
execute_queries() {
    echo "=== Step 3: Executing queries ==="
    
    # Query 1: Simple scan
    echo "Query 1: Scanning all records..."
    curl -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT * FROM '$DEMO_TABLE'"
        }' | jq '.' || {
        echo "ERROR: Failed to execute scan query"
        exit 1
    }
    
    echo
    echo "Scan query completed!"
    echo
    
    # Query 2: Filter by category
    echo "Query 2: Filtering Electronics products..."
    curl -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT product, amount, quantity FROM '$DEMO_TABLE' WHERE category = '\''Electronics'\''"
        }' | jq '.' || {
        echo "ERROR: Failed to execute filter query"
        exit 1
    }
    
    echo
    echo "Filter query completed!"
    echo
    
    # Query 3: GROUP BY aggregation
    echo "Query 3: GROUP BY category with aggregations..."
    curl -X POST "$COORDINATOR_URL/query" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT category, COUNT(*) as item_count, SUM(amount * quantity) as total_revenue FROM '$DEMO_TABLE' GROUP BY category"
        }' | jq '.' || {
        echo "ERROR: Failed to execute GROUP BY query"
        exit 1
    }
    
    echo
    echo "GROUP BY query completed!"
    echo
}

# Function to show table metadata
show_metadata() {
    echo "=== Step 4: Showing table metadata ==="
    
    echo "Getting table information..."
    curl -s "$COORDINATOR_URL/tables/$DEMO_TABLE" | jq '.' || {
        echo "ERROR: Failed to get table metadata"
        exit 1
    }
    
    echo
    echo "Getting table versions..."
    curl -s "$COORDINATOR_URL/tables/$DEMO_TABLE/versions" | jq '.' || {
        echo "ERROR: Failed to get table versions"
        exit 1
    }
    
    echo
}

# Main execution
main() {
    echo "Starting Mini Lakehouse demonstration..."
    echo "Make sure the system is running with: docker compose up -d"
    echo
    
    wait_for_coordinator
    create_table
    insert_data
    execute_queries
    show_metadata
    
    echo "=== Demonstration completed successfully! ==="
    echo
    echo "You can now:"
    echo "1. View Grafana dashboards at: http://localhost:3000 (admin/admin)"
    echo "2. View Jaeger traces at: http://localhost:16686"
    echo "3. View Prometheus metrics at: http://localhost:9090"
    echo "4. Access MinIO console at: http://localhost:9001 (minioadmin/minioadmin)"
    echo
}

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "ERROR: jq is required but not installed. Please install jq to run this demo."
    echo "On Ubuntu/Debian: sudo apt-get install jq"
    echo "On macOS: brew install jq"
    exit 1
fi

# Check if curl is installed
if ! command -v curl &> /dev/null; then
    echo "ERROR: curl is required but not installed. Please install curl to run this demo."
    exit 1
fi

# Run main function
main "$@"