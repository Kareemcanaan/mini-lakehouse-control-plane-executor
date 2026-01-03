# Mini Lakehouse Demonstration

This directory contains demonstration scripts that showcase the Mini Lakehouse system capabilities.

## Prerequisites

Before running the demonstrations, ensure you have:

1. **Docker and Docker Compose** installed
2. **jq** installed for JSON processing
3. **curl** installed for HTTP requests

### Installing Dependencies

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install jq curl docker.io docker-compose
```

**macOS:**
```bash
brew install jq curl docker docker-compose
```

**Windows:**
- Install Docker Desktop
- Install jq from https://stedolan.github.io/jq/download/
- curl is usually available by default

## Starting the System

1. **Start all services:**
   ```bash
   docker compose up -d
   ```

2. **Wait for services to be ready (2-3 minutes):**
   ```bash
   docker compose logs -f
   ```

3. **Verify services are running:**
   ```bash
   docker compose ps
   ```

## Demonstrations

### 1. Basic Workflow Demo (`demo.sh`)

This script demonstrates the complete Mini Lakehouse workflow:

- **Table Creation**: Creates a sample sales table with schema
- **Data Insertion**: Inserts sample data in multiple batches
- **Query Execution**: Runs various queries (scan, filter, GROUP BY)
- **Metadata Inspection**: Shows table metadata and versions

**Run the demo:**
```bash
chmod +x demo/demo.sh
./demo/demo.sh
```

**Expected Output:**
- Table creation confirmation
- Data insertion success messages
- Query results in JSON format
- Table metadata and version information

### 2. Fault Tolerance Demo (`fault-tolerance-demo.sh`)

This script demonstrates fault tolerance capabilities:

- **Worker Failure**: Kills a worker during query execution
- **Leader Failure**: Kills the Raft leader during operations
- **Concurrent Operations**: Tests multiple simultaneous operations
- **Recovery**: Shows system recovery after failures

**Run the demo:**
```bash
chmod +x demo/fault-tolerance-demo.sh
./demo/fault-tolerance-demo.sh
```

**Interactive Options:**
1. Worker failure tolerance only
2. Leader failure tolerance only
3. Concurrent operations only
4. All demonstrations
5. Show system status only

## Monitoring and Observability

While running demonstrations, you can monitor the system through:

### Grafana Dashboards
- **URL**: http://localhost:3000
- **Login**: admin/admin
- **Features**: System metrics, query performance, error rates

### Jaeger Tracing
- **URL**: http://localhost:16686
- **Features**: Distributed traces, request flow visualization

### Prometheus Metrics
- **URL**: http://localhost:9090
- **Features**: Raw metrics, custom queries, alerting rules

### MinIO Console
- **URL**: http://localhost:9001
- **Login**: minioadmin/minioadmin
- **Features**: Object storage browser, bucket management

## Expected Behavior

### Successful Operations
- **Table Creation**: Returns table metadata with version 0
- **Data Insertion**: Returns new version number and commit details
- **Queries**: Return results in JSON format with execution metadata
- **Fault Recovery**: System continues operating despite component failures

### Error Scenarios
- **Service Unavailable**: Scripts wait and retry automatically
- **Invalid Requests**: Clear error messages with HTTP status codes
- **Resource Conflicts**: Optimistic concurrency control prevents data corruption

## Troubleshooting

### Common Issues

1. **Services not starting:**
   ```bash
   # Check logs
   docker compose logs
   
   # Restart services
   docker compose down
   docker compose up -d
   ```

2. **Port conflicts:**
   ```bash
   # Check what's using ports
   netstat -tulpn | grep :8080
   
   # Stop conflicting services or modify docker-compose.yml
   ```

3. **Permission errors:**
   ```bash
   # Make scripts executable
   chmod +x demo/*.sh
   
   # Check Docker permissions
   sudo usermod -aG docker $USER
   # Then logout and login again
   ```

4. **Network issues:**
   ```bash
   # Reset Docker networks
   docker network prune
   docker compose down
   docker compose up -d
   ```

### Verification Commands

**Check service health:**
```bash
# Coordinator
curl http://localhost:8081/health

# Metadata services
curl http://localhost:8080/health
curl http://localhost:8090/health
curl http://localhost:8100/health

# MinIO
curl http://localhost:9000/minio/health/live
```

**Check Raft leader:**
```bash
curl http://localhost:8080/leader
curl http://localhost:8090/leader
curl http://localhost:8100/leader
```

**List tables:**
```bash
curl http://localhost:8081/tables
```

## Demo Data

The demonstrations use realistic sample data:

### Sales Data Schema
```json
{
  "fields": [
    {"name": "id", "type": "int64"},
    {"name": "product", "type": "string"},
    {"name": "category", "type": "string"},
    {"name": "amount", "type": "float64"},
    {"name": "quantity", "type": "int64"},
    {"name": "date", "type": "string"}
  ]
}
```

### Sample Records
- Electronics: Laptop, Mouse, Keyboard, Monitor, Phone, Tablet, Headphones
- Furniture: Chair, Desk, Lamp
- Various price points and quantities for realistic aggregations

## Performance Expectations

### Typical Response Times
- **Table Creation**: < 1 second
- **Data Insertion**: < 2 seconds per batch
- **Simple Queries**: < 1 second
- **GROUP BY Queries**: < 3 seconds
- **Fault Recovery**: 10-30 seconds

### Resource Usage
- **Memory**: ~2GB total for all services
- **CPU**: Low usage during normal operations
- **Storage**: ~1GB for logs and data
- **Network**: Minimal external traffic

## Next Steps

After running the demonstrations:

1. **Explore the codebase** to understand implementation details
2. **Modify queries** to test different scenarios
3. **Add more data** to test scalability
4. **Experiment with failures** to understand fault tolerance
5. **Monitor metrics** to understand system behavior
6. **Extend functionality** by adding new features

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Docker Compose logs: `docker compose logs`
3. Verify system requirements are met
4. Check network connectivity and port availability