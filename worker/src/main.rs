use std::env;
use tonic::transport::Server;
use tracing::info;

mod parquet_reader;
mod parquet_writer;
mod task_executor;
mod worker_service;
mod proto;
mod observability;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize OpenTelemetry tracing
    let tracing_config = observability::TracingConfig::default();
    if let Err(e) = observability::init_tracing(tracing_config) {
        eprintln!("Failed to initialize tracing: {}", e);
        // Continue without tracing rather than failing
        observability::WorkerLogger::init();
    }
    
    info!("Worker service starting...");

    // Get configuration from environment
    let worker_id = env::var("WORKER_ID").unwrap_or_else(|_| "worker-1".to_string());
    let worker_address = env::var("WORKER_ADDRESS").unwrap_or_else(|_| "0.0.0.0:50051".to_string());
    let coordinator_endpoint = env::var("COORDINATOR_ENDPOINT")
        .unwrap_or_else(|_| "http://coordinator:8080".to_string());

    info!("Worker ID: {}", worker_id);
    info!("Worker Address: {}", worker_address);
    info!("Coordinator Endpoint: {}", coordinator_endpoint);

    // Initialize metrics
    let metrics = std::sync::Arc::new(observability::WorkerMetrics::new());

    // Create worker service with metrics
    let mut worker_service = worker_service::WorkerServiceImpl::new_with_metrics(worker_id, metrics.clone());

    // Register with coordinator
    if let Err(e) = worker_service.register_with_coordinator(coordinator_endpoint).await {
        tracing::error!("Failed to register with coordinator: {}", e);
        return Err(e);
    }

    // Start heartbeat
    if let Err(e) = worker_service.start_heartbeat().await {
        tracing::error!("Failed to start heartbeat: {}", e);
        return Err(e);
    }

    // Start metrics server
    let metrics_server = start_metrics_server(metrics.clone());

    // Start gRPC server
    let addr = worker_address.parse()?;
    info!("Worker gRPC server listening on {}", addr);

    tokio::select! {
        result = Server::builder()
            .add_service(proto::worker::worker_service_server::WorkerServiceServer::new(worker_service))
            .serve(addr) => {
            result?;
        }
        result = metrics_server => {
            result?;
        }
    }

    // Shutdown tracing
    observability::shutdown_tracing();

    Ok(())
}

async fn start_metrics_server(metrics: std::sync::Arc<observability::WorkerMetrics>) -> Result<(), Box<dyn std::error::Error>> {
    use prometheus::{Encoder, TextEncoder};
    use std::convert::Infallible;
    use warp::Filter;

    let metrics_route = warp::path("metrics")
        .map(move || {
            let encoder = TextEncoder::new();
            let metric_families = metrics.registry().gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            String::from_utf8(buffer).unwrap()
        });

    let health_route = warp::path("health")
        .map(|| "OK");

    let routes = metrics_route.or(health_route);

    info!("Starting metrics server on :9091");
    warp::serve(routes)
        .run(([0, 0, 0, 0], 9091))
        .await;

    Ok(())
}