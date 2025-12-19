use std::env;
use tonic::transport::Server;
use tracing::info;

mod parquet_reader;
mod parquet_writer;
mod task_executor;
mod worker_service;
mod proto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    info!("Worker service starting...");

    // Get configuration from environment
    let worker_id = env::var("WORKER_ID").unwrap_or_else(|_| "worker-1".to_string());
    let worker_address = env::var("WORKER_ADDRESS").unwrap_or_else(|_| "0.0.0.0:50051".to_string());
    let coordinator_endpoint = env::var("COORDINATOR_ENDPOINT")
        .unwrap_or_else(|_| "http://coordinator:8080".to_string());

    info!("Worker ID: {}", worker_id);
    info!("Worker Address: {}", worker_address);
    info!("Coordinator Endpoint: {}", coordinator_endpoint);

    // Create worker service
    let mut worker_service = worker_service::WorkerServiceImpl::new(worker_id);

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

    // Start gRPC server
    let addr = worker_address.parse()?;
    info!("Worker gRPC server listening on {}", addr);

    Server::builder()
        .add_service(proto::worker::worker_service_server::WorkerServiceServer::new(worker_service))
        .serve(addr)
        .await?;

    Ok(())
}