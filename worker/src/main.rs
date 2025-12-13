use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::init();
    
    info!("Worker service starting...");
    // Implementation will be added in later tasks
    
    Ok(())
}