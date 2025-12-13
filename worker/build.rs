fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/proto")
        .compile(
            &[
                "../proto/metadata.proto",
                "../proto/coordinator.proto", 
                "../proto/worker.proto"
            ],
            &["../proto"],
        )?;
    Ok(())
}