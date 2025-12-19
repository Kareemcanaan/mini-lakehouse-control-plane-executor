fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate proto files for worker and coordinator services
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                "../proto/common.proto",
                "../proto/worker.proto", 
                "../proto/coordinator.proto"
            ],
            &["../proto"],
        )?;

    println!("cargo:rerun-if-changed=../proto/common.proto");
    println!("cargo:rerun-if-changed=../proto/metadata.proto");
    println!("cargo:rerun-if-changed=../proto/coordinator.proto");
    println!("cargo:rerun-if-changed=../proto/worker.proto");
    
    Ok(())
}