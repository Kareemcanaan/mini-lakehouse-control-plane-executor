# Mini Lakehouse Build Configuration

.PHONY: all clean proto proto-go proto-rust build build-go build-rust test docker

# Default target
all: proto build

# Clean generated files
clean:
	rm -rf proto/gen
	rm -rf worker/src/proto
	go clean ./...
	cargo clean

# Generate protobuf code for both Go and Rust
proto: proto-go proto-rust

# Generate Go protobuf code
proto-go:
	@echo "Generating Go protobuf code..."
	@mkdir -p proto/gen
	protoc --go_out=proto/gen --go_opt=paths=source_relative \
		--go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
		proto/*.proto

# Generate Rust protobuf code
proto-rust:
	@echo "Generating Rust protobuf code..."
	@mkdir -p worker/src/proto
	@cd worker && cargo build

# Build all components
build: build-go build-rust

# Build Go services
build-go: proto-go
	@echo "Building Go services..."
	go build -o bin/coordinator ./cmd/coordinator
	go build -o bin/metad ./cmd/metad

# Build Rust workers
build-rust: proto-rust
	@echo "Building Rust workers..."
	cargo build --release

# Run tests
test: test-go test-rust

test-go:
	@echo "Running Go tests..."
	go test ./...

test-rust:
	@echo "Running Rust tests..."
	cargo test

# Docker targets
docker: docker-coordinator docker-metad docker-worker

docker-coordinator:
	docker build -f docker/Dockerfile.coordinator -t mini-lakehouse/coordinator .

docker-metad:
	docker build -f docker/Dockerfile.metad -t mini-lakehouse/metad .

docker-worker:
	docker build -f docker/Dockerfile.worker -t mini-lakehouse/worker .

# Development environment
dev-up:
	docker-compose up -d

dev-down:
	docker-compose down

# Verify protobuf compilation (for CI)
verify-proto: proto
	@echo "Verifying protobuf compilation..."
	@if [ -d "proto/gen/go" ]; then \
		echo "✓ Protobuf generation successful"; \
	else \
		echo "✗ Protobuf generation failed"; \
		exit 1; \
	fi