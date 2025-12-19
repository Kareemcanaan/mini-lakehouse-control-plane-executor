package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"mini-lakehouse/pkg/coordinator"
	"mini-lakehouse/pkg/storage"
	"mini-lakehouse/proto/gen"

	"google.golang.org/grpc"
)

func main() {
	log.Println("Coordinator service starting...")

	// Initialize storage client
	storageConfig := storage.Config{
		Endpoint:        "localhost:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		BucketName:      "lake",
		UseSSL:          false,
	}
	storageClient, err := storage.NewClient(storageConfig)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}

	// Initialize components
	workerManager := coordinator.NewWorkerManager()
	queryPlanner := coordinator.NewQueryPlanner()
	taskScheduler := coordinator.NewTaskScheduler(workerManager, queryPlanner)
	faultToleranceManager := coordinator.NewFaultToleranceManager(storageClient, taskScheduler, workerManager)

	// Create gRPC service
	grpcService := coordinator.NewCoordinatorGRPCService(
		workerManager,
		taskScheduler,
		faultToleranceManager,
	)

	// Create query execution service
	queryExecutionService := coordinator.NewQueryExecutionService(taskScheduler, queryPlanner)
	_ = queryExecutionService // Will be used for REST API or other interfaces

	// Start background services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker health monitoring
	go workerManager.StartHealthCheck(ctx)

	// Start task scheduling
	go taskScheduler.StartScheduling(ctx)

	// Start fault detection
	go faultToleranceManager.StartFaultDetection(ctx)

	// Start gRPC server
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	gen.RegisterCoordinatorServiceServer(grpcServer, grpcService)

	log.Println("Coordinator gRPC server listening on :8080")

	// Start server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down coordinator service...")

	// Graceful shutdown
	cancel()
	grpcServer.GracefulStop()

	log.Println("Coordinator service stopped")
}
