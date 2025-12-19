package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	// Initialize metadata client (placeholder for now)
	// In production, this would connect to the actual metadata service
	var metadataClient gen.MetadataServiceClient = nil

	// Initialize components
	workerManager := coordinator.NewWorkerManager()
	queryPlanner := coordinator.NewQueryPlannerWithMetadata(metadataClient)
	taskScheduler := coordinator.NewTaskScheduler(workerManager, queryPlanner)
	faultToleranceManager := coordinator.NewFaultToleranceManager(storageClient, taskScheduler, workerManager)

	// Create services
	distributedExecutor := coordinator.NewDistributedQueryExecutor(taskScheduler, queryPlanner, metadataClient, faultToleranceManager)
	queryExecutionService := coordinator.NewQueryExecutionService(taskScheduler, queryPlanner, distributedExecutor)
	tableService := coordinator.NewTableService(metadataClient, storageClient, taskScheduler, queryPlanner)

	// Create gRPC service
	grpcService := coordinator.NewCoordinatorGRPCService(
		workerManager,
		taskScheduler,
		faultToleranceManager,
	)

	// Create REST API
	restAPI := coordinator.NewRestAPI(tableService, queryExecutionService, metadataClient, 8081)

	// Start background services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker health monitoring
	go workerManager.StartHealthCheck(ctx)

	// Start task scheduling
	go taskScheduler.StartScheduling(ctx)

	// Start fault detection
	go faultToleranceManager.StartFaultDetection(ctx)

	// Start distributed query executor background services
	go distributedExecutor.StartBackgroundServices(ctx)

	// Start gRPC server
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	gen.RegisterCoordinatorServiceServer(grpcServer, grpcService)

	log.Println("Coordinator gRPC server listening on :8080")

	// Start gRPC server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Start REST API in background
	go func() {
		if err := restAPI.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to serve REST API: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down coordinator service...")

	// Graceful shutdown
	cancel()

	// Stop REST API
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := restAPI.Stop(shutdownCtx); err != nil {
		log.Printf("Error stopping REST API: %v", err)
	}

	// Stop gRPC server
	grpcServer.GracefulStop()

	log.Println("Coordinator service stopped")
}
