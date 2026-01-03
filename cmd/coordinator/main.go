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
	"mini-lakehouse/pkg/observability"
	"mini-lakehouse/pkg/storage"
	"mini-lakehouse/proto/gen"

	"google.golang.org/grpc"
)

func main() {
	log.Println("Coordinator service starting...")

	// Initialize observability
	obsConfig := observability.DefaultConfig("mini-lakehouse-coordinator")
	obs, err := observability.New(obsConfig)
	if err != nil {
		log.Fatalf("Failed to initialize observability: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start observability services
	if err := obs.Start(ctx); err != nil {
		log.Fatalf("Failed to start observability: %v", err)
	}
	defer obs.Stop(context.Background())

	obs.Logger.Info("Observability initialized successfully")

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
		obs.Logger.WithError(err).Error("Failed to create storage client")
		log.Fatalf("Failed to create storage client: %v", err)
	}

	// Initialize metadata client manager
	metadataEndpoints := []string{
		"localhost:8090", // meta-1
		"localhost:8091", // meta-2
		"localhost:8092", // meta-3
	}
	metadataClientManager := coordinator.NewMetadataClientManager(metadataEndpoints)
	err = metadataClientManager.Start()
	if err != nil {
		obs.Logger.WithError(err).Error("Failed to start metadata client manager")
		log.Fatalf("Failed to start metadata client manager: %v", err)
	}
	defer metadataClientManager.Stop()

	// Initialize components with observability
	workerManager := coordinator.NewWorkerManager()
	queryPlanner := coordinator.NewQueryPlannerWithMetadata(metadataClientManager)
	taskScheduler := coordinator.NewTaskScheduler(workerManager, queryPlanner)
	faultToleranceManager := coordinator.NewFaultToleranceManager(storageClient, taskScheduler, workerManager)

	// Create services
	distributedExecutor := coordinator.NewDistributedQueryExecutor(taskScheduler, queryPlanner, metadataClientManager, faultToleranceManager)
	queryExecutionService := coordinator.NewQueryExecutionService(taskScheduler, queryPlanner, distributedExecutor)
	tableService := coordinator.NewTableService(metadataClientManager, storageClient, taskScheduler, queryPlanner)

	// Create gRPC service
	grpcService := coordinator.NewCoordinatorGRPCService(
		workerManager,
		taskScheduler,
		faultToleranceManager,
	)

	// Create compaction API (optional)
	var compactionAPI *coordinator.CompactionAPI
	// compactionAPI = coordinator.NewCompactionAPI(compactionCoordinator) // Uncomment when compaction is needed

	// Create REST API
	restAPI := coordinator.NewRestAPI(tableService, queryExecutionService, metadataClientManager, 8081)

	// Start background services
	obs.Logger.Info("Starting background services")

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
		obs.Logger.WithError(err).Error("Failed to listen on gRPC port")
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	gen.RegisterCoordinatorServiceServer(grpcServer, grpcService)

	obs.Logger.Info("Coordinator gRPC server listening on :8080")

	// Start gRPC server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			obs.Logger.WithError(err).Error("gRPC server failed")
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Start REST API in background
	go func() {
		if err := restAPI.Start(compactionAPI); err != nil && err != http.ErrServerClosed {
			obs.Logger.WithError(err).Error("REST API server failed")
			log.Fatalf("Failed to serve REST API: %v", err)
		}
	}()

	obs.Logger.Info("All services started successfully")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	obs.Logger.Info("Shutting down coordinator service...")

	// Graceful shutdown
	cancel()

	// Stop REST API
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := restAPI.Stop(shutdownCtx); err != nil {
		obs.Logger.WithError(err).Error("Error stopping REST API")
		log.Printf("Error stopping REST API: %v", err)
	}

	// Stop gRPC server
	grpcServer.GracefulStop()

	obs.Logger.Info("Coordinator service stopped")
}
