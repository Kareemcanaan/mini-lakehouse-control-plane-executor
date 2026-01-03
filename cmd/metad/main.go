package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"mini-lakehouse/pkg/metadata"
	"mini-lakehouse/pkg/observability"

	"go.uber.org/zap"
)

func main() {
	var (
		nodeID   = flag.String("node-id", "", "Raft node ID")
		bindAddr = flag.String("bind-addr", "", "Raft bind address")
		dataDir  = flag.String("data-dir", "", "Data directory")
		peers    = flag.String("peers", "", "Comma-separated list of peer addresses")
		grpcPort = flag.Int("grpc-port", 8080, "gRPC server port")
	)
	flag.Parse()

	// Validate required flags
	if *nodeID == "" {
		log.Fatal("node-id is required")
	}
	if *bindAddr == "" {
		log.Fatal("bind-addr is required")
	}
	if *dataDir == "" {
		log.Fatal("data-dir is required")
	}

	// Initialize observability
	obsConfig := observability.DefaultConfig("mini-lakehouse-metadata")
	obsConfig.Metrics.Port = 9092 // Different port for metadata service
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

	// Parse peers
	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	obs.Logger.WithRaft(*nodeID, false, 0, 0).Info("Starting metadata service",
		zap.String("bind_addr", *bindAddr),
		zap.String("data_dir", *dataDir))

	// Create service with observability
	service := metadata.NewService(*nodeID, *bindAddr, *dataDir)

	// Start Raft cluster
	if err := service.Start(peerList); err != nil {
		obs.Logger.WithError(err).Error("Failed to start Raft cluster")
		log.Fatalf("Failed to start Raft cluster: %v", err)
	}

	// Start gRPC server in a goroutine
	go func() {
		obs.Logger.Info("Starting gRPC server",
			zap.Int("port", *grpcPort))
		if err := service.StartGRPCServer(*grpcPort); err != nil {
			obs.Logger.WithError(err).Error("Failed to start gRPC server")
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	obs.Logger.Info("Metadata service started successfully")

	// Wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	obs.Logger.Info("Shutting down metadata service...")
	if err := service.Stop(); err != nil {
		obs.Logger.WithError(err).Error("Error during shutdown")
		log.Printf("Error during shutdown: %v", err)
	}
	obs.Logger.Info("Metadata service stopped")
}
