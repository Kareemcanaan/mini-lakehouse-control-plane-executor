package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"mini-lakehouse/pkg/metadata"
	"mini-lakehouse/pkg/observability"

	"go.uber.org/zap"
)

func main() {
	// Define flags for backward compatibility
	var (
		nodeIDFlag       = flag.String("node-id", "", "Raft node ID")
		bindAddrFlag     = flag.String("bind-addr", "", "Raft bind address")
		dataDirFlag      = flag.String("data-dir", "/data", "Data directory")
		peersFlag        = flag.String("peers", "", "Comma-separated list of peer addresses")
		grpcPortFlag     = flag.Int("grpc-port", 8080, "gRPC server port")
		grpcBindAddrFlag = flag.String("grpc-bind-addr", "0.0.0.0:8080", "gRPC bind address")
	)
	flag.Parse()

	// Get configuration from environment variables with fallbacks to flags
	nodeID := getEnvOrDefault("RAFT_NODE_ID", *nodeIDFlag)
	bindAddr := getEnvOrDefault("RAFT_BIND_ADDR", *bindAddrFlag)
	dataDir := getEnvOrDefault("DATA_DIR", *dataDirFlag)
	peers := getEnvOrDefault("RAFT_PEERS", *peersFlag)
	grpcBindAddr := getEnvOrDefault("GRPC_BIND_ADDR", *grpcBindAddrFlag)
	grpcPort := getEnvOrDefaultInt("GRPC_PORT", *grpcPortFlag)

	// Validate required configuration
	if nodeID == "" {
		log.Fatal("RAFT_NODE_ID environment variable or -node-id flag is required")
	}
	if bindAddr == "" {
		log.Fatal("RAFT_BIND_ADDR environment variable or -bind-addr flag is required")
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
	if peers != "" {
		peerList = strings.Split(peers, ",")
		// Trim whitespace from each peer
		for i, peer := range peerList {
			peerList[i] = strings.TrimSpace(peer)
		}
	}

	obs.Logger.WithRaft(nodeID, false, 0, 0).Info("Starting metadata service",
		zap.String("bind_addr", bindAddr),
		zap.String("data_dir", dataDir),
		zap.String("grpc_bind_addr", grpcBindAddr))

	// Create service with observability
	service := metadata.NewService(nodeID, bindAddr, dataDir)

	// Start Raft cluster
	if err := service.Start(peerList); err != nil {
		obs.Logger.WithError(err).Error("Failed to start Raft cluster")
		log.Fatalf("Failed to start Raft cluster: %v", err)
	}

	// Start gRPC server in a goroutine
	go func() {
		obs.Logger.Info("Starting gRPC server",
			zap.String("bind_addr", grpcBindAddr),
			zap.Int("port", grpcPort))
		if err := service.StartGRPCServer(grpcPort); err != nil {
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

// getEnvOrDefault returns environment variable value or default value
func getEnvOrDefault(envKey, defaultValue string) string {
	if value := os.Getenv(envKey); value != "" {
		return value
	}
	return defaultValue
}

// getEnvOrDefaultInt returns environment variable value as int or default value
func getEnvOrDefaultInt(envKey string, defaultValue int) int {
	if value := os.Getenv(envKey); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
