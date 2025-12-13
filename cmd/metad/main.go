package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"mini-lakehouse/pkg/metadata"
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

	// Parse peers
	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	log.Printf("Starting metadata service with node-id=%s, bind-addr=%s, data-dir=%s", *nodeID, *bindAddr, *dataDir)

	// Create service
	service := metadata.NewService(*nodeID, *bindAddr, *dataDir)

	// Start Raft cluster
	if err := service.Start(peerList); err != nil {
		log.Fatalf("Failed to start Raft cluster: %v", err)
	}

	// Start gRPC server in a goroutine
	go func() {
		log.Printf("Starting gRPC server on port %d", *grpcPort)
		if err := service.StartGRPCServer(*grpcPort); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	// Wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Println("Shutting down metadata service...")
	if err := service.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
