package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/config"
	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/service"
	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/store"
	. "github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/types"
)

func main() {
	fmt.Println("Running go..")

	// Load environment variables from .env for local development
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found or failed to load; relying on environment variables")
	}

	cfg := config.Load()

	// r := gin.Default()
	ctx := context.Background()

	pg := *store.NewPgClient(ctx, cfg)

	configs := &Configs{
		Cfg: cfg,
		Ctx: ctx,
		DB:  &pg,
	}

	dataIngestionSvc := service.NewDataIngestionSvc(configs)
	dataIngestionSvc.Initialize()
	dataIngestionSvc.Serve()
	os.Exit(1)

	// setup a grpc tcp listener no port 50051
	// lis, err := net.Listen("tcp", ":50051")
	// if err != nil {
	// 	log.Fatalf("Failed to listen: %v", err)
	// }
	// grpcServer := grpc.NewServer()
	// pb.RegisterDataIngestionServer(grpcServer, dataIngestionSvc)

	// Run server in a goroutine to allow graceful shutdown
	// go func() {
	// 	log.Printf("Listening to %v", lis.Addr())
	// 	if err := grpcServer.Serve(lis); err != nil {
	// 		log.Fatalf("Failed to serve grpc: %v", err)
	// 	}
	// }()

	// Wait for interrupt signal for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gRPC server...")
	// grpcServer.Stop()
	log.Println("Server stopped")

}
