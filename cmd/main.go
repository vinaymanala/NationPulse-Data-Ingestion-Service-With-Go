package main

import (
	"context"
	"fmt"
	"log"

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
	dataIngestionSvc.Serve()

}
