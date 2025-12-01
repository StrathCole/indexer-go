package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/ingest"
	"github.com/classic-terra/indexer-go/internal/logger"
	"github.com/rs/zerolog/log"
)

func main() {
	// Load Configuration
	cfg, err := config.LoadConfig(".")
	if err != nil {
		panic(err)
	}

	// Init Logger
	logPath := cfg.Log.FilePath
	if logPath == "" {
		logPath = "logs/indexer-ingest.log"
	}
	logger.Init(cfg.Log.Level, logPath)

	log.Info().Msg("Starting Indexer Ingest Service...")

	// Connect to DBs
	ch, err := db.NewClickHouse(cfg.Database.ClickHouseAddr, cfg.Database.ClickHouseDB, cfg.Database.ClickHouseUser, cfg.Database.ClickHousePassword)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
	}

	pg, err := db.NewPostgres(cfg.Database.PostgresConn)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to Postgres")
	}

	// Create Service
	svc, err := ingest.NewService(ch, pg, cfg.Node.RPC, cfg.Node.GRPC, cfg.Ingest.BlockPollInterval, cfg.Ingest.BackfillInterval, cfg.Ingest.BackfillBatchSize, cfg.Ingest.BackfillWorkers, cfg.Ingest.RichlistUpdateInterval, cfg.Ingest.StartHeight, cfg.Ingest.EndHeight, cfg.Ingest.FillGaps)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create ingest service")
	}

	// Start
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := svc.Start(ctx); err != nil {
			log.Error().Err(err).Msg("Service stopped with error")
		}
	}()

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info().Msg("Shutting down...")
}
