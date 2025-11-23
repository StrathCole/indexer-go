package main

import (
	"crypto/tls"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/classic-terra/core/v3/app"
	"github.com/classic-terra/indexer-go/internal/api"
	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/logger"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Load Configuration
	cfg, err := config.LoadConfig(".")
	if err != nil {
		// Fallback to standard log if config fails
		panic(err)
	}

	// Init Logger
	logPath := cfg.Log.FilePath
	if logPath == "" {
		logPath = "logs/indexer-api.log"
	}
	logger.Init(cfg.Log.Level, logPath)

	log.Info().Msg("Starting Indexer API Service...")

	// Connect to DBs
	ch, err := db.NewClickHouse(cfg.Database.ClickHouseAddr, cfg.Database.ClickHouseDB, cfg.Database.ClickHouseUser, cfg.Database.ClickHousePassword)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
	}

	pg, err := db.NewPostgres(cfg.Database.PostgresConn)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to Postgres")
	}

	// Connect to RPC
	rpcClient, err := rpchttp.New(cfg.Node.RPC, "/websocket")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create RPC client")
	}

	// Connect to gRPC
	var grpcConn *grpc.ClientConn
	if cfg.Node.GRPC != "" {
		grpcURL := cfg.Node.GRPC
		secure := false
		if strings.HasPrefix(grpcURL, "https://") || strings.HasSuffix(grpcURL, ":443") {
			secure = true
		}
		grpcURL = strings.TrimPrefix(grpcURL, "https://")
		grpcURL = strings.TrimPrefix(grpcURL, "http://")

		var creds credentials.TransportCredentials
		if secure {
			creds = credentials.NewTLS(&tls.Config{})
		} else {
			creds = insecure.NewCredentials()
		}

		grpcConn, err = grpc.Dial(grpcURL, grpc.WithTransportCredentials(creds))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to dial gRPC")
		}
	}

	// Create Client Context
	encodingConfig := app.MakeEncodingConfig()
	clientCtx := client.Context{}.
		WithClient(rpcClient).
		WithGRPCClient(grpcConn).
		WithCodec(encodingConfig.Marshaler).
		WithInterfaceRegistry(encodingConfig.InterfaceRegistry).
		WithTxConfig(encodingConfig.TxConfig).
		WithLegacyAmino(encodingConfig.Amino)

	// Create Server
	srv := api.NewServer(ch, pg, clientCtx, rpcClient, cfg.Server.CORSAllowedOrigins, cfg.Server.ExcludedAccounts)

	// Start Server
	go func() {
		log.Info().Msgf("Listening on %s", cfg.Server.ListenAddr)
		if err := http.ListenAndServe(cfg.Server.ListenAddr, srv.Router()); err != nil {
			log.Fatal().Err(err).Msg("Server failed")
		}
	}()

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info().Msg("Shutting down...")
}
