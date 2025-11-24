package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
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

		// Ensure scheme for parsing
		if !strings.Contains(grpcURL, "://") {
			grpcURL = "https://" + grpcURL
		}

		u, err := url.Parse(grpcURL)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to parse gRPC URL")
		}

		target := u.Host
		if !strings.Contains(target, ":") {
			if u.Scheme == "https" {
				target = target + ":443"
			} else {
				target = target + ":80"
			}
		}

		secure := u.Scheme == "https" || strings.HasSuffix(target, ":443")

		var creds credentials.TransportCredentials
		if secure {
			creds = credentials.NewTLS(&tls.Config{
				ServerName: u.Hostname(),
			})
		} else {
			creds = insecure.NewCredentials()
		}

		dialOpts := []grpc.DialOption{
			grpc.WithTransportCredentials(creds),
		}

		// If path is present, use it as a prefix for method calls (path-based routing)
		if len(u.Path) > 0 && u.Path != "/" {
			pathPrefix := u.Path
			if !strings.HasPrefix(pathPrefix, "/") {
				pathPrefix = "/" + pathPrefix
			}
			pathPrefix = strings.TrimSuffix(pathPrefix, "/")

			interceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoker(ctx, pathPrefix+method, req, reply, cc, opts...)
			}
			dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(interceptor))
		}

		// Force authority to match hostname (without port) to satisfy strict Nginx/Cloudflare checks
		dialOpts = append(dialOpts, grpc.WithAuthority(u.Hostname()))

		grpcConn, err = grpc.Dial(target, dialOpts...)
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
