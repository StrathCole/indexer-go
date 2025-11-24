package ingest

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/model"

	// Cosmos SDK imports
	"github.com/cosmos/cosmos-sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"

	// We might need to use the CometBFT RPC client for blocks if gRPC doesn't provide everything
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	"github.com/classic-terra/core/v3/app"
	tmtypes "github.com/cometbft/cometbft/types"
)

type Service struct {
	ch  *db.ClickHouse
	pg  *db.Postgres
	rpc *rpchttp.HTTP

	clientCtx client.Context

	dims *Dimensions
	rich *RichlistService
	tax  *TaxCalculator

	// gRPC connection
	grpcConn *grpc.ClientConn

	blockPollInterval time.Duration
	backfillInterval  time.Duration
	backfillBatchSize int64
	startHeight       int64
	endHeight         int64
	fillGaps          bool
	backfillCursor    int64
}

func NewService(ch *db.ClickHouse, pg *db.Postgres, nodeRPC string, nodeGRPC string, blockPollInterval time.Duration, backfillInterval time.Duration, backfillBatchSize int64, richlistInterval time.Duration, startHeight int64, endHeight int64, fillGaps bool) (*Service, error) {
	// Initialize the encoding config
	encCfg := app.MakeEncodingConfig()

	rpcClient, err := rpchttp.New(nodeRPC, "/websocket")
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	var grpcConn *grpc.ClientConn
	if nodeGRPC != "" {
		grpcURL := nodeGRPC

		// Ensure scheme for parsing
		if !strings.Contains(grpcURL, "://") {
			grpcURL = "https://" + grpcURL
		}

		u, err := url.Parse(grpcURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse gRPC URL: %w", err)
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
			return nil, fmt.Errorf("failed to dial gRPC: %w", err)
		}
	} // Initialize client context
	clientCtx := client.Context{}.
		WithCodec(encCfg.Marshaler).
		WithInterfaceRegistry(encCfg.InterfaceRegistry).
		WithTxConfig(encCfg.TxConfig).
		WithLegacyAmino(encCfg.Amino).
		WithClient(rpcClient).
		WithGRPCClient(grpcConn)

	return &Service{
		ch:                ch,
		pg:                pg,
		rpc:               rpcClient,
		clientCtx:         clientCtx,
		grpcConn:          grpcConn,
		dims:              NewDimensions(pg),
		rich:              NewRichlistService(pg, clientCtx, richlistInterval),
		tax:               NewTaxCalculator(grpcConn),
		blockPollInterval: blockPollInterval,
		backfillInterval:  backfillInterval,
		backfillBatchSize: backfillBatchSize,
		startHeight:       startHeight,
		endHeight:         endHeight,
		fillGaps:          fillGaps,
		backfillCursor:    startHeight,
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	// Start richlist service
	go s.rich.Start(ctx)

	// Start RPC client for WebSocket
	if err := s.rpc.Start(); err != nil {
		log.Printf("Failed to start RPC client for WebSocket: %v", err)
		// We continue, as polling will still work
	} else {
		defer s.rpc.Stop()
	}

	// Start Live Ingestion
	go s.startLiveIngest(ctx)

	// Start Backfill Ingestion
	go s.startBackfillIngest(ctx)

	<-ctx.Done()
	return nil
}

func (s *Service) startLiveIngest(ctx context.Context) {
	// Subscribe to NewBlock events
	var eventCh <-chan coretypes.ResultEvent
	if s.rpc.IsRunning() {
		var err error
		eventCh, err = s.rpc.Subscribe(ctx, "indexer-ingest", "tm.event='NewBlock'")
		if err != nil {
			log.Printf("Failed to subscribe to NewBlock events: %v", err)
		} else {
			log.Println("Subscribed to NewBlock events via WebSocket")
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-eventCh:
			data, ok := e.Data.(tmtypes.EventDataNewBlock)
			if !ok {
				continue
			}
			height := data.Block.Height
			log.Printf("Live Ingest: New block %d", height)

			// Check if exists first
			exists, err := s.ch.BlockExists(ctx, height)
			if err != nil {
				log.Printf("Live Ingest: Failed to check block %d: %v", height, err)
				continue
			}
			if exists {
				continue
			}

			if err := s.ProcessBlock(height); err != nil {
				log.Printf("Live Ingest: Failed to process block %d: %v", height, err)
			}
		}
	}
}

func (s *Service) startBackfillIngest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Perform backfill step
		synced := s.backfillStep(ctx)

		if synced {
			// If synced, wait for blockPollInterval before checking again
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.blockPollInterval):
			}
		} else {
			// If not synced, wait for backfillInterval (rate limit)
			if s.backfillInterval > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(s.backfillInterval):
				}
			}
		}
	}
}

func (s *Service) backfillStep(ctx context.Context) bool {
	// Strategy:
	// 1. Get latest height from RPC (target)
	// 2. Determine startBlock based on mode (FillGaps vs Append)
	// 3. Process batch

	status, err := s.rpc.Status(ctx)
	if err != nil {
		log.Printf("Backfill: Failed to get node status: %v", err)
		return true // Treat as synced/wait on error
	}
	latestHeight := status.SyncInfo.LatestBlockHeight

	var startBlock int64

	if s.fillGaps {
		if s.backfillCursor == 0 {
			if s.startHeight > 0 {
				s.backfillCursor = s.startHeight
			} else {
				s.backfillCursor = 1
			}
		}

		// Optimization: Skip full chunks
		chunkSize := int64(10000)
		for s.backfillCursor < latestHeight {
			endRange := s.backfillCursor + chunkSize
			// Check if chunk is full
			count, err := s.ch.CountBlocksInRange(ctx, s.backfillCursor, endRange)
			if err != nil {
				log.Printf("Backfill: Failed to count blocks: %v", err)
				break
			}

			expected := endRange - s.backfillCursor
			// If we are near the tip, expected count might be less if blocks are not produced yet?
			// But latestHeight is fixed for this step.
			if endRange > latestHeight {
				expected = latestHeight - s.backfillCursor + 1
			}

			if count >= expected {
				// Chunk is full, skip it
				s.backfillCursor = endRange
				continue
			} else {
				// Gap is in this chunk
				// Use FindNextGap restricted to this chunk
				nextGap, err := s.ch.FindNextGap(ctx, s.backfillCursor, endRange)
				if err != nil {
					log.Printf("Backfill: Failed to find next gap: %v", err)
				} else if nextGap > 0 {
					s.backfillCursor = nextGap
				} else {
					// No internal gap found, but count mismatch.
					// This means the gap is at the end of the chunk (tail gap).
					// Find max height in this range to skip the contiguous block.
					maxH, err := s.ch.GetMaxHeightInRange(ctx, s.backfillCursor, endRange)
					if err == nil && maxH > 0 {
						s.backfillCursor = maxH + 1
					}
				}
				break
			}
		}

		startBlock = s.backfillCursor
	} else {
		// If backfillCursor is not set (0), initialize it from DB
		if s.backfillCursor == 0 {
			maxHeight, err := s.ch.GetMaxHeight(ctx)
			if err != nil {
				log.Printf("Backfill: Failed to get max height: %v", err)
				return true
			}

			if maxHeight == 0 {
				if s.startHeight > 0 {
					s.backfillCursor = s.startHeight
				} else {
					s.backfillCursor = 1 // Genesis
				}
			} else {
				s.backfillCursor = maxHeight + 1
			}
		}
		startBlock = s.backfillCursor
	}

	if startBlock > latestHeight {
		// Synced
		return true
	}

	// Process a batch or single block
	// Let's process up to backfillBatchSize blocks per tick to catch up faster, but respect rate limits
	batchSize := s.backfillBatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	endBlock := startBlock + int64(batchSize) - 1
	if endBlock > latestHeight {
		endBlock = latestHeight
	}

	log.Printf("Backfill: Syncing range %d to %d (FillGaps: %v)", startBlock, endBlock, s.fillGaps)

	for h := startBlock; h <= endBlock; h++ {
		// Check if exists (Live ingest might have caught it)
		exists, err := s.ch.BlockExists(ctx, h)
		if err != nil {
			log.Printf("Backfill: Failed to check block %d: %v", h, err)
			break
		}
		if exists {
			continue
		}

		if err := s.ProcessBlock(h); err != nil {
			log.Printf("Backfill: Failed to process block %d: %v", h, err)
			break
		}
	}

	s.backfillCursor = endBlock + 1

	return false // Not synced yet
}

func (s *Service) fetchBlock(ctx context.Context, height int64) (*coretypes.ResultBlock, *coretypes.ResultBlockResults, error) {
	var block *coretypes.ResultBlock
	var results *coretypes.ResultBlockResults
	var err error

	// Retry fetching block and results
	// Sometimes the node has the block but not the results yet (race condition)
	maxRetries := 3 // Reduced retries to fail faster
	retryInterval := 200 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		// Check context before making request
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		block, err = s.rpc.Block(ctx, &height)
		if err == nil {
			results, err = s.rpc.BlockResults(ctx, &height)
		}

		if err == nil {
			break
		}

		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(retryInterval):
			}
		}
	}

	if err != nil {
		return nil, nil, err
	}
	return block, results, nil
}

func (s *Service) ProcessBlock(height int64) error {
	block, results, err := s.fetchBlock(context.Background(), height)
	if err != nil {
		return err
	}

	// Process
	modelBlock, modelTxs, modelEvents, modelAccountTxs, oraclePrices, err := s.processBlockData(block, results)
	if err != nil {
		return err
	}

	// Insert everything in one batch
	return s.BatchInsert(context.Background(), []model.Block{modelBlock}, modelTxs, modelEvents, modelAccountTxs, oraclePrices)
}

func (s *Service) processBlockData(block *coretypes.ResultBlock, results *coretypes.ResultBlockResults) (model.Block, []model.Tx, []model.Event, []model.AccountTx, []model.OraclePrice, error) {
	// Decode transactions
	txDecoder := app.MakeEncodingConfig().TxConfig.TxDecoder()

	// Convert Block
	modelBlock := s.convertBlock(block)

	// Extract Oracle Prices from EndBlock events
	oraclePrices := s.extractOraclePrices(
		uint64(block.Block.Height),
		block.Block.Time,
		results.EndBlockEvents,
	)

	var modelTxs []model.Tx
	var modelEvents []model.Event
	var modelAccountTxs []model.AccountTx

	// Convert Block Events (BeginBlock & EndBlock)
	beginBlockEvents := s.convertBlockEvents(
		uint64(block.Block.Height),
		block.Block.Time,
		results.BeginBlockEvents,
	)
	endBlockEvents := s.convertBlockEvents(
		uint64(block.Block.Height),
		block.Block.Time,
		results.EndBlockEvents,
	)

	modelEvents = append(modelEvents, beginBlockEvents...)
	modelEvents = append(modelEvents, endBlockEvents...)

	for i, txBytes := range block.Block.Txs {
		decodedTx, err := txDecoder(txBytes)
		if err != nil {
			log.Printf("Failed to decode tx at height %d index %d: %v", block.Block.Height, i, err)
			continue
		}

		txHash := fmt.Sprintf("%X", tmtypes.Tx(txBytes).Hash())

		modelTx, events, accountTxs, err := s.convertTx(
			uint64(block.Block.Height),
			uint16(i),
			block.Block.Time,
			txHash,
			decodedTx,
			results,
		)
		if err != nil {
			log.Printf("Failed to convert tx %s: %v", txHash, err)
			continue
		}

		modelTxs = append(modelTxs, *modelTx)
		modelEvents = append(modelEvents, events...)
		modelAccountTxs = append(modelAccountTxs, accountTxs...)
	}

	return modelBlock, modelTxs, modelEvents, modelAccountTxs, oraclePrices, nil
}
