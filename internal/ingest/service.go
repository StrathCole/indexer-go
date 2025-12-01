package ingest

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
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
	ch *db.ClickHouse
	pg *db.Postgres

	mu      sync.RWMutex
	rpc     *rpchttp.HTTP
	nodeRPC string

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

	// Concurrency settings for backfill
	backfillWorkers int
}

func NewService(ch *db.ClickHouse, pg *db.Postgres, nodeRPC string, nodeGRPC string, blockPollInterval time.Duration, backfillInterval time.Duration, backfillBatchSize int64, backfillWorkers int, richlistInterval time.Duration, startHeight int64, endHeight int64, fillGaps bool) (*Service, error) {
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

	// Determine number of workers based on batch size (cap at 8 to avoid overwhelming RPC)
	workers := backfillWorkers
	if workers <= 0 {
		// Auto-calculate based on batch size
		workers = int(backfillBatchSize / 10)
		if workers < 1 {
			workers = 1
		}
		if workers > 8 {
			workers = 8
		}
	}

	return &Service{
		ch:                ch,
		pg:                pg,
		rpc:               rpcClient,
		nodeRPC:           nodeRPC,
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
		backfillWorkers:   workers,
	}, nil
}

func (s *Service) getRPC() *rpchttp.HTTP {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rpc
}

func (s *Service) reconnectRPC() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rpc != nil {
		// Try to stop if running, ignore error
		_ = s.rpc.Stop()
	}

	rpcClient, err := rpchttp.New(s.nodeRPC, "/websocket")
	if err != nil {
		return fmt.Errorf("failed to create RPC client: %w", err)
	}

	if err := rpcClient.Start(); err != nil {
		return fmt.Errorf("failed to start RPC client: %w", err)
	}

	s.rpc = rpcClient
	s.clientCtx = s.clientCtx.WithClient(rpcClient)
	return nil
}

func (s *Service) Start(ctx context.Context) error {
	// Start richlist service
	go s.rich.Start(ctx)

	// RPC client lifecycle is managed by startLiveIngest for robustness

	// Start Live Ingestion
	go s.startLiveIngest(ctx)

	// Start Backfill Ingestion
	go s.startBackfillIngest(ctx)

	<-ctx.Done()
	return nil
}

func (s *Service) startLiveIngest(ctx context.Context) {
	// Stall detection timeout (e.g. 60s without a block)
	stallTimeout := 60 * time.Second
	timer := time.NewTimer(stallTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Ensure RPC is running
		rpc := s.getRPC()
		if rpc == nil || !rpc.IsRunning() {
			if err := s.reconnectRPC(); err != nil {
				log.Printf("Live Ingest: Failed to start RPC client: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			rpc = s.getRPC()
		}

		// Subscribe to NewBlock events
		eventCh, err := rpc.Subscribe(ctx, "indexer-ingest", "tm.event='NewBlock'")
		if err != nil {
			log.Printf("Live Ingest: Failed to subscribe to NewBlock events: %v", err)
			// Try to stop and restart
			if err := s.reconnectRPC(); err != nil {
				log.Printf("Live Ingest: Failed to reconnect RPC: %v", err)
			}
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("Live Ingest: Subscribed to NewBlock events via WebSocket")

		// Reset timer
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(stallTimeout)

		reconnect := false
		for !reconnect {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				log.Printf("Live Ingest: Stalled (no blocks for %v). Reconnecting...", stallTimeout)
				reconnect = true
			case e, ok := <-eventCh:
				if !ok {
					log.Println("Live Ingest: Event channel closed. Reconnecting...")
					reconnect = true
					break
				}

				// Reset timer on every event (heartbeat)
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(stallTimeout)

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

		// Cleanup before reconnecting
		ctxUnsub, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rpc.UnsubscribeAll(ctxUnsub, "indexer-ingest")
		cancel()
		rpc.Stop()
		time.Sleep(1 * time.Second)
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
	// 2. Determine blocks to process based on mode (FillGaps vs Append)
	// 3. Process batch in parallel with worker pool

	rpc := s.getRPC()
	if rpc == nil {
		log.Printf("Backfill: RPC client not available")
		return true
	}

	status, err := rpc.Status(ctx)
	if err != nil {
		log.Printf("Backfill: Failed to get node status: %v", err)
		return true // Treat as synced/wait on error
	}
	latestHeight := status.SyncInfo.LatestBlockHeight

	var blocksToProcess []int64

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
			if endRange > latestHeight {
				endRange = latestHeight
			}

			// Check if chunk is full
			count, err := s.ch.CountBlocksInRange(ctx, s.backfillCursor, endRange)
			if err != nil {
				log.Printf("Backfill: Failed to count blocks: %v", err)
				break
			}

			expected := endRange - s.backfillCursor
			if count >= expected {
				// Chunk is full, skip it
				s.backfillCursor = endRange
				continue
			}

			// Found a chunk with gaps - find the missing blocks
			batchSize := int(s.backfillBatchSize)
			if batchSize <= 0 {
				batchSize = 10
			}

			gaps, err := s.ch.FindGapsInRange(ctx, s.backfillCursor, endRange, batchSize)
			if err != nil {
				log.Printf("Backfill: Failed to find gaps: %v", err)
				break
			}

			if len(gaps) > 0 {
				blocksToProcess = gaps
				// Update cursor to after the last gap we're processing
				s.backfillCursor = gaps[len(gaps)-1] + 1
			} else {
				// No gaps found but count mismatch - move to next chunk
				s.backfillCursor = endRange
			}
			break
		}

		if len(blocksToProcess) == 0 && s.backfillCursor >= latestHeight {
			return true // Synced
		}
	} else {
		// Append mode - process consecutive blocks
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

		if s.backfillCursor > latestHeight {
			return true // Synced
		}

		batchSize := s.backfillBatchSize
		if batchSize <= 0 {
			batchSize = 10
		}
		endBlock := s.backfillCursor + batchSize - 1
		if endBlock > latestHeight {
			endBlock = latestHeight
		}

		// Get existing heights to skip already processed blocks
		existing, err := s.ch.GetExistingHeightsInRange(ctx, s.backfillCursor, endBlock)
		if err != nil {
			log.Printf("Backfill: Failed to check existing blocks: %v", err)
			return true
		}

		for h := s.backfillCursor; h <= endBlock; h++ {
			if !existing[h] {
				blocksToProcess = append(blocksToProcess, h)
			}
		}

		s.backfillCursor = endBlock + 1
	}

	if len(blocksToProcess) == 0 {
		return false // Not synced but no blocks to process in this batch
	}

	log.Printf("Backfill: Processing %d blocks (%d to %d, FillGaps: %v, Workers: %d)",
		len(blocksToProcess), blocksToProcess[0], blocksToProcess[len(blocksToProcess)-1], s.fillGaps, s.backfillWorkers)

	// Process blocks in parallel using worker pool
	s.processBlocksParallel(ctx, blocksToProcess)

	return false // Not synced yet
}

// processBlocksParallel processes multiple blocks concurrently using a worker pool
func (s *Service) processBlocksParallel(ctx context.Context, heights []int64) {
	if len(heights) == 0 {
		return
	}

	numWorkers := s.backfillWorkers
	if numWorkers > len(heights) {
		numWorkers = len(heights)
	}

	// Channel to distribute work
	heightChan := make(chan int64, len(heights))
	for _, h := range heights {
		heightChan <- h
	}
	close(heightChan)

	// Channel to collect results (block data + converted models)
	type blockResult struct {
		height           int64
		block            model.Block
		txs              []model.Tx
		events           []model.Event
		accountTxs       []model.AccountTx
		oraclePrices     []model.OraclePrice
		validatorReturns []model.ValidatorReturn
		blockRewards     []model.BlockReward
		err              error
	}
	resultChan := make(chan blockResult, len(heights))

	// Progress tracking
	var processed int64
	total := int64(len(heights))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for height := range heightChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				result := blockResult{height: height}

				// Fetch and process block
				block, txs, events, accountTxs, oraclePrices, validatorReturns, blockRewards, err := s.fetchAndConvertBlock(height)
				if err != nil {
					result.err = err
				} else {
					result.block = block
					result.txs = txs
					result.events = events
					result.accountTxs = accountTxs
					result.oraclePrices = oraclePrices
					result.validatorReturns = validatorReturns
					result.blockRewards = blockRewards
				}

				resultChan <- result

				// Log progress every 10 blocks
				count := atomic.AddInt64(&processed, 1)
				if count%10 == 0 || count == total {
					log.Printf("Backfill: Progress %d/%d blocks fetched", count, total)
				}
			}
		}(i)
	}

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results and batch insert
	// We collect all successful results first, then batch insert to maintain order integrity
	var allBlocks []model.Block
	var allTxs []model.Tx
	var allEvents []model.Event
	var allAccountTxs []model.AccountTx
	var allOraclePrices []model.OraclePrice
	var allValidatorReturns []model.ValidatorReturn
	var allBlockRewards []model.BlockReward

	for result := range resultChan {
		if result.err != nil {
			log.Printf("Backfill: Failed to process block %d: %v", result.height, result.err)
			continue
		}

		allBlocks = append(allBlocks, result.block)
		allTxs = append(allTxs, result.txs...)
		allEvents = append(allEvents, result.events...)
		allAccountTxs = append(allAccountTxs, result.accountTxs...)
		allOraclePrices = append(allOraclePrices, result.oraclePrices...)
		allValidatorReturns = append(allValidatorReturns, result.validatorReturns...)
		allBlockRewards = append(allBlockRewards, result.blockRewards...)
	}

	// Batch insert all collected data
	if len(allBlocks) > 0 {
		err := s.BatchInsert(ctx, allBlocks, allTxs, allEvents, allAccountTxs, allOraclePrices, allValidatorReturns, allBlockRewards)
		if err != nil {
			log.Printf("Backfill: Failed to batch insert: %v", err)
		}
	}
}

// fetchAndConvertBlock fetches a block from RPC and converts it to model types
func (s *Service) fetchAndConvertBlock(height int64) (
	model.Block,
	[]model.Tx,
	[]model.Event,
	[]model.AccountTx,
	[]model.OraclePrice,
	[]model.ValidatorReturn,
	[]model.BlockReward,
	error,
) {
	var block *coretypes.ResultBlock
	var results *coretypes.ResultBlockResults
	var err error

	rpc := s.getRPC()
	if rpc == nil {
		return model.Block{}, nil, nil, nil, nil, nil, nil, fmt.Errorf("RPC client not available")
	}

	// Retry fetching block and results with exponential backoff for rate limiting
	maxRetries := 10
	baseRetryInterval := 500 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		block, err = rpc.Block(context.Background(), &height)
		if err == nil {
			results, err = rpc.BlockResults(context.Background(), &height)
		}

		if err == nil {
			break
		}

		// Check if it's a rate limit error (429)
		errStr := err.Error()
		isRateLimited := strings.Contains(errStr, "429") || strings.Contains(errStr, "Too Many Requests")

		if i < maxRetries-1 {
			// Exponential backoff: 500ms, 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s
			retryInterval := baseRetryInterval * time.Duration(1<<uint(i))
			if isRateLimited {
				// For rate limiting, use longer backoff (minimum 2 seconds)
				if retryInterval < 2*time.Second {
					retryInterval = 2 * time.Second
				}
				// Cap at 30 seconds
				if retryInterval > 30*time.Second {
					retryInterval = 30 * time.Second
				}
			} else {
				// For other errors, cap at 5 seconds
				if retryInterval > 5*time.Second {
					retryInterval = 5 * time.Second
				}
			}
			time.Sleep(retryInterval)
		}
	}

	if err != nil {
		return model.Block{}, nil, nil, nil, nil, nil, nil, err
	}

	// Decode and convert
	txDecoder := app.MakeEncodingConfig().TxConfig.TxDecoder()

	modelBlock := s.convertBlock(block)

	oraclePrices := s.extractOraclePrices(
		uint64(block.Block.Height),
		block.Block.Time,
		results.EndBlockEvents,
	)

	allBlockEvents := append(results.BeginBlockEvents, results.EndBlockEvents...)
	blockRewards, validatorReturns := s.extractBlockRewardsAndReturns(
		uint64(block.Block.Height),
		block.Block.Time,
		allBlockEvents,
	)

	var modelTxs []model.Tx
	var modelEvents []model.Event
	var modelAccountTxs []model.AccountTx

	beginBlockEvents := s.convertBlockEvents(
		uint64(block.Block.Height),
		block.Block.Time,
		"begin_block",
		results.BeginBlockEvents,
	)
	endBlockEvents := s.convertBlockEvents(
		uint64(block.Block.Height),
		block.Block.Time,
		"end_block",
		results.EndBlockEvents,
	)

	modelEvents = append(modelEvents, beginBlockEvents...)
	modelEvents = append(modelEvents, endBlockEvents...)

	beginBlockAccountTxs, err := s.extractAccountBlockEvents(
		context.Background(),
		uint64(block.Block.Height),
		block.Block.Time,
		"begin_block",
		results.BeginBlockEvents,
	)
	if err != nil {
		log.Printf("Failed to extract begin_block account relations: %v", err)
	} else {
		modelAccountTxs = append(modelAccountTxs, beginBlockAccountTxs...)
	}

	endBlockAccountTxs, err := s.extractAccountBlockEvents(
		context.Background(),
		uint64(block.Block.Height),
		block.Block.Time,
		"end_block",
		results.EndBlockEvents,
	)
	if err != nil {
		log.Printf("Failed to extract end_block account relations: %v", err)
	} else {
		modelAccountTxs = append(modelAccountTxs, endBlockAccountTxs...)
	}

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

	return modelBlock, modelTxs, modelEvents, modelAccountTxs, oraclePrices, validatorReturns, blockRewards, nil
}

func (s *Service) ProcessBlock(height int64) error {
	var block *coretypes.ResultBlock
	var results *coretypes.ResultBlockResults
	var err error

	rpc := s.getRPC()
	if rpc == nil {
		return fmt.Errorf("RPC client not available")
	}

	// Retry fetching block and results
	// Sometimes the node has the block but not the results yet (race condition)
	maxRetries := 5
	retryInterval := 500 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		block, err = rpc.Block(context.Background(), &height)
		if err == nil {
			results, err = rpc.BlockResults(context.Background(), &height)
		}

		if err == nil {
			break
		}

		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}

	if err != nil {
		return err
	}

	// Process
	return s.saveBlock(block, results)
}

func (s *Service) saveBlock(block *coretypes.ResultBlock, results *coretypes.ResultBlockResults) error {
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

	// Extract Block Rewards and Validator Returns
	allBlockEvents := append(results.BeginBlockEvents, results.EndBlockEvents...)
	blockRewards, validatorReturns := s.extractBlockRewardsAndReturns(
		uint64(block.Block.Height),
		block.Block.Time,
		allBlockEvents,
	)

	var modelTxs []model.Tx
	var modelEvents []model.Event
	var modelAccountTxs []model.AccountTx

	// Convert Block Events (BeginBlock & EndBlock)
	beginBlockEvents := s.convertBlockEvents(
		uint64(block.Block.Height),
		block.Block.Time,
		"begin_block",
		results.BeginBlockEvents,
	)
	endBlockEvents := s.convertBlockEvents(
		uint64(block.Block.Height),
		block.Block.Time,
		"end_block",
		results.EndBlockEvents,
	)

	modelEvents = append(modelEvents, beginBlockEvents...)
	modelEvents = append(modelEvents, endBlockEvents...)

	// Extract account activity from block events (stored in account_txs with special index values)
	beginBlockAccountTxs, err := s.extractAccountBlockEvents(
		context.Background(),
		uint64(block.Block.Height),
		block.Block.Time,
		"begin_block",
		results.BeginBlockEvents,
	)
	if err != nil {
		log.Printf("Failed to extract begin_block account relations: %v", err)
	} else {
		modelAccountTxs = append(modelAccountTxs, beginBlockAccountTxs...)
	}

	endBlockAccountTxs, err := s.extractAccountBlockEvents(
		context.Background(),
		uint64(block.Block.Height),
		block.Block.Time,
		"end_block",
		results.EndBlockEvents,
	)
	if err != nil {
		log.Printf("Failed to extract end_block account relations: %v", err)
	} else {
		modelAccountTxs = append(modelAccountTxs, endBlockAccountTxs...)
	}

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

	// Insert everything in one batch
	err = s.BatchInsert(context.Background(), []model.Block{modelBlock}, modelTxs, modelEvents, modelAccountTxs, oraclePrices, validatorReturns, blockRewards)
	if err != nil {
		log.Printf("Failed to insert block/txs: %v", err)
		return err
	}

	return nil
}
