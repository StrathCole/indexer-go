package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/ingest"
)

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	height := flag.Int64("height", 0, "Single block height to reindex")
	fromHeight := flag.Int64("from-height", 0, "Inclusive start height for range reindex")
	toHeight := flag.Int64("to-height", 0, "Inclusive end height for range reindex")
	heightsCSV := flag.String("heights", "", "Comma-separated block heights to reindex")
	heightsFile := flag.String("heights-file", "", "File containing block heights to reindex, one per line")
	deleteTimeout := flag.Duration("delete-timeout", 15*time.Minute, "Timeout for each ClickHouse delete mutation")
	deletePollInterval := flag.Duration("delete-poll-interval", 2*time.Second, "Polling interval while waiting for ClickHouse deletes")
	progressInterval := flag.Duration("progress-interval", 10*time.Second, "How often to log long-running reindex sub-steps")
	batchSize := flag.Int("batch-size", 100, "Number of heights to reindex per batch (1 = legacy per-height mode)")
	fetchWorkers := flag.Int("fetch-workers", 6, "Number of concurrent block fetch/convert workers per chunk")
	skipAggregates := flag.Bool("skip-aggregates", false, "Skip aggregate-table refresh during reindex (faster catch-up; aggregates can be rebuilt later)")
	predeleteRange := flag.Bool("predelete-range", false, "Delete all target heights first (chunked), then reindex without per-chunk deletes")
	predeleteChunkSize := flag.Int("predelete-chunk-size", 1000, "Heights per pre-delete chunk when --predelete-range is enabled")
	dryRun := flag.Bool("dry-run", false, "Fetch and convert target blocks, but do not delete or insert")
	continueOnError := flag.Bool("continue-on-error", true, "Continue reindexing remaining heights after an error")
	flag.Parse()

	targetHeights, err := collectTargetHeights(*height, *fromHeight, *toHeight, *heightsCSV, *heightsFile)
	if err != nil {
		log.Fatalf("Invalid target heights: %v", err)
	}
	if len(targetHeights) == 0 {
		log.Fatalf("Provide --height, --from-height/--to-height, --heights, or --heights-file")
	}
	if *deleteTimeout <= 0 {
		log.Fatalf("--delete-timeout must be > 0")
	}
	if *deletePollInterval <= 0 {
		log.Fatalf("--delete-poll-interval must be > 0")
	}
	if *progressInterval <= 0 {
		log.Fatalf("--progress-interval must be > 0")
	}
	if *batchSize <= 0 {
		log.Fatalf("--batch-size must be > 0")
	}
	if *fetchWorkers <= 0 {
		log.Fatalf("--fetch-workers must be > 0")
	}
	if *predeleteChunkSize <= 0 {
		log.Fatalf("--predelete-chunk-size must be > 0")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ch, err := db.NewClickHouse(
		cfg.Database.ClickHouseAddr,
		cfg.Database.ClickHouseDB,
		cfg.Database.ClickHouseUser,
		cfg.Database.ClickHousePassword,
	)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}

	pg, err := db.NewPostgres(cfg.Database.PostgresConn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}

	svc, err := ingest.NewService(
		ch,
		pg,
		cfg.Node.RPC,
		cfg.Node.GRPC,
		0,
		0,
		1,
		1,
		0,
		0,
		0,
		false,
	)
	if err != nil {
		log.Fatalf("Failed to initialize ingest service: %v", err)
	}

	log.Printf("Reindexing %d block(s), dry_run=%t", len(targetHeights), *dryRun)
	log.Printf("Reindex settings: batch_size=%d fetch_workers=%d skip_aggregates=%t predelete_range=%t predelete_chunk_size=%d", *batchSize, *fetchWorkers, *skipAggregates, *predeleteRange, *predeleteChunkSize)
	if !*dryRun {
		log.Printf("Pause live ingest while this runs; reindex deletes height-scoped rows before reinserting fresh data.")
	}

	if !*dryRun && *predeleteRange {
		log.Printf("Pre-delete phase: deleting existing rows for all %d target heights", len(targetHeights))
		if err := svc.PreDeleteHeightsForReindex(context.Background(), targetHeights, ingest.ReindexOptions{
			DeleteTimeout:      *deleteTimeout,
			DeletePollInterval: *deletePollInterval,
			ProgressInterval:   *progressInterval,
			Progress:           log.Printf,
		}, *predeleteChunkSize); err != nil {
			log.Fatalf("Pre-delete phase failed: %v", err)
		}
		log.Printf("Pre-delete phase complete")
	}

	ctx := context.Background()
	var succeeded int
	var failed int
	runStarted := time.Now()
	total := len(targetHeights)
	for start := 0; start < len(targetHeights); start += *batchSize {
		end := start + *batchSize
		if end > len(targetHeights) {
			end = len(targetHeights)
		}
		chunk := targetHeights[start:end]
		started := time.Now()

		log.Printf("Progress %d/%d (%.1f%%): reindexing chunk heights %d..%d (size=%d)", start+1, total, percent(start, total), chunk[0], chunk[len(chunk)-1], len(chunk))
		summaries, err := svc.ReindexBlocks(ctx, chunk, ingest.ReindexOptions{
			DeleteTimeout:      *deleteTimeout,
			DeletePollInterval: *deletePollInterval,
			ProgressInterval:   *progressInterval,
			FetchWorkers:       *fetchWorkers,
			SkipAggregates:     *skipAggregates,
			SkipDelete:         *predeleteRange,
			DryRun:             *dryRun,
			Progress:           log.Printf,
		})
		if err != nil {
			if !*continueOnError {
				log.Fatalf("Chunk %d..%d failed: %v", chunk[0], chunk[len(chunk)-1], err)
			}

			log.Printf("Chunk %d..%d failed: %v", chunk[0], chunk[len(chunk)-1], err)
			log.Printf("Falling back to per-height mode for this chunk")
			for _, h := range chunk {
				summary, singleErr := svc.ReindexBlock(ctx, h, ingest.ReindexOptions{
					DeleteTimeout:      *deleteTimeout,
					DeletePollInterval: *deletePollInterval,
					ProgressInterval:   *progressInterval,
					FetchWorkers:       *fetchWorkers,
					SkipAggregates:     *skipAggregates,
					SkipDelete:         *predeleteRange,
					DryRun:             *dryRun,
					Progress:           log.Printf,
				})
				if singleErr != nil {
					failed++
					log.Printf("Height %d failed: %v", h, singleErr)
					continue
				}
				succeeded++
				processed := succeeded + failed
				avg := time.Since(runStarted) / time.Duration(processed)
				eta := avg * time.Duration(total-processed)
				log.Printf(
					"Progress %d/%d (%.1f%%): height %d reindexed in fallback mode, eta=%s, txs=%d events=%d block_events=%d tx_events=%d account_txs=%d oracle_prices=%d validator_returns=%d block_rewards=%d",
					processed,
					total,
					percent(processed, total),
					summary.Height,
					eta.Round(time.Second),
					summary.TxCount,
					summary.EventCount,
					summary.BlockEventCount,
					summary.TxEventCount,
					summary.AccountTxCount,
					summary.OraclePriceCount,
					summary.ValidatorCount,
					summary.BlockRewardCount,
				)
			}
			continue
		}

		succeeded += len(summaries)
		if len(summaries) < len(chunk) {
			skipped := len(chunk) - len(summaries)
			failed += skipped
			log.Printf("Chunk %d..%d: skipped %d height(s) due fetch/convert failures", chunk[0], chunk[len(chunk)-1], skipped)
		}
		processed := succeeded + failed
		avg := time.Since(runStarted) / time.Duration(processed)
		eta := avg * time.Duration(total-processed)

		txCount := 0
		eventCount := 0
		blockEventCount := 0
		txEventCount := 0
		accountTxCount := 0
		oraclePriceCount := 0
		validatorCount := 0
		blockRewardCount := 0
		for _, summary := range summaries {
			txCount += summary.TxCount
			eventCount += summary.EventCount
			blockEventCount += summary.BlockEventCount
			txEventCount += summary.TxEventCount
			accountTxCount += summary.AccountTxCount
			oraclePriceCount += summary.OraclePriceCount
			validatorCount += summary.ValidatorCount
			blockRewardCount += summary.BlockRewardCount
		}

		log.Printf(
			"Progress %d/%d (%.1f%%): chunk %d..%d reindexed in %s, eta=%s, txs=%d events=%d block_events=%d tx_events=%d account_txs=%d oracle_prices=%d validator_returns=%d block_rewards=%d",
			processed,
			total,
			percent(processed, total),
			chunk[0],
			chunk[len(chunk)-1],
			time.Since(started),
			eta.Round(time.Second),
			txCount,
			eventCount,
			blockEventCount,
			txEventCount,
			accountTxCount,
			oraclePriceCount,
			validatorCount,
			blockRewardCount,
		)
	}

	log.Printf("Finished. succeeded=%d failed=%d dry_run=%t", succeeded, failed, *dryRun)
}

func collectTargetHeights(singleHeight, fromHeight, toHeight int64, heightsCSV, heightsFile string) ([]int64, error) {
	var heights []int64

	if singleHeight < 0 || fromHeight < 0 || toHeight < 0 {
		return nil, fmt.Errorf("heights must be non-negative")
	}
	if singleHeight > 0 {
		heights = append(heights, singleHeight)
	}
	if fromHeight > 0 || toHeight > 0 {
		if fromHeight <= 0 || toHeight <= 0 {
			return nil, fmt.Errorf("--from-height and --to-height must be used together")
		}
		if fromHeight > toHeight {
			return nil, fmt.Errorf("--from-height %d is greater than --to-height %d", fromHeight, toHeight)
		}
		for h := fromHeight; h <= toHeight; h++ {
			heights = append(heights, h)
		}
	}

	csvHeights, err := parseHeightsCSV(heightsCSV)
	if err != nil {
		return nil, err
	}
	heights = append(heights, csvHeights...)

	fileHeights, err := readHeightsFile(heightsFile)
	if err != nil {
		return nil, err
	}
	heights = append(heights, fileHeights...)

	return uniqSortedHeights(heights), nil
}

func parseHeightsCSV(s string) ([]int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	parts := strings.Split(s, ",")
	heights := make([]int64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		h, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid height %q: %w", part, err)
		}
		if h <= 0 {
			return nil, fmt.Errorf("invalid height %d", h)
		}
		heights = append(heights, h)
	}
	return heights, nil
}

func readHeightsFile(path string) ([]int64, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var heights []int64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		h, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid height %q in file: %w", line, err)
		}
		if h <= 0 {
			return nil, fmt.Errorf("invalid height %d in file", h)
		}
		heights = append(heights, h)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return heights, nil
}

func uniqSortedHeights(heights []int64) []int64 {
	if len(heights) == 0 {
		return nil
	}

	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	out := heights[:0]
	var last int64
	for i, h := range heights {
		if i == 0 || h != last {
			out = append(out, h)
			last = h
		}
	}
	return out
}

func percent(done int, total int) float64 {
	if total == 0 {
		return 100
	}
	return float64(done) * 100 / float64(total)
}
