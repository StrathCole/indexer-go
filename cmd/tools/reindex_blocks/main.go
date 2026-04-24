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
	if !*dryRun {
		log.Printf("Pause live ingest while this runs; reindex deletes height-scoped rows before reinserting fresh data.")
	}

	ctx := context.Background()
	var succeeded int
	var failed int
	for _, h := range targetHeights {
		started := time.Now()
		summary, err := svc.ReindexBlock(ctx, h, ingest.ReindexOptions{
			DeleteTimeout:      *deleteTimeout,
			DeletePollInterval: *deletePollInterval,
			DryRun:             *dryRun,
		})
		if err != nil {
			failed++
			if *continueOnError {
				log.Printf("Height %d failed: %v", h, err)
				continue
			}
			log.Fatalf("Height %d failed: %v", h, err)
		}

		succeeded++
		log.Printf(
			"Height %d reindexed in %s: txs=%d events=%d account_txs=%d oracle_prices=%d validator_returns=%d block_rewards=%d",
			summary.Height,
			time.Since(started),
			summary.TxCount,
			summary.EventCount,
			summary.AccountTxCount,
			summary.OraclePriceCount,
			summary.ValidatorCount,
			summary.BlockRewardCount,
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
