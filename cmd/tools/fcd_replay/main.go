package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/ingest"
	"github.com/classic-terra/indexer-go/internal/model"
)

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	fcdBaseURL := flag.String("fcd-base-url", "https://terra-classic-fcd.publicnode.com", "FCD base URL or /v1 base URL")
	height := flag.Int64("height", 0, "Single block height to replay")
	fromHeight := flag.Int64("from-height", 0, "Inclusive start height for replay range")
	toHeight := flag.Int64("to-height", 0, "Inclusive end height for replay range")
	dryRun := flag.Bool("dry-run", false, "Fetch and convert blocks without inserting data")
	continueOnError := flag.Bool("continue-on-error", true, "Skip failed heights and continue")
	progressEvery := flag.Int("progress-every", 100, "Log a progress line every N processed heights")
	requestTimeout := flag.Duration("request-timeout", 30*time.Second, "Per-block fetch and insert timeout")
	flag.Parse()

	start, end, err := collectTargetRange(*height, *fromHeight, *toHeight)
	if err != nil {
		log.Fatalf("Invalid target range: %v", err)
	}
	if *progressEvery <= 0 {
		log.Fatalf("--progress-every must be > 0")
	}
	if *requestTimeout <= 0 {
		log.Fatalf("--request-timeout must be > 0")
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

	total := end - start + 1
	log.Printf("FCD replay range %d..%d (%d blocks), dry_run=%t", start, end, total, *dryRun)
	log.Printf("Safety rule: any height with existing block marker or tx rows is skipped and never rewritten")

	var replayed int64
	var skipped int64
	var failed int64
	var txsInserted int64
	var eventsInserted int64
	var accountTxsInserted int64

	for offset := int64(0); offset < total; offset++ {
		currentHeight := start + offset
		ctx, cancel := context.WithTimeout(context.Background(), *requestTimeout)

		exists, err := svc.HeightHasStoredData(ctx, currentHeight)
		if err != nil {
			cancel()
			if !*continueOnError {
				log.Fatalf("Height %d: failed to check existing data: %v", currentHeight, err)
			}
			failed++
			log.Printf("Height %d: existing-data check failed: %v", currentHeight, err)
			continue
		}
		if exists {
			cancel()
			skipped++
			if shouldLogProgress(offset+1, total, *progressEvery) {
				log.Printf("Progress %d/%d: skipped=%d replayed=%d failed=%d", offset+1, total, skipped, replayed, failed)
			}
			continue
		}

		block, txs, events, accountTxs, err := svc.FetchAndConvertFCDBlock(ctx, *fcdBaseURL, currentHeight)
		if err != nil {
			cancel()
			if !*continueOnError {
				log.Fatalf("Height %d: fetch/convert failed: %v", currentHeight, err)
			}
			failed++
			log.Printf("Height %d: fetch/convert failed: %v", currentHeight, err)
			continue
		}

		if !*dryRun {
			err = svc.BatchInsert(ctx, []model.Block{block}, txs, events, accountTxs, nil, nil, nil)
			if err != nil {
				cancel()
				if !*continueOnError {
					log.Fatalf("Height %d: insert failed: %v", currentHeight, err)
				}
				failed++
				log.Printf("Height %d: insert failed: %v", currentHeight, err)
				continue
			}
		}

		cancel()
		replayed++
		txsInserted += int64(len(txs))
		eventsInserted += int64(len(events))
		accountTxsInserted += int64(len(accountTxs))

		if shouldLogProgress(offset+1, total, *progressEvery) {
			log.Printf(
				"Progress %d/%d: skipped=%d replayed=%d failed=%d txs=%d events=%d account_txs=%d",
				offset+1,
				total,
				skipped,
				replayed,
				failed,
				txsInserted,
				eventsInserted,
				accountTxsInserted,
			)
		}
	}

	log.Printf(
		"Finished FCD replay. range=%d..%d replayed=%d skipped=%d failed=%d dry_run=%t txs=%d events=%d account_txs=%d",
		start,
		end,
		replayed,
		skipped,
		failed,
		*dryRun,
		txsInserted,
		eventsInserted,
		accountTxsInserted,
	)
}

func collectTargetRange(singleHeight, fromHeight, toHeight int64) (int64, int64, error) {
	if singleHeight < 0 || fromHeight < 0 || toHeight < 0 {
		return 0, 0, fmt.Errorf("heights must be non-negative")
	}
	if singleHeight > 0 && (fromHeight > 0 || toHeight > 0) {
		return 0, 0, fmt.Errorf("use either --height or --from-height/--to-height")
	}
	if singleHeight > 0 {
		return singleHeight, singleHeight, nil
	}
	if fromHeight <= 0 || toHeight <= 0 {
		return 0, 0, fmt.Errorf("provide --height or both --from-height and --to-height")
	}
	if fromHeight > toHeight {
		return 0, 0, fmt.Errorf("--from-height %d is greater than --to-height %d", fromHeight, toHeight)
	}
	return fromHeight, toHeight, nil
}

func shouldLogProgress(processed, total int64, every int) bool {
	if processed == total {
		return true
	}
	return processed%int64(every) == 0
}