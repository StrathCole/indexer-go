package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/ingest"
	"github.com/classic-terra/indexer-go/internal/model"
)

type repairPlan struct {
	height uint64
	txs    []model.Tx
	events []model.Event
}

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	fromHeight := flag.Uint64("from-height", 0, "Optional inclusive lower height bound")
	toHeight := flag.Uint64("to-height", 0, "Optional inclusive upper height bound")
	scanBatchSize := flag.Int("scan-batch-size", 500, "Number of heights to scan per ClickHouse query")
	rewriteBatchSize := flag.Int("rewrite-batch-size", 25, "Number of heights to rewrite per delete+insert batch")
	maxHeights := flag.Int("max-heights", 0, "Optional cap on affected heights to process in one run")
	rewriteEvents := flag.Bool("rewrite-events", true, "Also rewrite events and tx_event_lookup for affected heights")
	dryRun := flag.Bool("dry-run", false, "Scan and refetch only; do not mutate ClickHouse")
	continueOnError := flag.Bool("continue-on-error", true, "Skip individual heights that fail RPC refetch/conversion")
	deleteTimeout := flag.Duration("delete-timeout", 15*time.Minute, "Timeout for ClickHouse delete mutations per table batch")
	deletePollInterval := flag.Duration("delete-poll-interval", 2*time.Second, "Polling interval while waiting for ClickHouse deletes")
	flag.Parse()

	if *scanBatchSize <= 0 {
		log.Fatalf("--scan-batch-size must be > 0")
	}
	if *rewriteBatchSize <= 0 {
		log.Fatalf("--rewrite-batch-size must be > 0")
	}
	if *deletePollInterval <= 0 {
		log.Fatalf("--delete-poll-interval must be > 0")
	}
	if *deleteTimeout <= 0 {
		log.Fatalf("--delete-timeout must be > 0")
	}
	if *toHeight != 0 && *fromHeight != 0 && *fromHeight > *toHeight {
		log.Fatalf("invalid height range: from-height %d > to-height %d", *fromHeight, *toHeight)
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

	ctx := context.Background()
	hasLookupTable := false
	if *rewriteEvents {
		hasLookupTable, err = tableExists(ctx, ch, "tx_event_lookup")
		if err != nil {
			log.Fatalf("Failed to check tx_event_lookup existence: %v", err)
		}
	}

	cursor := uint64(0)
	if *fromHeight > 0 {
		cursor = *fromHeight - 1
	}

	var scannedHeights int
	var repairedHeights int
	var repairedTxs int
	var repairedEvents int

	for {
		limit := *scanBatchSize
		if *maxHeights > 0 {
			remaining := *maxHeights - scannedHeights
			if remaining <= 0 {
				break
			}
			if remaining < limit {
				limit = remaining
			}
		}

		heights, err := nextMissingHeights(ctx, ch, cursor, *fromHeight, *toHeight, limit)
		if err != nil {
			log.Fatalf("Failed to scan missing tx payload heights: %v", err)
		}
		if len(heights) == 0 {
			break
		}

		cursor = heights[len(heights)-1]
		scannedHeights += len(heights)
		log.Printf("Found %d affected heights (%d..%d)", len(heights), heights[0], heights[len(heights)-1])

		for start := 0; start < len(heights); start += *rewriteBatchSize {
			end := start + *rewriteBatchSize
			if end > len(heights) {
				end = len(heights)
			}

			plans := make([]repairPlan, 0, end-start)
			for _, height := range heights[start:end] {
				block, txs, events, _, _, _, _, err := svc.FetchAndConvertBlock(int64(height))
				if err != nil {
					if *continueOnError {
						log.Printf("Height %d: refetch failed: %v", height, err)
						continue
					}
					log.Fatalf("Height %d: refetch failed: %v", height, err)
				}
				if block.TxCount == 0 || len(txs) == 0 {
					msg := fmt.Sprintf("height %d refetched without tx rows", height)
					if *continueOnError {
						log.Printf("Skipping: %s", msg)
						continue
					}
					log.Fatal(msg)
				}
				if int(block.TxCount) != len(txs) {
					msg := fmt.Sprintf("height %d tx count mismatch: block=%d converted=%d", height, block.TxCount, len(txs))
					if *continueOnError {
						log.Printf("Skipping: %s", msg)
						continue
					}
					log.Fatal(msg)
				}

				plans = append(plans, repairPlan{height: height, txs: txs, events: events})
			}

			if len(plans) == 0 {
				continue
			}

			batchTxs, batchEvents := summarizePlans(plans)
			if *dryRun {
				log.Printf("Dry-run: would rewrite %d heights, %d txs, %d events", len(plans), batchTxs, batchEvents)
				repairedHeights += len(plans)
				repairedTxs += batchTxs
				repairedEvents += batchEvents
				continue
			}

			log.Printf("Rewriting %d heights, %d txs, %d events", len(plans), batchTxs, batchEvents)
			if err := rewriteBatch(ctx, ch, svc, plans, *rewriteEvents, hasLookupTable, *deleteTimeout, *deletePollInterval); err != nil {
				log.Fatalf("Failed to rewrite batch starting at height %d: %v", plans[0].height, err)
			}

			repairedHeights += len(plans)
			repairedTxs += batchTxs
			repairedEvents += batchEvents
		}
	}

	log.Printf("Finished. scanned_heights=%d repaired_heights=%d repaired_txs=%d repaired_events=%d dry_run=%t", scannedHeights, repairedHeights, repairedTxs, repairedEvents, *dryRun)
	if *rewriteEvents {
		log.Printf("Note: this repairs events only for heights selected from missing tx payload rows. Heights with valid tx payloads but stale attr_index still need explicit event refetch.")
	}
	log.Printf("Selection rule: txs where tx_bytes = ''")
}

func nextMissingHeights(ctx context.Context, ch *db.ClickHouse, after, fromHeight, toHeight uint64, limit int) ([]uint64, error) {
	query := `
SELECT DISTINCT height
FROM txs
WHERE tx_bytes = '' AND height > ?`
	args := []any{after}

	if fromHeight > 0 {
		query += ` AND height >= ?`
		args = append(args, fromHeight)
	}
	if toHeight > 0 {
		query += ` AND height <= ?`
		args = append(args, toHeight)
	}

	query += ` ORDER BY height LIMIT ?`
	args = append(args, limit)

	var heights []uint64
	if err := ch.Conn.Select(ctx, &heights, query, args...); err != nil {
		return nil, err
	}
	return heights, nil
}

func rewriteBatch(
	ctx context.Context,
	ch *db.ClickHouse,
	svc *ingest.Service,
	plans []repairPlan,
	rewriteEvents bool,
	hasLookupTable bool,
	deleteTimeout time.Duration,
	deletePollInterval time.Duration,
) error {
	heights := collectHeights(plans)
	if err := deleteHeights(ctx, ch, "txs", heights, deleteTimeout, deletePollInterval); err != nil {
		return err
	}

	var allTxs []model.Tx
	var allEvents []model.Event
	for _, plan := range plans {
		allTxs = append(allTxs, plan.txs...)
		if rewriteEvents {
			allEvents = append(allEvents, plan.events...)
		}
	}

	if rewriteEvents {
		if err := deleteHeights(ctx, ch, "events", heights, deleteTimeout, deletePollInterval); err != nil {
			return err
		}
		if hasLookupTable {
			if err := deleteHeights(ctx, ch, "tx_event_lookup", heights, deleteTimeout, deletePollInterval); err != nil {
				return err
			}
		}
	}

	if err := svc.BatchInsert(ctx, nil, allTxs, allEvents, nil, nil, nil, nil); err != nil {
		return fmt.Errorf("insert repaired rows: %w", err)
	}

	return nil
}

func deleteHeights(ctx context.Context, ch *db.ClickHouse, table string, heights []uint64, timeout time.Duration, pollInterval time.Duration) error {
	if len(heights) == 0 {
		return nil
	}

	deleteQuery := fmt.Sprintf("ALTER TABLE %s DELETE WHERE height IN (%s)", table, joinUint64s(heights))
	if err := ch.Conn.Exec(ctx, deleteQuery); err != nil {
		return fmt.Errorf("delete from %s: %w", table, err)
	}

	deadline := time.Now().Add(timeout)
	for {
		remaining, err := countRowsForHeights(ctx, ch, table, heights)
		if err != nil {
			return fmt.Errorf("count %s after delete: %w", table, err)
		}
		if remaining == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for delete on %s; %d rows still visible", table, remaining)
		}
		time.Sleep(pollInterval)
	}
}

func countRowsForHeights(ctx context.Context, ch *db.ClickHouse, table string, heights []uint64) (uint64, error) {
	query := fmt.Sprintf("SELECT count() FROM %s WHERE height IN (%s)", table, joinUint64s(heights))
	var count uint64
	if err := ch.Conn.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func tableExists(ctx context.Context, ch *db.ClickHouse, table string) (bool, error) {
	query := fmt.Sprintf("EXISTS TABLE %s", table)
	var exists uint8
	if err := ch.Conn.QueryRow(ctx, query).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

func collectHeights(plans []repairPlan) []uint64 {
	heights := make([]uint64, 0, len(plans))
	for _, plan := range plans {
		heights = append(heights, plan.height)
	}
	return heights
}

func summarizePlans(plans []repairPlan) (int, int) {
	var txCount int
	var eventCount int
	for _, plan := range plans {
		txCount += len(plan.txs)
		eventCount += len(plan.events)
	}
	return txCount, eventCount
}

func joinUint64s(values []uint64) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}
