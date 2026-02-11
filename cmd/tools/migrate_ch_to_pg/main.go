package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
)

// stripNull removes \x00 padding from ClickHouse FixedString values.
func stripNull(s string) string {
	return strings.TrimRight(s, "\x00")
}

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	tables := flag.String("tables", "blocks,oracle_prices,account_txs",
		"Comma-separated list of tables to migrate")
	batchSize := flag.Int("batch", 50000, "Rows per batch read from ClickHouse")
	flag.Parse()

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
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pg.Pool.Close()

	ctx := context.Background()
	tableList := strings.Split(*tables, ",")

	for _, t := range tableList {
		t = strings.TrimSpace(t)
		switch t {
		case "blocks":
			if err := migrateBlocks(ctx, ch, pg, *batchSize); err != nil {
				log.Fatalf("blocks migration failed: %v", err)
			}
		case "oracle_prices":
			if err := migrateOraclePrices(ctx, ch, pg, *batchSize); err != nil {
				log.Fatalf("oracle_prices migration failed: %v", err)
			}
		case "account_txs":
			if err := migrateAccountTxs(ctx, ch, pg, *batchSize); err != nil {
				log.Fatalf("account_txs migration failed: %v", err)
			}
		default:
			log.Fatalf("Unknown table: %s", t)
		}
	}

	log.Println("All migrations complete.")
}

// ---------------------------------------------------------------------------
// blocks
// ---------------------------------------------------------------------------

func migrateBlocks(ctx context.Context, ch *db.ClickHouse, pg *db.Postgres, batchSize int) error {
	log.Println("=== Migrating blocks ===")

	// Count total in CH
	var totalCH uint64
	if err := ch.Conn.QueryRow(ctx, "SELECT count() FROM blocks").Scan(&totalCH); err != nil {
		return fmt.Errorf("count blocks in CH: %w", err)
	}
	log.Printf("  ClickHouse blocks: %d", totalCH)

	// Find resume point: max height already in PG
	pgMax, err := pg.GetMaxHeight(ctx)
	if err != nil {
		return fmt.Errorf("pg max height: %w", err)
	}
	if pgMax > 0 {
		log.Printf("  Resuming from height > %d (already in PG)", pgMax)
	}

	var migrated int64
	startTime := time.Now()

	for {
		rows, err := ch.Conn.Query(ctx,
			"SELECT height, block_hash, block_time, proposer_address, tx_count "+
				"FROM blocks WHERE height > $1 ORDER BY height ASC LIMIT $2",
			pgMax, batchSize)
		if err != nil {
			return fmt.Errorf("query CH blocks: %w", err)
		}

		var batch []db.PgBlock
		var maxInBatch int64
		for rows.Next() {
			var b db.PgBlock
			if err := rows.Scan(&b.Height, &b.BlockHash, &b.BlockTime,
				&b.ProposerAddress, &b.TxCount); err != nil {
				rows.Close()
				return fmt.Errorf("scan block: %w", err)
			}
			b.BlockHash = stripNull(b.BlockHash)
			batch = append(batch, b)
			if int64(b.Height) > maxInBatch {
				maxInBatch = int64(b.Height)
			}
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		if err := pg.InsertBlocks(ctx, batch); err != nil {
			return fmt.Errorf("insert blocks to PG: %w", err)
		}

		migrated += int64(len(batch))
		pgMax = maxInBatch
		elapsed := time.Since(startTime)
		rate := float64(migrated) / elapsed.Seconds()
		remaining := float64(int64(totalCH)-migrated) / rate
		log.Printf("  blocks: %d / %d (%.1f%%)  rate: %.0f/s  ETA: %s",
			migrated, totalCH,
			float64(migrated)/float64(totalCH)*100,
			rate,
			(time.Duration(remaining) * time.Second).Truncate(time.Second))
	}

	log.Printf("  blocks done: %d rows migrated in %s",
		migrated, time.Since(startTime).Truncate(time.Second))
	return nil
}

// ---------------------------------------------------------------------------
// oracle_prices
// ---------------------------------------------------------------------------

func migrateOraclePrices(ctx context.Context, ch *db.ClickHouse, pg *db.Postgres, batchSize int) error {
	log.Println("=== Migrating oracle_prices ===")

	var totalCH uint64
	if err := ch.Conn.QueryRow(ctx, "SELECT count() FROM oracle_prices").Scan(&totalCH); err != nil {
		return fmt.Errorf("count oracle_prices in CH: %w", err)
	}
	log.Printf("  ClickHouse oracle_prices: %d", totalCH)

	// Resume point: max height already in PG for oracle_prices
	var pgMaxHeight uint64
	err := pg.Pool.QueryRow(ctx,
		"SELECT COALESCE(max(height), 0) FROM oracle_prices").Scan(&pgMaxHeight)
	if err != nil {
		return fmt.Errorf("pg max oracle_prices height: %w", err)
	}
	if pgMaxHeight > 0 {
		log.Printf("  Resuming from height > %d", pgMaxHeight)
	}

	var migrated int64
	startTime := time.Now()

	for {
		rows, err := ch.Conn.Query(ctx,
			"SELECT block_time, height, denom, price, currency "+
				"FROM oracle_prices WHERE height > $1 ORDER BY height ASC LIMIT $2",
			pgMaxHeight, batchSize)
		if err != nil {
			return fmt.Errorf("query CH oracle_prices: %w", err)
		}

		var batch []db.PgOraclePrice
		var maxInBatch uint64
		for rows.Next() {
			var p db.PgOraclePrice
			if err := rows.Scan(&p.BlockTime, &p.Height, &p.Denom,
				&p.Price, &p.Currency); err != nil {
				rows.Close()
				return fmt.Errorf("scan oracle_price: %w", err)
			}
			batch = append(batch, p)
			if p.Height > maxInBatch {
				maxInBatch = p.Height
			}
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		if err := pg.InsertOraclePrices(ctx, batch); err != nil {
			return fmt.Errorf("insert oracle_prices to PG: %w", err)
		}

		migrated += int64(len(batch))
		pgMaxHeight = maxInBatch
		elapsed := time.Since(startTime)
		rate := float64(migrated) / elapsed.Seconds()
		remaining := float64(int64(totalCH)-migrated) / rate
		log.Printf("  oracle_prices: %d / %d (%.1f%%)  rate: %.0f/s  ETA: %s",
			migrated, totalCH,
			float64(migrated)/float64(totalCH)*100,
			rate,
			(time.Duration(remaining) * time.Second).Truncate(time.Second))
	}

	log.Printf("  oracle_prices done: %d rows migrated in %s",
		migrated, time.Since(startTime).Truncate(time.Second))
	return nil
}

// ---------------------------------------------------------------------------
// account_txs  (largest table — uses parallel height-range workers)
// ---------------------------------------------------------------------------

func migrateAccountTxs(ctx context.Context, ch *db.ClickHouse, pg *db.Postgres, batchSize int) error {
	log.Println("=== Migrating account_txs ===")

	var totalCH uint64
	if err := ch.Conn.QueryRow(ctx, "SELECT count() FROM account_txs").Scan(&totalCH); err != nil {
		return fmt.Errorf("count account_txs in CH: %w", err)
	}
	log.Printf("  ClickHouse account_txs: %d", totalCH)

	// Find height range in CH
	var minHeight, maxHeight uint64
	if err := ch.Conn.QueryRow(ctx,
		"SELECT min(height), max(height) FROM account_txs").Scan(&minHeight, &maxHeight); err != nil {
		return fmt.Errorf("height range from CH: %w", err)
	}
	if maxHeight == 0 {
		log.Println("  No account_txs rows, nothing to migrate.")
		return nil
	}
	log.Printf("  Height range: %d - %d", minHeight, maxHeight)

	// Resume: find last fully migrated height range
	var pgMaxHeight uint64
	err := pg.Pool.QueryRow(ctx,
		"SELECT COALESCE(max(height), 0) FROM account_txs").Scan(&pgMaxHeight)
	if err != nil {
		return fmt.Errorf("pg max account_txs height: %w", err)
	}

	startFrom := minHeight
	if pgMaxHeight > 0 {
		// Re-process from the start of the chunk that contained pgMaxHeight.
		// ON CONFLICT DO NOTHING makes re-processing safe.
		chunkStart := (pgMaxHeight / uint64(batchSize)) * uint64(batchSize)
		if chunkStart > minHeight {
			startFrom = chunkStart
		}
		log.Printf("  Resuming from height >= %d (PG max: %d)", startFrom, pgMaxHeight)
	}

	// Process by height ranges to keep memory bounded.
	const workers = 4
	type chunk struct {
		lo, hi uint64
	}
	chunks := make(chan chunk, workers*2)

	// Producer: generate height-range chunks
	go func() {
		for lo := startFrom; lo <= maxHeight; lo += uint64(batchSize) {
			hi := lo + uint64(batchSize) - 1
			if hi > maxHeight {
				hi = maxHeight
			}
			chunks <- chunk{lo, hi}
		}
		close(chunks)
	}()

	var globalMigrated atomic.Int64
	startTime := time.Now()
	errChan := make(chan error, workers)

	for w := 0; w < workers; w++ {
		go func() {
			for c := range chunks {
				n, err := migrateAccountTxsChunk(ctx, ch, pg, c.lo, c.hi)
				if err != nil {
					errChan <- err
					return
				}
				globalMigrated.Add(n)

				m := globalMigrated.Load()
				elapsed := time.Since(startTime)
				rate := float64(m) / elapsed.Seconds()
				pct := float64(m) / float64(totalCH) * 100
				remaining := float64(int64(totalCH)-m) / rate
				fmt.Fprintf(os.Stderr,
					"\r  account_txs: %d / %d (%.1f%%)  rate: %.0f/s  ETA: %s   ",
					m, totalCH, pct, rate,
					(time.Duration(remaining) * time.Second).Truncate(time.Second))
			}
			errChan <- nil
		}()
	}

	// Wait for all workers
	for w := 0; w < workers; w++ {
		if e := <-errChan; e != nil {
			return e
		}
	}

	fmt.Fprintln(os.Stderr)
	log.Printf("  account_txs done: %d rows migrated in %s",
		globalMigrated.Load(), time.Since(startTime).Truncate(time.Second))
	return nil
}

// migrateAccountTxsChunk reads a height range from CH and writes to PG.
// Returns the number of rows migrated.
func migrateAccountTxsChunk(ctx context.Context, ch *db.ClickHouse,
	pg *db.Postgres, lo, hi uint64) (int64, error) {

	rows, err := ch.Conn.Query(ctx,
		"SELECT address_id, height, index_in_block, block_time, tx_hash, "+
			"direction, main_denom_id, main_amount, is_block_event, event_scope "+
			"FROM account_txs WHERE height >= $1 AND height <= $2 "+
			"ORDER BY height, index_in_block",
		lo, hi)
	if err != nil {
		return 0, fmt.Errorf("query CH account_txs [%d..%d]: %w", lo, hi, err)
	}

	var batch []db.PgAccountTx
	var count int64
	for rows.Next() {
		var t db.PgAccountTx
		if err := rows.Scan(
			&t.AddressID, &t.Height, &t.IndexInBlock, &t.BlockTime, &t.TxHash,
			&t.Direction, &t.MainDenomID, &t.MainAmount, &t.IsBlockEvent,
			&t.EventScope,
		); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan account_tx: %w", err)
		}
		t.TxHash = stripNull(t.TxHash)
		batch = append(batch, t)

		// Flush in sub-batches to keep PG batch size reasonable
		if len(batch) >= 5000 {
			if err := pg.InsertAccountTxs(ctx, batch); err != nil {
				rows.Close()
				return 0, fmt.Errorf("insert account_txs: %w", err)
			}
			count += int64(len(batch))
			batch = batch[:0]
		}
	}
	rows.Close()

	// Flush remainder
	if len(batch) > 0 {
		if err := pg.InsertAccountTxs(ctx, batch); err != nil {
			return 0, fmt.Errorf("insert account_txs remainder: %w", err)
		}
		count += int64(len(batch))
	}

	return count, nil
}
