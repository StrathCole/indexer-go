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
	"github.com/jackc/pgx/v5"
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
	startHeight := flag.Int64("start-height", 0, "Start height for account_txs (0 = from min)")
	endHeight := flag.Int64("end-height", 0, "End height for account_txs (0 = to max)")
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
			if err := migrateAccountTxs(ctx, ch, pg, *batchSize, *startHeight, *endHeight); err != nil {
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
// account_txs  (largest table — bulk COPY with indexes dropped)
// ---------------------------------------------------------------------------

func migrateAccountTxs(ctx context.Context, ch *db.ClickHouse, pg *db.Postgres, batchSize int, startHeight, endHeight int64) error {
	log.Println("=== Migrating account_txs ===")

	var minHeight, maxHeight uint64
	if err := ch.Conn.QueryRow(ctx,
		"SELECT min(height), max(height) FROM account_txs").Scan(&minHeight, &maxHeight); err != nil {
		return fmt.Errorf("height range from CH: %w", err)
	}
	if maxHeight == 0 {
		log.Println("  No account_txs rows, nothing to migrate.")
		return nil
	}
	effectiveMin := minHeight
	effectiveMax := maxHeight
	if startHeight > 0 {
		if uint64(startHeight) > maxHeight {
			return fmt.Errorf("start-height %d is above max height %d", startHeight, maxHeight)
		}
		if uint64(startHeight) > minHeight {
			effectiveMin = uint64(startHeight)
		}
	}
	if endHeight > 0 {
		if uint64(endHeight) < minHeight {
			return fmt.Errorf("end-height %d is below min height %d", endHeight, minHeight)
		}
		if uint64(endHeight) < maxHeight {
			effectiveMax = uint64(endHeight)
		}
	}
	if effectiveMin > effectiveMax {
		return fmt.Errorf("start-height %d is greater than end-height %d", effectiveMin, effectiveMax)
	}

	var totalCH uint64
	if err := ch.Conn.QueryRow(ctx,
		"SELECT count() FROM account_txs WHERE height >= $1 AND height <= $2",
		effectiveMin, effectiveMax).Scan(&totalCH); err != nil {
		return fmt.Errorf("count account_txs in CH range: %w", err)
	}
	log.Printf("  ClickHouse account_txs in range: %d", totalCH)
	log.Printf("  Height range: %d - %d", effectiveMin, effectiveMax)

	// Step 1: Stream data with parallel workers using temp tables and ON CONFLICT.
	const workers = 8
	type chunk struct{ lo, hi uint64 }
	chunks := make(chan chunk, workers*2)

	go func() {
		for lo := effectiveMin; lo <= effectiveMax; lo += uint64(batchSize) {
			hi := lo + uint64(batchSize) - 1
			if hi > effectiveMax {
				hi = effectiveMax
			}
			chunks <- chunk{lo, hi}
		}
		close(chunks)
	}()

	var globalMigrated atomic.Int64
	startTime := time.Now()
	errChan := make(chan error, workers)

	cols := []string{"address_id", "height", "index_in_block", "block_time", "tx_hash",
		"direction", "main_denom_id", "main_amount", "is_block_event", "event_scope"}

	for w := 0; w < workers; w++ {
		go func() {
			for c := range chunks {
				n, err := copyAccountTxsChunk(ctx, ch, pg, cols, c.lo, c.hi)
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

	for w := 0; w < workers; w++ {
		if e := <-errChan; e != nil {
			return e
		}
	}
	fmt.Fprintln(os.Stderr)

	total := globalMigrated.Load()
	log.Printf("  COPY complete: %d rows in %s", total, time.Since(startTime).Truncate(time.Second))

	log.Printf("  account_txs done: %d rows migrated in %s",
		total, time.Since(startTime).Truncate(time.Second))
	return nil
}

// copyAccountTxsChunk reads a height range from CH and COPY's directly into PG.
func copyAccountTxsChunk(ctx context.Context, ch *db.ClickHouse,
	pg *db.Postgres, cols []string, lo, hi uint64) (int64, error) {

	rows, err := ch.Conn.Query(ctx,
		"SELECT address_id, height, index_in_block, "+
			"any(block_time) AS block_time, "+
			"any(tx_hash) AS tx_hash, "+
			"any(direction) AS direction, "+
			"any(main_denom_id) AS main_denom_id, "+
			"any(main_amount) AS main_amount, "+
			"is_block_event, "+
			"any(event_scope) AS event_scope "+
			"FROM account_txs "+
			"WHERE height >= $1 AND height <= $2 "+
			"GROUP BY address_id, height, index_in_block, is_block_event",
		lo, hi)
	if err != nil {
		return 0, fmt.Errorf("query CH [%d..%d]: %w", lo, hi, err)
	}

	var allRows [][]interface{}
	for rows.Next() {
		var t db.PgAccountTx
		if err := rows.Scan(
			&t.AddressID, &t.Height, &t.IndexInBlock, &t.BlockTime, &t.TxHash,
			&t.Direction, &t.MainDenomID, &t.MainAmount, &t.IsBlockEvent,
			&t.EventScope,
		); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		t.TxHash = stripNull(t.TxHash)
		allRows = append(allRows, []interface{}{
			t.AddressID, t.Height, t.IndexInBlock, t.BlockTime, t.TxHash,
			t.Direction, t.MainDenomID, t.MainAmount, t.IsBlockEvent, t.EventScope,
		})
	}
	rows.Close()

	if len(allRows) == 0 {
		return 0, nil
	}

	conn, err := pg.Pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	tempName := fmt.Sprintf("_tmp_acctx_%d", lo)
	_, err = tx.Exec(ctx, fmt.Sprintf(
		`CREATE TEMP TABLE %s (
			address_id BIGINT, height BIGINT, index_in_block SMALLINT,
			block_time TIMESTAMPTZ, tx_hash CHAR(64), direction SMALLINT,
			main_denom_id SMALLINT, main_amount BIGINT,
			is_block_event BOOLEAN, event_scope SMALLINT
		) ON COMMIT DROP`, tempName))
	if err != nil {
		return 0, fmt.Errorf("create temp table: %w", err)
	}

	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{tempName},
		cols,
		pgx.CopyFromRows(allRows),
	)
	if err != nil {
		return 0, fmt.Errorf("COPY temp [%d..%d]: %w", lo, hi, err)
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO account_txs (%s)
		 SELECT %s FROM %s
		 ON CONFLICT (address_id, height, index_in_block, is_block_event) DO NOTHING`,
		strings.Join(cols, ","), strings.Join(cols, ","), tempName))
	if err != nil {
		return 0, fmt.Errorf("merge temp [%d..%d]: %w", lo, hi, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit [%d..%d]: %w", lo, hi, err)
	}

	return int64(len(allRows)), nil
}
