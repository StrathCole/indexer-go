package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
)

const (
	alterTxsAddTxBytes = `ALTER TABLE txs ADD COLUMN IF NOT EXISTS tx_bytes String AFTER tx_hash`
	alterTxsAddRespData = `ALTER TABLE txs ADD COLUMN IF NOT EXISTS tx_response_data String AFTER code`
	alterTxsAddRespInfo = `ALTER TABLE txs ADD COLUMN IF NOT EXISTS tx_response_info String AFTER tx_response_data`
	alterEventsAddAttrIndex = `ALTER TABLE events ADD COLUMN IF NOT EXISTS attr_index Bool DEFAULT false AFTER attr_value`
	createTxEventLookup = `
CREATE TABLE IF NOT EXISTS tx_event_lookup (
    event_type     LowCardinality(String),
    attr_key       LowCardinality(String),
    attr_value     String,
    height         UInt64,
    index_in_block UInt16,
    tx_hash        FixedString(64)
)
ENGINE = MergeTree
ORDER BY (event_type, attr_key, attr_value, height, index_in_block);
`
	createTxEventLookupMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tx_event_lookup
TO tx_event_lookup
AS
SELECT
    event_type,
    attr_key,
    attr_value,
    height,
    toUInt16(tx_index) AS index_in_block,
    tx_hash
FROM events
WHERE scope = 'tx' AND tx_index >= 0;
`
)

type yyyymm struct {
	year  int
	month int
}

func parseYYYYMM(v int) (yyyymm, error) {
	y := v / 100
	m := v % 100
	if y < 1970 || y > 3000 {
		return yyyymm{}, fmt.Errorf("invalid YYYYMM year: %d", y)
	}
	if m < 1 || m > 12 {
		return yyyymm{}, fmt.Errorf("invalid YYYYMM month: %d", m)
	}
	return yyyymm{year: y, month: m}, nil
}

func (p yyyymm) int() int {
	return p.year*100 + p.month
}

func (p yyyymm) next() yyyymm {
	if p.month == 12 {
		return yyyymm{year: p.year + 1, month: 1}
	}
	return yyyymm{year: p.year, month: p.month + 1}
}

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	createOnly := flag.Bool("create-only", false, "Only apply schema changes and create lookup objects")
	backfillOnly := flag.Bool("backfill-only", false, "Only backfill tx_event_lookup (assumes schema exists)")
	fromPartition := flag.Int("from-partition", 0, "Optional start partition YYYYMM")
	toPartition := flag.Int("to-partition", 0, "Optional end partition YYYYMM")
	truncateLookup := flag.Bool("truncate-lookup", false, "TRUNCATE tx_event_lookup before backfill (recommended when re-running; pause ingest first)")
	optimize := flag.Bool("optimize", false, "Run OPTIMIZE TABLE tx_event_lookup FINAL after backfill")
	flag.Parse()

	if *createOnly && *backfillOnly {
		log.Fatalf("--create-only and --backfill-only are mutually exclusive")
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

	ctx := context.Background()
	runCreate := !*backfillOnly
	runBackfill := !*createOnly

	if runCreate {
		log.Printf("Applying tx search schema migration to ClickHouse...")
		for _, stmt := range []string{alterTxsAddTxBytes, alterTxsAddRespData, alterTxsAddRespInfo, alterEventsAddAttrIndex, createTxEventLookup, createTxEventLookupMV} {
			if err := ch.Conn.Exec(ctx, stmt); err != nil {
				log.Fatalf("Failed to execute schema statement: %v", err)
			}
		}
		log.Printf("Schema migration complete.")
		log.Printf("Note: tx_bytes/tx_response_* are forward-filled only. Historical rows keep node fallback unless you selectively reindex hot ranges.")
	}

	if !runBackfill {
		log.Printf("Done (create-only).")
		return
	}

	if *truncateLookup {
		log.Printf("Truncating tx_event_lookup before backfill...")
		if err := ch.Conn.Exec(ctx, "TRUNCATE TABLE tx_event_lookup"); err != nil {
			log.Fatalf("Failed to truncate tx_event_lookup: %v", err)
		}
	} else {
		var existing uint64
		if err := ch.Conn.QueryRow(ctx, "SELECT count() FROM tx_event_lookup").Scan(&existing); err != nil {
			log.Fatalf("Failed to count tx_event_lookup rows: %v", err)
		}
		if existing > 0 {
			log.Fatalf("tx_event_lookup already has %d rows; aborting to avoid duplicate storage. Re-run with --truncate-lookup after pausing ingest, or only run this once on an empty lookup table.", existing)
		}
	}

	var minPart uint32
	var maxPart uint32
	if err := ch.Conn.QueryRow(ctx, "SELECT toYYYYMM(min(block_time)) FROM events WHERE scope = 'tx'").Scan(&minPart); err != nil {
		log.Fatalf("Failed to get min partition from events: %v", err)
	}
	if err := ch.Conn.QueryRow(ctx, "SELECT toYYYYMM(max(block_time)) FROM events WHERE scope = 'tx'").Scan(&maxPart); err != nil {
		log.Fatalf("Failed to get max partition from events: %v", err)
	}

	start := int(minPart)
	end := int(maxPart)
	if *fromPartition != 0 {
		start = *fromPartition
	}
	if *toPartition != 0 {
		end = *toPartition
	}

	startYM, err := parseYYYYMM(start)
	if err != nil {
		log.Fatalf("Invalid start partition: %v", err)
	}
	endYM, err := parseYYYYMM(end)
	if err != nil {
		log.Fatalf("Invalid end partition: %v", err)
	}
	if startYM.int() > endYM.int() {
		log.Fatalf("invalid partition range: %d > %d", startYM.int(), endYM.int())
	}

	insertSQL := `
INSERT INTO tx_event_lookup
SELECT
    event_type,
    attr_key,
    attr_value,
    height,
    toUInt16(tx_index) AS index_in_block,
    tx_hash
FROM events
WHERE scope = 'tx' AND tx_index >= 0 AND toYYYYMM(block_time) = ?
`

	log.Printf("Backfilling tx_event_lookup from events partitions %d..%d", startYM.int(), endYM.int())
	for ym := startYM; ym.int() <= endYM.int(); ym = ym.next() {
		part := ym.int()
		started := time.Now()
		log.Printf("Partition %d: backfilling tx_event_lookup...", part)
		if err := ch.Conn.Exec(ctx, insertSQL, part); err != nil {
			log.Fatalf("Partition %d: failed to backfill tx_event_lookup: %v", part, err)
		}
		log.Printf("Partition %d: done in %s", part, time.Since(started))
	}

	if *optimize {
		log.Printf("Optimizing tx_event_lookup (FINAL)...")
		if err := ch.Conn.Exec(ctx, "OPTIMIZE TABLE tx_event_lookup FINAL"); err != nil {
			log.Fatalf("Failed to optimize tx_event_lookup: %v", err)
		}
	}

	log.Printf("Backfill complete.")
}