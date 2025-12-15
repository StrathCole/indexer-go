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
	createDailyActiveTable = `
CREATE TABLE IF NOT EXISTS account_txs_daily_active_tx (
    day Date,
    active_state AggregateFunction(uniqCombined64, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day);
`
	createDailyActiveMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_account_txs_daily_active_tx
TO account_txs_daily_active_tx
AS
SELECT
    toDate(block_time) AS day,
    uniqCombined64State(address_id) AS active_state
FROM account_txs
WHERE is_block_event = 0
GROUP BY day;
`
	createFirstSeenTable = `
CREATE TABLE IF NOT EXISTS address_first_seen (
    address_id UInt64,
    first_seen_state AggregateFunction(min, DateTime64(3))
)
ENGINE = AggregatingMergeTree
ORDER BY (address_id);
`
	createFirstSeenMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_address_first_seen
TO address_first_seen
AS
SELECT
    address_id,
    minState(block_time) AS first_seen_state
FROM account_txs
GROUP BY address_id;
`
	createRegisteredDailyTable = `
CREATE TABLE IF NOT EXISTS registered_accounts_daily (
	day Date,
	value UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day);
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
	createOnly := flag.Bool("create-only", false, "Only create tables/materialized views")
	backfillOnly := flag.Bool("backfill-only", false, "Only backfill data (assumes tables exist)")
	fromPartition := flag.Int("from-partition", 0, "Optional start partition YYYYMM (e.g. 202311)")
	toPartition := flag.Int("to-partition", 0, "Optional end partition YYYYMM (e.g. 202412)")
	rebuildRegisteredDaily := flag.Bool("rebuild-registered-daily", false, "Rebuild registered_accounts_daily from address_first_seen (can take minutes for millions of accounts)")
	truncateRegisteredDaily := flag.Bool("truncate-registered-daily", false, "TRUNCATE registered_accounts_daily before rebuilding")
	optimize := flag.Bool("optimize", false, "Run OPTIMIZE TABLE ... FINAL after backfill (can be expensive)")
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
		log.Printf("Creating ClickHouse aggregate tables/views if missing...")
		for _, stmt := range []string{createDailyActiveTable, createDailyActiveMV, createFirstSeenTable, createFirstSeenMV, createRegisteredDailyTable} {
			if err := ch.Conn.Exec(ctx, stmt); err != nil {
				log.Fatalf("Failed to execute create statement: %v", err)
			}
		}
	}

	if !runBackfill {
		log.Printf("Done (create-only).")
		return
	}

	// Determine partition range from account_txs if not provided.
	var minPart uint32
	var maxPart uint32
	if err := ch.Conn.QueryRow(ctx, "SELECT toYYYYMM(min(block_time)) FROM account_txs").Scan(&minPart); err != nil {
		log.Fatalf("Failed to get min partition from account_txs: %v", err)
	}
	if err := ch.Conn.QueryRow(ctx, "SELECT toYYYYMM(max(block_time)) FROM account_txs").Scan(&maxPart); err != nil {
		log.Fatalf("Failed to get max partition from account_txs: %v", err)
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
		log.Fatalf("Invalid --from-partition/%d: %v", start, err)
	}
	endYM, err := parseYYYYMM(end)
	if err != nil {
		log.Fatalf("Invalid --to-partition/%d: %v", end, err)
	}
	if startYM.int() > endYM.int() {
		log.Fatalf("invalid partition range: %d > %d", startYM.int(), endYM.int())
	}

	log.Printf("Backfilling aggregates from account_txs partitions %d..%d", startYM.int(), endYM.int())

	insertDailyActive := "INSERT INTO account_txs_daily_active_tx SELECT toDate(block_time) AS day, uniqCombined64State(address_id) AS active_state FROM account_txs WHERE is_block_event = 0 AND toYYYYMM(block_time) = ? GROUP BY day"
	insertFirstSeen := "INSERT INTO address_first_seen SELECT address_id, minState(block_time) AS first_seen_state FROM account_txs WHERE toYYYYMM(block_time) = ? GROUP BY address_id"

	for ym := startYM; ym.int() <= endYM.int(); ym = ym.next() {
		part := ym.int()
		startTime := time.Now()
		log.Printf("Partition %d: backfilling daily active...", part)
		if err := ch.Conn.Exec(ctx, insertDailyActive, part); err != nil {
			log.Fatalf("Partition %d: failed to backfill account_txs_daily_active_tx: %v", part, err)
		}

		log.Printf("Partition %d: backfilling address first-seen...", part)
		if err := ch.Conn.Exec(ctx, insertFirstSeen, part); err != nil {
			log.Fatalf("Partition %d: failed to backfill address_first_seen: %v", part, err)
		}

		log.Printf("Partition %d: done in %s", part, time.Since(startTime))
	}

	if *optimize {
		log.Printf("Optimizing aggregate tables (FINAL)...")
		if err := ch.Conn.Exec(ctx, "OPTIMIZE TABLE account_txs_daily_active_tx FINAL"); err != nil {
			log.Fatalf("Failed to optimize account_txs_daily_active_tx: %v", err)
		}
		if err := ch.Conn.Exec(ctx, "OPTIMIZE TABLE address_first_seen FINAL"); err != nil {
			log.Fatalf("Failed to optimize address_first_seen: %v", err)
		}
	}

	if *rebuildRegisteredDaily {
		log.Printf("Rebuilding registered_accounts_daily from address_first_seen...")
		if *truncateRegisteredDaily {
			if err := ch.Conn.Exec(ctx, "TRUNCATE TABLE registered_accounts_daily"); err != nil {
				log.Fatalf("Failed to truncate registered_accounts_daily: %v", err)
			}
		}

		// One-time heavy aggregation (millions of accounts) but results in a tiny daily table.
		buildSQL := `
			INSERT INTO registered_accounts_daily
			SELECT
				toDate(first_seen) AS day,
				count() AS value
			FROM (
				SELECT
					address_id,
					minMerge(first_seen_state) AS first_seen
				FROM address_first_seen
				GROUP BY address_id
			)
			GROUP BY day
			ORDER BY day ASC
		`
		start := time.Now()
		if err := ch.Conn.Exec(ctx, buildSQL); err != nil {
			log.Fatalf("Failed to rebuild registered_accounts_daily: %v", err)
		}
		log.Printf("registered_accounts_daily rebuild complete in %s", time.Since(start))

		if *optimize {
			if err := ch.Conn.Exec(ctx, "OPTIMIZE TABLE registered_accounts_daily FINAL"); err != nil {
				log.Fatalf("Failed to optimize registered_accounts_daily: %v", err)
			}
		}
	}

	log.Printf("Backfill complete.")
}
