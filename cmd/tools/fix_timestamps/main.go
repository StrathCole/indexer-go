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
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
)

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	migrate := flag.Bool("migrate", false, "Run schema migration (ALTER TABLE)")
	fix := flag.Bool("fix", false, "Run data fix")
	limit := flag.Int("limit", 1000, "Limit number of blocks to process per run")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Connect to ClickHouse
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

	// Helper to get columns
	getColumns := func(tableName string) ([]string, error) {
		rows, err := ch.Conn.Query(ctx, fmt.Sprintf("SELECT name FROM system.columns WHERE database = '%s' AND table = '%s' ORDER BY position", cfg.Database.ClickHouseDB, tableName))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var cols []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			cols = append(cols, name)
		}
		return cols, nil
	}

	if *migrate {
		log.Println("Running schema migration...")

		tables := map[string]string{
			"blocks": `CREATE TABLE IF NOT EXISTS blocks (
    height          UInt64,
    block_hash      FixedString(64),
    block_time      DateTime64(3),
    proposer_address String,
    tx_count        UInt32
) ENGINE = MergeTree PARTITION BY toYYYYMM(block_time) ORDER BY (height)`,

			"txs": `CREATE TABLE IF NOT EXISTS txs (
    height          UInt64,
    index_in_block  UInt16,
    block_time      DateTime64(3),
    tx_hash         FixedString(64),
	INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
    codespace       LowCardinality(String),
    code            UInt32,
    gas_wanted      UInt64,
    gas_used        UInt64,
    fee_amounts     Array(Int64),
    fee_denom_ids   Array(UInt16),
    tax_amounts     Array(Int64),
    tax_denom_ids   Array(UInt16),
    msg_type_ids    Array(UInt16),
    msgs_json       Array(String),
    signatures_json Array(String),
    memo            String,
    raw_log         String,
    logs_json       String
) ENGINE = MergeTree PARTITION BY toYYYYMM(block_time) ORDER BY (height, index_in_block)`,

			"events": `CREATE TABLE IF NOT EXISTS events (
    height          UInt64,
	INDEX idx_events_height height TYPE minmax GRANULARITY 1,
    block_time      DateTime64(3),
    scope           Enum8('block' = 0, 'tx' = 1, 'begin_block' = 2, 'end_block' = 3),
    tx_index        Int16,
    event_index     UInt16,
    event_type      LowCardinality(String),
	INDEX idx_events_event_type event_type TYPE bloom_filter(0.01) GRANULARITY 4,
    attr_key        LowCardinality(String),
	INDEX idx_events_attr_key attr_key TYPE bloom_filter(0.01) GRANULARITY 4,
	INDEX idx_events_type_key (event_type, attr_key) TYPE bloom_filter(0.01) GRANULARITY 4,
    attr_value      String,
    tx_hash         FixedString(64) DEFAULT ''
) ENGINE = MergeTree PARTITION BY toYYYYMM(block_time) ORDER BY (height, scope, tx_index, event_index, event_type, attr_key)`,

			"account_txs": `CREATE TABLE IF NOT EXISTS account_txs (
    address_id      UInt64,
    height          UInt64,
    index_in_block  UInt16,
    block_time      DateTime64(3),
    tx_hash         FixedString(64),
    direction       Int8,
    main_denom_id   UInt16,
	main_amount     Int64,
	is_block_event  Bool DEFAULT false,
	event_scope     Int8 DEFAULT 0
) ENGINE = MergeTree PARTITION BY toYYYYMM(block_time) ORDER BY (address_id, height, index_in_block, is_block_event)`,

			"oracle_prices": `CREATE TABLE IF NOT EXISTS oracle_prices (
    block_time      DateTime64(3),
    height          UInt64,
    denom           LowCardinality(String),
    price           Float64,
    currency        LowCardinality(String)
) ENGINE = MergeTree PARTITION BY toYYYYMM(block_time) ORDER BY (denom, block_time)`,

			"validator_returns": `CREATE TABLE IF NOT EXISTS validator_returns (
    block_time      DateTime64(3),
    height          UInt64,
    operator_address String,
    commission      Map(String, Float64),
    reward          Map(String, Float64)
) ENGINE = MergeTree PARTITION BY toYYYYMM(block_time) ORDER BY (operator_address, block_time)`,

			"block_rewards": `CREATE TABLE IF NOT EXISTS block_rewards (
    block_time      DateTime64(3),
    height          UInt64,
    total_reward    Map(String, Float64),
    total_commission Map(String, Float64)
) ENGINE = MergeTree PARTITION BY toYYYYMM(block_time) ORDER BY (block_time)`,
		}

		tableNames := []string{"blocks", "txs", "events", "account_txs", "oracle_prices", "validator_returns", "block_rewards"}

		for _, tableName := range tableNames {
			createStmt := tables[tableName]
			log.Printf("Processing table %s...", tableName)

			var exists uint8
			err := ch.Conn.QueryRow(ctx, fmt.Sprintf("EXISTS TABLE %s", tableName)).Scan(&exists)
			if err != nil {
				log.Printf("Error checking existence of table %s: %v", tableName, err)
				continue
			}

			if exists == 0 {
				log.Printf("Table %s does not exist. Creating...", tableName)
				if err := ch.Conn.Exec(ctx, createStmt); err != nil {
					log.Printf("Error creating table %s: %v", tableName, err)
				} else {
					log.Printf("Successfully created table %s", tableName)
				}
				continue
			}

			var colType string
			err = ch.Conn.QueryRow(ctx, fmt.Sprintf("SELECT type FROM system.columns WHERE table = '%s' AND name = 'block_time' AND database = '%s'", tableName, cfg.Database.ClickHouseDB)).Scan(&colType)
			if err != nil {
				log.Printf("Error checking column type for table %s: %v", tableName, err)
				continue
			}

			if colType == "DateTime64(3)" {
				log.Printf("Table %s already has correct schema. Skipping.", tableName)
				continue
			}

			log.Printf("Table %s has type %s. Migrating...", tableName, colType)

			backupName := tableName + "_backup_migration"
			_ = ch.Conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", backupName))

			if err := ch.Conn.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", tableName, backupName)); err != nil {
				log.Printf("Error renaming table %s: %v", tableName, err)
				continue
			}

			if err := ch.Conn.Exec(ctx, createStmt); err != nil {
				log.Printf("Error creating new table %s: %v", tableName, err)
				_ = ch.Conn.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", backupName, tableName))
				continue
			}

			log.Printf("Copying data from %s to %s...", backupName, tableName)
			if err := ch.Conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableName, backupName)); err != nil {
				log.Printf("Error copying data for table %s: %v", tableName, err)
				log.Println("Manual intervention required. Original data is in " + backupName)
				continue
			}

			log.Printf("Successfully migrated table %s", tableName)
			log.Printf("Dropping backup table %s...", backupName)
			_ = ch.Conn.Exec(ctx, fmt.Sprintf("DROP TABLE %s", backupName))
		}
	}

	if *fix {
		log.Println("Running data fix...")

		// Connect to RPC
		client, err := rpchttp.New(cfg.Node.RPC, "/websocket")
		if err != nil {
			log.Fatalf("Failed to create RPC client: %v", err)
		}
		err = client.Start()
		if err != nil {
			log.Fatalf("Failed to start RPC client: %v", err)
		}
		defer client.Stop()

		// Find blocks with 0 milliseconds
		// We look for blocks where the millisecond part is 0
		// toUnixTimestamp64Nano returns Int64 (nanoseconds).
		// We check if modulo 1 second (1,000,000,000 ns) is 0.
		query := fmt.Sprintf(`
			SELECT height, block_time 
			FROM blocks 
			WHERE (toUnixTimestamp64Nano(block_time) %% 1000000000) = 0 
			ORDER BY height DESC 
			LIMIT %d
		`, *limit)

		rows, err := ch.Conn.Query(ctx, query)
		if err != nil {
			log.Fatalf("Failed to query blocks: %v", err)
		}
		defer rows.Close()

		var blocksToFix []struct {
			Height    int64
			BlockTime time.Time
		}

		for rows.Next() {
			var h uint64
			var t time.Time
			if err := rows.Scan(&h, &t); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}
			blocksToFix = append(blocksToFix, struct {
				Height    int64
				BlockTime time.Time
			}{int64(h), t})
		}
		rows.Close()

		log.Printf("Found %d blocks to check", len(blocksToFix))

		for _, b := range blocksToFix {
			// Fetch block from RPC
			block, err := client.Block(ctx, &b.Height)
			if err != nil {
				log.Printf("Failed to fetch block %d: %v", b.Height, err)
				continue
			}

			rpcTime := block.Block.Time
			// Check if RPC time actually has milliseconds
			if rpcTime.Nanosecond()/1000000 == 0 {
				// RPC also has 0 ms, so it's correct (or RPC is also truncating, but we can't do much)
				// Actually, if RPC returns .000, then our DB is correct.
				// But we should check if they are equal.
				// Note: DB time might be slightly different due to precision?
				// DateTime64(3) stores ms.
				// If DB has X.000 and RPC has X.000, we are good.
				// If DB has X.000 and RPC has X.123, we need update.
				// So we don't skip just because RPC has 0 ms (though unlikely if we are fixing missing ms).
				// Wait, if RPC has 0 ms, then we don't need to update anything because DB already has 0 ms.
				// So we can skip.
				// log.Printf("Block %d: RPC time has 0 ms (%v), skipping", b.Height, rpcTime)
				// continue
			}

			// Compare times
			// Truncate RPC time to ms for comparison
			rpcTimeMs := rpcTime.Truncate(time.Millisecond)
			dbTimeMs := b.BlockTime.Truncate(time.Millisecond)

			if rpcTimeMs.Equal(dbTimeMs) {
				// log.Printf("Block %d: Time matches (%v), skipping", b.Height, rpcTimeMs)
				continue
			}

			log.Printf("Fixing Block %d: DB=%v, RPC=%v", b.Height, dbTimeMs, rpcTimeMs)

			tables := []string{
				"blocks",
				"txs",
				"events",
				"account_txs",
				"oracle_prices",
				"validator_returns",
				"block_rewards",
			}

			for _, table := range tables {
				// Get columns
				cols, err := getColumns(table)
				if err != nil {
					log.Printf("Failed to get columns for table %s: %v", table, err)
					continue
				}

				// Build SELECT list and Column list
				var selectList []string
				var colList []string
				for _, col := range cols {
					colList = append(colList, col)
					if col == "block_time" {
						// Use the new time literal
						selectList = append(selectList, fmt.Sprintf("toDateTime64('%s', 3)", rpcTimeMs.Format("2006-01-02 15:04:05.000")))
					} else {
						selectList = append(selectList, col)
					}
				}
				selectStmt := strings.Join(selectList, ", ")
				colStmt := strings.Join(colList, ", ")

				// INSERT INTO table (cols) SELECT ... WHERE height = ?
				insertQuery := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s WHERE height = %d", table, colStmt, selectStmt, table, b.Height)
				if err := ch.Conn.Exec(ctx, insertQuery); err != nil {
					log.Printf("Failed to insert fixed row for table %s height %d: %v", table, b.Height, err)
					continue
				}

				// Verify insertion
				var count uint64
				verifyQuery := fmt.Sprintf("SELECT count() FROM %s WHERE height = %d AND block_time = toDateTime64('%s', 3)",
					table, b.Height, rpcTimeMs.Format("2006-01-02 15:04:05.000"))
				if err := ch.Conn.QueryRow(ctx, verifyQuery).Scan(&count); err != nil {
					log.Printf("Failed to verify insertion for table %s height %d: %v", table, b.Height, err)
					continue
				}

				if count == 0 {
					// If count is 0, it means either the insert failed silently (unlikely) or the original row didn't exist.
					// If original row didn't exist, we shouldn't delete anything anyway.
					// But we should check if original row existed.
					// Actually, if original row didn't exist, INSERT SELECT ... FROM ... WHERE height = ... would insert 0 rows.
					// So count=0 is expected if no data for that height.
					// In that case, we don't need to delete anything.
					// log.Printf("No rows inserted for table %s height %d (probably no data)", table, b.Height)
					continue
				}

				// DELETE WHERE height = ? AND block_time != ?
				deleteQuery := fmt.Sprintf("ALTER TABLE %s DELETE WHERE height = %d AND block_time != toDateTime64('%s', 3)",
					table, b.Height, rpcTimeMs.Format("2006-01-02 15:04:05.000"))

				if err := ch.Conn.Exec(ctx, deleteQuery); err != nil {
					log.Printf("Failed to delete old rows for table %s height %d: %v", table, b.Height, err)
				}
			}

			// Sleep a bit to be nice to CH
			time.Sleep(10 * time.Millisecond)
		}
	}
}
