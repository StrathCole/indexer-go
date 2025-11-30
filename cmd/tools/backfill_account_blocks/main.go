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
	"github.com/classic-terra/indexer-go/internal/model"
)

// isTerraAddress checks if a string is a valid Terra address (terra1... bech32)
// Returns false for terravaloper addresses as they are not relevant for account tracking
func isTerraAddress(s string) bool {
	// Must start with "terra1" and be a reasonable length (39-59 chars for bech32)
	if len(s) < 39 || len(s) > 64 {
		return false
	}
	// Exclude terravaloper addresses
	if strings.HasPrefix(s, "terravaloper") {
		return false
	}
	// Check for terra1 prefix (accounts and contracts)
	return strings.HasPrefix(s, "terra1")
}

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	startHeight := flag.Int64("start", 0, "Start height (0 = from beginning)")
	endHeight := flag.Int64("end", 0, "End height (0 = to latest)")
	batchSize := flag.Int("batch", 10000, "Batch size for processing")
	createTable := flag.Bool("create-table", false, "Create account_blocks table if not exists")
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

	// Connect to Postgres
	pg, err := db.NewPostgres(cfg.Database.PostgresConn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}

	ctx := context.Background()

	if *createTable {
		log.Println("Creating account_blocks table...")
		createTableSQL := `
			CREATE TABLE IF NOT EXISTS account_blocks (
				address_id      UInt64,
				height          UInt64,
				block_time      DateTime64(3),
				scope           Enum8('begin_block' = 0, 'end_block' = 1)
			)
			ENGINE = MergeTree
			PARTITION BY toYYYYMM(block_time)
			ORDER BY (address_id, height)
		`
		if err := ch.Conn.Exec(ctx, createTableSQL); err != nil {
			log.Fatalf("Failed to create table: %v", err)
		}
		log.Println("Table created successfully")
	}

	// Get height range
	var minHeight, maxHeight uint64

	if *startHeight > 0 {
		minHeight = uint64(*startHeight)
	} else {
		// Get min height from events
		err := ch.Conn.QueryRow(ctx, "SELECT min(height) FROM events WHERE scope IN ('begin_block', 'end_block', 'block')").Scan(&minHeight)
		if err != nil {
			log.Fatalf("Failed to get min height: %v", err)
		}
	}

	if *endHeight > 0 {
		maxHeight = uint64(*endHeight)
	} else {
		// Get max height from events
		err := ch.Conn.QueryRow(ctx, "SELECT max(height) FROM events WHERE scope IN ('begin_block', 'end_block', 'block')").Scan(&maxHeight)
		if err != nil {
			log.Fatalf("Failed to get max height: %v", err)
		}
	}

	log.Printf("Processing blocks from height %d to %d", minHeight, maxHeight)

	// Load address cache from Postgres
	addressCache := make(map[string]uint64)
	addressCacheMu := make(map[string]bool) // Track addresses we've already tried to insert

	getOrCreateAddressID := func(ctx context.Context, address string) (uint64, error) {
		if id, ok := addressCache[address]; ok {
			return id, nil
		}

		// Try to select first
		var id uint64
		err := pg.Pool.QueryRow(ctx, "SELECT id FROM addresses WHERE address = $1", address).Scan(&id)
		if err == nil {
			addressCache[address] = id
			return id, nil
		}

		// Insert if not found
		query := `
			WITH ins AS (
				INSERT INTO addresses (address, type) VALUES ($1, 'account')
				ON CONFLICT (address) DO NOTHING
				RETURNING id
			)
			SELECT id FROM ins
			UNION ALL
			SELECT id FROM addresses WHERE address = $1
			LIMIT 1;
		`

		err = pg.Pool.QueryRow(ctx, query, address).Scan(&id)
		if err != nil {
			return 0, fmt.Errorf("failed to get or create address id: %w", err)
		}

		addressCache[address] = id
		return id, nil
	}
	_ = addressCacheMu

	// Process in batches
	currentHeight := minHeight
	totalProcessed := 0
	totalAccountBlocks := 0

	for currentHeight <= maxHeight {
		batchEnd := currentHeight + uint64(*batchSize) - 1
		if batchEnd > maxHeight {
			batchEnd = maxHeight
		}

		log.Printf("Processing heights %d to %d...", currentHeight, batchEnd)

		// Query events in batch
		query := `
			SELECT height, block_time, scope, attr_value
			FROM events
			WHERE height >= $1 AND height <= $2
			  AND scope IN ('begin_block', 'end_block', 'block')
		`

		rows, err := ch.Conn.Query(ctx, query, currentHeight, batchEnd)
		if err != nil {
			log.Fatalf("Failed to query events: %v", err)
		}

		// Group by height+scope to deduplicate addresses
		type blockKey struct {
			Height    uint64
			BlockTime time.Time
			Scope     string
		}
		blockAddresses := make(map[blockKey]map[string]bool)

		for rows.Next() {
			var height uint64
			var blockTime time.Time
			var scope string
			var attrValue string

			if err := rows.Scan(&height, &blockTime, &scope, &attrValue); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}

			// Check if this is a Terra address
			if !isTerraAddress(attrValue) {
				continue
			}

			key := blockKey{Height: height, BlockTime: blockTime, Scope: scope}
			if blockAddresses[key] == nil {
				blockAddresses[key] = make(map[string]bool)
			}
			blockAddresses[key][attrValue] = true
		}
		rows.Close()

		// Convert to AccountBlocks
		var accountBlocks []model.AccountBlock

		for key, addresses := range blockAddresses {
			// Map legacy 'block' scope to 'begin_block'
			scope := key.Scope
			if scope == "block" {
				scope = "begin_block"
			}

			for address := range addresses {
				addressID, err := getOrCreateAddressID(ctx, address)
				if err != nil {
					log.Printf("Failed to get address ID for %s: %v", address, err)
					continue
				}

				accountBlocks = append(accountBlocks, model.AccountBlock{
					AddressID: addressID,
					Height:    key.Height,
					BlockTime: key.BlockTime,
					Scope:     scope,
				})
			}
		}

		// Insert into ClickHouse
		if len(accountBlocks) > 0 {
			batch, err := ch.Conn.PrepareBatch(ctx, "INSERT INTO account_blocks")
			if err != nil {
				log.Fatalf("Failed to prepare batch: %v", err)
			}

			for _, ab := range accountBlocks {
				err := batch.Append(
					ab.AddressID,
					ab.Height,
					ab.BlockTime,
					ab.Scope,
				)
				if err != nil {
					log.Printf("Failed to append account_block: %v", err)
				}
			}

			if err := batch.Send(); err != nil {
				log.Fatalf("Failed to send batch: %v", err)
			}

			totalAccountBlocks += len(accountBlocks)
		}

		totalProcessed += int(batchEnd - currentHeight + 1)
		log.Printf("Processed %d blocks, inserted %d account_blocks so far", totalProcessed, totalAccountBlocks)

		currentHeight = batchEnd + 1
	}

	log.Printf("Backfill complete. Total blocks processed: %d, Total account_blocks inserted: %d", totalProcessed, totalAccountBlocks)
}
