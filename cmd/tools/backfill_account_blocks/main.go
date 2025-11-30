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

	log.Printf("Backfilling block events into account_txs from height %d to %d", minHeight, maxHeight)

	// Load address cache from Postgres
	addressCache := make(map[string]uint64)
	denomCache := make(map[string]uint16)

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

	getOrCreateDenomID := func(ctx context.Context, denom string) (uint16, error) {
		if id, ok := denomCache[denom]; ok {
			return id, nil
		}

		var id uint16
		err := pg.Pool.QueryRow(ctx, "SELECT id FROM denoms WHERE denom = $1", denom).Scan(&id)
		if err == nil {
			denomCache[denom] = id
			return id, nil
		}

		query := `
			WITH ins AS (
				INSERT INTO denoms (denom) VALUES ($1)
				ON CONFLICT (denom) DO NOTHING
				RETURNING id
			)
			SELECT id FROM ins
			UNION ALL
			SELECT id FROM denoms WHERE denom = $1
			LIMIT 1;
		`

		err = pg.Pool.QueryRow(ctx, query, denom).Scan(&id)
		if err != nil {
			return 0, fmt.Errorf("failed to get or create denom id: %w", err)
		}

		denomCache[denom] = id
		return id, nil
	}

	// Direction keys
	outKeys := map[string]bool{
		"sender": true, "spender": true, "from": true, "delegator": true,
		"proposer": true, "depositor": true, "voter": true, "validator": true,
	}
	inKeys := map[string]bool{
		"recipient": true, "receiver": true, "to": true, "withdraw_address": true,
	}

	// Parse coins helper
	parseCoins := func(s string) (string, int64) {
		// Simple coin parsing: e.g. "1000000uluna" -> ("uluna", 1000000)
		// Handle comma-separated coins and return the largest
		var largestDenom string
		var largestAmount int64

		coins := strings.Split(s, ",")
		for _, coin := range coins {
			coin = strings.TrimSpace(coin)
			if coin == "" {
				continue
			}
			// Find where digits end
			i := 0
			for i < len(coin) && (coin[i] >= '0' && coin[i] <= '9') {
				i++
			}
			if i == 0 || i == len(coin) {
				continue
			}
			amountStr := coin[:i]
			denom := coin[i:]
			var amount int64
			fmt.Sscanf(amountStr, "%d", &amount)
			if amount > largestAmount {
				largestAmount = amount
				largestDenom = denom
			}
		}
		return largestDenom, largestAmount
	}

	// Process in batches
	currentHeight := minHeight
	totalProcessed := 0
	totalAccountTxs := 0

	for currentHeight <= maxHeight {
		batchEnd := currentHeight + uint64(*batchSize) - 1
		if batchEnd > maxHeight {
			batchEnd = maxHeight
		}

		log.Printf("Processing heights %d to %d...", currentHeight, batchEnd)

		// Query events in batch - get attr_key too for direction and amount extraction
		query := `
			SELECT height, block_time, scope, attr_key, attr_value
			FROM events
			WHERE height >= $1 AND height <= $2
			  AND scope IN ('begin_block', 'end_block', 'block')
		`

		rows, err := ch.Conn.Query(ctx, query, currentHeight, batchEnd)
		if err != nil {
			log.Fatalf("Failed to query events: %v", err)
		}

		// Group by height+scope, then by address -> info
		type blockKey struct {
			Height    uint64
			BlockTime time.Time
			Scope     string
		}
		type addressInfo struct {
			direction int8
			denom     string
			amount    int64
		}
		blockData := make(map[blockKey]map[string]*addressInfo)
		blockAmounts := make(map[blockKey]map[string]int64) // denom -> total amount per block

		for rows.Next() {
			var height uint64
			var blockTime time.Time
			var scope string
			var attrKey string
			var attrValue string

			if err := rows.Scan(&height, &blockTime, &scope, &attrKey, &attrValue); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}

			key := blockKey{Height: height, BlockTime: blockTime, Scope: scope}

			// Track amounts per block
			if attrKey == "amount" || attrKey == "coins" {
				if blockAmounts[key] == nil {
					blockAmounts[key] = make(map[string]int64)
				}
				denom, amt := parseCoins(attrValue)
				if denom != "" {
					blockAmounts[key][denom] += amt
				}
			}

			// Check if this is a Terra address
			if !isTerraAddress(attrValue) {
				continue
			}

			if blockData[key] == nil {
				blockData[key] = make(map[string]*addressInfo)
			}

			// Determine direction
			var direction int8 = 0
			if outKeys[attrKey] {
				direction = 2
			} else if inKeys[attrKey] {
				direction = 1
			}

			if existing, ok := blockData[key][attrValue]; ok {
				if existing.direction == 0 && direction != 0 {
					existing.direction = direction
				}
			} else {
				blockData[key][attrValue] = &addressInfo{direction: direction}
			}
		}
		rows.Close()

		// Convert to AccountTxs with is_block_event=true
		var accountTxs []model.AccountTx

		for key, addresses := range blockData {
			// Determine event scope as int8
			var eventScope int8 = model.EventScopeBeginBlock
			if key.Scope == "end_block" {
				eventScope = model.EventScopeEndBlock
			}

			// Find largest amount for this block
			var mainDenom string
			var mainAmount int64
			if amounts, ok := blockAmounts[key]; ok {
				for denom, amt := range amounts {
					if amt > mainAmount {
						mainAmount = amt
						mainDenom = denom
					}
				}
			}

			var mainDenomID uint16
			if mainDenom != "" {
				id, err := getOrCreateDenomID(ctx, mainDenom)
				if err == nil {
					mainDenomID = id
				}
			}

			for address, info := range addresses {
				addressID, err := getOrCreateAddressID(ctx, address)
				if err != nil {
					log.Printf("Failed to get address ID for %s: %v", address, err)
					continue
				}

				accountTxs = append(accountTxs, model.AccountTx{
					AddressID:    addressID,
					Height:       key.Height,
					IndexInBlock: 0,
					BlockTime:    key.BlockTime,
					TxHash:       "",
					Direction:    info.direction,
					MainDenomID:  mainDenomID,
					MainAmount:   mainAmount,
					IsBlockEvent: true,
					EventScope:   eventScope,
				})
			}
		}

		// Insert into ClickHouse account_txs table
		if len(accountTxs) > 0 {
			batch, err := ch.Conn.PrepareBatch(ctx, "INSERT INTO account_txs")
			if err != nil {
				log.Fatalf("Failed to prepare batch: %v", err)
			}

			for _, at := range accountTxs {
				err := batch.Append(
					at.AddressID,
					at.Height,
					at.IndexInBlock,
					at.BlockTime,
					at.TxHash,
					at.Direction,
					at.MainDenomID,
					at.MainAmount,
					at.IsBlockEvent,
					at.EventScope,
				)
				if err != nil {
					log.Printf("Failed to append account_tx: %v", err)
				}
			}

			if err := batch.Send(); err != nil {
				log.Fatalf("Failed to send batch: %v", err)
			}

			totalAccountTxs += len(accountTxs)
		}

		totalProcessed += int(batchEnd - currentHeight + 1)
		log.Printf("Processed %d blocks, inserted %d account_txs so far", totalProcessed, totalAccountTxs)

		currentHeight = batchEnd + 1
	}

	log.Printf("Backfill complete. Total blocks processed: %d, Total account_txs inserted: %d", totalProcessed, totalAccountTxs)
}
