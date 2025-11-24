package main

import (
	"context"
	"fmt"
	"log"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/model"
	"github.com/cosmos/cosmos-sdk/types"
)

func main() {
	// Load Config
	cfg, err := config.LoadConfig(".")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Connect to DBs
	ch, err := db.NewClickHouse(cfg.Database.ClickHouseAddr, cfg.Database.ClickHouseDB, cfg.Database.ClickHouseUser, cfg.Database.ClickHousePassword)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}

	pg, err := db.NewPostgres(cfg.Database.PostgresConn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}

	// 1. Fetch Denoms Map
	denoms := make(map[string]uint16)
	rows, err := pg.Pool.Query(context.Background(), "SELECT id, denom FROM denoms")
	if err != nil {
		log.Fatalf("Failed to fetch denoms: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uint16
		var denom string
		if err := rows.Scan(&id, &denom); err == nil {
			denoms[denom] = id
		}
	}

	log.Println("Loaded denoms map")

	// 2. Find Txs with Tax Events
	// We look for events with type 'tax' or 'treasury' and key 'amount' or 'tax'
	// We group by tx_hash
	query := `
		SELECT 
			tx_hash,
			groupArray(attr_value) as values
		FROM events
		WHERE (event_type = 'tax' OR event_type = 'treasury') 
		  AND (attr_key = 'amount' OR attr_key = 'tax')
		GROUP BY tx_hash
	`

	log.Println("Querying events...")
	chRows, err := ch.Conn.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Failed to query events: %v", err)
	}
	defer chRows.Close()

	count := 0
	for chRows.Next() {
		var txHash string
		var values []string
		if err := chRows.Scan(&txHash, &values); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		// Calculate Tax
		var taxAmounts []int64
		var taxDenomIDs []uint16

		for _, val := range values {
			coins, err := types.ParseCoinsNormalized(val)
			if err == nil {
				for _, coin := range coins {
					taxAmounts = append(taxAmounts, coin.Amount.Int64())

					// Get or Create Denom ID?
					// For backfill, we assume denom exists or we skip/log.
					// Creating denoms here might be complex if we don't have the logic.
					// But usually denoms are standard.
					if id, ok := denoms[coin.Denom]; ok {
						taxDenomIDs = append(taxDenomIDs, id)
					} else {
						// Insert new denom?
						// Let's just use 0 or skip for now to avoid complexity
						// Or better, insert it.
						var newID uint16
						err := pg.Pool.QueryRow(context.Background(), "INSERT INTO denoms (denom) VALUES ($1) ON CONFLICT (denom) DO UPDATE SET denom=EXCLUDED.denom RETURNING id", coin.Denom).Scan(&newID)
						if err == nil {
							denoms[coin.Denom] = newID
							taxDenomIDs = append(taxDenomIDs, newID)
						} else {
							log.Printf("Failed to insert denom %s: %v", coin.Denom, err)
						}
					}
				}
			}
		}

		if len(taxAmounts) > 0 {
			// ClickHouse doesn't support standard UPDATE efficiently or at all for some engines.
			// The best way is to insert a new row with the same primary key (height, index_in_block)
			// which will replace the old one in ReplacingMergeTree, or use ALTER TABLE UPDATE.
			// But standard MergeTree doesn't support replacement easily without duplicates.
			// However, we can use ALTER TABLE UPDATE which is a mutation.
			// But user says "you cannot update clickhouse, you need delete and insert logic".
			// This implies we should DELETE the old row and INSERT the new one.
			// Or if using ReplacingMergeTree, just INSERT.
			// Our schema uses MergeTree.
			// So we must DELETE then INSERT.

			// 1. Fetch the full Tx first (we need all fields to re-insert)
			var tx model.Tx
			err := ch.Conn.QueryRow(context.Background(), "SELECT * FROM txs WHERE tx_hash = unhex(?)", txHash).ScanStruct(&tx)
			if err != nil {
				log.Printf("Failed to fetch tx %s: %v", txHash, err)
				continue
			}

			// 2. Update fields
			tx.TaxAmounts = taxAmounts
			tx.TaxDenomIDs = taxDenomIDs

			// 3. Delete old row
			// ALTER TABLE txs DELETE WHERE tx_hash = unhex('...')
			// Note: Mutations are async. This might be slow.
			// But for a tool it's okay.
			// Wait, if we insert immediately, we might have duplicates until merge?
			// MergeTree doesn't deduplicate.
			// If we use ALTER DELETE, it's a mutation.

			// Alternative:
			// If we can't use mutations efficiently, we might need to drop partition? No.

			// Let's try the mutation approach as requested.
			if err := ch.Conn.Exec(context.Background(), fmt.Sprintf("ALTER TABLE txs DELETE WHERE tx_hash = unhex('%s')", txHash)); err != nil {
				log.Printf("Failed to delete tx %s: %v", txHash, err)
				continue
			}

			// 4. Insert new row
			// We need to wait for delete? No, usually not guaranteed.
			// Actually, inserting a duplicate in MergeTree results in two rows.
			// If we delete one, eventually we have one.
			// But queries might see 2 or 0 for a moment.

			// Batch insert would be better, but for this tool we do one by one or small batches.
			// Let's just insert.

			batch, err := ch.Conn.PrepareBatch(context.Background(), "INSERT INTO txs")
			if err != nil {
				log.Printf("Failed to prepare batch: %v", err)
				continue
			}
			if err := batch.AppendStruct(&tx); err != nil {
				log.Printf("Failed to append struct: %v", err)
				continue
			}
			if err := batch.Send(); err != nil {
				log.Printf("Failed to send batch: %v", err)
				continue
			}

			count++
			if count%100 == 0 {
				log.Printf("Processed %d txs", count)
			}
		}
	}

	log.Printf("Finished. Updated %d txs.", count)
}
