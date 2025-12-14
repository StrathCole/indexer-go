package ingest

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/types/query"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
)

type RichlistService struct {
	pg             *db.Postgres
	clientCtx      client.Context
	mu             sync.Mutex
	updateInterval time.Duration
}

const richlistFullRebuildInterval = 7 * 24 * time.Hour

func NewRichlistService(pg *db.Postgres, clientCtx client.Context, updateInterval time.Duration) *RichlistService {
	return &RichlistService{
		pg:             pg,
		clientCtx:      clientCtx,
		updateInterval: updateInterval,
	}
}

func (s *RichlistService) Start(ctx context.Context) {
	ticker := time.NewTicker(s.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.UpdateRichlist(ctx)
		}
	}
}

func (s *RichlistService) UpdateRichlist(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Println("Updating richlist...")

	bankClient := banktypes.NewQueryClient(s.clientCtx)

	// Ensure metadata row exists and load schedule state.
	_, _ = s.pg.Pool.Exec(ctx, "INSERT INTO rich_list_meta (id) VALUES (1) ON CONFLICT (id) DO NOTHING")

	var lastFull time.Time
	var lastInc time.Time
	err := s.pg.Pool.QueryRow(ctx, "SELECT last_full_rebuild, last_incremental_run FROM rich_list_meta WHERE id = 1").Scan(&lastFull, &lastInc)
	if err != nil {
		log.Printf("Failed to load rich_list_meta: %v", err)
		return
	}

	now := time.Now().UTC()
	needFull := now.Sub(lastFull) >= richlistFullRebuildInterval
	if needFull {
		log.Println("Running full richlist rebuild")
		if err := s.fullRebuild(ctx, bankClient); err != nil {
			log.Printf("Full richlist rebuild failed: %v", err)
			return
		}
		_, _ = s.pg.Pool.Exec(ctx, "UPDATE rich_list_meta SET last_full_rebuild = $1, last_incremental_run = $1 WHERE id = 1", now)
		log.Println("Richlist full rebuild complete")
		return
	}

	if lastInc.Before(lastFull) {
		lastInc = lastFull
	}

	log.Printf("Running incremental richlist update (since %s)", lastInc.Format(time.RFC3339))
	if err := s.incrementalUpdate(ctx, bankClient, lastInc); err != nil {
		log.Printf("Incremental richlist update failed: %v", err)
		return
	}
	_, _ = s.pg.Pool.Exec(ctx, "UPDATE rich_list_meta SET last_incremental_run = $1 WHERE id = 1", now)
	log.Println("Richlist incremental update complete")
}

func (s *RichlistService) buildSupplyMap(ctx context.Context, bankClient banktypes.QueryClient) (map[string]float64, error) {
	supplyMap := make(map[string]float64)

	var nextKey []byte
	for {
		req := &banktypes.QueryTotalSupplyRequest{
			Pagination: &query.PageRequest{
				Key:   nextKey,
				Limit: 1000,
			},
		}
		supplyResp, err := bankClient.TotalSupply(ctx, req)
		if err != nil {
			return supplyMap, err
		}

		for _, coin := range supplyResp.Supply {
			f, _ := strconv.ParseFloat(coin.Amount.String(), 64)
			supplyMap[coin.Denom] = f
		}

		if supplyResp.Pagination == nil || len(supplyResp.Pagination.NextKey) == 0 {
			break
		}
		nextKey = supplyResp.Pagination.NextKey
	}

	return supplyMap, nil
}

func (s *RichlistService) fullRebuild(ctx context.Context, bankClient banktypes.QueryClient) error {
	// Use cursor-based pagination to avoid loading all addresses into memory
	const batchSize = 1000
	var lastID int64 = 0

	// Ensure staging table exists
	if _, err := s.pg.Pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS rich_list_build (LIKE rich_list INCLUDING ALL)"); err != nil {
		return err
	}

	supplyMap, err := s.buildSupplyMap(ctx, bankClient)
	if err != nil {
		return err
	}

	tx, err := s.pg.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Build the new snapshot into the staging table, leaving rich_list readable.
	if _, err := tx.Exec(ctx, "TRUNCATE TABLE rich_list_build"); err != nil {
		return err
	}

	for {
		rows, err := s.pg.Pool.Query(ctx,
			"SELECT id, address FROM addresses WHERE id > $1 ORDER BY id LIMIT $2",
			lastID, batchSize,
		)
		if err != nil {
			return err
		}
		var addresses []string
		var maxID int64
		for rows.Next() {
			var id int64
			var addr string
			if err := rows.Scan(&id, &addr); err == nil {
				addresses = append(addresses, addr)
				if id > maxID {
					maxID = id
				}
			}
		}
		rows.Close()

		if len(addresses) == 0 {
			break
		}
		lastID = maxID

		for _, addr := range addresses {
			// Rate limit
			time.Sleep(10 * time.Millisecond)

			var balNextKey []byte
			for {
				resp, err := bankClient.AllBalances(ctx, &banktypes.QueryAllBalancesRequest{
					Address: addr,
					Pagination: &query.PageRequest{
						Key:   balNextKey,
						Limit: 1000,
					},
				})
				if err != nil {
					break
				}

				for _, coin := range resp.Balances {
					total := supplyMap[coin.Denom]
					amountF, _ := strconv.ParseFloat(coin.Amount.String(), 64)
					percentage := 0.0
					if total > 0 {
						percentage = amountF / total
					}

					_, err := tx.Exec(ctx,
						"INSERT INTO rich_list_build (denom, account, amount, percentage) VALUES ($1, $2, $3, $4)",
						coin.Denom, addr, coin.Amount.String(), percentage,
					)
					if err != nil {
						log.Printf("Failed to insert rich list entry: %v", err)
					}
				}

				if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
					break
				}
				balNextKey = resp.Pagination.NextKey
			}
		}

		if len(addresses) < batchSize {
			break
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Atomically swap tables so readers always see a complete snapshot.
	swapTx, err := s.pg.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer swapTx.Rollback(ctx)

	// Rename sequence:
	// rich_list -> rich_list_old
	// rich_list_build -> rich_list
	// rich_list_old -> rich_list_build (reused as the staging table)
	if _, err := swapTx.Exec(ctx, "ALTER TABLE rich_list RENAME TO rich_list_old"); err != nil {
		return err
	}
	if _, err := swapTx.Exec(ctx, "ALTER TABLE rich_list_build RENAME TO rich_list"); err != nil {
		return err
	}
	if _, err := swapTx.Exec(ctx, "ALTER TABLE rich_list_old RENAME TO rich_list_build"); err != nil {
		return err
	}

	// Clear the staging table for the next rebuild; this does not affect live reads.
	if _, err := swapTx.Exec(ctx, "TRUNCATE TABLE rich_list_build"); err != nil {
		return err
	}

	return swapTx.Commit(ctx)
}

func (s *RichlistService) incrementalUpdate(ctx context.Context, bankClient banktypes.QueryClient, since time.Time) error {
	const batchSize = 1000
	var lastID int64 = 0

	supplyMap, err := s.buildSupplyMap(ctx, bankClient)
	if err != nil {
		return err
	}

	for {
		rows, err := s.pg.Pool.Query(ctx,
			"SELECT id, address FROM addresses WHERE last_seen_at >= $1 AND id > $2 ORDER BY id LIMIT $3",
			since, lastID, batchSize,
		)
		if err != nil {
			return err
		}
		type addrRow struct {
			id      int64
			address string
		}
		var batch []addrRow
		var maxID int64
		for rows.Next() {
			var id int64
			var addr string
			if err := rows.Scan(&id, &addr); err == nil {
				batch = append(batch, addrRow{id: id, address: addr})
				if id > maxID {
					maxID = id
				}
			}
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}
		lastID = maxID

		tx, err := s.pg.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		var batchErr error
		for _, row := range batch {
			// Rate limit
			time.Sleep(10 * time.Millisecond)

			// Fetch current balances
			balances := make(map[string]string)
			var balNextKey []byte
			for {
				resp, err := bankClient.AllBalances(ctx, &banktypes.QueryAllBalancesRequest{
					Address: row.address,
					Pagination: &query.PageRequest{
						Key:   balNextKey,
						Limit: 1000,
					},
				})
				if err != nil {
					balances = nil
					break
				}

				for _, coin := range resp.Balances {
					balances[coin.Denom] = coin.Amount.String()
				}

				if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
					break
				}
				balNextKey = resp.Pagination.NextKey
			}

			if balances == nil {
				continue
			}

			// Replace all existing rich_list rows for this account.
			if _, execErr := tx.Exec(ctx, "DELETE FROM rich_list WHERE account = $1", row.address); execErr != nil {
				batchErr = execErr
				break
			}

			for denom, amount := range balances {
				total := supplyMap[denom]
				amountF, _ := strconv.ParseFloat(amount, 64)
				percentage := 0.0
				if total > 0 {
					percentage = amountF / total
				}

				if _, execErr := tx.Exec(ctx,
					"INSERT INTO rich_list (denom, account, amount, percentage) VALUES ($1, $2, $3, $4)",
					denom, row.address, amount, percentage,
				); execErr != nil {
					log.Printf("Failed to insert rich list entry: %v", execErr)
				}
			}
		}

		if batchErr != nil {
			_ = tx.Rollback(ctx)
			return batchErr
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}

		if len(batch) < batchSize {
			break
		}
	}

	return nil
}
