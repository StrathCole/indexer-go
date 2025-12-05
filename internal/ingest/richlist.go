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

	// Use cursor-based pagination to avoid loading all addresses into memory
	const batchSize = 1000
	var lastID int64 = 0

	// 2. Fetch balances and aggregate
	type AccBal struct {
		Address string
		Amount  string
	}
	richList := make(map[string][]AccBal)

	bankClient := banktypes.NewQueryClient(s.clientCtx)

	for {
		// Fetch addresses in batches using cursor pagination
		rows, err := s.pg.Pool.Query(ctx, 
			"SELECT id, address FROM addresses WHERE id > $1 ORDER BY id LIMIT $2", 
			lastID, batchSize)
		if err != nil {
			log.Printf("Failed to fetch addresses: %v", err)
			return
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
			break // No more addresses
		}

		lastID = maxID

		// Process this batch of addresses
		for _, addr := range addresses {
			// Rate limit
			time.Sleep(10 * time.Millisecond)

			resp, err := bankClient.AllBalances(ctx, &banktypes.QueryAllBalancesRequest{Address: addr})
			if err != nil {
				continue
			}

			for _, coin := range resp.Balances {
				richList[coin.Denom] = append(richList[coin.Denom], AccBal{
					Address: addr,
					Amount:  coin.Amount.String(),
				})
			}
		}

		// If we got fewer than batchSize, we're done
		if len(addresses) < batchSize {
			break
		}
	}

	// 3. Update DB
	tx, err := s.pg.Pool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "TRUNCATE TABLE rich_list")
	if err != nil {
		log.Printf("Failed to truncate rich_list: %v", err)
		return
	}

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
			log.Printf("Failed to fetch total supply: %v", err)
			break
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

	for denom, accounts := range richList {
		total := supplyMap[denom]

		for _, acc := range accounts {
			amountF, _ := strconv.ParseFloat(acc.Amount, 64)
			percentage := 0.0
			if total > 0 {
				percentage = amountF / total
			}

			_, err := tx.Exec(ctx,
				"INSERT INTO rich_list (denom, account, amount, percentage) VALUES ($1, $2, $3, $4)",
				denom, acc.Address, acc.Amount, percentage,
			)
			if err != nil {
				log.Printf("Failed to insert rich list entry: %v", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
	}

	log.Println("Richlist updated")
}
