package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/types/query"

	oracletypes "github.com/classic-terra/core/v3/x/oracle/types"
	treasurytypes "github.com/classic-terra/core/v3/x/treasury/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

func (s *Server) safeQuery(ctx context.Context, route string, req codec.ProtoMarshaler, res codec.ProtoMarshaler) error {
	reqBytes, err := s.clientCtx.Codec.Marshal(req)
	if err != nil {
		return err
	}
	resBytes, _, err := s.clientCtx.QueryWithData(route, reqBytes)
	if err != nil {
		return err
	}
	return s.clientCtx.Codec.Unmarshal(resBytes, res)
}

func (s *Server) GetDashboard(w http.ResponseWriter, r *http.Request) {
	// Fetch data in parallel
	g, ctx := errgroup.WithContext(r.Context())

	var (
		pricesResp        = &oracletypes.QueryExchangeRatesResponse{}
		taxRateResp       = &treasurytypes.QueryTaxRateResponse{}
		communityPoolResp = &distrtypes.QueryCommunityPoolResponse{}
		stakingPoolResp   = &stakingtypes.QueryPoolResponse{}
		taxCapsResp       = &treasurytypes.QueryTaxCapsResponse{}
		supplyResp        = &banktypes.QueryTotalSupplyResponse{}
	)

	// 1. Prices
	g.Go(func() error {
		return s.safeQuery(ctx, "/terra.oracle.v1beta1.Query/ExchangeRates", &oracletypes.QueryExchangeRatesRequest{}, pricesResp)
	})

	// 2. Tax Rate
	g.Go(func() error {
		return s.safeQuery(ctx, "/terra.treasury.v1beta1.Query/TaxRate", &treasurytypes.QueryTaxRateRequest{}, taxRateResp)
	})

	// 3. Community Pool
	g.Go(func() error {
		return s.safeQuery(ctx, "/cosmos.distribution.v1beta1.Query/CommunityPool", &distrtypes.QueryCommunityPoolRequest{}, communityPoolResp)
	})

	// 4. Staking Pool
	g.Go(func() error {
		return s.safeQuery(ctx, "/cosmos.staking.v1beta1.Query/Pool", &stakingtypes.QueryPoolRequest{}, stakingPoolResp)
	})

	// 5. Tax Caps (Optional)
	g.Go(func() error {
		err := s.safeQuery(ctx, "/terra.treasury.v1beta1.Query/TaxCaps", &treasurytypes.QueryTaxCapsRequest{}, taxCapsResp)
		if err != nil {
			// Log error but don't fail request
			taxCapsResp = nil
		}
		return nil
	})

	// 6. Issuances (Optional)
	g.Go(func() error {
		// Fetch all pages
		supplyResp = &banktypes.QueryTotalSupplyResponse{}
		var nextKey []byte

		for {
			req := &banktypes.QueryTotalSupplyRequest{
				Pagination: &query.PageRequest{
					Key:        nextKey,
					Limit:      1000,
					CountTotal: false,
				},
			}
			var resp banktypes.QueryTotalSupplyResponse
			err := s.safeQuery(ctx, "/cosmos.bank.v1beta1.Query/TotalSupply", req, &resp)
			if err != nil {
				supplyResp = nil
				return nil
			}

			supplyResp.Supply = append(supplyResp.Supply, resp.Supply...)

			if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
				break
			}
			nextKey = resp.Pagination.NextKey
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to fetch dashboard data: "+err.Error())
		return
	}

	// Process Results
	prices := make(map[string]string)
	for _, p := range pricesResp.ExchangeRates {
		prices[p.Denom] = p.Amount.String()
	}

	taxRate := taxRateResp.TaxRate.String()
	// Format taxRate to match FCD (remove trailing zeros)
	if strings.Contains(taxRate, ".") {
		taxRate = strings.TrimRight(taxRate, "0")
		taxRate = strings.TrimRight(taxRate, ".")
	}

	communityPool := make(map[string]string)
	for _, c := range communityPoolResp.Pool {
		communityPool[c.Denom] = c.Amount.String()
	}

	bonded := stakingPoolResp.Pool.BondedTokens.String() + ".0000000000"
	notBonded := stakingPoolResp.Pool.NotBondedTokens.String() + ".0000000000"

	type TaxCapResponse struct {
		Denom  string `json:"denom"`
		TaxCap string `json:"taxCap"`
	}
	taxCaps := []TaxCapResponse{}
	if taxCapsResp != nil {
		for _, tc := range taxCapsResp.TaxCaps {
			taxCaps = append(taxCaps, TaxCapResponse{
				Denom:  tc.Denom,
				TaxCap: tc.TaxCap.String(),
			})
		}
	}

	issuanceMap := make(map[string]string)
	if supplyResp != nil {
		for _, coin := range supplyResp.Supply {
			issuanceMap[coin.Denom] = coin.Amount.String()
		}
	}

	stakingRatio := "0"
	if bonded != "" && issuanceMap["uluna"] != "" {
		var b, t float64
		fmt.Sscanf(bonded, "%f", &b)
		fmt.Sscanf(issuanceMap["uluna"], "%f", &t)
		if t > 0 {
			stakingRatio = fmt.Sprintf("%.17f", b/t)
		}
	}

	response := map[string]interface{}{
		"prices":        prices,
		"taxRate":       taxRate,
		"taxCaps":       taxCaps,
		"communityPool": communityPool,
		"stakingPool": map[string]string{
			"bondedTokens":    bonded,
			"notBondedTokens": notBonded,
			"stakingRatio":    stakingRatio,
		},
		"issuances": issuanceMap,
	}

	respondJSON(w, http.StatusOK, response)
}

func (s *Server) respondWithCache(w http.ResponseWriter, key string, duration time.Duration, fetchFunc func() (interface{}, error)) {
	if cached, found := s.cache.Get(key); found {
		respondJSON(w, http.StatusOK, cached)
		return
	}

	data, err := fetchFunc()
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.cache.Set(key, data, duration)
	respondJSON(w, http.StatusOK, data)
}

func (s *Server) fetchTxVolume() (interface{}, error) {
	// Fetch denoms
	denoms, _ := s.getDenoms(context.Background())
	if denoms == nil {
		denoms = make(map[uint16]string)
	}

	type AggRow struct {
		Time    uint64 `ch:"datetime"`
		DenomID uint16 `ch:"denom_id"`
		Volume  uint64 `ch:"volume"`
	}

	var rows []AggRow
	sql := `
		SELECT 
			toUnixTimestamp(toStartOfDay(block_time))*1000 as datetime, 
			main_denom_id as denom_id, 
			sum(abs(main_amount)) as volume 
		FROM account_txs 
		WHERE main_denom_id > 0 AND direction = 1
		GROUP BY datetime, main_denom_id 
		ORDER BY datetime ASC
	`
	err := s.ch.Conn.Select(context.Background(), &rows, sql)
	if err != nil {
		return nil, err
	}

	periodicMap := make(map[string][]map[string]interface{})
	cumulativeMap := make(map[string][]map[string]interface{})
	runningTotal := make(map[string]*big.Int)

	for _, row := range rows {
		denom := denoms[row.DenomID]
		if denom == "" {
			continue
		}

		if _, ok := runningTotal[denom]; !ok {
			runningTotal[denom] = big.NewInt(0)
		}

		vol := new(big.Int).SetUint64(row.Volume)
		runningTotal[denom].Add(runningTotal[denom], vol)

		// Periodic
		periodicMap[denom] = append(periodicMap[denom], map[string]interface{}{
			"datetime": row.Time,
			"txVolume": fmt.Sprintf("%d", row.Volume),
		})

		// Cumulative
		cumulativeMap[denom] = append(cumulativeMap[denom], map[string]interface{}{
			"datetime": row.Time,
			"txVolume": runningTotal[denom].String(),
		})
	}

	var periodic []interface{}
	for denom, data := range periodicMap {
		periodic = append(periodic, map[string]interface{}{
			"denom": denom,
			"data":  data,
		})
	}

	var cumulative []interface{}
	for denom, data := range cumulativeMap {
		cumulative = append(cumulative, map[string]interface{}{
			"denom": denom,
			"data":  data,
		})
	}

	// Initialize empty slices if nil
	if periodic == nil {
		periodic = []interface{}{}
	}
	if cumulative == nil {
		cumulative = []interface{}{}
	}

	return map[string]interface{}{
		"periodic":   periodic,
		"cumulative": cumulative,
	}, nil
}

func (s *Server) GetTxVolume(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "tx_volume", 5*time.Minute, s.fetchTxVolume)
}

func (s *Server) GetBlockRewards(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "block_rewards", 5*time.Minute, func() (interface{}, error) {
		type Reward struct {
			Time        uint64 `json:"datetime" ch:"datetime"`
			BlockReward string `json:"blockReward" ch:"blockReward"`
		}

		var rewards []Reward
		sql := `
			SELECT 
				toUnixTimestamp(toStartOfDay(block_time))*1000 as datetime, 
				toString(sum(cast(extract(reward_str, '^[\\d\\.]+') as Decimal(38, 10)))) as blockReward 
			FROM (
				SELECT 
					block_time,
					arrayJoin(splitByChar(',', attr_value)) as reward_str
				FROM events 
				PREWHERE event_type = 'rewards' AND attr_key = 'amount'
			)
			WHERE replaceRegexpOne(reward_str, '^[\\d\\.]+', '') = 'ukrw'
			GROUP BY datetime 
			ORDER BY datetime ASC
		`
		err := s.ch.Conn.Select(context.Background(), &rewards, sql)
		if err != nil {
			return map[string]interface{}{
				"periodic":   []interface{}{},
				"cumulative": []interface{}{},
			}, nil
		}

		// Calculate cumulative
		var cumulative []Reward
		runningTotal := new(big.Float)

		for _, r := range rewards {
			amount := new(big.Float)
			amount.SetString(r.BlockReward)
			runningTotal.Add(runningTotal, amount)
			cumulative = append(cumulative, Reward{
				Time:        r.Time,
				BlockReward: runningTotal.Text('f', 10),
			})
		}

		if rewards == nil {
			rewards = []Reward{}
		}
		if cumulative == nil {
			cumulative = []Reward{}
		}

		return map[string]interface{}{
			"periodic":   rewards,
			"cumulative": cumulative,
		}, nil
	})
}

func (s *Server) GetSeigniorageProceeds(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "seigniorage_proceeds", 5*time.Minute, func() (interface{}, error) {
		type Proceeds struct {
			Time   uint64 `json:"datetime" ch:"datetime"`
			Amount string `json:"seigniorageProceeds" ch:"seigniorageProceeds"`
		}

		var proceeds []Proceeds
		// Note: seigniorage_proceeds event might not exist in all blocks or chains (Columbus-5 vs Classic).
		// In Classic, it might be 'seigniorage_proceeds' or similar.
		// Also check if 'amount' is the key.
		// If empty, return empty array.

		sql := `
		 SELECT 
				toUnixTimestamp(toStartOfDay(block_time))*1000 as datetime, 
				toString(sum(cast(extract(attr_value, '^[\\d\\.]+') as Decimal(38, 10)))) as seigniorageProceeds 
			FROM events 
			PREWHERE event_type = 'seigniorage_proceeds' AND attr_key = 'amount'
			GROUP BY datetime 
			ORDER BY datetime ASC
		`
		err := s.ch.Conn.Select(context.Background(), &proceeds, sql)
		if err != nil {
			return []interface{}{}, nil
		}

		if proceeds == nil {
			proceeds = []Proceeds{}
		}

		return proceeds, nil
	})
}

func (s *Server) GetStakingReturn(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "staking_return", 5*time.Minute, func() (interface{}, error) {
		// We need daily return = (rewards + airdrop) / avg_staking
		// We lack avg_staking history.
		// We will return 0s for now but correct structure.

		type Return struct {
			Time   uint64 `json:"datetime" ch:"datetime"`
			Amount string `json:"dailyReturn" ch:"dailyReturn"`
		}

		var returns []Return
		// Use blocks table to generate timeline if events are missing
		sql := `
			SELECT 
				toUnixTimestamp(toStartOfDay(block_time))*1000 as datetime, 
				'0' as dailyReturn 
			FROM blocks
			GROUP BY datetime 
			ORDER BY datetime ASC
		`
		err := s.ch.Conn.Select(context.Background(), &returns, sql)
		if err != nil {
			return []interface{}{}, nil
		}

		var result []map[string]interface{}
		for _, r := range returns {
			result = append(result, map[string]interface{}{
				"datetime":         r.Time,
				"dailyReturn":      "0",
				"annualizedReturn": "0",
			})
		}

		if result == nil {
			result = []map[string]interface{}{}
		}

		return result, nil
	})
}

func (s *Server) GetStakingRatio(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "staking_ratio_history", 5*time.Minute, func() (interface{}, error) {
		// Fetch current staking pool and total supply
		stakingClient := stakingtypes.NewQueryClient(s.clientCtx)
		stakingPoolResp, err := stakingClient.Pool(context.Background(), &stakingtypes.QueryPoolRequest{})
		if err != nil {
			return nil, err
		}
		bonded := stakingPoolResp.Pool.BondedTokens.String()

		bankClient := banktypes.NewQueryClient(s.clientCtx)

		// Fetch all pages for TotalSupply
		var ulunaSupply string
		var nextKey []byte

		for {
			req := &banktypes.QueryTotalSupplyRequest{
				Pagination: &query.PageRequest{
					Key:        nextKey,
					Limit:      1000,
					CountTotal: false,
				},
			}
			resp, err := bankClient.TotalSupply(context.Background(), req)
			if err != nil {
				return nil, err
			}

			for _, coin := range resp.Supply {
				if coin.Denom == "uluna" {
					ulunaSupply = coin.Amount.String()
					break
				}
			}
			if ulunaSupply != "" {
				break
			}

			if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
				break
			}
			nextKey = resp.Pagination.NextKey
		}

		stakingRatio := "0"
		if bonded != "" && ulunaSupply != "" {
			var b, t float64
			fmt.Sscanf(bonded, "%f", &b)
			fmt.Sscanf(ulunaSupply, "%f", &t)
			if t > 0 {
				stakingRatio = fmt.Sprintf("%.15f", b/t)
			}
		}

		// Return as a single history point with current timestamp
		// FCD returns stakingRatio as a number (float)
		var ratioFloat float64
		if stakingRatio != "" {
			fmt.Sscanf(stakingRatio, "%f", &ratioFloat)
		}

		return []map[string]interface{}{
			{
				"datetime":     uint64(time.Now().Unix() * 1000),
				"stakingRatio": ratioFloat,
			},
		}, nil
	})
}

func (s *Server) GetAccountGrowth(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "account_growth", 1*time.Hour, func() (interface{}, error) {
		type Growth struct {
			Time   uint64 `json:"datetime" ch:"datetime"`
			Count  uint64 `json:"totalAccountCount" ch:"totalAccount"`
			Active uint64 `json:"activeAccountCount" ch:"activeAccount"`
		}

		// Active accounts per day
		var active []struct {
			Time  uint64 `ch:"datetime"`
			Count uint64 `ch:"count"`
		}
		sqlActiveAgg := `
			SELECT
				toUnixTimestamp(day) * 1000 AS datetime,
				uniqCombined64Merge(active_state) AS count
			FROM account_txs_daily_active_tx
			GROUP BY day
			ORDER BY day ASC
		`
		err := s.ch.Conn.Select(context.Background(), &active, sqlActiveAgg)
		if err != nil {
			sqlActiveRaw := `
				SELECT 
					toUnixTimestamp(toStartOfDay(block_time))*1000 as datetime, 
					uniq(address_id) as count 
				FROM account_txs
				WHERE is_block_event = 0
				GROUP BY datetime 
				ORDER BY datetime ASC
			`
			_ = s.ch.Conn.Select(context.Background(), &active, sqlActiveRaw)
		}
		activeMap := make(map[uint64]uint64)
		for _, a := range active {
			activeMap[a.Time] = a.Count
		}

		// New accounts per day (prefer precomputed daily table).
		var newAccs []struct {
			Time  uint64 `ch:"datetime"`
			Count uint64 `ch:"count"`
		}
		sqlNewDaily := `
			SELECT
				toUnixTimestamp(day) * 1000 AS datetime,
				sum(value) AS count
			FROM registered_accounts_daily
			GROUP BY day
			ORDER BY day ASC
		`
		err = s.ch.Conn.Select(context.Background(), &newAccs, sqlNewDaily)
		if err != nil {
			// Fallback: compute from address_first_seen states (still expensive at large scale).
			sqlNewFromStates := `
				SELECT
					toUnixTimestamp(toStartOfDay(first_seen))*1000 AS datetime,
					count() AS count
				FROM (
					SELECT
						address_id,
						minMerge(first_seen_state) AS first_seen
					FROM address_first_seen
					GROUP BY address_id
				)
				GROUP BY datetime
				ORDER BY datetime ASC
			`
			err = s.ch.Conn.Select(context.Background(), &newAccs, sqlNewFromStates)
			if err != nil {
				// Final fallback: original full scan.
				sqlNewRaw := `
					SELECT 
						toUnixTimestamp(toStartOfDay(min_time))*1000 as datetime, 
						count() as count 
					FROM (
						SELECT address_id, min(block_time) as min_time 
						FROM account_txs 
						GROUP BY address_id
					) 
					GROUP BY datetime 
					ORDER BY datetime ASC
				`
				_ = s.ch.Conn.Select(context.Background(), &newAccs, sqlNewRaw)
			}
		}

		newAccMap := make(map[uint64]uint64)
		for _, n := range newAccs {
			newAccMap[n.Time] = n.Count
		}

		allDates := make(map[uint64]struct{})
		for _, a := range active {
			allDates[a.Time] = struct{}{}
		}
		for _, n := range newAccs {
			allDates[n.Time] = struct{}{}
		}
		dates := make([]uint64, 0, len(allDates))
		for d := range allDates {
			dates = append(dates, d)
		}
		sort.Slice(dates, func(i, j int) bool { return dates[i] < dates[j] })

		var periodic []Growth
		var cumulative []Growth
		var runningTotal uint64
		var cumulativeActive uint64

		for _, d := range dates {
			newCount := newAccMap[d]
			activeCount := activeMap[d]
			runningTotal += newCount
			cumulativeActive += activeCount

			cumulative = append(cumulative, Growth{Time: d, Count: runningTotal, Active: cumulativeActive})
			periodic = append(periodic, Growth{Time: d, Count: newCount, Active: activeCount})
		}

		// Match fcd-classic: omit the first data point.
		if len(periodic) > 0 {
			periodic = periodic[1:]
		}
		if len(cumulative) > 0 {
			cumulative = cumulative[1:]
		}

		if periodic == nil {
			periodic = []Growth{}
		}
		if cumulative == nil {
			cumulative = []Growth{}
		}

		return map[string]interface{}{
			"periodic":   periodic,
			"cumulative": cumulative,
		}, nil
	})
}

func (s *Server) GetActiveAccounts(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "active_accounts", 1*time.Hour, func() (interface{}, error) {
		type Active struct {
			Time  uint64 `json:"datetime" ch:"datetime"`
			Count uint64 `json:"value" ch:"value"`
		}

		var active []Active
		// Prefer a pre-aggregated table if present. This avoids a full scan over `account_txs`.
		// See `schema.sql` for the materialized view/table that populate this.
		sqlAgg := `
			SELECT
				toUnixTimestamp(day) * 1000 AS datetime,
				uniqCombined64Merge(active_state) AS value
			FROM account_txs_daily_active_tx
			GROUP BY day
			ORDER BY day ASC
		`
		err := s.ch.Conn.Select(context.Background(), &active, sqlAgg)
		if err != nil {
			// Fallback to raw scan if the aggregate table doesn't exist (or query fails).
			sqlRaw := `
				SELECT
					toUnixTimestamp(toStartOfDay(block_time))*1000 as datetime,
					uniq(address_id) as value
				FROM account_txs
				WHERE is_block_event = 0
				GROUP BY datetime
				ORDER BY datetime ASC
			`
			err = s.ch.Conn.Select(context.Background(), &active, sqlRaw)
		}
		if err != nil {
			return map[string]interface{}{
				"total":    0,
				"periodic": []interface{}{},
			}, nil
		}

		var total uint64 = 0
		// Total active accounts ever? Or in period?
		// FCD `getActiveAccounts` returns `total` as `last totalAccount`.
		// Wait, FCD code: `total: dashboardHistory[last].totalAccount`.
		// So it returns total registered accounts count as "total" in active_accounts endpoint?
		// Yes, `getActiveAccounts.ts` imports `getDashboardHistory` and returns `totalAccount`.
		// Weird naming.

		// We need total accounts count.
		// Prefer ClickHouse daily table (fast). Fallback to Postgres count(*) if needed.
		if err := s.ch.Conn.QueryRow(context.Background(), "SELECT sum(value) FROM registered_accounts_daily").Scan(&total); err != nil || total == 0 {
			_ = s.pg.Pool.QueryRow(context.Background(), "SELECT count(*) FROM addresses").Scan(&total)
		}

		if active == nil {
			active = []Active{}
		}

		return map[string]interface{}{
			"total":    total,
			"periodic": active,
		}, nil
	})
}

func (s *Server) GetRegisteredAccounts(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "registered_accounts", 1*time.Hour, func() (interface{}, error) {
		var total uint64
		var err error
		// Prefer ClickHouse daily table (fast). Fallback to Postgres count(*) if needed.
		err = s.ch.Conn.QueryRow(context.Background(), "SELECT sum(value) FROM registered_accounts_daily").Scan(&total)
		if err != nil || total == 0 {
			err = s.pg.Pool.QueryRow(context.Background(), "SELECT count(*) FROM addresses").Scan(&total)
			if err != nil {
				return nil, err
			}
		}

		type Growth struct {
			Time  uint64 `json:"datetime" ch:"datetime"`
			Count uint64 `json:"value" ch:"value"`
		}
		var growth []Growth
		sqlDaily := `
			SELECT
				toUnixTimestamp(day) * 1000 AS datetime,
				sum(value) AS value
			FROM registered_accounts_daily
			GROUP BY day
			ORDER BY day ASC
		`
		err = s.ch.Conn.Select(context.Background(), &growth, sqlDaily)
		if err != nil {
			// Fallback: compute from address_first_seen states.
			sqlFromStates := `
				SELECT
					toUnixTimestamp(toStartOfDay(first_seen))*1000 AS datetime,
					count() AS value
				FROM (
					SELECT
						address_id,
						minMerge(first_seen_state) AS first_seen
					FROM address_first_seen
					GROUP BY address_id
				)
				GROUP BY datetime
				ORDER BY datetime ASC
			`
			err = s.ch.Conn.Select(context.Background(), &growth, sqlFromStates)
			if err != nil {
				// Final fallback: original full scan.
				sqlRaw := `
					SELECT 
						toUnixTimestamp(toStartOfDay(min_time))*1000 as datetime, 
						count() as value 
					FROM (
						SELECT address_id, min(block_time) as min_time 
						FROM account_txs 
						GROUP BY address_id
					) 
					GROUP BY datetime 
					ORDER BY datetime ASC
				`
				_ = s.ch.Conn.Select(context.Background(), &growth, sqlRaw)
			}
		}

		var cumulative []Growth
		var runningTotal uint64 = 0
		for _, g := range growth {
			runningTotal += g.Count
			cumulative = append(cumulative, Growth{
				Time:  g.Time,
				Count: runningTotal,
			})
		}

		if growth == nil {
			growth = []Growth{}
		}
		if cumulative == nil {
			cumulative = []Growth{}
		}

		// Match fcd-classic: omit the first data point.
		if len(growth) > 0 {
			growth = growth[1:]
		}
		if len(cumulative) > 0 {
			cumulative = cumulative[1:]
		}

		return map[string]interface{}{
			"total":      total,
			"periodic":   growth,
			"cumulative": cumulative,
		}, nil
	})
}

func (s *Server) GetLastHourOpsAndTxs(w http.ResponseWriter, r *http.Request) {
	var txCount uint64
	var opCount uint64

	_ = s.ch.Conn.QueryRow(context.Background(), `
		SELECT uniq(tx_hash) 
		FROM account_txs 
		WHERE block_time >= now() - INTERVAL 1 HOUR
	`).Scan(&txCount)

	opCount = txCount // Approximation

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"last_1h_op": opCount,
		"last_1h_tx": txCount,
	})
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil {
		// Headers are already written; best-effort error body.
		_, _ = w.Write([]byte(`{"error":"failed to encode response"}`))
	}
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
