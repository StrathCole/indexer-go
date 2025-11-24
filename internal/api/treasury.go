package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	oracletypes "github.com/classic-terra/core/v3/x/oracle/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	"github.com/gorilla/mux"
)

func (s *Server) GetTaxProceeds(w http.ResponseWriter, r *http.Request) {
	// Query ClickHouse for tax logs
	type TaxProceed struct {
		Denom          string `json:"denom" ch:"denom"`
		Total          string `json:"amount" ch:"amount"`
		AdjustedAmount string `json:"adjustedAmount" ch:"adjustedAmount"`
	}

	var proceeds []TaxProceed
	sql := `
SELECT 
extract(attr_value, '([a-z]+)') as denom,
toString(sum(cast(extract(attr_value, '(\\d+)') as UInt64))) as amount,
toString(sum(cast(extract(attr_value, '(\\d+)') as UInt64))) as adjustedAmount
FROM events 
WHERE event_type = 'tax' AND attr_key = 'amount'
GROUP BY denom
`

	err := s.ch.Conn.Select(context.Background(), &proceeds, sql)
	if err != nil {
		// Log error?
	}

	// Fetch Oracle Prices
	oracleClient := oracletypes.NewQueryClient(s.clientCtx)
	priceMap := make(map[string]sdk.Dec)
	ratesResp, err := oracleClient.ExchangeRates(context.Background(), &oracletypes.QueryExchangeRatesRequest{})
	if err == nil {
		for _, r := range ratesResp.ExchangeRates {
			priceMap[r.Denom] = r.Amount
		}
	}

	var filteredProceeds []TaxProceed
	total := sdk.ZeroDec()

	for _, p := range proceeds {
		amount, err := sdk.NewDecFromStr(p.Total)
		if err != nil {
			continue
		}

		if p.Denom == "uluna" {
			p.AdjustedAmount = p.Total
			total = total.Add(amount)
			filteredProceeds = append(filteredProceeds, p)
		} else {
			if price, ok := priceMap[p.Denom]; ok && !price.IsZero() {
				adj := amount.Quo(price)
				p.AdjustedAmount = adj.String()
				total = total.Add(adj)
				filteredProceeds = append(filteredProceeds, p)
			}
		}
	}

	if filteredProceeds == nil {
		filteredProceeds = []TaxProceed{}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"total":       total.String(),
		"taxProceeds": filteredProceeds,
	})
}

func (s *Server) GetRichlist(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	denom := vars["denom"]

	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")

	page := 1
	if pageStr != "" {
		p, err := strconv.Atoi(pageStr)
		if err == nil && p > 0 {
			page = p
		}
	}

	limit := 100
	if limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err == nil && l > 0 {
			limit = l
		}
	}

	offset := (page - 1) * limit

	type RichListEntry struct {
		Account    string  `json:"account"`
		Amount     string  `json:"amount"`
		Percentage float64 `json:"percentage"`
	}

	rows, err := s.pg.Pool.Query(context.Background(),
		"SELECT account, amount, percentage FROM rich_list WHERE denom = $1 ORDER BY cast(amount as NUMERIC) DESC LIMIT $2 OFFSET $3",
		denom, limit, offset)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to fetch richlist")
		return
	}
	defer rows.Close()

	var richList []RichListEntry
	for rows.Next() {
		var entry RichListEntry
		if err := rows.Scan(&entry.Account, &entry.Amount, &entry.Percentage); err != nil {
			continue
		}
		richList = append(richList, entry)
	}

	if richList == nil {
		richList = []RichListEntry{}
	}

	respondJSON(w, http.StatusOK, richList)
}

func (s *Server) GetTotalSupply(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	denom := vars["denom"]
	ctx := context.Background()

	bankClient := banktypes.NewQueryClient(s.clientCtx)

	// Fetch all supply with pagination
	var supply sdk.Coins
	var nextKey []byte

	for {
		req := &banktypes.QueryTotalSupplyRequest{
			Pagination: &query.PageRequest{
				Key:   nextKey,
				Limit: 1000,
			},
		}
		resp, err := bankClient.TotalSupply(ctx, req)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to fetch supply")
			return
		}
		supply = append(supply, resp.Supply...)

		if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
			break
		}
		nextKey = resp.Pagination.NextKey
	}

	if denom != "" {
		for _, coin := range supply {
			if coin.Denom == denom {
				// FCD returns number (unquoted)
				// Use json.RawMessage to avoid float64 precision loss and quotes
				respondJSON(w, http.StatusOK, json.RawMessage(coin.Amount.String()))
				return
			}
		}
		respondJSON(w, http.StatusOK, 0)
		return
	}

	respondJSON(w, http.StatusOK, supply)
}

func (s *Server) GetCirculatingSupply(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	denom := vars["denom"]
	ctx := context.Background()

	bankClient := banktypes.NewQueryClient(s.clientCtx)
	distrClient := distrtypes.NewQueryClient(s.clientCtx)

	// FCD logic: Total Supply - Community Pool
	// Fetch all supply with pagination
	var supply sdk.Coins
	var nextKey []byte

	for {
		req := &banktypes.QueryTotalSupplyRequest{
			Pagination: &query.PageRequest{
				Key:   nextKey,
				Limit: 1000,
			},
		}
		resp, err := bankClient.TotalSupply(ctx, req)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to fetch supply")
			return
		}
		supply = append(supply, resp.Supply...)

		if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
			break
		}
		nextKey = resp.Pagination.NextKey
	}

	poolResp, err := distrClient.CommunityPool(ctx, &distrtypes.QueryCommunityPoolRequest{})
	var pool sdk.DecCoins
	if err == nil {
		pool = poolResp.Pool
	} else {
		pool = sdk.DecCoins{}
	}

	// Fetch balances of excluded accounts
	var excludedCoins sdk.Coins
	for _, acc := range s.excludedAccounts {
		balResp, err := bankClient.AllBalances(ctx, &banktypes.QueryAllBalancesRequest{Address: acc})
		if err == nil {
			excludedCoins = excludedCoins.Add(balResp.Balances...)
		}
	}

	// Helper to find amount in list and convert to sdk.Dec
	findAmount := func(coins sdk.Coins, d string) sdk.Dec {
		for _, c := range coins {
			if c.Denom == d {
				return sdk.NewDecFromInt(c.Amount)
			}
		}
		return sdk.ZeroDec()
	}

	findPoolAmount := func(coins sdk.DecCoins, d string) sdk.Dec {
		for _, c := range coins {
			if c.Denom == d {
				return c.Amount
			}
		}
		return sdk.ZeroDec()
	}

	if denom != "" {
		totalSupply := findAmount(supply, denom)
		poolAmount := findPoolAmount(pool, denom)
		excludedAmount := findAmount(excludedCoins, denom)

		circulating := totalSupply.Sub(poolAmount).Sub(excludedAmount)

		if circulating.IsNegative() {
			circulating = sdk.ZeroDec()
		}

		// FCD returns number
		respondJSON(w, http.StatusOK, json.RawMessage(circulating.String()))
		return
	}

	var result []map[string]interface{}
	for _, sCoin := range supply {
		poolAmount := findPoolAmount(pool, sCoin.Denom)
		excludedAmount := findAmount(excludedCoins, sCoin.Denom)
		totalSupply := sdk.NewDecFromInt(sCoin.Amount)

		circulating := totalSupply.Sub(poolAmount).Sub(excludedAmount)

		if circulating.IsNegative() {
			circulating = sdk.ZeroDec()
		}

		result = append(result, map[string]interface{}{
			"denom":  sCoin.Denom,
			"amount": json.RawMessage(circulating.String()),
		})
	}

	respondJSON(w, http.StatusOK, result)
}
