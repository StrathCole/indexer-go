package api

import (
	"context"
	"math/big"
	"net/http"
	"strconv"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	"github.com/gorilla/mux"
)

func (s *Server) GetTaxProceeds(w http.ResponseWriter, r *http.Request) {
	// Query ClickHouse for tax logs
	type TaxProceed struct {
		Denom string `json:"denom"`
		Total string `json:"amount"`
	}

	var proceeds []TaxProceed
	sql := `
SELECT 
extract(attr_value, '([a-z]+)') as denom,
toString(sum(cast(extract(attr_value, '(\\d+)') as Int64))) as total
FROM events 
WHERE event_type = 'tax' AND attr_key = 'amount'
GROUP BY denom
`

	err := s.ch.Conn.Select(context.Background(), &proceeds, sql)
	if err != nil {
		// Log error?
	}

	if proceeds == nil {
		proceeds = []TaxProceed{}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"total":        "0",
		"tax_proceeds": proceeds,
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
				// FCD returns number
				f, _ := strconv.ParseFloat(coin.Amount.String(), 64)
				respondJSON(w, http.StatusOK, f)
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

	// Helper to find amount in list and convert to big.Int
	findAmount := func(coins sdk.Coins, d string) *big.Int {
		for _, c := range coins {
			if c.Denom == d {
				return c.Amount.BigInt()
			}
		}
		return big.NewInt(0)
	}

	findPoolAmount := func(coins sdk.DecCoins, d string) *big.Int {
		for _, c := range coins {
			if c.Denom == d {
				return c.Amount.TruncateInt().BigInt()
			}
		}
		return big.NewInt(0)
	}

	if denom != "" {
		totalSupply := findAmount(supply, denom)
		poolAmount := findPoolAmount(pool, denom)
		excludedAmount := findAmount(excludedCoins, denom)

		circulating := new(big.Int).Sub(totalSupply, poolAmount)
		circulating.Sub(circulating, excludedAmount)

		if circulating.Sign() < 0 {
			circulating = big.NewInt(0)
		}

		// FCD returns number
		f, _ := new(big.Float).SetInt(circulating).Float64()
		respondJSON(w, http.StatusOK, f)
		return
	}

	var result []map[string]interface{}
	for _, sCoin := range supply {
		poolAmount := findPoolAmount(pool, sCoin.Denom)
		excludedAmount := findAmount(excludedCoins, sCoin.Denom)
		totalSupply := sCoin.Amount.BigInt()

		circulating := new(big.Int).Sub(totalSupply, poolAmount)
		circulating.Sub(circulating, excludedAmount)

		if circulating.Sign() < 0 {
			circulating = big.NewInt(0)
		}

		f, _ := new(big.Float).SetInt(circulating).Float64()
		result = append(result, map[string]interface{}{
			"denom":  sCoin.Denom,
			"amount": f,
		})
	}

	respondJSON(w, http.StatusOK, result)
}
