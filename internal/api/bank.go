package api

import (
	"context"
	"net/http"
	"sort"

	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/gorilla/mux"
)

func (s *Server) GetBalances(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	account := vars["account"]
	ctx := context.Background()

	bankClient := banktypes.NewQueryClient(s.clientCtx)
	stakingClient := stakingtypes.NewQueryClient(s.clientCtx)

	// Balances
	balResp, err := bankClient.AllBalances(ctx, &banktypes.QueryAllBalancesRequest{Address: account})
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to fetch balances")
		return
	}
	balances := balResp.Balances

	// Delegations
	delResp, err := stakingClient.DelegatorDelegations(ctx, &stakingtypes.QueryDelegatorDelegationsRequest{DelegatorAddr: account})
	var delegations []stakingtypes.DelegationResponse
	if err == nil {
		delegations = delResp.DelegationResponses
	} else {
		delegations = []stakingtypes.DelegationResponse{}
	}

	// Unbondings
	unbondResp, err := stakingClient.DelegatorUnbondingDelegations(ctx, &stakingtypes.QueryDelegatorUnbondingDelegationsRequest{DelegatorAddr: account})
	unbondings := []stakingtypes.UnbondingDelegation{}
	if err == nil {
		unbondings = unbondResp.UnbondingResponses
	}
	if unbondings == nil {
		unbondings = []stakingtypes.UnbondingDelegation{}
	}

	// Vesting
	var vesting []interface{} = []interface{}{}
	// TODO: Implement vesting check using authtypes if needed

	// Transform balances to FCD format
	type BalanceEntry struct {
		Denom            string `json:"denom"`
		Available        string `json:"available"`
		Delegatable      string `json:"delegatable"`
		DelegatedVesting string `json:"delegatedVesting"`
		FreedVesting     string `json:"freedVesting"`
		RemainingVesting string `json:"remainingVesting"`
		Unbonding        string `json:"unbonding"`
	}

	var fcdBalances []BalanceEntry
	for _, b := range balances {
		delegatable := "0"
		if b.Denom == "uluna" {
			delegatable = b.Amount.String()
		}

		fcdBalances = append(fcdBalances, BalanceEntry{
			Denom:            b.Denom,
			Available:        b.Amount.String(),
			Delegatable:      delegatable,
			DelegatedVesting: "0",
			FreedVesting:     "0",
			RemainingVesting: "0",
			Unbonding:        "0",
		})
	}
	if fcdBalances == nil {
		fcdBalances = []BalanceEntry{}
	}

	// Sort: uluna, uusd, then alphabetical
	sort.Slice(fcdBalances, func(i, j int) bool {
		d1 := fcdBalances[i].Denom
		d2 := fcdBalances[j].Denom
		if d1 == "uluna" {
			return true
		}
		if d2 == "uluna" {
			return false
		}
		if d1 == "uusd" {
			return true
		}
		if d2 == "uusd" {
			return false
		}
		return d1 < d2
	})

	// FCD response structure
	response := map[string]interface{}{
		"balance":     fcdBalances,
		"vesting":     vesting,
		"delegations": delegations,
		"unbondings":  unbondings,
	}
	respondJSON(w, http.StatusOK, response)
}
