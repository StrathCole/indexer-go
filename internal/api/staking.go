package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	oracletypes "github.com/classic-terra/core/v3/x/oracle/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/cosmos-sdk/types/query"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/gorilla/mux"
)

type EnrichedValidator struct {
	OperatorAddress string `json:"operatorAddress"`
	ConsensusPubkey string `json:"consensusPubkey"`
	Description     struct {
		Moniker     string `json:"moniker"`
		Identity    string `json:"identity"`
		Website     string `json:"website"`
		Details     string `json:"details"`
		ProfileIcon string `json:"profileIcon"`
	} `json:"description"`
	Tokens          string `json:"tokens"`
	DelegatorShares string `json:"delegatorShares"`
	VotingPower     struct {
		Amount string `json:"amount"`
		Weight string `json:"weight"`
	} `json:"votingPower"`
	CommissionInfo struct {
		Rate          string `json:"rate"`
		MaxRate       string `json:"maxRate"`
		MaxChangeRate string `json:"maxChangeRate"`
		UpdateTime    string `json:"updateTime"`
	} `json:"commissionInfo"`
	UpTime      float64 `json:"upTime"`
	Status      string  `json:"status"`
	RewardsPool struct {
		Total  string        `json:"total"`
		Denoms []interface{} `json:"denoms"`
	} `json:"rewardsPool"`
	StakingReturn  string `json:"stakingReturn"`
	AccountAddress string `json:"accountAddress"`
	SelfDelegation struct {
		Amount string `json:"amount"`
		Weight string `json:"weight"`
	} `json:"selfDelegation"`
	// For GetValidator (Detail)
	Commissions []interface{} `json:"commissions,omitempty"`
	// For GetStakingAccount
	MyDelegation   string        `json:"myDelegation,omitempty"`
	MyUndelegation []interface{} `json:"myUndelegation,omitempty"`
}

func (s *Server) enrichValidator(ctx context.Context, v stakingtypes.Validator, totalBonded float64) EnrichedValidator {
	// Convert Operator Address to Account Address
	accAddr := ""
	hrp, data, err := bech32.DecodeAndConvert(v.OperatorAddress)
	if err == nil {
		// terravaloper -> terra
		// Assuming hrp is terravaloper, we want terra
		// But we should be careful.
		// Standard cosmos: cosmosvaloper -> cosmos
		// Terra: terravaloper -> terra
		accHrp := strings.TrimSuffix(hrp, "valoper")
		accAddr, _ = bech32.ConvertAndEncode(accHrp, data)
	}

	ev := EnrichedValidator{
		OperatorAddress: v.OperatorAddress,
		Tokens:          v.Tokens.String(),
		DelegatorShares: v.DelegatorShares.String(),
		Status:          v.Status.String(),
		UpTime:          1.0, // Stub
		StakingReturn:   "0", // Stub
		AccountAddress:  accAddr,
	}

	// Consensus Pubkey
	if v.ConsensusPubkey != nil {
		// We want the bech32 encoded pubkey? Or the JSON representation?
		// Legacy FCD returns: "terravalconspub..."
		// We need to unpack and encode.
		// For now, let's leave it empty or try to stringify.
		// v.ConsensusPubkey.String() returns proto string.
		// Let's try to get the cached value if available.
	}

	ev.Description.Moniker = v.Description.Moniker
	ev.Description.Identity = v.Description.Identity
	ev.Description.Website = v.Description.Website
	ev.Description.Details = v.Description.Details
	ev.Description.ProfileIcon = ""

	ev.CommissionInfo.Rate = v.Commission.CommissionRates.Rate.String()
	ev.CommissionInfo.MaxRate = v.Commission.CommissionRates.MaxRate.String()
	ev.CommissionInfo.MaxChangeRate = v.Commission.CommissionRates.MaxChangeRate.String()
	ev.CommissionInfo.UpdateTime = v.Commission.UpdateTime.String()

	// Voting Power
	tokens, _ := strconv.ParseFloat(v.Tokens.String(), 64)
	ev.VotingPower.Amount = v.Tokens.String()
	if totalBonded > 0 {
		ev.VotingPower.Weight = fmt.Sprintf("%.10f", tokens/totalBonded)
	} else {
		ev.VotingPower.Weight = "0"
	}

	// Rewards Pool
	// Fetch outstanding rewards
	distrClient := distrtypes.NewQueryClient(s.clientCtx)
	rewardsResp, err := distrClient.ValidatorOutstandingRewards(ctx, &distrtypes.QueryValidatorOutstandingRewardsRequest{
		ValidatorAddress: v.OperatorAddress,
	})

	ev.RewardsPool.Total = "0"
	ev.RewardsPool.Denoms = []interface{}{}

	if err == nil {
		// Calculate total in some unit? Or just list denoms?
		// Legacy returns "total": "0" (string) and "denoms": [ {denom, amount, adjustedAmount} ]
		// adjustedAmount seems to be same as amount in diff.
		var denoms []interface{}
		for _, r := range rewardsResp.Rewards.Rewards {
			denoms = append(denoms, map[string]string{
				"denom":          r.Denom,
				"amount":         r.Amount.String(),
				"adjustedAmount": r.Amount.String(),
			})
		}
		ev.RewardsPool.Denoms = denoms
	}

	ev.SelfDelegation.Amount = "0"
	ev.SelfDelegation.Weight = "0"

	return ev
}

func (s *Server) GetValidators(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "validators", 1*time.Minute, func() (interface{}, error) {
		stakingClient := stakingtypes.NewQueryClient(s.clientCtx)
		ctx := context.Background()

		// Pagination loop
		var allValidators []stakingtypes.Validator
		var nextKey []byte

		for {
			req := &stakingtypes.QueryValidatorsRequest{
				Pagination: &query.PageRequest{
					Key:   nextKey,
					Limit: 100,
				},
			}
			resp, err := stakingClient.Validators(ctx, req)
			if err != nil {
				return nil, err
			}
			allValidators = append(allValidators, resp.Validators...)
			if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
				break
			}
			nextKey = resp.Pagination.NextKey
		}

		poolResp, err := stakingClient.Pool(ctx, &stakingtypes.QueryPoolRequest{})
		var totalBonded float64
		if err == nil {
			fmt.Sscanf(poolResp.Pool.BondedTokens.String(), "%f", &totalBonded)
		}

		var enriched []EnrichedValidator
		for _, v := range allValidators {
			enriched = append(enriched, s.enrichValidator(ctx, v, totalBonded))
		}
		return enriched, nil
	})
}

func (s *Server) GetValidator(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	operatorAddr := vars["operatorAddr"]
	accountAddr := r.URL.Query().Get("account")

	key := "validator_" + operatorAddr
	if accountAddr != "" {
		key += "_" + accountAddr
	}

	s.respondWithCache(w, key, 1*time.Minute, func() (interface{}, error) {
		stakingClient := stakingtypes.NewQueryClient(s.clientCtx)
		ctx := context.Background()

		// 1. Get Validator
		valResp, err := stakingClient.Validator(ctx, &stakingtypes.QueryValidatorRequest{
			ValidatorAddr: operatorAddr,
		})
		if err != nil {
			return nil, err
		}
		validator := valResp.Validator

		// 2. Get Pool
		poolResp, err := stakingClient.Pool(ctx, &stakingtypes.QueryPoolRequest{})
		var totalBonded float64
		if err == nil {
			fmt.Sscanf(poolResp.Pool.BondedTokens.String(), "%f", &totalBonded)
		}

		ev := s.enrichValidator(ctx, validator, totalBonded)

		// 3. Get Commissions (Detail only)
		distrClient := distrtypes.NewQueryClient(s.clientCtx)
		commResp, err := distrClient.ValidatorCommission(ctx, &distrtypes.QueryValidatorCommissionRequest{
			ValidatorAddress: operatorAddr,
		})
		if err == nil {
			var commissions []interface{}
			for _, c := range commResp.Commission.Commission {
				commissions = append(commissions, map[string]string{
					"denom":  c.Denom,
					"amount": c.Amount.String(),
				})
			}
			ev.Commissions = commissions
		}

		if accountAddr != "" {
			// 4. Get Delegation
			delResp, err := stakingClient.Delegation(ctx, &stakingtypes.QueryDelegationRequest{
				DelegatorAddr: accountAddr,
				ValidatorAddr: operatorAddr,
			})
			if err == nil && delResp.DelegationResponse != nil {
				ev.MyDelegation = delResp.DelegationResponse.Balance.Amount.String()
			}

			// 4. Get Unbonding Delegation
			unbondResp, err := stakingClient.UnbondingDelegation(context.Background(), &stakingtypes.QueryUnbondingDelegationRequest{
				DelegatorAddr: accountAddr,
				ValidatorAddr: operatorAddr,
			})
			if err == nil {
				var myUnbondings []interface{}
				for _, entry := range unbondResp.Unbond.Entries {
					myUnbondings = append(myUnbondings, map[string]interface{}{
						"releaseTime":      entry.CompletionTime,
						"amount":           entry.Balance.String(),
						"validatorName":    ev.Description.Moniker,
						"validatorAddress": operatorAddr,
						"creationHeight":   entry.CreationHeight,
					})
				}
				ev.MyUndelegation = myUnbondings
			}
		}

		return ev, nil
	})
}

func (s *Server) GetClaims(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	operatorAddr := vars["operatorAddr"]

	// Parse pagination
	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")

	page := 1
	if pageStr != "" {
		p, err := strconv.Atoi(pageStr)
		if err == nil && p > 0 {
			page = p
		}
	}

	limit := 5
	if limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err == nil && l > 0 {
			limit = l
		}
	}

	offset := (page - 1) * limit

	// Convert operator address to account address
	hrp, data, err := bech32.DecodeAndConvert(operatorAddr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid operator address")
		return
	}

	// terravaloper -> terra
	accHrp := strings.TrimSuffix(hrp, "valoper")
	accountAddr, err := bech32.ConvertAndEncode(accHrp, data)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to convert address")
		return
	}

	// Get MsgTypeIDs
	getMsgTypeID := func(msgType string) uint16 {
		var id uint16
		err := s.pg.Pool.QueryRow(context.Background(), "SELECT id FROM msg_types WHERE msg_type = $1", msgType).Scan(&id)
		if err != nil {
			return 0
		}
		return id
	}

	commissionIDs := []uint16{}
	if id := getMsgTypeID("cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission"); id > 0 {
		commissionIDs = append(commissionIDs, id)
	}
	if id := getMsgTypeID("distribution/MsgWithdrawValidatorCommission"); id > 0 {
		commissionIDs = append(commissionIDs, id)
	}

	rewardIDs := []uint16{}
	if id := getMsgTypeID("cosmos.distribution.v1beta1.MsgWithdrawDelegationReward"); id > 0 {
		rewardIDs = append(rewardIDs, id)
	}
	if id := getMsgTypeID("distribution/MsgWithdrawDelegationReward"); id > 0 {
		rewardIDs = append(rewardIDs, id)
	}

	// If both are empty, we have no such messages indexed yet
	if len(commissionIDs) == 0 && len(rewardIDs) == 0 {
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"claims": []interface{}{},
			"limit":  limit,
			"page":   page,
		})
		return
	}

	// Query ClickHouse
	sql := `
		SELECT 
			tx_hash,
			toUnixTimestamp(block_time) as timestamp,
			msgs_json,
			logs_json
		FROM txs
		WHERE 
			code = 0 AND (
				(
					hasAny(msg_type_ids, ?) AND 
					(arrayExists(x -> JSONExtractString(x, 'validator_address') = ?, msgs_json) OR arrayExists(x -> JSONExtractString(x, 'validatorAddress') = ?, msgs_json))
				) OR (
					hasAny(msg_type_ids, ?) AND 
					(arrayExists(x -> JSONExtractString(x, 'validator_address') = ?, msgs_json) OR arrayExists(x -> JSONExtractString(x, 'validatorAddress') = ?, msgs_json)) AND
					(arrayExists(x -> JSONExtractString(x, 'delegator_address') = ?, msgs_json) OR arrayExists(x -> JSONExtractString(x, 'delegatorAddress') = ?, msgs_json))
				)
			)
		ORDER BY block_time DESC
		LIMIT ? OFFSET ?
	`

	type TxRow struct {
		TxHash    string   `ch:"tx_hash"`
		Timestamp int64    `ch:"timestamp"`
		MsgsJSON  []string `ch:"msgs_json"`
		LogsJSON  string   `ch:"logs_json"`
	}

	var rows []TxRow
	err = s.ch.Conn.Select(context.Background(), &rows, sql,
		commissionIDs, operatorAddr, operatorAddr,
		rewardIDs, operatorAddr, operatorAddr, accountAddr, accountAddr,
		limit, offset,
	)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to fetch claims")
		return
	}

	type Claim struct {
		ChainID   string        `json:"chainId"`
		TxHash    string        `json:"txhash"`
		Type      string        `json:"type"`
		Amounts   []interface{} `json:"amounts"`
		Timestamp string        `json:"timestamp"`
	}

	var claims []Claim

	chainID := s.clientCtx.ChainID
	if chainID == "" {
		status, err := s.rpc.Status(context.Background())
		if err == nil {
			chainID = status.NodeInfo.Network
		}
	}

	for _, row := range rows {
		var logs []sdk.ABCIMessageLog
		if err := json.Unmarshal([]byte(row.LogsJSON), &logs); err != nil {
			continue
		}

		for i, msgJSON := range row.MsgsJSON {
			var msg map[string]interface{}
			if err := json.Unmarshal([]byte(msgJSON), &msg); err != nil {
				continue
			}

			if i >= len(logs) {
				break
			}

			logEvents := logs[i].Events

			claimType := ""
			valAddr, _ := msg["validator_address"].(string)
			if valAddr == "" {
				valAddr, _ = msg["validatorAddress"].(string)
			}
			delAddr, _ := msg["delegator_address"].(string)
			if delAddr == "" {
				delAddr, _ = msg["delegatorAddress"].(string)
			}

			if valAddr == operatorAddr && delAddr == "" {
				claimType = "Commission"
			} else if valAddr == operatorAddr && delAddr == accountAddr {
				claimType = "Reward"
			} else {
				continue
			}

			var amounts []map[string]string
			parseAmounts := func(amtStr string) {
				parts := strings.Split(amtStr, ",")
				for _, part := range parts {
					coin, err := sdk.ParseCoinNormalized(part)
					if err == nil {
						amounts = append(amounts, map[string]string{
							"denom":  coin.Denom,
							"amount": coin.Amount.String(),
						})
					}
				}
			}

			if claimType == "Commission" {
				for _, e := range logEvents {
					if e.Type == "withdraw_commission" {
						for _, attr := range e.Attributes {
							if attr.Key == "amount" {
								parseAmounts(attr.Value)
							}
						}
					}
				}
			} else {
				for _, e := range logEvents {
					if e.Type == "withdraw_rewards" {
						for _, attr := range e.Attributes {
							if attr.Key == "amount" {
								parseAmounts(attr.Value)
							}
						}
					}
				}
			}

			if len(amounts) > 0 {
				// Sort amounts
				sort.Slice(amounts, func(i, j int) bool {
					denomOrder := map[string]int{
						"uluna": 0,
						"ukrw":  1,
						"usdr":  2,
						"uusd":  3,
					}
					d1 := amounts[i]["denom"]
					d2 := amounts[j]["denom"]
					o1, ok1 := denomOrder[d1]
					o2, ok2 := denomOrder[d2]
					if ok1 && ok2 {
						return o1 < o2
					}
					if ok1 {
						return true
					}
					if ok2 {
						return false
					}
					return d1 < d2
				})

				var amountsIf []interface{}
				for _, a := range amounts {
					amountsIf = append(amountsIf, a)
				}

				claims = append(claims, Claim{
					ChainID:   chainID,
					TxHash:    row.TxHash,
					Type:      claimType,
					Amounts:   amountsIf,
					Timestamp: time.Unix(row.Timestamp, 0).Format(time.RFC3339),
				})
			}
		}
	}

	if claims == nil {
		claims = []Claim{}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"claims": claims,
		"limit":  limit,
		"page":   page,
	})
}

func (s *Server) GetStakingAccount(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	account := vars["account"]

	s.respondWithCache(w, "staking_account_"+account, 1*time.Minute, func() (interface{}, error) {
		stakingClient := stakingtypes.NewQueryClient(s.clientCtx)
		distrClient := distrtypes.NewQueryClient(s.clientCtx)
		bankClient := banktypes.NewQueryClient(s.clientCtx)
		oracleClient := oracletypes.NewQueryClient(s.clientCtx)
		ctx := context.Background()

		// 1. Fetch all validators (needed for names)
		var allValidators []stakingtypes.Validator
		var nextKey []byte
		for {
			req := &stakingtypes.QueryValidatorsRequest{
				Pagination: &query.PageRequest{Key: nextKey, Limit: 100},
			}
			resp, err := stakingClient.Validators(ctx, req)
			if err != nil {
				return nil, err
			}
			allValidators = append(allValidators, resp.Validators...)
			if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
				break
			}
			nextKey = resp.Pagination.NextKey
		}

		// 2. Fetch Delegations
		var delegations []stakingtypes.DelegationResponse
		nextKey = nil
		for {
			req := &stakingtypes.QueryDelegatorDelegationsRequest{
				DelegatorAddr: account,
				Pagination:    &query.PageRequest{Key: nextKey, Limit: 100},
			}
			resp, err := stakingClient.DelegatorDelegations(ctx, req)
			if err != nil {
				break
			}
			delegations = append(delegations, resp.DelegationResponses...)
			if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
				break
			}
			nextKey = resp.Pagination.NextKey
		}

		// 3. Fetch Unbonding Delegations
		var unbondings []stakingtypes.UnbondingDelegation
		nextKey = nil
		for {
			req := &stakingtypes.QueryDelegatorUnbondingDelegationsRequest{
				DelegatorAddr: account,
				Pagination:    &query.PageRequest{Key: nextKey, Limit: 100},
			}
			resp, err := stakingClient.DelegatorUnbondingDelegations(ctx, req)
			if err != nil {
				break
			}
			unbondings = append(unbondings, resp.UnbondingResponses...)
			if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
				break
			}
			nextKey = resp.Pagination.NextKey
		}

		// 4. Fetch Rewards
		rewardsResp, err := distrClient.DelegationTotalRewards(ctx, &distrtypes.QueryDelegationTotalRewardsRequest{
			DelegatorAddress: account,
		})

		// 5. Fetch Balances
		balancesResp, err := bankClient.AllBalances(ctx, &banktypes.QueryAllBalancesRequest{
			Address: account,
		})
		var balances sdk.Coins
		if err == nil {
			balances = balancesResp.Balances
		}

		// 6. Pool
		poolResp, err := stakingClient.Pool(ctx, &stakingtypes.QueryPoolRequest{})
		var totalBonded float64
		if err == nil {
			fmt.Sscanf(poolResp.Pool.BondedTokens.String(), "%f", &totalBonded)
		}

		// 7. Oracle Prices
		priceMapDec := make(map[string]sdk.Dec)
		ratesResp, err := oracleClient.ExchangeRates(ctx, &oracletypes.QueryExchangeRatesRequest{})
		if err == nil {
			for _, r := range ratesResp.ExchangeRates {
				priceMapDec[r.Denom] = r.Amount
			}
		}

		// Helper to calculate total rewards in Luna
		getTotalRewardsAdjustedToLuna := func(rewards sdk.DecCoins) string {
			total := sdk.ZeroDec()
			for _, r := range rewards {
				if r.Denom == "uluna" {
					total = total.Add(r.Amount)
				} else {
					if price, ok := priceMapDec[r.Denom]; ok && !price.IsZero() {
						total = total.Add(r.Amount.Quo(price))
					}
				}
			}
			return total.String()
		}

		// Helper to sort rewards
		sortDecCoins := func(coins sdk.DecCoins) []map[string]string {
			var res []map[string]string
			for _, c := range coins {
				res = append(res, map[string]string{
					"denom":  c.Denom,
					"amount": c.Amount.String(),
				})
			}
			sort.Slice(res, func(i, j int) bool {
				denomOrder := map[string]int{
					"uluna": 0,
					"ukrw":  1,
					"usdr":  2,
					"uusd":  3,
				}
				d1 := res[i]["denom"]
				d2 := res[j]["denom"]
				o1, ok1 := denomOrder[d1]
				o2, ok2 := denomOrder[d2]
				if ok1 && ok2 {
					return o1 < o2
				}
				if ok1 {
					return true
				}
				if ok2 {
					return false
				}
				return d1 < d2
			})
			return res
		}

		// Process Validators
		valMap := make(map[string]EnrichedValidator)
		var enrichedValidators []EnrichedValidator
		for _, v := range allValidators {
			ev := s.enrichValidator(ctx, v, totalBonded)
			valMap[v.OperatorAddress] = ev
			enrichedValidators = append(enrichedValidators, ev)
		}

		// My Delegations
		var myDelegations []interface{}
		var delegationTotal float64
		delegationMap := make(map[string]string)

		for _, d := range delegations {
			amount, _ := strconv.ParseFloat(d.Balance.Amount.String(), 64)
			delegationTotal += amount
			delegationMap[d.Delegation.ValidatorAddress] = d.Balance.Amount.String()

			valName := ""
			if v, ok := valMap[d.Delegation.ValidatorAddress]; ok {
				valName = v.Description.Moniker
			}

			// Find reward for this validator
			var valReward sdk.DecCoins
			if rewardsResp != nil {
				for _, r := range rewardsResp.Rewards {
					if r.ValidatorAddress == d.Delegation.ValidatorAddress {
						valReward = r.Reward
						break
					}
				}
			}

			myDelegations = append(myDelegations, map[string]interface{}{
				"validatorName":    valName,
				"validatorAddress": d.Delegation.ValidatorAddress,
				"amountDelegated":  d.Balance.Amount.String(),
				"rewards":          sortDecCoins(valReward),
				"totalReward":      getTotalRewardsAdjustedToLuna(valReward),
			})
		}

		// Enrich validators with my delegation info
		for i, v := range enrichedValidators {
			if amt, ok := delegationMap[v.OperatorAddress]; ok {
				enrichedValidators[i].MyDelegation = amt
			}
			// Add undelegations if any
			for _, u := range unbondings {
				if u.ValidatorAddress == v.OperatorAddress {
					var myUnbondings []interface{}
					for _, entry := range u.Entries {
						myUnbondings = append(myUnbondings, map[string]interface{}{
							"releaseTime":      entry.CompletionTime,
							"amount":           entry.Balance.String(),
							"validatorName":    v.Description.Moniker,
							"validatorAddress": v.OperatorAddress,
							"creationHeight":   entry.CreationHeight,
						})
					}
					enrichedValidators[i].MyUndelegation = myUnbondings
				}
			}
		}

		// Available Luna
		availableLuna := "0"
		for _, b := range balances {
			if b.Denom == "uluna" {
				availableLuna = b.Amount.String()
				break
			}
		}

		// Rewards Total
		rewardsTotal := "0"
		var rewardsDenoms []map[string]string
		if rewardsResp != nil {
			rewardsTotal = getTotalRewardsAdjustedToLuna(rewardsResp.Total)
			rewardsDenoms = sortDecCoins(rewardsResp.Total)
		} else {
			rewardsDenoms = []map[string]string{}
		}

		// Undelegations flat list
		var undelegationsFlat []interface{}
		for _, u := range unbondings {
			valName := ""
			if v, ok := valMap[u.ValidatorAddress]; ok {
				valName = v.Description.Moniker
			}
			for _, entry := range u.Entries {
				undelegationsFlat = append(undelegationsFlat, map[string]interface{}{
					"validatorName":    valName,
					"validatorAddress": u.ValidatorAddress,
					"amount":           entry.Balance.String(),
					"creationHeight":   entry.CreationHeight,
					"releaseTime":      entry.CompletionTime,
				})
			}
		}
		if undelegationsFlat == nil {
			undelegationsFlat = []interface{}{}
		}
		if myDelegations == nil {
			myDelegations = []interface{}{}
		}

		return map[string]interface{}{
			"validators":      enrichedValidators,
			"delegationTotal": fmt.Sprintf("%.0f", delegationTotal),
			"availableLuna":   availableLuna,
			"undelegations":   undelegationsFlat,
			"myDelegations":   myDelegations,
			"rewards": map[string]interface{}{
				"total":  rewardsTotal,
				"denoms": rewardsDenoms,
			},
		}, nil
	})
}

func (s *Server) GetTotalStakingReturn(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, 0)
}

func (s *Server) GetValidatorReturn(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, 0)
}
