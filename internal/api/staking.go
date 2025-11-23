package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	// For GetStakingAccount
	MyDelegation   string        `json:"myDelegation,omitempty"`
	MyUndelegation []interface{} `json:"myUndelegation,omitempty"`
}

func (s *Server) enrichValidator(v stakingtypes.Validator, totalBonded float64) EnrichedValidator {
	ev := EnrichedValidator{
		OperatorAddress: v.OperatorAddress,
		// ConsensusPubkey: v.ConsensusPubkey, // LCD doesn't return this in simple view? Check struct.
		Tokens:          v.Tokens.String(),
		DelegatorShares: v.DelegatorShares.String(),
		Status:          v.Status.String(),
		UpTime:          1.0,               // Stub
		StakingReturn:   "0",               // Stub
		AccountAddress:  v.OperatorAddress, // Should convert bech32 valoper -> acc
	}

	ev.Description.Moniker = v.Description.Moniker
	ev.Description.Identity = v.Description.Identity
	ev.Description.Website = v.Description.Website
	ev.Description.Details = v.Description.Details
	ev.Description.ProfileIcon = "" // Need external service for this usually

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

	// Stubs
	ev.RewardsPool.Total = "0"
	ev.RewardsPool.Denoms = []interface{}{}
	ev.SelfDelegation.Amount = "0"
	ev.SelfDelegation.Weight = "0"

	return ev
}

func (s *Server) GetValidators(w http.ResponseWriter, r *http.Request) {
	s.respondWithCache(w, "validators", 1*time.Minute, func() (interface{}, error) {
		stakingClient := stakingtypes.NewQueryClient(s.clientCtx)

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
			resp, err := stakingClient.Validators(context.Background(), req)
			if err != nil {
				return nil, err
			}
			allValidators = append(allValidators, resp.Validators...)
			if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
				break
			}
			nextKey = resp.Pagination.NextKey
		}

		poolResp, err := stakingClient.Pool(context.Background(), &stakingtypes.QueryPoolRequest{})
		var totalBonded float64
		if err == nil {
			fmt.Sscanf(poolResp.Pool.BondedTokens.String(), "%f", &totalBonded)
		}

		var enriched []EnrichedValidator
		for _, v := range allValidators {
			enriched = append(enriched, s.enrichValidator(v, totalBonded))
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

		// 1. Get Validator
		valResp, err := stakingClient.Validator(context.Background(), &stakingtypes.QueryValidatorRequest{
			ValidatorAddr: operatorAddr,
		})
		if err != nil {
			return nil, err
		}
		validator := valResp.Validator

		// 2. Get Pool
		poolResp, err := stakingClient.Pool(context.Background(), &stakingtypes.QueryPoolRequest{})
		var totalBonded float64
		if err == nil {
			fmt.Sscanf(poolResp.Pool.BondedTokens.String(), "%f", &totalBonded)
		}

		ev := s.enrichValidator(validator, totalBonded)

		if accountAddr != "" {
			// 3. Get Delegation
			delResp, err := stakingClient.Delegation(context.Background(), &stakingtypes.QueryDelegationRequest{
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

	commissionID := getMsgTypeID("cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission")
	rewardID := getMsgTypeID("cosmos.distribution.v1beta1.MsgWithdrawDelegationReward")

	// If both are 0, we have no such messages indexed yet
	if commissionID == 0 && rewardID == 0 {
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
					has(msg_type_ids, ?) AND 
					(arrayExists(x -> JSONExtractString(x, 'validator_address') = ?, msgs_json) OR arrayExists(x -> JSONExtractString(x, 'validatorAddress') = ?, msgs_json))
				) OR (
					has(msg_type_ids, ?) AND 
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
		commissionID, operatorAddr, operatorAddr,
		rewardID, operatorAddr, operatorAddr, accountAddr, accountAddr,
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

			var amounts []interface{}
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
				claims = append(claims, Claim{
					ChainID:   chainID,
					TxHash:    row.TxHash,
					Type:      claimType,
					Amounts:   amounts,
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

		// Process Validators
		valMap := make(map[string]EnrichedValidator)
		var enrichedValidators []EnrichedValidator
		for _, v := range allValidators {
			ev := s.enrichValidator(v, totalBonded)
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
				"rewards":          valReward,
				"totalReward":      "0",
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
		// rewardsResp.Total is sdk.DecCoins

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
				"denoms": rewardsResp.Total,
			},
		}, nil
	})
}

func (s *Server) GetTotalStakingReturn(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, "0")
}

func (s *Server) GetValidatorReturn(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, "0")
}
