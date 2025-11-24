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
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	"github.com/cosmos/cosmos-sdk/types/query"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
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
	UpTime         float64     `json:"upTime"`
	Status         string      `json:"status"`
	RewardsPool    RewardsPool `json:"rewardsPool"`
	StakingReturn  string      `json:"stakingReturn"`
	AccountAddress string      `json:"accountAddress"`
	SelfDelegation struct {
		Amount string `json:"amount"`
		Weight string `json:"weight"`
	} `json:"selfDelegation"`
	// For GetValidator (Detail)
	Commissions     []interface{} `json:"commissions,omitempty"`
	CommissionTotal string        `json:"total,omitempty"`
	// For GetStakingAccount
	MyDelegation   string        `json:"myDelegation,omitempty"`
	MyDelegatable  string        `json:"myDelegatable,omitempty"`
	MyRewards      *RewardsPool  `json:"myRewards,omitempty"`
	MyUndelegation []interface{} `json:"myUndelegation,omitempty"`
}

type RewardsPool struct {
	Total  string        `json:"total"`
	Denoms []interface{} `json:"denoms"`
}

func (s *Server) enrichValidator(ctx context.Context, v stakingtypes.Validator, totalBonded float64, priceMap map[string]sdk.Dec) EnrichedValidator {
	// Convert Operator Address to Account Address
	accAddr := ""
	hrp, data, err := bech32.DecodeAndConvert(v.OperatorAddress)
	if err == nil {
		// terravaloper -> terra
		accHrp := strings.TrimSuffix(hrp, "valoper")
		accAddr, _ = bech32.ConvertAndEncode(accHrp, data)
	}

	// Status Mapping
	status := "inactive"
	if v.Jailed {
		status = "jailed"
	} else if v.Status == stakingtypes.Bonded {
		status = "active"
	} else if v.Status == stakingtypes.Unbonding {
		status = "unbonding"
	}

	ev := EnrichedValidator{
		OperatorAddress: v.OperatorAddress,
		Tokens:          v.Tokens.String(),
		DelegatorShares: v.DelegatorShares.String(),
		Status:          status,
		UpTime:          1.0, // Stub
		StakingReturn:   "0", // Stub
		AccountAddress:  accAddr,
	}

	// Format DelegatorShares to 10 decimals to match FCD
	if idx := strings.Index(ev.DelegatorShares, "."); idx != -1 {
		if len(ev.DelegatorShares) > idx+11 {
			ev.DelegatorShares = ev.DelegatorShares[:idx+11]
		}
	}

	ev.RewardsPool.Total = "0"
	ev.RewardsPool.Denoms = []interface{}{}

	// Consensus Pubkey
	if v.ConsensusPubkey != nil {
		var pubKey cryptotypes.PubKey
		if err := s.clientCtx.InterfaceRegistry.UnpackAny(v.ConsensusPubkey, &pubKey); err == nil {
			if pubKeyStr, err := bech32.ConvertAndEncode("terravalconspub", pubKey.Bytes()); err == nil {
				ev.ConsensusPubkey = pubKeyStr
			}
		}
	}

	ev.Description.Moniker = v.Description.Moniker
	ev.Description.Identity = v.Description.Identity
	ev.Description.Website = v.Description.Website
	ev.Description.Details = v.Description.Details
	ev.Description.ProfileIcon = ""

	ev.CommissionInfo.Rate = v.Commission.CommissionRates.Rate.String()
	if d, err := sdk.NewDecFromStr(ev.CommissionInfo.Rate); err == nil {
		ev.CommissionInfo.Rate = fmt.Sprintf("%.10f", d.MustFloat64())
	}
	ev.CommissionInfo.MaxRate = v.Commission.CommissionRates.MaxRate.String()
	if d, err := sdk.NewDecFromStr(ev.CommissionInfo.MaxRate); err == nil {
		ev.CommissionInfo.MaxRate = fmt.Sprintf("%.10f", d.MustFloat64())
	}
	ev.CommissionInfo.MaxChangeRate = v.Commission.CommissionRates.MaxChangeRate.String()
	if d, err := sdk.NewDecFromStr(ev.CommissionInfo.MaxChangeRate); err == nil {
		ev.CommissionInfo.MaxChangeRate = fmt.Sprintf("%.10f", d.MustFloat64())
	}
	ev.CommissionInfo.UpdateTime = v.Commission.UpdateTime.UTC().Format("2006-01-02T15:04:05.000Z")

	// Voting Power
	tokens, _ := strconv.ParseFloat(v.Tokens.String(), 64)
	ev.Tokens = fmt.Sprintf("%.10f", tokens)
	ev.VotingPower.Amount = fmt.Sprintf("%.10f", tokens)
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
		var denoms []map[string]string
		totalRewards := sdk.ZeroDec()

		for _, r := range rewardsResp.Rewards.Rewards {
			denoms = append(denoms, map[string]string{
				"denom":          r.Denom,
				"amount":         r.Amount.String(),
				"adjustedAmount": r.Amount.String(),
			})

			// Calculate Total in Luna
			if r.Denom == "uluna" {
				totalRewards = totalRewards.Add(r.Amount)
			} else {
				if price, ok := priceMap[r.Denom]; ok && !price.IsZero() {
					totalRewards = totalRewards.Add(r.Amount.Quo(price))
				}
			}
		}

		// Sort denoms
		sort.Slice(denoms, func(i, j int) bool {
			denomOrder := map[string]int{
				"uluna": 0,
				"ukrw":  1,
				"usdr":  2,
				"uusd":  3,
			}
			d1 := denoms[i]["denom"]
			d2 := denoms[j]["denom"]
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

		ev.RewardsPool.Denoms = make([]interface{}, len(denoms))
		for i, d := range denoms {
			ev.RewardsPool.Denoms[i] = d
		}
		ev.RewardsPool.Total = totalRewards.String()
	}

	// Self Delegation
	ev.SelfDelegation.Amount = "0"
	ev.SelfDelegation.Weight = "0"

	if accAddr != "" {
		stakingClient := stakingtypes.NewQueryClient(s.clientCtx)
		delResp, err := stakingClient.Delegation(ctx, &stakingtypes.QueryDelegationRequest{
			DelegatorAddr: accAddr,
			ValidatorAddr: v.OperatorAddress,
		})
		if err == nil && delResp.DelegationResponse != nil {
			amountStr := delResp.DelegationResponse.Balance.Amount.String()
			ev.SelfDelegation.Amount = amountStr + ".0000000000"

			amount, _ := strconv.ParseFloat(amountStr, 64)
			if tokens > 0 {
				ev.SelfDelegation.Weight = fmt.Sprintf("%.10f", amount/tokens)
			}
		}
	}

	return ev
}

func (s *Server) enrichValidatorsParallel(ctx context.Context, validators []stakingtypes.Validator, totalBonded float64, priceMap map[string]sdk.Dec) ([]EnrichedValidator, error) {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(20) // Limit concurrency to avoid overwhelming the node

	enriched := make([]EnrichedValidator, len(validators))

	for i, v := range validators {
		i, v := i, v
		g.Go(func() error {
			enriched[i] = s.enrichValidator(ctx, v, totalBonded, priceMap)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return enriched, nil
}

func (s *Server) getCachedValidators(ctx context.Context) ([]EnrichedValidator, error) {
	key := "all_validators_struct"
	if val, found := s.cache.Get(key); found {
		if validators, ok := val.([]EnrichedValidator); ok {
			return validators, nil
		}
	}

	// Fetch logic
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

	// Fetch Oracle Prices
	oracleClient := oracletypes.NewQueryClient(s.clientCtx)
	priceMap := make(map[string]sdk.Dec)
	ratesResp, err := oracleClient.ExchangeRates(ctx, &oracletypes.QueryExchangeRatesRequest{})
	if err == nil {
		for _, r := range ratesResp.ExchangeRates {
			priceMap[r.Denom] = r.Amount
		}
	}

	// Sort by Voting Power (Tokens) Descending
	sort.Slice(allValidators, func(i, j int) bool {
		return allValidators[i].Tokens.GT(allValidators[j].Tokens)
	})

	enriched, err := s.enrichValidatorsParallel(ctx, allValidators, totalBonded, priceMap)
	if err != nil {
		return nil, err
	}

	s.cache.Set(key, enriched, 1*time.Minute)
	return enriched, nil
}

func (s *Server) GetValidators(w http.ResponseWriter, r *http.Request) {
	validators, err := s.getCachedValidators(context.Background())
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, validators)
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

		// Fetch Oracle Prices
		oracleClient := oracletypes.NewQueryClient(s.clientCtx)
		priceMap := make(map[string]sdk.Dec)
		ratesResp, err := oracleClient.ExchangeRates(ctx, &oracletypes.QueryExchangeRatesRequest{})
		if err == nil {
			for _, r := range ratesResp.ExchangeRates {
				priceMap[r.Denom] = r.Amount
			}
		}

		ev := s.enrichValidator(ctx, validator, totalBonded, priceMap)

		// 3. Get Commissions (Detail only)
		distrClient := distrtypes.NewQueryClient(s.clientCtx)
		commResp, err := distrClient.ValidatorCommission(ctx, &distrtypes.QueryValidatorCommissionRequest{
			ValidatorAddress: operatorAddr,
		})
		if err == nil {
			var commissions []map[string]string
			totalCommission := sdk.ZeroDec()

			for _, c := range commResp.Commission.Commission {
				commissions = append(commissions, map[string]string{
					"denom":  c.Denom,
					"amount": c.Amount.String(),
				})

				// Calculate Total in Luna
				if c.Denom == "uluna" {
					totalCommission = totalCommission.Add(c.Amount)
				} else {
					if price, ok := priceMap[c.Denom]; ok && !price.IsZero() {
						totalCommission = totalCommission.Add(c.Amount.Quo(price))
					}
				}
			}

			// Sort commissions
			sort.Slice(commissions, func(i, j int) bool {
				denomOrder := map[string]int{
					"uluna": 0,
					"ukrw":  1,
					"usdr":  2,
					"uusd":  3,
				}
				d1 := commissions[i]["denom"]
				d2 := commissions[j]["denom"]
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

			ev.Commissions = make([]interface{}, len(commissions))
			for i, c := range commissions {
				ev.Commissions[i] = c
			}
			ev.CommissionTotal = totalCommission.String()
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

			// 5. Get Unbonding Delegation
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

			// 6. Get MyDelegatable (Available Luna)
			bankClient := banktypes.NewQueryClient(s.clientCtx)
			balResp, err := bankClient.Balance(ctx, &banktypes.QueryBalanceRequest{
				Address: accountAddr,
				Denom:   "uluna",
			})
			if err == nil {
				ev.MyDelegatable = balResp.Balance.Amount.String()
			}

			// 7. Get MyRewards
			rewResp, err := distrClient.DelegationRewards(ctx, &distrtypes.QueryDelegationRewardsRequest{
				DelegatorAddress: accountAddr,
				ValidatorAddress: operatorAddr,
			})
			if err == nil {
				var denoms []map[string]string
				totalRewards := sdk.ZeroDec()

				for _, r := range rewResp.Rewards {
					denoms = append(denoms, map[string]string{
						"denom":          r.Denom,
						"amount":         r.Amount.String(),
						"adjustedAmount": r.Amount.String(),
					})

					if r.Denom == "uluna" {
						totalRewards = totalRewards.Add(r.Amount)
					} else {
						if price, ok := priceMap[r.Denom]; ok && !price.IsZero() {
							totalRewards = totalRewards.Add(r.Amount.Quo(price))
						}
					}
				}

				// Sort denoms
				sort.Slice(denoms, func(i, j int) bool {
					denomOrder := map[string]int{
						"uluna": 0,
						"ukrw":  1,
						"usdr":  2,
						"uusd":  3,
					}
					d1 := denoms[i]["denom"]
					d2 := denoms[j]["denom"]
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

				denomIf := make([]interface{}, len(denoms))
				for i, d := range denoms {
					denomIf[i] = d
				}

				ev.MyRewards = &RewardsPool{
					Total:  totalRewards.String(),
					Denoms: denomIf,
				}
			}
		}

		return ev, nil
	})
}

func (s *Server) GetClaims(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	operatorAddr := vars["operatorAddr"]

	// Parse pagination
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 5
	if limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err == nil && l > 0 {
			limit = l
		}
	}

	var offset uint64
	if offsetStr != "" {
		o, err := strconv.ParseUint(offsetStr, 10, 64)
		if err == nil {
			offset = o
		}
	}

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

	// Query ClickHouse
	// We use height * 65536 + index_in_block as ID
	sql := `
		SELECT 
			cast(height as UInt64) * 65536 + index_in_block as id,
			tx_hash,
			toUnixTimestamp(block_time) as timestamp,
			msgs_json,
			logs_json
		FROM txs
		WHERE 
			code = 0 
	`

	args := []interface{}{}

	// Add ID filter if offset is provided
	if offset > 0 {
		sql += " AND id < ? "
		args = append(args, offset)
	}

	// Add Type and Address filters
	// We check msgs_json directly to be robust against missing msg_type_ids
	sql += ` AND (
		(
			(arrayExists(x -> JSONExtractString(x, '@type') = '/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission' OR JSONExtractString(x, 'type') = 'distribution/MsgWithdrawValidatorCommission', msgs_json)) AND 
			(arrayExists(x -> JSONExtractString(x, 'validator_address') = ? OR JSONExtractString(x, 'validatorAddress') = ?, msgs_json))
		) OR (
			(arrayExists(x -> JSONExtractString(x, '@type') = '/cosmos.distribution.v1beta1.MsgWithdrawDelegationReward' OR JSONExtractString(x, 'type') = 'distribution/MsgWithdrawDelegationReward', msgs_json)) AND 
			(arrayExists(x -> JSONExtractString(x, 'validator_address') = ? OR JSONExtractString(x, 'validatorAddress') = ?, msgs_json)) AND
			(arrayExists(x -> JSONExtractString(x, 'delegator_address') = ? OR JSONExtractString(x, 'delegatorAddress') = ?, msgs_json))
		)
	)`
	args = append(args, operatorAddr, operatorAddr, operatorAddr, operatorAddr, accountAddr, accountAddr)

	sql += " ORDER BY id DESC LIMIT ? "
	args = append(args, limit)

	type TxRow struct {
		ID        uint64   `ch:"id"`
		TxHash    string   `ch:"tx_hash"`
		Timestamp int64    `ch:"timestamp"`
		MsgsJSON  []string `ch:"msgs_json"`
		LogsJSON  string   `ch:"logs_json"`
	}

	var rows []TxRow
	err = s.ch.Conn.Select(context.Background(), &rows, sql, args...)
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
	var next uint64

	chainID := s.clientCtx.ChainID
	if chainID == "" {
		status, err := s.rpc.Status(context.Background())
		if err == nil {
			chainID = status.NodeInfo.Network
		}
	}

	for _, row := range rows {
		next = row.ID // The last one will be the next offset

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

			// Determine type based on content or explicit type check if needed
			// But here we just check addresses as before, assuming the query filtered correctly.
			// However, since we query for specific types, we should be good.
			// But wait, a tx can have multiple messages. We should check if THIS message is the one we want.

			msgType, _ := msg["@type"].(string)
			if msgType == "" {
				msgType, _ = msg["type"].(string)
			}

			isCommission := msgType == "/cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission" || msgType == "distribution/MsgWithdrawValidatorCommission"
			isReward := msgType == "/cosmos.distribution.v1beta1.MsgWithdrawDelegationReward" || msgType == "distribution/MsgWithdrawDelegationReward"

			if isCommission && valAddr == operatorAddr {
				claimType = "Commission"
			} else if isReward && valAddr == operatorAddr && delAddr == accountAddr {
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
					Timestamp: time.Unix(row.Timestamp, 0).Format("2006-01-02T15:04:05Z"), // Match FCD format
				})
			}
		}
	}

	if claims == nil {
		claims = []Claim{}
	}

	resp := map[string]interface{}{
		"claims": claims,
		"limit":  limit,
	}
	if next > 0 {
		resp["next"] = next
	}

	respondJSON(w, http.StatusOK, resp)
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

		g, ctx := errgroup.WithContext(ctx)

		// 1. Fetch all validators (cached)
		var enrichedValidators []EnrichedValidator
		g.Go(func() error {
			var err error
			enrichedValidators, err = s.getCachedValidators(ctx)
			return err
		})

		// 2. Fetch Delegations
		var delegations []stakingtypes.DelegationResponse
		g.Go(func() error {
			var nextKey []byte
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
			return nil
		})

		// 3. Fetch Unbonding Delegations
		var unbondings []stakingtypes.UnbondingDelegation
		g.Go(func() error {
			var nextKey []byte
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
			return nil
		})

		// 4. Fetch Rewards
		var rewardsResp *distrtypes.QueryDelegationTotalRewardsResponse
		g.Go(func() error {
			var err error
			rewardsResp, err = distrClient.DelegationTotalRewards(ctx, &distrtypes.QueryDelegationTotalRewardsRequest{
				DelegatorAddress: account,
			})
			if err != nil {
				// Proceed with nil rewardsResp
				return nil
			}
			return nil
		})

		// 5. Fetch Balances
		var balances sdk.Coins
		g.Go(func() error {
			balancesResp, err := bankClient.AllBalances(ctx, &banktypes.QueryAllBalancesRequest{
				Address: account,
			})
			if err == nil {
				balances = balancesResp.Balances
			}
			return nil
		})

		// 6. Oracle Prices (needed for user rewards calculation)
		priceMapDec := make(map[string]sdk.Dec)
		g.Go(func() error {
			ratesResp, err := oracleClient.ExchangeRates(ctx, &oracletypes.QueryExchangeRatesRequest{})
			if err == nil {
				for _, r := range ratesResp.ExchangeRates {
					priceMapDec[r.Denom] = r.Amount
				}
			}
			return nil
		})

		if err := g.Wait(); err != nil {
			return nil, err
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
					"amount": c.Amount.TruncateInt().String(),
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

		// Clone validators to avoid modifying cache
		validatorsCopy := make([]EnrichedValidator, len(enrichedValidators))
		for i, v := range enrichedValidators {
			validatorsCopy[i] = v
			// Reset user specific fields
			validatorsCopy[i].MyDelegation = ""
			validatorsCopy[i].MyDelegatable = ""
			validatorsCopy[i].MyRewards = nil
			validatorsCopy[i].MyUndelegation = nil
		}

		valMap := make(map[string]*EnrichedValidator)
		for i := range validatorsCopy {
			valMap[validatorsCopy[i].OperatorAddress] = &validatorsCopy[i]
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
		for i := range validatorsCopy {
			v := &validatorsCopy[i]
			if amt, ok := delegationMap[v.OperatorAddress]; ok {
				v.MyDelegation = amt
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
					v.MyUndelegation = myUnbondings
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
			"validators":      validatorsCopy,
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
