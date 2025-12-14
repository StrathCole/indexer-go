package ingest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/model"
	abcitypes "github.com/cometbft/cometbft/abci/types"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	txsigning "github.com/cosmos/cosmos-sdk/types/tx/signing"
	"github.com/cosmos/cosmos-sdk/x/auth/signing"
	"github.com/cosmos/gogoproto/proto"
)

func (s *Service) convertBlock(block *coretypes.ResultBlock) model.Block {
	return model.Block{
		Height:          uint64(block.Block.Height),
		BlockHash:       block.BlockID.Hash.String(),
		BlockTime:       block.Block.Time,
		ProposerAddress: block.Block.ProposerAddress.String(),
		TxCount:         uint32(len(block.Block.Txs)),
	}
}

func (s *Service) extractOraclePrices(
	height uint64,
	blockTime time.Time,
	events []abcitypes.Event,
) []model.OraclePrice {
	var prices []model.OraclePrice

	for _, event := range events {
		if event.Type == "exchange_rate_update" {
			var denom string
			var rate float64

			for _, attr := range event.Attributes {
				key := string(attr.Key)
				value := string(attr.Value)

				if key == "denom" {
					denom = value
				} else if key == "exchange_rate" {
					rate, _ = strconv.ParseFloat(value, 64)
				}
			}

			if denom != "" && rate > 0 {
				prices = append(prices, model.OraclePrice{
					BlockTime: blockTime,
					Height:    height,
					Denom:     denom,
					Price:     rate,
					Currency:  "uluna", // Base currency
				})
			}
		}
	}
	return prices
}

func (s *Service) convertTx(
	height uint64,
	index uint16,
	blockTime time.Time,
	txHash string,
	tx sdk.Tx,
	txRes *coretypes.ResultBlockResults,
) (*model.Tx, []model.Event, []model.AccountTx, error) {
	// txRes contains DeliverTx results which has gas used, logs, events etc.

	// We need to match the index with the result in txRes.TxsResults
	if int(index) >= len(txRes.TxsResults) {
		return nil, nil, nil, fmt.Errorf("tx index out of bounds")
	}
	res := txRes.TxsResults[index]

	// txHash is already Hex string. Use it directly for ClickHouse FixedString(64)
	txHashRaw := txHash

	// Use cached codec from service
	cdc := s.cdc

	// Extract fees
	feeTx, ok := tx.(sdk.FeeTx)
	var feeAmounts []int64
	var feeDenomIDs []uint16

	if ok {
		for _, coin := range feeTx.GetFee() {
			feeAmounts = append(feeAmounts, coin.Amount.Int64())
			denomID, _ := s.dims.GetOrCreateDenomID(context.Background(), coin.Denom)
			feeDenomIDs = append(feeDenomIDs, denomID)
		}
	}

	// Extract Tax
	var taxAmounts []int64
	var taxDenomIDs []uint16

	for _, event := range res.Events {
		if event.Type == "tax_payment" {
			for _, attr := range event.Attributes {
				if string(attr.Key) == "tax_amount" {
					coins, err := sdk.ParseCoinsNormalized(string(attr.Value))
					if err == nil {
						for _, coin := range coins {
							taxAmounts = append(taxAmounts, coin.Amount.Int64())
							denomID, _ := s.dims.GetOrCreateDenomID(context.Background(), coin.Denom)
							taxDenomIDs = append(taxDenomIDs, denomID)
						}
					}
				}
			}
		}
	}

	// Fallback to legacy calculation if no tax_payment event found
	if len(taxAmounts) == 0 && s.tax != nil {
		calculatedTax, err := s.tax.CalculateTax(context.Background(), int64(height), tx, cdc)
		if err == nil {
			for _, coin := range calculatedTax {
				taxAmounts = append(taxAmounts, coin.Amount.Int64())
				denomID, _ := s.dims.GetOrCreateDenomID(context.Background(), coin.Denom)
				taxDenomIDs = append(taxDenomIDs, denomID)
			}
		}
	}

	// Extract Msgs
	var msgTypeIDs []uint16
	var msgsJSON []string

	for _, msg := range tx.GetMsgs() {
		msgType := proto.MessageName(msg)
		id, _ := s.dims.GetOrCreateMsgTypeID(context.Background(), msgType)
		msgTypeIDs = append(msgTypeIDs, id)

		// Marshal msg to JSON compatible with FCD
		jsonBytes, err := cdc.MarshalJSON(msg)
		if err != nil {
			// Fallback to standard json marshal if codec fails (unlikely)
			jsonBytes, _ = json.Marshal(msg)
		}
		msgsJSON = append(msgsJSON, string(jsonBytes))
	}

	// Extract Memo
	memoTx, ok := tx.(sdk.TxWithMemo)
	memo := ""
	if ok {
		memo = memoTx.GetMemo()
	}

	// Extract Signatures
	var signaturesJSON []string
	if sigTx, ok := tx.(signing.Tx); ok {
		sigs, err := sigTx.GetSignaturesV2()
		if err == nil {
			for _, sig := range sigs {
				var sigBytes []byte
				if single, ok := sig.Data.(*txsigning.SingleSignatureData); ok {
					sigBytes = single.Signature
				}

				if sigBytes != nil {
					pubKeyJSON, err := cdc.MarshalJSON(sig.PubKey)
					if err == nil {
						sigBase64 := base64.StdEncoding.EncodeToString(sigBytes)
						sigMap := map[string]interface{}{
							"pub_key":   json.RawMessage(pubKeyJSON),
							"signature": sigBase64,
						}
						b, _ := json.Marshal(sigMap)
						signaturesJSON = append(signaturesJSON, string(b))
					}
				}
			}
		}
	}

	modelTx := &model.Tx{
		Height:         height,
		IndexInBlock:   index,
		BlockTime:      blockTime,
		TxHash:         txHashRaw,
		Codespace:      res.Codespace,
		Code:           res.Code,
		GasWanted:      uint64(res.GasWanted),
		GasUsed:        uint64(res.GasUsed),
		FeeAmounts:     feeAmounts,
		FeeDenomIDs:    feeDenomIDs,
		TaxAmounts:     taxAmounts,
		TaxDenomIDs:    taxDenomIDs,
		MsgTypeIDs:     msgTypeIDs,
		MsgsJSON:       msgsJSON,
		SignaturesJSON: signaturesJSON,
		Memo:           memo,
		RawLog:         res.Log,
		LogsJSON:       res.Log,
	}

	// Extract Events
	var events []model.Event
	for i, event := range res.Events {
		for _, attr := range event.Attributes {
			events = append(events, model.Event{
				Height:     height,
				BlockTime:  blockTime,
				Scope:      "tx",
				TxIndex:    int16(index),
				EventIndex: uint16(i),
				EventType:  event.Type,
				AttrKey:    string(attr.Key),
				AttrValue:  string(attr.Value),
				TxHash:     txHashRaw,
			})
		}
	}

	// Extract AccountTxs
	accountTxs, err := s.extractAccountTxs(context.Background(), height, index, blockTime, txHashRaw, res.Events)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to extract account txs: %w", err)
	}

	return modelTx, events, accountTxs, nil
}

// isTerraAddress checks if a string is a valid Terra address (terra1... bech32)
// Returns false for terravaloper addresses as they are not relevant for account tracking
func isTerraAddress(s string) bool {
	// Must start with "terra1" and be a reasonable length (39-59 chars for bech32)
	if len(s) < 39 || len(s) > 64 {
		return false
	}
	// Exclude terravaloper addresses
	if strings.HasPrefix(s, "terravaloper") {
		return false
	}
	// Check for terra1 prefix (accounts and contracts)
	return strings.HasPrefix(s, "terra1")
}

// extractAddressesFromEvents scans all event attributes for Terra addresses
// Returns a map of address -> direction (used for determining in/out/neutral)
func (s *Service) extractAddressesFromEvents(events []abcitypes.Event) map[string]int8 {
	addresses := make(map[string]int8)

	// Define which attribute keys indicate direction
	outKeys := map[string]bool{
		"sender": true, "spender": true, "from": true, "trader": true,
		"delegator": true, "proposer": true, "depositor": true, "voter": true,
		"granter": true, "creator": true, "owner": true, "admin": true,
		"sender_address": true, "from_address": true,
	}
	inKeys := map[string]bool{
		"recipient": true, "receiver": true, "to": true, "to_address": true,
		"grantee": true, "recipient_address": true,
	}

	for _, event := range events {
		for _, attr := range event.Attributes {
			key := string(attr.Key)
			value := string(attr.Value)

			if !isTerraAddress(value) {
				continue
			}

			// Determine direction based on attribute key
			var direction int8 = 0 // neutral
			if outKeys[key] {
				direction = 2 // out
			} else if inKeys[key] {
				direction = 1 // in
			}

			// If we already have this address, prefer a non-neutral direction
			if existing, ok := addresses[value]; ok {
				if existing == 0 && direction != 0 {
					addresses[value] = direction
				}
				// If existing is non-zero, keep it (first meaningful direction wins)
			} else {
				addresses[value] = direction
			}
		}
	}

	return addresses
}

func (s *Service) extractAccountTxs(
	ctx context.Context,
	height uint64,
	index uint16,
	blockTime time.Time,
	txHash string,
	events []abcitypes.Event,
) ([]model.AccountTx, error) {
	accountTxsMap := make(map[uint64]*model.AccountTx)

	// First, extract main amount from transfer events for context
	var mainAmount sdk.Coins
	for _, event := range events {
		if event.Type == "transfer" {
			for _, attr := range event.Attributes {
				if string(attr.Key) == "amount" {
					coins, err := sdk.ParseCoinsNormalized(string(attr.Value))
					if err == nil && len(coins) > 0 {
						mainAmount = coins
						break
					}
				}
			}
			if len(mainAmount) > 0 {
				break
			}
		}
	}

	// Extract all addresses from all events
	addressDirections := s.extractAddressesFromEvents(events)

	// Create AccountTx entries for each unique address
	for address, direction := range addressDirections {
		addressID, err := s.dims.GetOrCreateAddressID(ctx, address, height, blockTime)
		if err != nil {
			continue
		}
		s.addAccountTx(ctx, accountTxsMap, addressID, height, index, blockTime, txHash, direction, mainAmount)
	}

	var accountTxs []model.AccountTx
	for _, atx := range accountTxsMap {
		accountTxs = append(accountTxs, *atx)
	}

	return accountTxs, nil
}

func (s *Service) addAccountTx(
	ctx context.Context,
	m map[uint64]*model.AccountTx,
	addressID uint64,
	height uint64,
	index uint16,
	blockTime time.Time,
	txHash string,
	direction int8,
	amount sdk.Coins,
) {
	var mainDenomID uint16
	var mainAmount int64

	if len(amount) > 0 {
		coin := amount[0]
		mainAmount = coin.Amount.Int64()
		id, err := s.dims.GetOrCreateDenomID(ctx, coin.Denom)
		if err == nil {
			mainDenomID = id
		}
	}

	if existing, ok := m[addressID]; ok {
		// If we already have an entry, we might want to update it.
		// For example, if we have multiple transfers involving the same account in one tx.
		// If direction is same, add amount?
		// For now, let's keep it simple and just keep the first one or overwrite.
		// FCD logic is complex here, usually it tries to find the "main" action.
		_ = existing
	} else {
		m[addressID] = &model.AccountTx{
			AddressID:    addressID,
			Height:       height,
			IndexInBlock: index,
			BlockTime:    blockTime,
			TxHash:       txHash,
			Direction:    direction,
			MainDenomID:  mainDenomID,
			MainAmount:   mainAmount,
		}
	}
}

// extractAccountBlockEvents extracts all Terra addresses from block-level events (begin_block/end_block)
// and creates AccountTx entries for them with is_block_event=true
func (s *Service) extractAccountBlockEvents(
	ctx context.Context,
	height uint64,
	blockTime time.Time,
	scope string,
	events []abcitypes.Event,
) ([]model.AccountTx, error) {
	// Track address -> (direction, amount) for each address found
	type addressInfo struct {
		direction int8
		amount    sdk.Coins
	}
	addressMap := make(map[uint64]*addressInfo)

	// Define which attribute keys indicate direction
	outKeys := map[string]bool{
		"sender": true, "spender": true, "from": true, "delegator": true,
		"proposer": true, "depositor": true, "voter": true, "validator": true,
	}
	inKeys := map[string]bool{
		"recipient": true, "receiver": true, "to": true, "withdraw_address": true,
	}

	// Extract addresses and amounts from all events
	for _, event := range events {
		var eventAmount sdk.Coins

		// First pass: find amount in this event
		for _, attr := range event.Attributes {
			key := string(attr.Key)
			value := string(attr.Value)
			if key == "amount" || key == "coins" {
				coins, err := sdk.ParseCoinsNormalized(value)
				if err == nil && len(coins) > 0 {
					eventAmount = coins
				}
			}
		}

		// Second pass: find addresses and associate with amount
		for _, attr := range event.Attributes {
			key := string(attr.Key)
			value := string(attr.Value)

			if !isTerraAddress(value) {
				continue
			}

			addressID, err := s.dims.GetOrCreateAddressID(ctx, value, height, blockTime)
			if err != nil {
				continue
			}

			// Determine direction based on attribute key
			var direction int8 = 0
			if outKeys[key] {
				direction = 2 // out
			} else if inKeys[key] {
				direction = 1 // in
			}

			if existing, ok := addressMap[addressID]; ok {
				// Update direction if we have a more specific one
				if existing.direction == 0 && direction != 0 {
					existing.direction = direction
				}
				// Add to amount if this event has coins
				if len(eventAmount) > 0 {
					existing.amount = existing.amount.Add(eventAmount...)
				}
			} else {
				addressMap[addressID] = &addressInfo{
					direction: direction,
					amount:    eventAmount,
				}
			}
		}
	}

	// Determine event scope
	var eventScope int8 = model.EventScopeBeginBlock
	if scope == "end_block" {
		eventScope = model.EventScopeEndBlock
	}

	// Convert map to slice of AccountTx
	var accountTxs []model.AccountTx
	for addressID, info := range addressMap {
		var mainDenomID uint16
		var mainAmount int64

		if len(info.amount) > 0 {
			// Use the largest coin as main amount
			largestCoin := info.amount[0]
			for _, coin := range info.amount {
				if coin.Amount.GT(largestCoin.Amount) {
					largestCoin = coin
				}
			}
			mainAmount = largestCoin.Amount.Int64()
			id, err := s.dims.GetOrCreateDenomID(ctx, largestCoin.Denom)
			if err == nil {
				mainDenomID = id
			}
		}

		accountTxs = append(accountTxs, model.AccountTx{
			AddressID:    addressID,
			Height:       height,
			IndexInBlock: 0, // Not relevant for block events
			BlockTime:    blockTime,
			TxHash:       "", // Empty for block events
			Direction:    info.direction,
			MainDenomID:  mainDenomID,
			MainAmount:   mainAmount,
			IsBlockEvent: true,
			EventScope:   eventScope,
		})
	}

	return accountTxs, nil
}

func (s *Service) convertBlockEvents(
	height uint64,
	blockTime time.Time,
	scope string,
	events []abcitypes.Event,
) []model.Event {
	var modelEvents []model.Event
	for i, event := range events {
		for _, attr := range event.Attributes {
			modelEvents = append(modelEvents, model.Event{
				Height:     height,
				BlockTime:  blockTime,
				Scope:      scope,
				TxIndex:    -1,
				EventIndex: uint16(i),
				EventType:  event.Type,
				AttrKey:    string(attr.Key),
				AttrValue:  string(attr.Value),
				TxHash:     "",
			})
		}
	}
	return modelEvents
}

func (s *Service) extractBlockRewardsAndReturns(
	height uint64,
	blockTime time.Time,
	events []abcitypes.Event,
) ([]model.BlockReward, []model.ValidatorReturn) {
	var blockRewards []model.BlockReward
	var validatorReturns []model.ValidatorReturn

	totalReward := make(map[string]float64)
	totalCommission := make(map[string]float64)

	// Temporary maps to aggregate per validator
	valRewards := make(map[string]map[string]float64)
	valCommissions := make(map[string]map[string]float64)

	for _, event := range events {
		if event.Type == "rewards" || event.Type == "commission" {
			var amountStr string
			var validator string

			for _, attr := range event.Attributes {
				key := string(attr.Key)
				value := string(attr.Value)
				if key == "amount" {
					amountStr = value
				} else if key == "validator" {
					validator = value
				}
			}

			if amountStr != "" && validator != "" {
				coins, err := sdk.ParseCoinsNormalized(amountStr)
				if err == nil {
					for _, coin := range coins {
						amount := float64(coin.Amount.Int64()) // Use float for aggregation simplicity

						if event.Type == "rewards" {
							// Total Reward
							totalReward[coin.Denom] += amount

							// Per Validator Reward
							if _, ok := valRewards[validator]; !ok {
								valRewards[validator] = make(map[string]float64)
							}
							valRewards[validator][coin.Denom] += amount
						} else {
							// Total Commission
							totalCommission[coin.Denom] += amount

							// Per Validator Commission
							if _, ok := valCommissions[validator]; !ok {
								valCommissions[validator] = make(map[string]float64)
							}
							valCommissions[validator][coin.Denom] += amount
						}
					}
				}
			}
		}
	}

	// Create BlockReward
	if len(totalReward) > 0 || len(totalCommission) > 0 {
		blockRewards = append(blockRewards, model.BlockReward{
			BlockTime:       blockTime,
			Height:          height,
			TotalReward:     totalReward,
			TotalCommission: totalCommission,
		})
	}

	// Create ValidatorReturns
	// We iterate over all validators found in either rewards or commissions
	validators := make(map[string]bool)
	for v := range valRewards {
		validators[v] = true
	}
	for v := range valCommissions {
		validators[v] = true
	}

	for v := range validators {
		validatorReturns = append(validatorReturns, model.ValidatorReturn{
			BlockTime:       blockTime,
			Height:          height,
			OperatorAddress: v,
			Reward:          valRewards[v],
			Commission:      valCommissions[v],
		})
	}

	return blockRewards, validatorReturns
}
