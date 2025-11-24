package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/classic-terra/core/v3/app"
	"github.com/classic-terra/indexer-go/internal/model"
	abcitypes "github.com/cometbft/cometbft/abci/types"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
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

	cdc := app.MakeEncodingConfig().Marshaler

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

	modelTx := &model.Tx{
		Height:       height,
		IndexInBlock: index,
		BlockTime:    blockTime,
		TxHash:       txHashRaw,
		Codespace:    res.Codespace,
		Code:         res.Code,
		GasWanted:    uint64(res.GasWanted),
		GasUsed:      uint64(res.GasUsed),
		FeeAmounts:   feeAmounts,
		FeeDenomIDs:  feeDenomIDs,
		TaxAmounts:   taxAmounts,
		TaxDenomIDs:  taxDenomIDs,
		MsgTypeIDs:   msgTypeIDs,
		MsgsJSON:     msgsJSON,
		Memo:         memo,
		RawLog:       res.Log,
		LogsJSON:     res.Log,
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

func (s *Service) extractAccountTxs(
	ctx context.Context,
	height uint64,
	index uint16,
	blockTime time.Time,
	txHash string,
	events []abcitypes.Event,
) ([]model.AccountTx, error) {
	accountTxsMap := make(map[uint64]*model.AccountTx)

	for _, event := range events {
		// Handle various event types to capture account activity

		// 1. Transfer
		if event.Type == "transfer" {
			var sender, recipient string
			var amount sdk.Coins

			for _, attr := range event.Attributes {
				key := string(attr.Key)
				value := string(attr.Value)

				switch key {
				case "sender":
					sender = value
				case "recipient":
					recipient = value
				case "amount":
					amount, _ = sdk.ParseCoinsNormalized(value)
				}
			}

			if sender != "" {
				senderID, err := s.dims.GetOrCreateAddressID(ctx, sender)
				if err == nil {
					s.addAccountTx(ctx, accountTxsMap, senderID, height, index, blockTime, txHash, 2, amount) // 2: Out
				}
			}

			if recipient != "" {
				recipientID, err := s.dims.GetOrCreateAddressID(ctx, recipient)
				if err == nil {
					s.addAccountTx(ctx, accountTxsMap, recipientID, height, index, blockTime, txHash, 1, amount) // 1: In
				}
			}
		}

		// 2. Message (Sender)
		if event.Type == "message" {
			var sender string
			for _, attr := range event.Attributes {
				if string(attr.Key) == "sender" {
					sender = string(attr.Value)
					break
				}
			}
			if sender != "" {
				senderID, err := s.dims.GetOrCreateAddressID(ctx, sender)
				if err == nil {
					// Direction 0 or 2? Usually sender pays fees, so it's an action.
					// FCD marks this as 'out' usually if it involves funds, or just 'action'.
					// We use 2 (Out) as default for sender.
					s.addAccountTx(ctx, accountTxsMap, senderID, height, index, blockTime, txHash, 2, nil)
				}
			}
		}

		// 3. Delegate / Unbond / Withdraw Rewards
		if event.Type == "delegate" || event.Type == "unbond" || event.Type == "withdraw_rewards" {
			var validator string
			var amount sdk.Coins

			for _, attr := range event.Attributes {
				key := string(attr.Key)
				value := string(attr.Value)
				switch key {
				case "validator":
					validator = value
				case "amount":
					amount, _ = sdk.ParseCoinsNormalized(value)
				}
			}

			// Note: delegator is usually in 'message' event as sender, but sometimes in specific events.
			// In 'delegate', 'unbond', 'withdraw_rewards', the user is the sender of the msg.
			// So 'message' handler above catches the user.
			// But we might want to capture the validator interaction too?
			// FCD usually indexes the validator address too if it's relevant.
			if validator != "" {
				valID, err := s.dims.GetOrCreateAddressID(ctx, validator)
				if err == nil {
					s.addAccountTx(ctx, accountTxsMap, valID, height, index, blockTime, txHash, 0, amount) // 0: Neutral/Info
				}
			}
		}

		// 4. Swap (Terra specific)
		if event.Type == "swap" {
			var trader, offerAsset, askAsset string
			// var offerAmount, askAmount sdk.Coin

			for _, attr := range event.Attributes {
				key := string(attr.Key)
				value := string(attr.Value)
				switch key {
				case "trader":
					trader = value
				case "offer_asset":
					offerAsset = value
				case "ask_asset":
					askAsset = value
				}
			}

			if trader != "" {
				traderID, err := s.dims.GetOrCreateAddressID(ctx, trader)
				if err == nil {
					// Swap involves both in and out.
					// We just mark it.
					_ = offerAsset
					_ = askAsset
					s.addAccountTx(ctx, accountTxsMap, traderID, height, index, blockTime, txHash, 2, nil)
				}
			}
		}
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
