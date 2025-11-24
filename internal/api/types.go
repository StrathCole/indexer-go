package api

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/model"
)

// FCDTxResponse mirrors the old FCD transaction response format
type FCDTxResponse struct {
	ID        uint64   `json:"id"` // Using Height * 100000 + IndexInBlock as ID
	ChainID   string   `json:"chainId"`
	Tx        FCDTx    `json:"tx"`
	Logs      []FCDLog `json:"logs"`
	Height    string   `json:"height"`
	TxHash    string   `json:"txhash"`
	RawLog    string   `json:"raw_log"`
	GasUsed   string   `json:"gas_used"`
	Timestamp string   `json:"timestamp"`
	GasWanted string   `json:"gas_wanted"`
	Codespace string   `json:"codespace,omitempty"`
	Code      int      `json:"code,omitempty"`
}

type FCDTx struct {
	Type  string     `json:"type"`
	Value FCDTxValue `json:"value"`
}

type FCDTxValue struct {
	Fee        FCDFee        `json:"fee"`
	Msg        []interface{} `json:"msg"`
	Memo       string        `json:"memo"`
	Signatures []interface{} `json:"signatures"` // We don't store signatures, so empty
	Timeout    string        `json:"timeout_height"`
	Tax        string        `json:"tax,omitempty"`
}

type FCDFee struct {
	Gas    string    `json:"gas"`
	Amount []FCDCoin `json:"amount"`
}

type FCDCoin struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

type FCDLog struct {
	MsgIndex int           `json:"msg_index"`
	Log      string        `json:"log"`
	Events   []interface{} `json:"events"`
}

// MapTxToFCD converts our internal model.Tx to FCDTxResponse
func (s *Server) MapTxToFCD(tx model.Tx, denoms map[uint16]string) FCDTxResponse {
	// Convert Hash
	txHash := tx.TxHash

	// Convert Tax
	var taxParts []string
	taxMap := make(map[string]int64)

	for i, amount := range tx.TaxAmounts {
		denomID := tx.TaxDenomIDs[i]
		denom, ok := denoms[denomID]
		if !ok {
			denom = fmt.Sprintf("unknown-%d", denomID)
		}
		// FCD format: "amountdenom" e.g. "1000uluna"
		taxParts = append(taxParts, fmt.Sprintf("%d%s", amount, denom))
		taxMap[denom] = amount
	}
	taxStr := strings.Join(taxParts, ",")

	// Convert Fees (Subtract Tax from Fee to get Gas Fee)
	var feeCoins []FCDCoin
	for i, amount := range tx.FeeAmounts {
		denomID := tx.FeeDenomIDs[i]
		denom, ok := denoms[denomID]
		if !ok {
			denom = fmt.Sprintf("unknown-%d", denomID)
		}

		// Subtract tax if present
		if tax, ok := taxMap[denom]; ok {
			amount -= tax
		}

		if amount > 0 {
			feeCoins = append(feeCoins, FCDCoin{
				Denom:  denom,
				Amount: strconv.FormatInt(amount, 10),
			})
		}
	}

	// Convert Msgs
	var msgs []interface{}
	for _, msgJSON := range tx.MsgsJSON {
		var msg interface{}
		if err := json.Unmarshal([]byte(msgJSON), &msg); err == nil {
			msgs = append(msgs, msg)
		} else {
			msgs = append(msgs, msgJSON) // Fallback
		}
	}

	// Convert Logs
	var logs []FCDLog
	if tx.LogsJSON != "" {
		if err := json.Unmarshal([]byte(tx.LogsJSON), &logs); err != nil {
			// If unmarshal fails, maybe it's not a list of FCDLog?
			// Or maybe it's just raw log string?
			// FCD logs are usually a list of objects with msg_index, log, events.
			// Our LogsJSON comes from txRes.Log which might be string or json.
			// If it's a string, we can't do much.
		}
	}

	// Construct Response
	return FCDTxResponse{
		ID:      tx.Height*100000 + uint64(tx.IndexInBlock), // Synthetic ID
		ChainID: "columbus-5",                               // Hardcoded for now, or fetch from config
		Tx: FCDTx{
			Type: "core/StdTx", // Generic type
			Value: FCDTxValue{
				Fee: FCDFee{
					Gas:    strconv.FormatUint(tx.GasWanted, 10),
					Amount: feeCoins,
				},
				Msg:        msgs,
				Memo:       tx.Memo,
				Signatures: []interface{}{}, // Not stored
				Timeout:    "0",
				Tax:        taxStr,
			},
		},
		Logs:      logs,
		Height:    strconv.FormatUint(tx.Height, 10),
		TxHash:    txHash,
		RawLog:    tx.RawLog,
		GasUsed:   strconv.FormatUint(tx.GasUsed, 10),
		Timestamp: tx.BlockTime.Format(time.RFC3339),
		GasWanted: strconv.FormatUint(tx.GasWanted, 10),
		Codespace: tx.Codespace,
		Code:      int(tx.Code),
	}
}
