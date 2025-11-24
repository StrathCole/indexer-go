package api

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
	Log      interface{}   `json:"log"`
	Events   []interface{} `json:"events"`
}

// MapTxToFCD converts our internal model.Tx to FCDTxResponse
func (s *Server) MapTxToFCD(tx model.Tx, denoms map[uint16]string, msgTypes map[uint16]string) FCDTxResponse {
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
	feeCoins := []FCDCoin{}
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
	for i, msgJSON := range tx.MsgsJSON {
		var msgContent interface{}
		if err := json.Unmarshal([]byte(msgJSON), &msgContent); err != nil {
			msgContent = msgJSON
		}

		// Get Type
		typeName := "unknown"
		if i < len(tx.MsgTypeIDs) {
			id := tx.MsgTypeIDs[i]
			if name, ok := msgTypes[id]; ok {
				typeName = mapProtoToAmino(name)
			}
		}

		msgs = append(msgs, map[string]interface{}{
			"type":  typeName,
			"value": msgContent,
		})
	}

	// Convert Logs
	logs := []FCDLog{}
	if tx.LogsJSON != "" {
		if err := json.Unmarshal([]byte(tx.LogsJSON), &logs); err != nil {
			// If unmarshal fails, maybe it's not a list of FCDLog?
			// Or maybe it's just raw log string?
			// FCD logs are usually a list of objects with msg_index, log, events.
			// Our LogsJSON comes from txRes.Log which might be string or json.
			// If it's a string, we can't do much.
		} else {
			// Sanitize logs: ensure Log field is empty string if nil
			for i := range logs {
				if logs[i].Log == nil {
					logs[i].Log = ""
				}
			}
		}
	}

	// Convert Signatures
	signatures := []interface{}{}
	for _, sigJSON := range tx.SignaturesJSON {
		var sigMap map[string]interface{}
		if err := json.Unmarshal([]byte(sigJSON), &sigMap); err == nil {
			// Fix pub_key format
			if pubKey, ok := sigMap["pub_key"].(map[string]interface{}); ok {
				// Check if we have "key" but missing "value"
				if key, hasKey := pubKey["key"].(string); hasKey {
					if _, hasValue := pubKey["value"]; !hasValue {
						// We need to transform it
						newPubKey := map[string]interface{}{
							"value": key,
						}

						// Determine type
						if typeURL, ok := pubKey["@type"].(string); ok {
							switch typeURL {
							case "/cosmos.crypto.secp256k1.PubKey":
								newPubKey["type"] = "tendermint/PubKeySecp256k1"
							case "/cosmos.crypto.ed25519.PubKey":
								newPubKey["type"] = "tendermint/PubKeyEd25519"
							case "/cosmos.crypto.multisig.LegacyAminoPubKey":
								newPubKey["type"] = "tendermint/PubKeyMultisigThreshold"
							default:
								newPubKey["type"] = "tendermint/PubKeySecp256k1" // Fallback
							}
						} else {
							// Default if no type
							newPubKey["type"] = "tendermint/PubKeySecp256k1"
						}

						sigMap["pub_key"] = newPubKey
					}
				}
			}
			signatures = append(signatures, sigMap)
		}
	}

	// Construct Response
	return FCDTxResponse{
		ID:      tx.Height*100000 + uint64(tx.IndexInBlock), // Synthetic ID
		ChainID: "columbus-5",                               // Hardcoded for now
		Tx: FCDTx{
			Type: "core/StdTx", // Generic type
			Value: FCDTxValue{
				Fee: FCDFee{
					Gas:    strconv.FormatUint(tx.GasWanted, 10),
					Amount: feeCoins,
				},
				Msg:        msgs,
				Memo:       tx.Memo,
				Signatures: signatures,
				Timeout:    "0",
				Tax:        taxStr,
			},
		},
		Logs:      logs,
		Height:    strconv.FormatUint(tx.Height, 10),
		TxHash:    txHash,
		RawLog:    tx.RawLog,
		GasUsed:   strconv.FormatUint(tx.GasUsed, 10),
		Timestamp: tx.BlockTime.UTC().Format("2006-01-02T15:04:05Z"),
		GasWanted: strconv.FormatUint(tx.GasWanted, 10),
		Codespace: tx.Codespace,
		Code:      int(tx.Code),
	}
}

func mapProtoToAmino(protoName string) string {
	// terra.oracle.v1beta1.MsgAggregateExchangeRatePrevote -> oracle/MsgAggregateExchangeRatePrevote
	// cosmos.bank.v1beta1.MsgSend -> bank/MsgSend
	// cosmwasm.wasm.v1.MsgExecuteContract -> wasm/MsgExecuteContract

	parts := strings.Split(protoName, ".")
	if len(parts) > 2 {
		// Check for cosmos, terra, or cosmwasm prefix
		if parts[0] == "cosmos" || parts[0] == "terra" {
			// module is parts[1]
			msgName := parts[len(parts)-1]
			return fmt.Sprintf("%s/%s", parts[1], msgName)
		}
		if parts[0] == "cosmwasm" && parts[1] == "wasm" {
			msgName := parts[len(parts)-1]
			return fmt.Sprintf("wasm/%s", msgName)
		}
	}
	return protoName
}
