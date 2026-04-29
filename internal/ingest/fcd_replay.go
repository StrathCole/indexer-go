package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/model"
	abcitypes "github.com/cometbft/cometbft/abci/types"
)

const fcdReplayPlaceholderBlockHash = "0000000000000000000000000000000000000000000000000000000000000000"

type fcdBlockResponse struct {
	ChainID   string        `json:"chainId"`
	Height    int64         `json:"height"`
	Timestamp time.Time     `json:"timestamp"`
	Proposer  fcdProposer   `json:"proposer"`
	Txs       []fcdTxResult `json:"txs"`
}

type fcdProposer struct {
	Moniker         string `json:"moniker"`
	Identity        string `json:"identity"`
	OperatorAddress string `json:"operatorAddress"`
}

type fcdTxResult struct {
	Code      uint32         `json:"code"`
	Codespace string         `json:"codespace"`
	Events    []fcdEvent     `json:"events"`
	GasUsed   string         `json:"gas_used"`
	GasWanted string         `json:"gas_wanted"`
	Height    string         `json:"height"`
	Logs      []fcdLogEntry  `json:"logs"`
	RawLog    string         `json:"raw_log"`
	Timestamp time.Time      `json:"timestamp"`
	Tx        fcdTxEnvelope  `json:"tx"`
	TxHash    string         `json:"txhash"`
}

type fcdTxEnvelope struct {
	Type  string     `json:"type"`
	Value fcdTxValue `json:"value"`
}

type fcdTxValue struct {
	Fee        fcdFee            `json:"fee"`
	Msg        []fcdMessage      `json:"msg"`
	Memo       string            `json:"memo"`
	Signatures []json.RawMessage `json:"signatures"`
	Tax        string            `json:"tax"`
}

type fcdFee struct {
	Gas    string    `json:"gas"`
	Amount []fcdCoin `json:"amount"`
}

type fcdCoin struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

type fcdMessage struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type fcdLogEntry struct {
	Log      json.RawMessage `json:"log"`
	Events   []fcdEvent      `json:"events"`
	Success  bool            `json:"success"`
	MsgIndex flexibleInt     `json:"msg_index"`
}

type flexibleInt int

func (v *flexibleInt) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		*v = 0
		return nil
	}

	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		trimmed = trimmed[1 : len(trimmed)-1]
		if trimmed == "" {
			*v = 0
			return nil
		}
	}

	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return fmt.Errorf("parse flexible int %q: %w", trimmed, err)
	}
	*v = flexibleInt(parsed)
	return nil
}

type fcdEvent struct {
	Type       string             `json:"type"`
	Attributes []fcdEventAttrPair `json:"attributes"`
}

type fcdEventAttrPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Service) HeightHasStoredData(ctx context.Context, height int64) (bool, error) {
	exists, err := s.pg.BlockExists(ctx, height)
	if err != nil {
		return false, fmt.Errorf("check postgres blocks for height %d: %w", height, err)
	}
	if exists {
		return true, nil
	}

	var txCount uint64
	if err := s.ch.Conn.QueryRow(ctx, "SELECT count() FROM txs WHERE height = ?", uint64(height)).Scan(&txCount); err != nil {
		return false, fmt.Errorf("check clickhouse txs for height %d: %w", height, err)
	}
	return txCount > 0, nil

}

func (s *Service) FetchAndConvertFCDBlock(ctx context.Context, baseURL string, height int64) (model.Block, []model.Tx, []model.Event, []model.AccountTx, error) {
	if height <= 0 {
		return model.Block{}, nil, nil, nil, fmt.Errorf("height must be > 0")
	}

	endpoint := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(endpoint, "/v1") {
		endpoint = fmt.Sprintf("%s/blocks/%d", endpoint, height)
	} else {
		endpoint = fmt.Sprintf("%s/v1/blocks/%d", endpoint, height)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return model.Block{}, nil, nil, nil, fmt.Errorf("build fcd request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return model.Block{}, nil, nil, nil, fmt.Errorf("fetch fcd block %d: %w", height, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return model.Block{}, nil, nil, nil, fmt.Errorf("fcd block %d returned %s: %s", height, resp.Status, strings.TrimSpace(string(body)))
	}

	var payload fcdBlockResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return model.Block{}, nil, nil, nil, fmt.Errorf("decode fcd block %d: %w", height, err)
	}
	if payload.Height != height {
		return model.Block{}, nil, nil, nil, fmt.Errorf("fcd height mismatch: requested %d got %d", height, payload.Height)
	}

	modelBlock := model.Block{
		Height:          uint64(payload.Height),
		BlockHash:       fcdReplayPlaceholderBlockHash,
		BlockTime:       payload.Timestamp,
		ProposerAddress: payload.Proposer.OperatorAddress,
		TxCount:         uint32(len(payload.Txs)),
	}

	modelTxs := make([]model.Tx, 0, len(payload.Txs))
	modelEvents := make([]model.Event, 0)
	modelAccountTxs := make([]model.AccountTx, 0)

	for idx, tx := range payload.Txs {
		modelTx, txEvents, accountTxs, err := s.convertFCDTx(uint64(payload.Height), uint16(idx), payload.Timestamp, tx)
		if err != nil {
			return model.Block{}, nil, nil, nil, fmt.Errorf("convert fcd tx at height %d index %d: %w", height, idx, err)
		}
		modelTxs = append(modelTxs, modelTx)
		modelEvents = append(modelEvents, txEvents...)
		modelAccountTxs = append(modelAccountTxs, accountTxs...)
	}

	return modelBlock, modelTxs, modelEvents, modelAccountTxs, nil
}

func (s *Service) convertFCDTx(height uint64, index uint16, blockTime time.Time, tx fcdTxResult) (model.Tx, []model.Event, []model.AccountTx, error) {
	feeAmounts, feeDenomIDs, err := s.fcdCoinsToModel(context.Background(), tx.Tx.Value.Fee.Amount)
	if err != nil {
		return model.Tx{}, nil, nil, fmt.Errorf("convert fees: %w", err)
	}

	taxAmounts, taxDenomIDs, err := s.fcdTaxToModel(context.Background(), tx.Tx.Value.Tax)
	if err != nil {
		return model.Tx{}, nil, nil, fmt.Errorf("convert tax: %w", err)
	}

	msgTypeIDs := make([]uint16, 0, len(tx.Tx.Value.Msg))
	msgsJSON := make([]string, 0, len(tx.Tx.Value.Msg))
	for _, msg := range tx.Tx.Value.Msg {
		id, err := s.dims.GetOrCreateMsgTypeID(context.Background(), msg.Type)
		if err != nil {
			return model.Tx{}, nil, nil, fmt.Errorf("get msg type id for %s: %w", msg.Type, err)
		}
		msgTypeIDs = append(msgTypeIDs, id)
		msgsJSON = append(msgsJSON, string(msg.Value))
	}

	signaturesJSON := make([]string, 0, len(tx.Tx.Value.Signatures))
	for _, signature := range tx.Tx.Value.Signatures {
		signaturesJSON = append(signaturesJSON, string(signature))
	}

	gasWanted, err := parseUint64String(tx.GasWanted)
	if err != nil {
		return model.Tx{}, nil, nil, fmt.Errorf("parse gas_wanted: %w", err)
	}
	gasUsed, err := parseUint64String(tx.GasUsed)
	if err != nil {
		return model.Tx{}, nil, nil, fmt.Errorf("parse gas_used: %w", err)
	}

	logsJSON := ""
	if len(tx.Logs) > 0 {
		encodedLogs, err := json.Marshal(tx.Logs)
		if err != nil {
			return model.Tx{}, nil, nil, fmt.Errorf("marshal logs json: %w", err)
		}
		logsJSON = string(encodedLogs)
	}

	txHash := strings.ToUpper(strings.TrimSpace(tx.TxHash))
	if txHash == "" {
		return model.Tx{}, nil, nil, fmt.Errorf("missing txhash")
	}

	modelTx := model.Tx{
		Height:         height,
		IndexInBlock:   index,
		BlockTime:      blockTime,
		TxHash:         txHash,
		TxBytes:        "",
		Codespace:      fcdCodespace(tx),
		Code:           tx.Code,
		TxResponseData: "",
		TxResponseInfo: "",
		GasWanted:      gasWanted,
		GasUsed:        gasUsed,
		FeeAmounts:     feeAmounts,
		FeeDenomIDs:    feeDenomIDs,
		TaxAmounts:     taxAmounts,
		TaxDenomIDs:    taxDenomIDs,
		MsgTypeIDs:     msgTypeIDs,
		MsgsJSON:       msgsJSON,
		SignaturesJSON: signaturesJSON,
		Memo:           tx.Tx.Value.Memo,
		RawLog:         tx.RawLog,
		LogsJSON:       logsJSON,
	}

	abciEvents := fcdEventsToABCI(tx.Events)
	modelEvents := make([]model.Event, 0)
	for i, event := range abciEvents {
		for _, attr := range event.Attributes {
			modelEvents = append(modelEvents, model.Event{
				Height:     height,
				BlockTime:  blockTime,
				Scope:      "tx",
				TxIndex:    int16(index),
				EventIndex: uint16(i),
				EventType:  event.Type,
				AttrKey:    string(attr.Key),
				AttrValue:  string(attr.Value),
				AttrIndex:  false,
				TxHash:     txHash,
			})
		}
	}

	accountTxs, _, err := s.extractAccountTxs(context.Background(), height, index, blockTime, txHash, abciEvents)
	if err != nil {
		return model.Tx{}, nil, nil, fmt.Errorf("extract account txs: %w", err)
	}

	return modelTx, modelEvents, accountTxs, nil
}

func (s *Service) fcdCoinsToModel(ctx context.Context, coins []fcdCoin) ([]int64, []uint16, error) {
	amounts := make([]int64, 0, len(coins))
	denomIDs := make([]uint16, 0, len(coins))
	for _, coin := range coins {
		if coin.Denom == "" || coin.Amount == "" {
			continue
		}
		amount, err := strconv.ParseInt(coin.Amount, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("parse amount %q: %w", coin.Amount, err)
		}
		denomID, err := s.dims.GetOrCreateDenomID(ctx, coin.Denom)
		if err != nil {
			return nil, nil, fmt.Errorf("get denom id for %s: %w", coin.Denom, err)
		}
		amounts = append(amounts, amount)
		denomIDs = append(denomIDs, denomID)
	}
	return amounts, denomIDs, nil
}

func (s *Service) fcdTaxToModel(ctx context.Context, tax string) ([]int64, []uint16, error) {
	tax = strings.TrimSpace(tax)
	if tax == "" {
		return nil, nil, nil
	}

	parts := strings.Split(tax, ",")
	coins := make([]fcdCoin, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		split := strings.IndexFunc(part, func(r rune) bool {
			return r < '0' || r > '9'
		})
		if split <= 0 || split >= len(part) {
			return nil, nil, fmt.Errorf("invalid tax coin %q", part)
		}
		coins = append(coins, fcdCoin{
			Amount: part[:split],
			Denom:  part[split:],
		})
	}
	return s.fcdCoinsToModel(ctx, coins)
}

func fcdEventsToABCI(events []fcdEvent) []abcitypes.Event {
	result := make([]abcitypes.Event, 0, len(events))
	for _, event := range events {
		attrs := make([]abcitypes.EventAttribute, 0, len(event.Attributes))
		for _, attr := range event.Attributes {
			attrs = append(attrs, abcitypes.EventAttribute{
				Key:   attr.Key,
				Value: attr.Value,
				Index: false,
			})
		}
		result = append(result, abcitypes.Event{Type: event.Type, Attributes: attrs})
	}
	return result
}

func parseUint64String(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func fcdCodespace(tx fcdTxResult) string {
	if tx.Codespace != "" {
		return tx.Codespace
	}
	for _, entry := range tx.Logs {
		var payload map[string]any
		if err := json.Unmarshal(entry.Log, &payload); err == nil {
			if codespace, ok := payload["codespace"].(string); ok {
				return codespace
			}
		}

		var nested string
		if err := json.Unmarshal(entry.Log, &nested); err == nil && nested != "" {
			if err := json.Unmarshal([]byte(nested), &payload); err == nil {
				if codespace, ok := payload["codespace"].(string); ok {
					return codespace
				}
			}
		}
	}
	return ""
}