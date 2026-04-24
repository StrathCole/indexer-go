package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// Mantlemint-compatible response types
// ---------------------------------------------------------------------------

// mmBlockRecord mirrors mantlemint's BlockRecord: {block_id, block}.
// Fields are json.RawMessage so we forward the CometBFT JSON verbatim.
type mmBlockRecord struct {
	BlockID json.RawMessage `json:"block_id"`
	Block   json.RawMessage `json:"block"`
}

// mmTxRecord mirrors mantlemint's TxRecord (by-hash response).
type mmTxRecord struct {
	Tx         json.RawMessage `json:"tx"`
	TxResponse json.RawMessage `json:"tx_response"`
}

// mmTxByHeightRecord mirrors mantlemint's TxByHeightRecord.
type mmTxByHeightRecord struct {
	Code      uint32          `json:"code"`
	Codespace string          `json:"codespace"`
	GasUsed   int64           `json:"gas_used"`
	GasWanted int64           `json:"gas_wanted"`
	Height    int64           `json:"height"`
	RawLog    string          `json:"raw_log"`
	Logs      json.RawMessage `json:"logs"`
	TxHash    string          `json:"txhash"`
	Timestamp time.Time       `json:"timestamp"`
	Tx        json.RawMessage `json:"tx"`
}

// mmResponseDeliverTx mirrors mantlemint's custom ResponseDeliverTx.
type mmResponseDeliverTx struct {
	Code      uint32    `json:"code"`
	Data      []byte    `json:"data,omitempty"`
	Log       string    `json:"log,omitempty"`
	Info      string    `json:"info,omitempty"`
	GasWanted int64     `json:"gas_wanted,omitempty"`
	GasUsed   int64     `json:"gas_used,omitempty"`
	Events    []mmEvent `json:"events,omitempty"`
	Codespace string    `json:"codespace,omitempty"`
}

type mmEvent struct {
	Type       string             `json:"type,omitempty"`
	Attributes []mmEventAttribute `json:"attributes,omitempty"`
}

type mmEventAttribute struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// ---------------------------------------------------------------------------
// GET /index/blocks/{height}
// Returns the full raw block as {block_id, block} — fetched from upstream RPC.
// ---------------------------------------------------------------------------

func (s *Server) MantlemintGetBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr, ok := vars["height"]
	if !ok || heightStr == "" {
		http.Error(w, fmt.Sprintf("invalid height %s", heightStr), http.StatusBadRequest)
		return
	}

	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid height %s", heightStr), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	block, err := s.rpc.Block(ctx, &height)
	if err != nil {
		http.Error(w, fmt.Sprintf("block %s not found... yet.", heightStr), http.StatusBadRequest)
		return
	}

	// Marshal block and block_id to JSON using CometBFT's JSON encoder (Amino-compatible)
	blockJSON, err := json.Marshal(block.Block)
	if err != nil {
		http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
		return
	}

	blockIDJSON, err := json.Marshal(block.BlockID)
	if err != nil {
		http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
		return
	}

	record := mmBlockRecord{
		BlockID: blockIDJSON,
		Block:   blockJSON,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(record)
}

// ---------------------------------------------------------------------------
// GET /index/tx/by_hash/{hash}
// Returns {tx, tx_response} — the Cosmos SDK Tx JSON + ABCI deliver result.
// Reconstructs from upstream RPC (block + block_results) by scanning blocks.
//
// Since we have the tx hash → height mapping in ClickHouse, we can resolve
// the block height first, then fetch from RPC.
// ---------------------------------------------------------------------------

func (s *Server) MantlemintGetTxByHash(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash, ok := vars["hash"]
	if !ok || hash == "" {
		http.Error(w, fmt.Sprintf("invalid hash %s", hash), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Look up the tx height + index from ClickHouse
	var height uint64
	var indexInBlock uint16
	err := s.ch.Conn.QueryRow(ctx,
		"SELECT height, index_in_block FROM txs WHERE tx_hash = ? LIMIT 1",
		hash,
	).Scan(&height, &indexInBlock)
	if err != nil {
		http.Error(w, fmt.Sprintf("tx (%s) not found... yet or forever.", hash), http.StatusBadRequest)
		return
	}

	// Fetch block and results from RPC
	h := int64(height)
	block, err := s.rpc.Block(ctx, &h)
	if err != nil {
		log.Error().Err(err).Uint64("height", height).Msg("mantlemint: failed to fetch block for tx")
		http.Error(w, fmt.Sprintf("internal error fetching block %d", height), http.StatusInternalServerError)
		return
	}

	results, err := s.rpc.BlockResults(ctx, &h)
	if err != nil {
		log.Error().Err(err).Uint64("height", height).Msg("mantlemint: failed to fetch block results for tx")
		http.Error(w, fmt.Sprintf("internal error fetching block results %d", height), http.StatusInternalServerError)
		return
	}

	if int(indexInBlock) >= len(block.Block.Txs) {
		http.Error(w, fmt.Sprintf("tx (%s) not found... yet or forever.", hash), http.StatusBadRequest)
		return
	}

	// Decode and re-encode the tx to JSON (matching mantlemint's encoding path)
	txBytes := block.Block.Txs[indexInBlock]
	txDecoder := s.clientCtx.TxConfig.TxDecoder()
	txJSONEncoder := s.clientCtx.TxConfig.TxJSONEncoder()

	decodedTx, err := txDecoder(txBytes)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to decode tx: %v", err), http.StatusInternalServerError)
		return
	}

	txJSON, err := txJSONEncoder(decodedTx)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to encode tx: %v", err), http.StatusInternalServerError)
		return
	}

	// Build the tx_response matching mantlemint's ResponseDeliverTx format
	if int(indexInBlock) >= len(results.TxsResults) {
		http.Error(w, fmt.Sprintf("tx result at index %d not found", indexInBlock), http.StatusInternalServerError)
		return
	}
	txResult := results.TxsResults[indexInBlock]

	response := mmResponseDeliverTx{
		Code:      txResult.Code,
		Data:      txResult.Data,
		Log:       txResult.Log,
		Info:      txResult.Info,
		GasWanted: txResult.GasWanted,
		GasUsed:   txResult.GasUsed,
		Codespace: txResult.Codespace,
		Events:    make([]mmEvent, 0, len(txResult.Events)),
	}
	for _, event := range txResult.Events {
		e := mmEvent{Type: event.Type, Attributes: make([]mmEventAttribute, 0, len(event.Attributes))}
		for _, attr := range event.Attributes {
			e.Attributes = append(e.Attributes, mmEventAttribute{
				Key:   attr.Key,
				Value: attr.Value,
			})
		}
		response.Events = append(response.Events, e)
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to encode tx_response: %v", err), http.StatusInternalServerError)
		return
	}

	record := mmTxRecord{
		Tx:         txJSON,
		TxResponse: responseJSON,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(record)
}

// ---------------------------------------------------------------------------
// GET /index/tx/by_height/{height}
// Returns []TxByHeightRecord for all transactions in a block.
// ---------------------------------------------------------------------------

func (s *Server) MantlemintGetTxsByHeight(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr, ok := vars["height"]
	if !ok || heightStr == "" {
		http.Error(w, fmt.Sprintf("invalid height %s", heightStr), http.StatusBadRequest)
		return
	}

	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid height %s", heightStr), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	block, err := s.rpc.Block(ctx, &height)
	if err != nil {
		http.Error(w, fmt.Sprintf("txs at height %s not found... yet.", heightStr), http.StatusBadRequest)
		return
	}

	results, err := s.rpc.BlockResults(ctx, &height)
	if err != nil {
		http.Error(w, fmt.Sprintf("txs at height %s not found... yet.", heightStr), http.StatusBadRequest)
		return
	}

	txDecoder := s.clientCtx.TxConfig.TxDecoder()
	txJSONEncoder := s.clientCtx.TxConfig.TxJSONEncoder()

	payload := make([]mmTxByHeightRecord, 0, len(block.Block.Txs))

	for i, txBytes := range block.Block.Txs {
		if i >= len(results.TxsResults) {
			break
		}
		txResult := results.TxsResults[i]

		decodedTx, err := txDecoder(txBytes)
		if err != nil {
			log.Error().Err(err).Int64("height", height).Int("index", i).Msg("mantlemint: failed to decode tx")
			continue
		}

		txJSON, err := txJSONEncoder(decodedTx)
		if err != nil {
			log.Error().Err(err).Int64("height", height).Int("index", i).Msg("mantlemint: failed to encode tx")
			continue
		}

		txHash := fmt.Sprintf("%X", block.Block.Txs[i].Hash())

		var logsJSON json.RawMessage
		if txResult.Code == 0 {
			logsJSON = json.RawMessage(txResult.Log)
		} else {
			logsJSON = json.RawMessage(`[]`)
		}

		payload = append(payload, mmTxByHeightRecord{
			Code:      txResult.Code,
			Codespace: txResult.Codespace,
			GasUsed:   txResult.GasUsed,
			GasWanted: txResult.GasWanted,
			Height:    height,
			RawLog:    txResult.Log,
			Logs:      logsJSON,
			TxHash:    txHash,
			Timestamp: block.Block.Time,
			Tx:        txJSON,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(payload)
}

// ---------------------------------------------------------------------------
// GET /health
// Returns 200 "OK" when indexed state is caught up, otherwise 503 "NOK".
// ---------------------------------------------------------------------------

func (s *Server) MantlemintHealth(w http.ResponseWriter, r *http.Request) {
	synced, _, _ := s.currentRuntimeSync()
	if synced {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("NOK"))
}
