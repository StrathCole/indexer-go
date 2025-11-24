package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/core/v3/app"
	oracletypes "github.com/classic-terra/core/v3/x/oracle/types"
	"github.com/classic-terra/indexer-go/internal/model"
	tmtypes "github.com/cometbft/cometbft/types"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/gorilla/mux"
)

func (s *Server) GetTxs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	account := query.Get("account")
	block := query.Get("block")
	limitStr := query.Get("limit")
	offsetStr := query.Get("offset")

	limit := 10
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	if limit > 100 {
		limit = 100
	}

	offset := 0
	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil {
			offset = o
		}
	}

	var txs []model.Tx
	var err error

	if account != "" {
		// Get address ID
		var addressID uint64
		err = s.pg.Pool.QueryRow(context.Background(), "SELECT id FROM addresses WHERE address = $1", account).Scan(&addressID)
		if err != nil {
			// Address not found, return empty list
			respondJSON(w, http.StatusOK, map[string]interface{}{
				"txs": []model.Tx{},
			})
			return
		}

		// Query account_txs
		// We need to join with txs to get full details.
		// ClickHouse SQL:
		// SELECT t.* FROM txs t
		// INNER JOIN account_txs a ON t.height = a.height AND t.index_in_block = a.index_in_block
		// WHERE a.address_id = ?
		// ORDER BY t.height DESC, t.index_in_block DESC
		// LIMIT ? OFFSET ?

		sql := `
			SELECT t.* 
			FROM txs t
			INNER JOIN account_txs a ON t.height = a.height AND t.index_in_block = a.index_in_block AND t.tx_hash = a.tx_hash
			WHERE a.address_id = ?
			ORDER BY t.height DESC, t.index_in_block DESC
			LIMIT ? OFFSET ?
		`
		err = s.ch.Conn.Select(context.Background(), &txs, sql, addressID, limit, offset)

	} else if block != "" {
		height, err := strconv.ParseUint(block, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid block height")
			return
		}

		sql := `
			SELECT * FROM txs 
			WHERE height = ? 
			ORDER BY index_in_block ASC 
			LIMIT ? OFFSET ?
		`
		err = s.ch.Conn.Select(context.Background(), &txs, sql, height, limit, offset)

	} else {
		sql := `
			SELECT * FROM txs 
			ORDER BY height DESC, index_in_block DESC 
			LIMIT ? OFFSET ?
		`
		err = s.ch.Conn.Select(context.Background(), &txs, sql, limit, offset)
	}

	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch txs: %v", err))
		return
	}

	// Fetch Denoms
	denoms, err := s.getDenoms(context.Background())
	if err != nil {
		// Log error but continue with unknown denoms?
		// For now, just empty map
		denoms = make(map[uint16]string)
	}

	// Map to FCD format
	var fcdTxs []FCDTxResponse
	for _, tx := range txs {
		fcdTxs = append(fcdTxs, s.MapTxToFCD(tx, denoms))
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"txs":   fcdTxs,
		"limit": limit,
		"next":  offset + limit, // Simple pagination
	})
}

func (s *Server) GetTx(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash := vars["hash"]

	// Validate hash
	if len(hash) != 64 {
		respondError(w, http.StatusBadRequest, "Invalid hash format")
		return
	}

	var tx model.Tx
	// Use unhex(?) for FixedString(32)
	err := s.ch.Conn.QueryRow(context.Background(), "SELECT * FROM txs WHERE tx_hash = unhex(?)", hash).ScanStruct(&tx)
	if err != nil {
		respondError(w, http.StatusNotFound, "Transaction not found")
		return
	}

	// Fetch Denoms
	denoms, err := s.getDenoms(context.Background())
	if err != nil {
		denoms = make(map[uint16]string)
	}

	respondJSON(w, http.StatusOK, s.MapTxToFCD(tx, denoms))
}

func (s *Server) getDenoms(ctx context.Context) (map[uint16]string, error) {
	// TODO: Cache this
	rows, err := s.pg.Pool.Query(ctx, "SELECT id, denom FROM denoms")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	denoms := make(map[uint16]string)
	for rows.Next() {
		var id uint16
		var denom string
		if err := rows.Scan(&id, &denom); err == nil {
			denoms[id] = denom
		}
	}
	return denoms, nil
}

func (s *Server) getValidatorMap(ctx context.Context) (map[string]map[string]string, error) {
	if val, found := s.cache.Get("validator_map"); found {
		return val.(map[string]map[string]string), nil
	}

	stakingClient := stakingtypes.NewQueryClient(s.clientCtx)
	var validators []stakingtypes.Validator
	var nextKey []byte

	for {
		res, err := stakingClient.Validators(ctx, &stakingtypes.QueryValidatorsRequest{
			Pagination: &query.PageRequest{Key: nextKey, Limit: 100},
		})
		if err != nil {
			return nil, err
		}
		validators = append(validators, res.Validators...)
		if res.Pagination == nil || len(res.Pagination.NextKey) == 0 {
			break
		}
		nextKey = res.Pagination.NextKey
	}

	valMap := make(map[string]map[string]string)
	for _, val := range validators {
		var pubKey cryptotypes.PubKey
		if err := s.clientCtx.InterfaceRegistry.UnpackAny(val.ConsensusPubkey, &pubKey); err != nil {
			continue
		}
		consAddr := sdk.ConsAddress(pubKey.Address()).String()

		info := map[string]string{
			"operatorAddress": val.OperatorAddress,
			"moniker":         val.Description.Moniker,
			"identity":        val.Description.Identity,
		}

		valMap[consAddr] = info

		// Add hex address support (both upper and lower case)
		hexAddr := hex.EncodeToString(pubKey.Address())
		valMap[strings.ToUpper(hexAddr)] = info
		valMap[strings.ToLower(hexAddr)] = info
	}

	s.cache.Set("validator_map", valMap, 5*time.Minute)
	return valMap, nil
}

func (s *Server) GetBlockLatest(w http.ResponseWriter, r *http.Request) {
	var block model.Block
	err := s.ch.Conn.QueryRow(context.Background(), "SELECT * FROM blocks ORDER BY height DESC LIMIT 1").ScanStruct(&block)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get latest block: %v", err))
		return
	}
	s.respondBlock(w, block)
}

func (s *Server) GetBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr := vars["height"]
	height, err := strconv.ParseUint(heightStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid height")
		return
	}

	var block model.Block
	err = s.ch.Conn.QueryRow(context.Background(), "SELECT * FROM blocks WHERE height = ?", height).ScanStruct(&block)
	if err != nil {
		respondError(w, http.StatusNotFound, "Block not found")
		return
	}
	s.respondBlock(w, block)
}

func (s *Server) respondBlock(w http.ResponseWriter, block model.Block) {
	// Fetch Txs for this block
	var txs []model.Tx
	sql := `SELECT * FROM txs WHERE height = ? ORDER BY index_in_block ASC`
	err := s.ch.Conn.Select(context.Background(), &txs, sql, block.Height)
	if err != nil {
		// Log error, but continue with empty txs
		txs = []model.Tx{}
	}

	// Fetch Denoms
	denoms, _ := s.getDenoms(context.Background())
	if denoms == nil {
		denoms = make(map[uint16]string)
	}

	// Map Txs
	var fcdTxs []FCDTxResponse
	for _, tx := range txs {
		fcdTxs = append(fcdTxs, s.MapTxToFCD(tx, denoms))
	}
	if fcdTxs == nil {
		fcdTxs = []FCDTxResponse{}
	}

	// Fetch Proposer Info (Validator)
	valMap, err := s.getValidatorMap(context.Background())
	if err != nil {
		// Log error
		fmt.Printf("Failed to get validator map: %v\n", err)
	}
	var proposer map[string]string
	if valMap != nil {
		if p, ok := valMap[block.ProposerAddress]; ok {
			proposer = p
		}
	}

	if proposer == nil {
		proposer = map[string]string{
			"operatorAddress": block.ProposerAddress,
		}
	}

	// Fetch Block Events (BeginBlock and EndBlock only)
	var dbEvents []model.Event
	// Fetch events for the block, excluding tx events
	// Order by scope to ensure begin_block (2) comes before end_block (3). 'block' (0) is legacy and comes first.
	sqlEvents := `SELECT * FROM events WHERE height = ? AND scope != 'tx' ORDER BY scope ASC, event_index ASC`
	err = s.ch.Conn.Select(context.Background(), &dbEvents, sqlEvents, block.Height)

	var blockEvents []map[string]interface{}
	if err == nil && len(dbEvents) > 0 {
		// Group by (Scope, TxIndex, EventIndex)
		var currentEvent map[string]interface{}
		var currentAttrs []map[string]string

		// Helper to check if event changed
		lastScope := ""
		lastTxIndex := int16(-999)
		lastEventIndex := uint16(65535)

		for _, row := range dbEvents {
			isNewEvent := false
			if row.Scope != lastScope || row.TxIndex != lastTxIndex || row.EventIndex != lastEventIndex {
				isNewEvent = true
			}

			if isNewEvent {
				if currentEvent != nil {
					currentEvent["attributes"] = currentAttrs
					blockEvents = append(blockEvents, currentEvent)
				}

				currentEvent = map[string]interface{}{
					"type":  row.EventType,
					"stage": row.Scope,
				}
				// Clean up TxHash (remove null bytes)
				txHash := strings.Trim(row.TxHash, "\x00")
				if txHash != "" {
					currentEvent["tx_hash"] = txHash
				}
				currentAttrs = []map[string]string{}

				lastScope = row.Scope
				lastTxIndex = row.TxIndex
				lastEventIndex = row.EventIndex
			}

			currentAttrs = append(currentAttrs, map[string]string{
				"key":   row.AttrKey,
				"value": row.AttrValue,
			})
		}
		// Append last one
		if currentEvent != nil {
			currentEvent["attributes"] = currentAttrs
			blockEvents = append(blockEvents, currentEvent)
		}
	} else {
		blockEvents = []map[string]interface{}{}
	}

	response := map[string]interface{}{
		"chainId":   "columbus-5", // Hardcoded
		"height":    block.Height,
		"timestamp": block.BlockTime.Format(time.RFC3339),
		"proposer":  proposer,
		"txs":       fcdTxs,
		"events":    blockEvents,
	}

	respondJSON(w, http.StatusOK, response)
}

func (s *Server) GetGasPrices(w http.ResponseWriter, r *http.Request) {
	// Fetch oracle exchange rates
	oracleClient := oracletypes.NewQueryClient(s.clientCtx)
	resp, err := oracleClient.ExchangeRates(context.Background(), &oracletypes.QueryExchangeRatesRequest{})
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to fetch gas prices")
		return
	}

	// Convert to map[string]string
	gasPrices := make(map[string]string)
	for _, rate := range resp.ExchangeRates {
		gasPrices[rate.Denom] = rate.Amount.String()
	}

	// Add native denom (uluna)
	// FCD returns uluna price? Usually gas prices are defined in params or config.
	// FCD uses config.MIN_GAS_PRICES.
	// We should probably use a config value too.
	// For now, hardcode or use what we have.
	if _, ok := gasPrices["uluna"]; !ok {
		gasPrices["uluna"] = "0.01133"
	}

	respondJSON(w, http.StatusOK, gasPrices)
}

func (s *Server) GetMempool(w http.ResponseWriter, r *http.Request) {
	limit := 100
	res, err := s.rpc.UnconfirmedTxs(context.Background(), &limit)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to fetch mempool")
		return
	}

	txDecoder := app.MakeEncodingConfig().TxConfig.TxDecoder()
	txEncoder := app.MakeEncodingConfig().TxConfig.TxJSONEncoder()

	var decodedTxs []interface{}
	for _, txBytes := range res.Txs {
		tx, err := txDecoder(txBytes)
		if err == nil {
			// Marshal to JSON
			jsonBytes, err := txEncoder(tx)
			if err == nil {
				// We need to wrap it in FCD structure: { timestamp, txhash, tx: { type, value... } }
				// But txEncoder returns just the tx value usually?
				// No, it returns the standard Cosmos JSON tx.
				// FCD expects:
				// {
				//   "timestamp": "...",
				//   "txhash": "...",
				//   "tx": { ... }
				// }

				var txObj interface{}
				json.Unmarshal(jsonBytes, &txObj)

				decodedTxs = append(decodedTxs, map[string]interface{}{
					"timestamp": time.Now().Format(time.RFC3339), // Approx
					"txhash":    fmt.Sprintf("%X", tmtypes.Tx(txBytes).Hash()),
					"tx":        txObj,
				})
			}
		}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"total": res.Total,
		"txs":   decodedTxs,
	})
}

func (s *Server) GetMempoolTx(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash := vars["hash"]

	// Fetch all (up to limit) and search
	limit := 1000
	res, err := s.rpc.UnconfirmedTxs(context.Background(), &limit)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to fetch mempool")
		return
	}

	txDecoder := app.MakeEncodingConfig().TxConfig.TxDecoder()
	txEncoder := app.MakeEncodingConfig().TxConfig.TxJSONEncoder()

	for _, txBytes := range res.Txs {
		// Calculate hash
		currentHash := fmt.Sprintf("%X", tmtypes.Tx(txBytes).Hash())
		if currentHash == hash {
			tx, err := txDecoder(txBytes)
			if err != nil {
				respondError(w, http.StatusInternalServerError, "Failed to decode tx")
				return
			}
			jsonBytes, err := txEncoder(tx)
			if err != nil {
				respondError(w, http.StatusInternalServerError, "Failed to marshal tx")
				return
			}

			var txObj interface{}
			json.Unmarshal(jsonBytes, &txObj)

			respondJSON(w, http.StatusOK, map[string]interface{}{
				"timestamp": time.Now().Format(time.RFC3339),
				"txhash":    currentHash,
				"tx":        txObj,
			})
			return
		}
	}

	respondError(w, http.StatusNotFound, "Transaction not found in mempool")
}
