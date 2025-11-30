package api

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/model"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/gorilla/mux"
)

func (s *Server) GetTxs(w http.ResponseWriter, r *http.Request) {
	queryParams := r.URL.Query()
	account := strings.TrimSpace(queryParams.Get("account"))
	block := queryParams.Get("block")
	limitStr := queryParams.Get("limit")
	offsetStr := queryParams.Get("offset")

	limit := 10
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	if limit > 100 {
		limit = 100
	}

	offset := uint64(0)
	if offsetStr != "" {
		if o, err := strconv.ParseUint(offsetStr, 10, 64); err == nil {
			offset = o
		}
	}

	var txs []model.Tx
	var err error

	// Fetch Denoms and MsgTypes early for use in both tx and block event responses
	denoms, _ := s.getDenoms(context.Background())
	if denoms == nil {
		denoms = make(map[uint16]string)
	}
	msgTypes, _ := s.getMsgTypes(context.Background())
	if msgTypes == nil {
		msgTypes = make(map[uint16]string)
	}

	if account != "" {
		// Get address ID
		var addressID uint64
		err = s.pg.Pool.QueryRow(context.Background(), "SELECT id FROM addresses WHERE address = $1", account).Scan(&addressID)
		if err != nil {
			// Address not found, return empty list
			respondJSON(w, http.StatusOK, map[string]interface{}{
				"txs":   []interface{}{},
				"limit": limit,
				"next":  0,
			})
			return
		}

		// Query account_txs to get all activity (both txs and block events)
		var args []interface{}
		whereClause := "address_id = ?"
		args = append(args, addressID)

		if offset > 0 {
			height := offset / 100000
			index := offset % 100000
			whereClause += " AND (height < ? OR (height = ? AND index_in_block < ?))"
			args = append(args, height, height, index)
		}

		sql := fmt.Sprintf(`
			SELECT address_id, height, index_in_block, block_time, tx_hash, direction, main_denom_id, main_amount, is_block_event, event_scope
			FROM account_txs
			WHERE %s
			ORDER BY height DESC, index_in_block DESC
			LIMIT ?
		`, whereClause)
		args = append(args, limit)

		var accountTxs []model.AccountTx
		err = s.ch.Conn.Select(context.Background(), &accountTxs, sql, args...)
		if err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch activity: %v", err))
			return
		}

		// Separate into tx entries and block event entries
		var txHeightIndexPairs []string
		var blockEventEntries []model.AccountTx

		for _, at := range accountTxs {
			if at.IsBlockEvent {
				// This is a block event
				blockEventEntries = append(blockEventEntries, at)
			} else {
				// This is a transaction
				txHeightIndexPairs = append(txHeightIndexPairs, fmt.Sprintf("(%d, %d)", at.Height, at.IndexInBlock))
			}
		}

		// Fetch actual transaction data for tx entries
		if len(txHeightIndexPairs) > 0 {
			txSQL := fmt.Sprintf(`
				SELECT * FROM txs 
				WHERE (height, index_in_block) IN (%s)
				ORDER BY height DESC, index_in_block DESC
			`, strings.Join(txHeightIndexPairs, ","))
			err = s.ch.Conn.Select(context.Background(), &txs, txSQL)
			if err != nil {
				respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch txs: %v", err))
				return
			}
		}

		// Build unified response
		var responses []interface{}

		// Create a map of txs by height+index for quick lookup
		txMap := make(map[string]model.Tx)
		for _, tx := range txs {
			key := fmt.Sprintf("%d-%d", tx.Height, tx.IndexInBlock)
			txMap[key] = tx
		}

		// Process in order of accountTxs (which is already sorted)
		for _, at := range accountTxs {
			if at.IsBlockEvent {
				// Block event - create special response
				eventType := "begin_block"
				if at.EventScope == model.EventScopeEndBlock {
					eventType = "end_block"
				}
				responses = append(responses, map[string]interface{}{
					"id":        at.Height*100000 + uint64(at.IndexInBlock),
					"type":      "block_event",
					"eventType": eventType,
					"height":    strconv.FormatUint(at.Height, 10),
					"timestamp": at.BlockTime.UTC().Format("2006-01-02T15:04:05Z"),
				})
			} else {
				// Transaction - use FCD format
				key := fmt.Sprintf("%d-%d", at.Height, at.IndexInBlock)
				if tx, ok := txMap[key]; ok {
					responses = append(responses, s.MapTxToFCD(tx, denoms, msgTypes))
				}
			}
		}

		var next uint64
		if len(accountTxs) > 0 {
			last := accountTxs[len(accountTxs)-1]
			next = last.Height*100000 + uint64(last.IndexInBlock)
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"txs":   responses,
			"limit": limit,
			"next":  next,
		})
		return

	} else if block != "" {
		height, err := strconv.ParseUint(block, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "Invalid block height")
			return
		}

		var args []interface{}
		whereClause := "height = ?"
		args = append(args, height)

		if offset > 0 {
			oHeight := offset / 100000
			oIndex := offset % 100000
			if oHeight == height {
				whereClause += " AND index_in_block > ?"
				args = append(args, oIndex)
			}
		}

		sql := fmt.Sprintf(`
			SELECT * FROM txs 
			WHERE %s
			ORDER BY index_in_block ASC 
			LIMIT ?
		`, whereClause)
		args = append(args, limit)

		err = s.ch.Conn.Select(context.Background(), &txs, sql, args...)

	} else {
		var args []interface{}
		whereClause := "1=1"
		if offset > 0 {
			height := offset / 100000
			index := offset % 100000
			whereClause = "(height < ? OR (height = ? AND index_in_block < ?))"
			args = append(args, height, height, index)
		}

		sql := fmt.Sprintf(`
			SELECT * FROM txs 
			WHERE %s
			ORDER BY height DESC, index_in_block DESC 
			LIMIT ?
		`, whereClause)
		args = append(args, limit)

		err = s.ch.Conn.Select(context.Background(), &txs, sql, args...)
	}

	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch txs: %v", err))
		return
	}

	// Map to FCD format
	var fcdTxs []FCDTxResponse
	for _, tx := range txs {
		fcdTxs = append(fcdTxs, s.MapTxToFCD(tx, denoms, msgTypes))
	}

	var next uint64
	if len(fcdTxs) > 0 {
		next = fcdTxs[len(fcdTxs)-1].ID
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"txs":   fcdTxs,
		"limit": limit,
		"next":  next,
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
	// tx_hash is FixedString(64) (hex string)
	err := s.ch.Conn.QueryRow(context.Background(), "SELECT * FROM txs WHERE tx_hash = ?", strings.ToUpper(hash)).ScanStruct(&tx)
	if err != nil {
		respondError(w, http.StatusNotFound, "Transaction not found")
		return
	}

	// Fetch Denoms
	denoms, err := s.getDenoms(context.Background())
	if err != nil {
		denoms = make(map[uint16]string)
	}

	// Fetch MsgTypes
	msgTypes, err := s.getMsgTypes(context.Background())
	if err != nil {
		msgTypes = make(map[uint16]string)
	}

	respondJSON(w, http.StatusOK, s.MapTxToFCD(tx, denoms, msgTypes))
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

func (s *Server) getMsgTypes(ctx context.Context) (map[uint16]string, error) {
	// TODO: Cache this
	rows, err := s.pg.Pool.Query(ctx, "SELECT id, msg_type FROM msg_types")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	msgTypes := make(map[uint16]string)
	for rows.Next() {
		var id uint16
		var msgType string
		if err := rows.Scan(&id, &msgType); err == nil {
			msgTypes[id] = msgType
		}
	}
	return msgTypes, nil
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

// GetBlockEvents returns block events for a specific height, optionally filtered by account
// GET /v1/blocks/{height}/events?account=terra1...&scope=begin_block|end_block
func (s *Server) GetBlockEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr := vars["height"]
	height, err := strconv.ParseUint(heightStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid height")
		return
	}

	queryParams := r.URL.Query()
	account := strings.TrimSpace(queryParams.Get("account"))
	scope := strings.TrimSpace(queryParams.Get("scope")) // "begin_block" or "end_block"

	// Build scope filter
	var scopeFilter string
	if scope == "begin_block" {
		scopeFilter = "AND scope = 'begin_block'"
	} else if scope == "end_block" {
		scopeFilter = "AND scope = 'end_block'"
	} else {
		// Include both begin_block and end_block (and legacy 'block')
		scopeFilter = "AND scope IN ('begin_block', 'end_block', 'block')"
	}

	// Query events
	sql := fmt.Sprintf(`SELECT * FROM events WHERE height = ? %s ORDER BY scope ASC, event_index ASC`, scopeFilter)
	var dbEvents []model.Event
	err = s.ch.Conn.Select(context.Background(), &dbEvents, sql, height)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch events: %v", err))
		return
	}

	// If account filter is provided, filter events to only those involving this address
	if account != "" {
		var filteredEvents []model.Event
		var relevantEventIndices = make(map[string]bool) // scope+event_index -> bool

		// First pass: find events that involve this account
		for _, ev := range dbEvents {
			if ev.AttrValue == account {
				key := fmt.Sprintf("%s-%d", ev.Scope, ev.EventIndex)
				relevantEventIndices[key] = true
			}
		}

		// Second pass: include all attributes of relevant events
		for _, ev := range dbEvents {
			key := fmt.Sprintf("%s-%d", ev.Scope, ev.EventIndex)
			if relevantEventIndices[key] {
				filteredEvents = append(filteredEvents, ev)
			}
		}

		dbEvents = filteredEvents
	}

	// Group events by (scope, event_index)
	var events []map[string]interface{}
	var currentEvent map[string]interface{}
	var currentAttrs []map[string]string

	lastScope := ""
	lastEventIndex := uint16(65535)

	for _, row := range dbEvents {
		isNewEvent := row.Scope != lastScope || row.EventIndex != lastEventIndex

		if isNewEvent {
			if currentEvent != nil {
				currentEvent["attributes"] = currentAttrs
				events = append(events, currentEvent)
			}

			// Normalize scope name
			scopeName := row.Scope
			if scopeName == "block" {
				scopeName = "begin_block"
			}

			currentEvent = map[string]interface{}{
				"type":  row.EventType,
				"scope": scopeName,
			}
			currentAttrs = []map[string]string{}

			lastScope = row.Scope
			lastEventIndex = row.EventIndex
		}

		currentAttrs = append(currentAttrs, map[string]string{
			"key":   row.AttrKey,
			"value": row.AttrValue,
		})
	}
	// Append last event
	if currentEvent != nil {
		currentEvent["attributes"] = currentAttrs
		events = append(events, currentEvent)
	}

	if events == nil {
		events = []map[string]interface{}{}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"height": height,
		"events": events,
	})
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

	// Fetch MsgTypes
	msgTypes, _ := s.getMsgTypes(context.Background())
	if msgTypes == nil {
		msgTypes = make(map[uint16]string)
	}

	// Map Txs
	var fcdTxs []FCDTxResponse
	for _, tx := range txs {
		fcdTxs = append(fcdTxs, s.MapTxToFCD(tx, denoms, msgTypes))
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
		"timestamp": block.BlockTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		"proposer":  proposer,
		"txs":       fcdTxs,
		"events":    blockEvents,
	}

	respondJSON(w, http.StatusOK, response)
}
