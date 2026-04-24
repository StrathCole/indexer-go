package api

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/model"
	abcitypes "github.com/cometbft/cometbft/abci/types"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkquery "github.com/cosmos/cosmos-sdk/types/query"
	txtypes "github.com/cosmos/cosmos-sdk/types/tx"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	gateway "github.com/cosmos/gogogateway"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

var txSearchAndRE = regexp.MustCompile(`(?i)\s+AND\s+`)

type txSearchFilter struct {
	height          *uint64
	hash            string
	eventConditions []txEventCondition
}

type txEventCondition struct {
	eventType string
	attrKey   string
	attrValue string
}

type txLocation struct {
	Height       uint64
	IndexInBlock uint16
	TxHash       string
}

func (s *Server) GetCosmosTxByHash(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash := strings.ToUpper(strings.TrimSpace(vars["hash"]))
	if len(hash) != 64 {
		respondError(w, http.StatusBadRequest, "tx hash cannot be empty")
		return
	}
	if _, err := hex.DecodeString(hash); err != nil {
		respondError(w, http.StatusBadRequest, "invalid tx hash")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	loc, found, err := s.findTxLocationByHash(ctx, hash)
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Msg("local tx hash lookup failed; falling back to node query")
		s.fallbackCosmosTxByHash(w, hash)
		return
	}
	if !found {
		s.fallbackCosmosTxByHash(w, hash)
		return
	}

	rows, err := s.getTxRowsForLocations(ctx, []txLocation{loc})
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Msg("local tx row hydration failed; falling back to node query")
		s.fallbackCosmosTxByHash(w, hash)
		return
	}
	eventsByTx, err := s.getTxEventsForLocations(ctx, []txLocation{loc})
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Msg("local tx event hydration failed; falling back to node query")
		s.fallbackCosmosTxByHash(w, hash)
		return
	}

	key := txLocationKey(loc)
	row, ok := rows[key]
	if !ok {
		s.fallbackCosmosTxByHash(w, hash)
		return
	}

	protoTx, txResp, err := s.buildTxSearchEntry(row, eventsByTx[key])
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Msg("local tx response build failed; falling back to node query")
		s.fallbackCosmosTxByHash(w, hash)
		return
	}

	writeGatewayJSON(w, http.StatusOK, &txtypes.GetTxResponse{
		Tx:         protoTx,
		TxResponse: txResp,
	})
}

func (s *Server) GetCosmosTxsEvent(w http.ResponseWriter, r *http.Request) {
	requestQuery := strings.TrimSpace(r.URL.Query().Get("query"))
	events := r.URL.Query()["events"]
	if requestQuery == "" && len(events) == 0 {
		respondError(w, http.StatusBadRequest, "query cannot be empty")
		return
	}

	page := 1
	if pageStr := strings.TrimSpace(r.URL.Query().Get("page")); pageStr != "" {
		parsed, err := strconv.Atoi(pageStr)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid page")
			return
		}
		if parsed > 0 {
			page = parsed
		}
	}

	limit := int(sdkquery.DefaultLimit)
	if limitStr := strings.TrimSpace(r.URL.Query().Get("limit")); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if parsed > 0 {
			limit = parsed
		}
	}

	orderBy := parseOrderByParam(strings.TrimSpace(r.URL.Query().Get("order_by")))
	combinedQuery := joinTxSearchQuery(requestQuery, events)

	filter, supported, err := parseTxSearchFilter(requestQuery, events)
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	if !supported {
		s.fallbackCosmosTxSearch(w, combinedQuery, page, limit, orderBy)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	locations, total, err := s.searchTxLocations(ctx, filter, page, limit, orderBy)
	if err != nil {
		log.Warn().Err(err).Str("query", combinedQuery).Msg("local tx search failed; falling back to node search")
		s.fallbackCosmosTxSearch(w, combinedQuery, page, limit, orderBy)
		return
	}

	response, err := s.buildCosmosTxSearchResponse(ctx, locations, total)
	if err != nil {
		log.Warn().Err(err).Str("query", combinedQuery).Msg("local tx search hydration failed; falling back to node search")
		s.fallbackCosmosTxSearch(w, combinedQuery, page, limit, orderBy)
		return
	}

	writeGatewayJSON(w, http.StatusOK, response)
}

func parseOrderByParam(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "ORDER_BY_DESC", "DESC", "2":
		return "desc"
	case "ORDER_BY_ASC", "ASC", "1":
		return "asc"
	default:
		return ""
	}
}

func joinTxSearchQuery(query string, events []string) string {
	parts := make([]string, 0, 1+len(events))
	if strings.TrimSpace(query) != "" {
		parts = append(parts, strings.TrimSpace(query))
	}
	for _, event := range events {
		event = strings.TrimSpace(event)
		if event != "" {
			parts = append(parts, event)
		}
	}
	return strings.Join(parts, " AND ")
}

func parseTxSearchFilter(query string, events []string) (txSearchFilter, bool, error) {
	var filter txSearchFilter
	terms := make([]string, 0, len(events)+4)
	if strings.TrimSpace(query) != "" {
		if containsUnsupportedTxSearchSyntax(query) {
			return filter, false, nil
		}
		terms = append(terms, splitTxSearchTerms(query)...)
	}
	for _, event := range events {
		event = strings.TrimSpace(event)
		if event != "" {
			terms = append(terms, event)
		}
	}

	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		condition, special, supported, err := parseTxSearchTerm(term)
		if err != nil {
			return filter, false, err
		}
		if !supported {
			return filter, false, nil
		}
		if special != nil {
			if special.height != nil {
				if filter.height != nil && *filter.height != *special.height {
					filter.height = new(uint64)
					*filter.height = ^uint64(0)
					return filter, true, nil
				}
				filter.height = special.height
			}
			if special.hash != "" {
				if filter.hash != "" && filter.hash != special.hash {
					filter.hash = strings.Repeat("0", 64)
					return filter, true, nil
				}
				filter.hash = special.hash
			}
			continue
		}
		filter.eventConditions = append(filter.eventConditions, condition)
	}

	return filter, true, nil
}

func containsUnsupportedTxSearchSyntax(query string) bool {
	upper := strings.ToUpper(query)
	return strings.Contains(upper, " OR ") || strings.ContainsAny(query, "()><")
}

func splitTxSearchTerms(query string) []string {
	parts := txSearchAndRE.Split(strings.TrimSpace(query), -1)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseTxSearchTerm(term string) (txEventCondition, *txSearchFilter, bool, error) {
	var empty txEventCondition
	idx := strings.Index(term, "=")
	if idx <= 0 {
		return empty, nil, false, nil
	}

	key := strings.TrimSpace(term[:idx])
	value := normalizeTxSearchValue(term[idx+1:])
	if value == "" {
		return empty, nil, false, fmt.Errorf("invalid empty tx search value")
	}

	if strings.EqualFold(key, "tx.height") {
		height, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return empty, nil, false, fmt.Errorf("invalid tx.height value")
		}
		return empty, &txSearchFilter{height: &height}, true, nil
	}

	if strings.EqualFold(key, "tx.hash") {
		hash := strings.ToUpper(value)
		if len(hash) != 64 {
			return empty, nil, false, fmt.Errorf("invalid tx.hash value")
		}
		if _, err := hex.DecodeString(hash); err != nil {
			return empty, nil, false, fmt.Errorf("invalid tx.hash value")
		}
		return empty, &txSearchFilter{hash: hash}, true, nil
	}

	parts := strings.SplitN(key, ".", 2)
	if len(parts) != 2 {
		return empty, nil, false, nil
	}

	return txEventCondition{eventType: parts[0], attrKey: parts[1], attrValue: value}, nil, true, nil
}

func normalizeTxSearchValue(raw string) string {
	value := strings.TrimSpace(raw)
	if len(value) >= 2 {
		if (value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"') {
			value = value[1 : len(value)-1]
		}
	}
	return strings.TrimSpace(value)
}

func (s *Server) searchTxLocations(ctx context.Context, filter txSearchFilter, page int, limit int, orderBy string) ([]txLocation, uint64, error) {
	if filter.height != nil && *filter.height == ^uint64(0) {
		return []txLocation{}, 0, nil
	}
	if filter.hash == strings.Repeat("0", 64) {
		return []txLocation{}, 0, nil
	}

	locations, total, err := s.searchTxLocationsFromLookup(ctx, filter, page, limit, orderBy)
	if err == nil {
		return locations, total, nil
	}
	if !isMissingTableErr(err) {
		return nil, 0, err
	}
	return s.searchTxLocationsFromEvents(ctx, filter, page, limit, orderBy)
}

func (s *Server) searchTxLocationsFromLookup(ctx context.Context, filter txSearchFilter, page int, limit int, orderBy string) ([]txLocation, uint64, error) {
	return s.searchTxLocationsWithTable(ctx, filter, page, limit, orderBy, true)
}

func (s *Server) searchTxLocationsFromEvents(ctx context.Context, filter txSearchFilter, page int, limit int, orderBy string) ([]txLocation, uint64, error) {
	return s.searchTxLocationsWithTable(ctx, filter, page, limit, orderBy, false)
}

func (s *Server) searchTxLocationsWithTable(ctx context.Context, filter txSearchFilter, page int, limit int, orderBy string, useLookup bool) ([]txLocation, uint64, error) {
	if page <= 0 {
		page = 1
	}
	if limit <= 0 {
		limit = int(sdkquery.DefaultLimit)
	}
	offset := (page - 1) * limit

	fromClause, whereClause, args := buildTxSearchSQL(filter, useLookup)
	countSQL := "SELECT count() FROM (SELECT DISTINCT base.height, base.index_in_block " + fromClause + whereClause + ")"

	var total uint64
	if err := s.ch.Conn.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	orderClause := " ORDER BY base.height ASC, base.index_in_block ASC "
	if orderBy == "desc" {
		orderClause = " ORDER BY base.height DESC, base.index_in_block DESC "
	}

	pairsSQL := "SELECT DISTINCT base.height, base.index_in_block, base.tx_hash " + fromClause + whereClause + orderClause + " LIMIT ? OFFSET ?"
	argsWithPage := append(append([]interface{}{}, args...), limit, offset)
	var locations []txLocation
	if err := s.ch.Conn.Select(ctx, &locations, pairsSQL, argsWithPage...); err != nil {
		return nil, 0, err
	}
	return locations, total, nil
}

func buildTxSearchSQL(filter txSearchFilter, useLookup bool) (string, string, []interface{}) {
	fromClause := " FROM txs AS base"
	args := make([]interface{}, 0, len(filter.eventConditions)*3+2)
	for i, cond := range filter.eventConditions {
		alias := fmt.Sprintf("e%d", i)
		if useLookup {
			fromClause += fmt.Sprintf(" INNER JOIN tx_event_lookup AS %s ON %s.height = base.height AND %s.index_in_block = base.index_in_block AND %s.event_type = ? AND %s.attr_key = ? AND %s.attr_value = ?", alias, alias, alias, alias, alias, alias)
		} else {
			fromClause += fmt.Sprintf(" INNER JOIN events AS %s ON %s.height = base.height AND %s.scope = 'tx' AND %s.tx_index = toInt16(base.index_in_block) AND %s.event_type = ? AND %s.attr_key = ? AND %s.attr_value = ?", alias, alias, alias, alias, alias, alias, alias)
		}
		args = append(args, cond.eventType, cond.attrKey, cond.attrValue)
	}

	whereParts := []string{" WHERE 1=1"}
	if filter.height != nil {
		whereParts = append(whereParts, " AND base.height = ?")
		args = append(args, *filter.height)
	}
	if filter.hash != "" {
		whereParts = append(whereParts, " AND base.tx_hash = ?")
		args = append(args, filter.hash)
	}
	return fromClause, strings.Join(whereParts, ""), args
}

func isMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown table") || strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "unknown identifier")
}

func (s *Server) buildCosmosTxSearchResponse(ctx context.Context, locations []txLocation, total uint64) (*txtypes.GetTxsEventResponse, error) {
	rows, err := s.getTxRowsForLocations(ctx, locations)
	if err != nil {
		return nil, err
	}

	eventsByTx, err := s.getTxEventsForLocations(ctx, locations)
	if err != nil {
		return nil, err
	}

	txs := make([]*txtypes.Tx, 0, len(locations))
	txResponses := make([]*sdk.TxResponse, 0, len(locations))

	for _, loc := range locations {
		key := txLocationKey(loc)
		row, ok := rows[key]
		if !ok {
			fallbackTx, fallbackResp, err := s.fetchTxFromNode(loc.TxHash)
			if err != nil {
				return nil, err
			}
			txs = append(txs, fallbackTx)
			txResponses = append(txResponses, fallbackResp)
			continue
		}

		protoTx, txResp, err := s.buildTxSearchEntry(row, eventsByTx[key])
		if err != nil {
			fallbackTx, fallbackResp, fallbackErr := s.fetchTxFromNode(loc.TxHash)
			if fallbackErr != nil {
				return nil, err
			}
			txs = append(txs, fallbackTx)
			txResponses = append(txResponses, fallbackResp)
			continue
		}

		txs = append(txs, protoTx)
		txResponses = append(txResponses, txResp)
	}

	return &txtypes.GetTxsEventResponse{
		Txs:         txs,
		TxResponses: txResponses,
		Total:       total,
	}, nil
}

func txLocationKey(loc txLocation) string {
	return fmt.Sprintf("%d-%d", loc.Height, loc.IndexInBlock)
}

func (s *Server) findTxLocationByHash(ctx context.Context, hash string) (txLocation, bool, error) {
	var loc txLocation
	if cached, found := s.cache.Get("tx_loc_" + hash); found {
		if v, ok := cached.(txLocation); ok {
			return v, true, nil
		}
	}

	err := s.ch.Conn.QueryRow(
		ctx,
		"SELECT height, index_in_block, tx_hash FROM txs PREWHERE tx_hash = ? LIMIT 1",
		hash,
	).Scan(&loc.Height, &loc.IndexInBlock, &loc.TxHash)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no rows") {
			return txLocation{}, false, nil
		}
		return txLocation{}, false, err
	}
	if loc.TxHash == "" {
		loc.TxHash = hash
	}
	s.cache.Set("tx_loc_"+hash, loc, time.Hour)
	return loc, true, nil
}

func (s *Server) getTxRowsForLocations(ctx context.Context, locations []txLocation) (map[string]model.Tx, error) {
	if len(locations) == 0 {
		return map[string]model.Tx{}, nil
	}
	pairs := make([]string, 0, len(locations))
	for _, loc := range locations {
		pairs = append(pairs, fmt.Sprintf("(%d,%d)", loc.Height, loc.IndexInBlock))
	}
	sql := fmt.Sprintf("SELECT * FROM txs WHERE (height, index_in_block) IN (%s)", strings.Join(pairs, ","))
	var rows []model.Tx
	if err := s.ch.Conn.Select(ctx, &rows, sql); err != nil {
		return nil, err
	}
	out := make(map[string]model.Tx, len(rows))
	for _, row := range rows {
		out[txLocationKey(txLocation{Height: row.Height, IndexInBlock: row.IndexInBlock})] = row
	}
	return out, nil
}

func (s *Server) getTxEventsForLocations(ctx context.Context, locations []txLocation) (map[string][]model.Event, error) {
	if len(locations) == 0 {
		return map[string][]model.Event{}, nil
	}
	pairs := make([]string, 0, len(locations))
	for _, loc := range locations {
		pairs = append(pairs, fmt.Sprintf("(%d,%d)", loc.Height, loc.IndexInBlock))
	}
	sql := fmt.Sprintf("SELECT * FROM events WHERE scope = 'tx' AND (height, tx_index) IN (%s) ORDER BY height ASC, tx_index ASC, event_index ASC", strings.Join(pairs, ","))
	var rows []model.Event
	if err := s.ch.Conn.Select(ctx, &rows, sql); err != nil {
		if isMissingColumnErr(err) {
			legacySQL := fmt.Sprintf("SELECT height, block_time, scope, tx_index, event_index, event_type, attr_key, attr_value, tx_hash FROM events WHERE scope = 'tx' AND (height, tx_index) IN (%s) ORDER BY height ASC, tx_index ASC, event_index ASC", strings.Join(pairs, ","))
			if legacyErr := s.ch.Conn.Select(ctx, &rows, legacySQL); legacyErr != nil {
				return nil, legacyErr
			}
		} else {
			return nil, err
		}
	}
	out := make(map[string][]model.Event)
	for _, row := range rows {
		key := txLocationKey(txLocation{Height: row.Height, IndexInBlock: uint16(row.TxIndex)})
		out[key] = append(out[key], row)
	}
	return out, nil
}

func isMissingColumnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown column") || strings.Contains(msg, "no such column")
}

func (s *Server) buildTxSearchEntry(row model.Tx, dbEvents []model.Event) (*txtypes.Tx, *sdk.TxResponse, error) {
	if row.TxBytes == "" {
		return nil, nil, fmt.Errorf("missing tx_bytes for %s", row.TxHash)
	}

	decodedTx, err := s.clientCtx.TxConfig.TxDecoder()([]byte(row.TxBytes))
	if err != nil {
		return nil, nil, err
	}

	protoCarrier, ok := decodedTx.(interface{ AsTx() (*txtypes.Tx, error) })
	if !ok {
		return nil, nil, fmt.Errorf("decoded tx cannot be converted to proto")
	}
	protoTx, err := protoCarrier.AsTx()
	if err != nil {
		return nil, nil, err
	}
	anyTx, err := codectypes.NewAnyWithValue(protoTx)
	if err != nil {
		return nil, nil, err
	}

	parsedLogs, _ := sdk.ParseABCILogs(row.RawLog)
	txResp := &sdk.TxResponse{
		Height:    int64(row.Height),
		TxHash:    row.TxHash,
		Codespace: row.Codespace,
		Code:      row.Code,
		Data:      row.TxResponseData,
		RawLog:    row.RawLog,
		Logs:      parsedLogs,
		Info:      row.TxResponseInfo,
		GasWanted: int64(row.GasWanted),
		GasUsed:   int64(row.GasUsed),
		Tx:        anyTx,
		Timestamp: row.BlockTime.UTC().Format(time.RFC3339),
		Events:    groupABCIEvents(dbEvents),
	}
	return protoTx, txResp, nil
}

func groupABCIEvents(rows []model.Event) []abcitypes.Event {
	if len(rows) == 0 {
		return []abcitypes.Event{}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EventIndex != rows[j].EventIndex {
			return rows[i].EventIndex < rows[j].EventIndex
		}
		if rows[i].EventType != rows[j].EventType {
			return rows[i].EventType < rows[j].EventType
		}
		return rows[i].AttrKey < rows[j].AttrKey
	})

	events := make([]abcitypes.Event, 0)
	var current *abcitypes.Event
	var lastIndex uint16 = ^uint16(0)
	for _, row := range rows {
		if current == nil || row.EventIndex != lastIndex {
			if current != nil {
				events = append(events, *current)
			}
			current = &abcitypes.Event{Type: row.EventType, Attributes: []abcitypes.EventAttribute{}}
			lastIndex = row.EventIndex
		}
		current.Attributes = append(current.Attributes, abcitypes.EventAttribute{
			Key:   row.AttrKey,
			Value: row.AttrValue,
			Index: row.AttrIndex,
		})
	}
	if current != nil {
		events = append(events, *current)
	}
	return events
}

func (s *Server) fetchTxFromNode(hash string) (*txtypes.Tx, *sdk.TxResponse, error) {
	response, err := authtx.QueryTx(s.clientCtx, hash)
	if err != nil {
		return nil, nil, err
	}
	if response.Tx == nil {
		return nil, nil, fmt.Errorf("missing tx payload for %s", hash)
	}
	protoTx, ok := response.Tx.GetCachedValue().(*txtypes.Tx)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected tx payload type for %s", hash)
	}
	return protoTx, response, nil
}

func (s *Server) fallbackCosmosTxSearch(w http.ResponseWriter, query string, page int, limit int, orderBy string) {
	result, err := authtx.QueryTxsByEvents(s.clientCtx, page, limit, query, orderBy)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	txsList := make([]*txtypes.Tx, 0, len(result.Txs))
	for _, txResp := range result.Txs {
		if txResp.Tx == nil {
			continue
		}
		protoTx, ok := txResp.Tx.GetCachedValue().(*txtypes.Tx)
		if !ok {
			continue
		}
		txsList = append(txsList, protoTx)
	}

	response := &txtypes.GetTxsEventResponse{
		Txs:         txsList,
		TxResponses: result.Txs,
		Total:       result.TotalCount,
	}
	writeGatewayJSON(w, http.StatusOK, response)
}

func (s *Server) fallbackCosmosTxByHash(w http.ResponseWriter, hash string) {
	response, err := authtx.QueryTx(s.clientCtx, hash)
	if err != nil {
		status := http.StatusInternalServerError
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "not found") {
			status = http.StatusNotFound
		}
		respondError(w, status, err.Error())
		return
	}
	if response.Tx == nil {
		respondError(w, http.StatusNotFound, "tx not found")
		return
	}
	protoTx, ok := response.Tx.GetCachedValue().(*txtypes.Tx)
	if !ok {
		respondError(w, http.StatusInternalServerError, "unexpected tx payload type")
		return
	}
	writeGatewayJSON(w, http.StatusOK, &txtypes.GetTxResponse{
		Tx:         protoTx,
		TxResponse: response,
	})
}

func writeGatewayJSON(w http.ResponseWriter, status int, payload interface{}) {
	marshaler := &gateway.JSONPb{
		EmitDefaults: true,
		OrigName:     true,
		Indent:       "  ",
	}
	bz, err := marshaler.Marshal(payload)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to encode response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(bz)
}
