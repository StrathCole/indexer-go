package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	oracletypes "github.com/classic-terra/core/v3/x/oracle/types"
	"github.com/gorilla/mux"
)

func (s *Server) GetMarketPrice(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	interval := query.Get("interval")
	denom := query.Get("denom")

	if denom == "" {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"code":    400,
			"message": "child \"denom\" fails because [\"denom\" is required]",
			"type":    "INVALID_REQUEST_ERROR",
		})
		return
	}

	key := "market_price"
	if denom != "" {
		key += "_" + denom
	}
	if interval != "" {
		validIntervals := map[string]bool{
			"1m": true, "5m": true, "15m": true, "30m": true, "1h": true, "1d": true,
		}
		if !validIntervals[interval] {
			respondJSON(w, http.StatusBadRequest, map[string]interface{}{
				"code":    400,
				"message": "child \"interval\" fails because [\"interval\" must be one of [1m, 5m, 15m, 30m, 1h, 1d]]",
				"type":    "INVALID_REQUEST_ERROR",
			})
			return
		}
		key += "_" + interval
	}

	s.respondWithCache(w, key, 1*time.Minute, func() (interface{}, error) {
		// Always return FCD structure
		// {
		//   lastPrice: number,
		//   oneDayVariation: string,
		//   oneDayVariationRate: string,
		//   prices: [{denom, datetime, price}]
		// }

		// 1. Get Current Price
		oracleClient := oracletypes.NewQueryClient(s.clientCtx)
		ratesResp, err := oracleClient.ExchangeRates(context.Background(), &oracletypes.QueryExchangeRatesRequest{})
		if err != nil {
			return nil, err
		}

		var lastPrice float64
		found := false
		for _, r := range ratesResp.ExchangeRates {
			if r.Denom == denom {
				fmt.Sscanf(r.Amount.String(), "%f", &lastPrice)
				found = true
				break
			}
		}
		if !found && denom == "uluna" {
			lastPrice = 1.0
		}

		// 2. Get History (from PostgreSQL)
		history, err := s.fetchMarketHistory(denom, interval)
		if err != nil {
			// Log error but return partial?
			history = []interface{}{}
		}

		// 3. Calculate Variation
		oneDayVariation := "0"
		oneDayVariationRate := "0"

		// Fetch price 24h ago from PostgreSQL
		price24hAgo, err := s.pg.GetPrice24hAgo(context.Background(), denom)

		if err == nil && price24hAgo > 0 && lastPrice > 0 {
			diff := lastPrice - price24hAgo
			rate := diff / lastPrice // Legacy uses division by lastPrice
			oneDayVariation = fmt.Sprintf("%.15g", diff)
			oneDayVariationRate = fmt.Sprintf("%.15g", rate)
		}

		return map[string]interface{}{
			"lastPrice":           lastPrice,
			"oneDayVariation":     oneDayVariation,
			"oneDayVariationRate": oneDayVariationRate,
			"prices":              history,
		}, nil
	})
}

func (s *Server) fetchMarketHistory(denom string, interval string) ([]interface{}, error) {
	limit := 50

	rows, err := s.pg.GetPriceHistory(context.Background(), denom, interval, limit)
	if err != nil {
		return nil, err
	}

	// Reverse to ASC order (GetPriceHistory returns DESC)
	var result []interface{}
	for i := len(rows) - 1; i >= 0; i-- {
		result = append(result, rows[i])
	}

	if result == nil {
		result = []interface{}{}
	}

	return result, nil
}

func (s *Server) GetMarketSwapRate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	base := vars["base"]

	s.respondWithCache(w, "swaprate_"+base, 1*time.Minute, func() (interface{}, error) {
		oracleClient := oracletypes.NewQueryClient(s.clientCtx)
		ratesResp, err := oracleClient.ExchangeRates(context.Background(), &oracletypes.QueryExchangeRatesRequest{})
		if err != nil {
			return nil, err
		}

		priceMap := make(map[string]float64)
		for _, r := range ratesResp.ExchangeRates {
			val, _ := strconv.ParseFloat(r.Amount.String(), 64)
			priceMap[r.Denom] = val
		}

		// Fetch prices 24h ago from PostgreSQL
		lastDayPriceMap, _ := s.pg.GetAllPrices24hAgo(context.Background())
		if lastDayPriceMap == nil {
			lastDayPriceMap = make(map[string]float64)
		}

		var result []map[string]interface{}

		// Helper to calculate swap rate
		getSwapRate := func(prices map[string]float64, base string, target string) float64 {
			if base == "uluna" {
				if target == "uluna" {
					return 1.0
				}
				return prices[target]
			}
			// Base is not uluna
			basePrice, ok := prices[base]
			if !ok || basePrice == 0 {
				return 0
			}
			if target == "uluna" {
				return 1.0 / basePrice
			}
			if target == base {
				return 1.0
			}
			return prices[target] / basePrice
		}

		// Helper to add result
		addResult := func(denom string) {
			currentRate := getSwapRate(priceMap, base, denom)
			lastDayRate := getSwapRate(lastDayPriceMap, base, denom)

			oneDayVariation := "0"
			oneDayVariationRate := "0"

			if lastDayRate > 0 {
				diff := currentRate - lastDayRate
				rate := diff / lastDayRate
				oneDayVariation = strconv.FormatFloat(diff, 'f', -1, 64)
				oneDayVariationRate = strconv.FormatFloat(rate, 'f', -1, 64)
			}

			result = append(result, map[string]interface{}{
				"denom":               denom,
				"swaprate":            strconv.FormatFloat(currentRate, 'f', -1, 64),
				"oneDayVariation":     oneDayVariation,
				"oneDayVariationRate": oneDayVariationRate,
			})
		}

		// Iterate over all denoms in current price map + uluna + base
		denoms := make(map[string]bool)
		for d := range priceMap {
			denoms[d] = true
		}
		denoms["uluna"] = true
		denoms[base] = true

		var sortedDenoms []string
		for d := range denoms {
			sortedDenoms = append(sortedDenoms, d)
		}
		sort.Strings(sortedDenoms)

		for _, denom := range sortedDenoms {
			if denom == base && base != "uluna" {
				continue
			}
			addResult(denom)
		}

		return result, nil
	})
}
