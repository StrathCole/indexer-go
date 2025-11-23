package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	oracletypes "github.com/classic-terra/core/v3/x/oracle/types"
	"github.com/gorilla/mux"
)

func (s *Server) GetMarketPrice(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	interval := query.Get("interval")
	denom := query.Get("denom")

	key := "market_price"
	if denom != "" {
		key += "_" + denom
	}
	if interval != "" {
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

		// 2. Get History
		history, err := s.fetchMarketHistory(denom, interval)
		if err != nil {
			// Log error but return partial?
			history = []interface{}{}
		}

		// 3. Calculate Variation (Stub for now)
		oneDayVariation := "0"
		oneDayVariationRate := "0"

		return map[string]interface{}{
			"lastPrice":           lastPrice,
			"oneDayVariation":     oneDayVariation,
			"oneDayVariationRate": oneDayVariationRate,
			"prices":              history,
		}, nil
	})
}

func (s *Server) fetchMarketHistory(denom string, interval string) ([]interface{}, error) {
	var timeFunc string
	var duration time.Duration

	switch interval {
	case "1m":
		timeFunc = "toStartOfMinute(block_time)"
		duration = 24 * time.Hour
	case "5m":
		timeFunc = "toStartOfFiveMinute(block_time)"
		duration = 24 * time.Hour
	case "15m":
		timeFunc = "toStartOfFifteenMinutes(block_time)"
		duration = 3 * 24 * time.Hour
	case "30m":
		timeFunc = "toStartOfInterval(block_time, INTERVAL 30 minute)"
		duration = 5 * 24 * time.Hour
	case "1h":
		timeFunc = "toStartOfHour(block_time)"
		duration = 7 * 24 * time.Hour
	case "1d":
		timeFunc = "toStartOfDay(block_time)"
		duration = 30 * 24 * time.Hour // 30 days default
	default:
		timeFunc = "toStartOfDay(block_time)"
		duration = 30 * 24 * time.Hour
	}

	startTime := time.Now().Add(-duration)

	type PriceRow struct {
		Time  uint64  `ch:"datetime"`
		Price float64 `ch:"price"`
	}

	var rows []PriceRow
	sql := fmt.Sprintf(`
		SELECT 
			toUnixTimestamp(%s)*1000 as datetime, 
			avg(price) as price 
		FROM oracle_prices 
		WHERE denom = ? AND block_time >= ?
		GROUP BY datetime 
		ORDER BY datetime ASC
	`, timeFunc)

	// Note: context.Background() is used here, but we should ideally use request context if available.
	// But fetchMarketHistory signature doesn't have it.
	// Using context.Background() is fine for now.
	err := s.ch.Conn.Select(context.Background(), &rows, sql, denom, startTime)
	if err != nil {
		return nil, err
	}

	var result []interface{}
	for _, r := range rows {
		result = append(result, map[string]interface{}{
			"denom":    denom,
			"datetime": r.Time,
			"price":    r.Price,
		})
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

		var result []map[string]interface{}

		// Helper to add result
		addResult := func(denom string, rate float64) {
			result = append(result, map[string]interface{}{
				"denom":               denom,
				"swaprate":            fmt.Sprintf("%.10f", rate),
				"oneDayVariation":     "0",
				"oneDayVariationRate": "0",
			})
		}

		if base == "uluna" {
			// Base is Luna. Rates are already Price of 1 Luna in Denom.
			for denom, price := range priceMap {
				addResult(denom, price)
			}
			// Add uluna itself
			addResult("uluna", 1.0)
		} else {
			// Base is something else (e.g. uusd)
			basePrice, ok := priceMap[base]
			if !ok {
				// Base not found in oracle rates.
				// If base is not uluna and not in rates, we can't calculate.
				return nil, fmt.Errorf("base denom %s not found in exchange rates", base)
			}

			// 1 Base = (1/basePrice) Luna
			// Price of 1 Base in Denom X = (Price of 1 Luna in X) * (1/basePrice) = priceMap[X] / basePrice
			for denom, price := range priceMap {
				if denom == base {
					continue
				}
				addResult(denom, price/basePrice)
			}
			// Add uluna
			// Price of 1 Base in Luna = 1 / basePrice
			addResult("uluna", 1.0/basePrice)
			// Add base itself
			addResult(base, 1.0)
		}

		return result, nil
	})
}
