package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/classic-terra/core/v3/app"
	"github.com/classic-terra/indexer-go/internal/db"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	tmtypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/client"
	gateway "github.com/cosmos/gogogateway"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rs/zerolog/log"
)

type Server struct {
	ch               *db.ClickHouse
	pg               *db.Postgres
	clientCtx        client.Context
	rpc              *rpchttp.HTTP
	cache            *Cache
	corsOrigins      []string
	excludedAccounts []string
	lcdURL           string
	swaggerCache     swaggerDocCache
}

func NewServer(ch *db.ClickHouse, pg *db.Postgres, clientCtx client.Context, rpcClient *rpchttp.HTTP, corsOrigins []string, excludedAccounts []string, lcdURL string) *Server {
	return &Server{
		ch:               ch,
		pg:               pg,
		clientCtx:        clientCtx,
		rpc:              rpcClient,
		cache:            NewCache(),
		corsOrigins:      corsOrigins,
		excludedAccounts: excludedAccounts,
		lcdURL:           lcdURL,
	}
}

func (s *Server) Router() http.Handler {
	r := mux.NewRouter()

	// Middleware
	r.Use(loggingMiddleware)
	r.Use(recoveryMiddleware)
	r.Use(s.corsMiddleware)

	// Routes
	v1 := r.PathPrefix("/v1").Subrouter()

	// Swagger
	r.HandleFunc("/swagger", s.SwaggerRedirect).Methods("GET")
	r.HandleFunc("/swagger/", s.SwaggerUI).Methods("GET")
	r.HandleFunc("/swagger/doc.json", s.SwaggerDoc).Methods("GET")

	// Dashboard
	v1.HandleFunc("/dashboard", s.GetDashboard).Methods("GET")
	v1.HandleFunc("/dashboard/tx_volume", s.GetTxVolume).Methods("GET")
	v1.HandleFunc("/dashboard/block_rewards", s.GetBlockRewards).Methods("GET")
	v1.HandleFunc("/dashboard/seigniorage_proceeds", s.GetSeigniorageProceeds).Methods("GET")
	v1.HandleFunc("/dashboard/staking_return", s.GetStakingReturn).Methods("GET")
	v1.HandleFunc("/dashboard/staking_ratio", s.GetStakingRatio).Methods("GET")
	v1.HandleFunc("/dashboard/account_growth", s.GetAccountGrowth).Methods("GET")
	v1.HandleFunc("/dashboard/active_accounts", s.GetActiveAccounts).Methods("GET")
	v1.HandleFunc("/dashboard/registered_accounts", s.GetRegisteredAccounts).Methods("GET")
	v1.HandleFunc("/dashboard/last_hour_ops_txs_count", s.GetLastHourOpsAndTxs).Methods("GET")

	// Transactions
	v1.HandleFunc("/txs/gas_prices", s.GetGasPrices).Methods("GET")
	v1.HandleFunc("/txs", s.GetTxs).Methods("GET")
	v1.HandleFunc("/txs/{hash}", s.GetTx).Methods("GET")
	v1.HandleFunc("/tx/{hash}", s.GetTx).Methods("GET") // Alias
	v1.HandleFunc("/mempool", s.GetMempool).Methods("GET")
	v1.HandleFunc("/mempool/{hash}", s.GetMempoolTx).Methods("GET")

	// Blocks
	v1.HandleFunc("/blocks/latest", s.GetBlockLatest).Methods("GET")
	v1.HandleFunc("/blocks/{height}", s.GetBlock).Methods("GET")
	v1.HandleFunc("/blocks/{height}/events", s.GetBlockEvents).Methods("GET")

	// Bank
	v1.HandleFunc("/bank/{account}", s.GetBalances).Methods("GET")

	// Market
	v1.HandleFunc("/market/price", s.GetMarketPrice).Methods("GET")
	v1.HandleFunc("/market/swaprate/{base}", s.GetMarketSwapRate).Methods("GET")

	// Staking
	v1.HandleFunc("/staking/validators", s.GetValidators).Methods("GET")
	v1.HandleFunc("/staking/validators/{operatorAddr}", s.GetValidator).Methods("GET")
	v1.HandleFunc("/staking/validators/{operatorAddr}/claims", s.GetClaims).Methods("GET")
	v1.HandleFunc("/staking/account/{account}", s.GetStakingAccount).Methods("GET")
	v1.HandleFunc("/staking/return", s.GetTotalStakingReturn).Methods("GET")
	v1.HandleFunc("/staking/return/{operatorAddr}", s.GetValidatorReturn).Methods("GET")

	// Treasury
	v1.HandleFunc("/taxproceeds", s.GetTaxProceeds).Methods("GET")
	v1.HandleFunc("/richlist/{denom}", s.GetRichlist).Methods("GET")
	v1.HandleFunc("/totalsupply/{denom}", s.GetTotalSupply).Methods("GET")
	v1.HandleFunc("/circulatingsupply/{denom}", s.GetCirculatingSupply).Methods("GET")

	// Root
	r.HandleFunc("/", s.GetRoot).Methods("GET")

	// Proxy to LCD (Embedded GRPC Gateway)
	// We create a new ServeMux for the gateway
	gwMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &gateway.JSONPb{
			EmitDefaults: true,
			OrigName:     true,
			Indent:       "  ",
		}),
	)
	// Register routes
	app.ModuleBasics.RegisterGRPCGatewayRoutes(s.clientCtx, gwMux)

	// Mount the gateway
	r.PathPrefix("/").Handler(gwMux)

	return r
}

func (s *Server) GetRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"name":"Terra Classic Indexer API","version":"1.0.0","status":"online"}`))
}

func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Error().Msgf("Panic recovered: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Dur("duration", time.Since(start)).
			Msg("Request")
	})
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simple implementation: just take the first one or *
		// For multiple origins, we need to check Origin header
		origin := r.Header.Get("Origin")
		allowed := false
		for _, o := range s.corsOrigins {
			if o == "*" {
				allowed = true
				w.Header().Set("Access-Control-Allow-Origin", "*")
				break
			}
			if o == origin {
				allowed = true
				w.Header().Set("Access-Control-Allow-Origin", origin)
				break
			}
		}

		// If not allowed but we have origins configured, maybe we should default to first?
		// Or just not set header.
		if !allowed && len(s.corsOrigins) > 0 && s.corsOrigins[0] == "*" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) GetGasPrices(w http.ResponseWriter, r *http.Request) {
	// Hardcoded gas prices from Classic FCD
	gasPrices := map[string]string{
		"uaud":  "0.95",
		"ucad":  "0.95",
		"uchf":  "0.7",
		"ucny":  "4.9",
		"udkk":  "4.5",
		"ueur":  "0.625",
		"ugbp":  "0.55",
		"uhkd":  "5.85",
		"uidr":  "10900.0",
		"uinr":  "54.4",
		"ujpy":  "81.85",
		"ukrw":  "850.0",
		"uluna": "28.325",
		"umnt":  "2142.855",
		"umyr":  "3.0",
		"unok":  "6.25",
		"uphp":  "38.0",
		"usdr":  "0.52469",
		"usek":  "6.25",
		"usgd":  "1.0",
		"uthb":  "23.1",
		"utwd":  "20.0",
		"uusd":  "0.75",
	}

	respondJSON(w, http.StatusOK, gasPrices)
}

func (s *Server) GetMempool(w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
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
			if account != "" {
				found := false
				for _, msg := range tx.GetMsgs() {
					for _, signer := range msg.GetSigners() {
						if signer.String() == account {
							found = true
							break
						}
					}
					if found {
						break
					}
				}
				if !found {
					continue
				}
			}

			// Marshal to JSON
			jsonBytes, err := txEncoder(tx)
			if err == nil {
				var txObj interface{}
				json.Unmarshal(jsonBytes, &txObj)

				decodedTxs = append(decodedTxs, map[string]interface{}{
					"timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
					"chainId":   "columbus-5",
					"txhash":    fmt.Sprintf("%X", tmtypes.Tx(txBytes).Hash()),
					"tx":        txObj,
				})
			}
		}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"total": len(decodedTxs),
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
				"timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
				"chainId":   "columbus-5",
				"txhash":    currentHash,
				"tx":        txObj,
			})
			return
		}
	}

	respondError(w, http.StatusNotFound, "Transaction not found in mempool")
}
