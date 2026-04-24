package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/classic-terra/core/v4/app"
	customauthtx "github.com/classic-terra/core/v4/custom/auth/tx"
	"github.com/classic-terra/indexer-go/internal/db"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	tmtypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
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
	archivalCache    *ArchivalCache
	corsOrigins      []string
	excludedAccounts []string
	lcdURL           string
	swaggerCache     swaggerDocCache
	runtimeStatus    *runtimeStatus
}

func NewServer(ch *db.ClickHouse, pg *db.Postgres, clientCtx client.Context, rpcClient *rpchttp.HTTP, corsOrigins []string, excludedAccounts []string, lcdURL string) *Server {
	srv := &Server{
		ch:               ch,
		pg:               pg,
		clientCtx:        clientCtx,
		rpc:              rpcClient,
		cache:            NewCache(),
		archivalCache:    NewArchivalCache(),
		corsOrigins:      corsOrigins,
		excludedAccounts: excludedAccounts,
		lcdURL:           lcdURL,
		runtimeStatus:    newRuntimeStatus(),
	}
	srv.startRuntimeMonitor()
	return srv
}

func (s *Server) Router() http.Handler {
	r := mux.NewRouter()

	// Middleware
	r.Use(loggingMiddleware)
	r.Use(recoveryMiddleware)
	r.Use(gzipMiddleware)
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

	// Mantlemint-compatible endpoints
	r.HandleFunc("/index/blocks/{height}", s.MantlemintGetBlock).Methods("GET")
	r.HandleFunc("/index/tx/by_hash/{hash}", s.MantlemintGetTxByHash).Methods("GET")
	r.HandleFunc("/index/tx/by_height/{height}", s.MantlemintGetTxsByHeight).Methods("GET")
	r.HandleFunc("/health", s.MantlemintHealth).Methods("GET")
	r.HandleFunc("/cosmos/tx/v1beta1/txs", s.GetCosmosTxsEvent).Methods("GET")
	r.HandleFunc("/cosmos/tx/v1beta1/txs/{hash}", s.GetCosmosTxByHash).Methods("GET")

	// Proxy to LCD (Embedded GRPC Gateway)
	// We create a new ServeMux for the gateway
	gwMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &gateway.JSONPb{
			EmitDefaults: true,
			OrigName:     true,
			Indent:       "  ",
		}),
	)
	// Register routes — matching mantlemint's full Cosmos SDK surface area:
	// 1. All module query routes (bank, staking, auth, distribution, gov, wasm, oracle, etc.)
	app.ModuleBasics.RegisterGRPCGatewayRoutes(s.clientCtx, gwMux)
	// 2. TX service routes (/cosmos/tx/v1beta1/txs, simulate, broadcast, encode, decode)
	authtx.RegisterGRPCGatewayRoutes(s.clientCtx, gwMux)
	// 3. Terra custom TX routes (tax computation on tx)
	customauthtx.RegisterGRPCGatewayRoutes(s.clientCtx, gwMux)
	// 4. Tendermint/CometBFT service routes (blocks, validator sets, node_info, syncing, ABCIQuery)
	cmtservice.RegisterGRPCGatewayRoutes(s.clientCtx, gwMux)

	// Mount the gateway behind height middleware + archival cache (mantlemint compat:
	// ?height=N → x-cosmos-block-height header for historical state queries)
	r.PathPrefix("/").Handler(blockHeightMiddleware(s.archivalCache.Middleware(gwMux)))

	return r
}

type gzipResponseWriter struct {
	http.ResponseWriter
	writer io.Writer
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.writer.Write(b)
}

func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip if client doesn't accept gzip.
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		// Skip if already encoded.
		if w.Header().Get("Content-Encoding") != "" {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")

		gz, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		defer gz.Close()

		grw := gzipResponseWriter{ResponseWriter: w, writer: gz}
		next.ServeHTTP(grw, r)
	})
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

// blockHeightMiddleware converts ?height=N query parameter to the
// x-cosmos-block-height gRPC metadata header, matching mantlemint behavior.
// This enables historical state queries on archive nodes.
func blockHeightMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		heightStr := r.FormValue("height")
		if heightStr != "" {
			height, err := strconv.ParseInt(heightStr, 10, 64)
			if err != nil {
				http.Error(w, `{"code":0,"error":"syntax error"}`, http.StatusBadRequest)
				return
			}
			if height < 0 {
				http.Error(w, `{"code":0,"error":"height must be equal or greater than zero"}`, http.StatusBadRequest)
				return
			}
			if height > 0 {
				r.Header.Set("x-cosmos-block-height", heightStr)
			}
		}
		next.ServeHTTP(w, r)
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
				// Check if the fee payer matches the account
				if feeTx, ok := tx.(sdk.FeeTx); ok {
					payer := sdk.AccAddress(feeTx.FeePayer())
					if payer.String() == account {
						found = true
					}
				}
				if !found {
					// Fallback: check if account appears in the JSON representation
					jsonBytes, err := txEncoder(tx)
					if err == nil && strings.Contains(string(jsonBytes), account) {
						found = true
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
