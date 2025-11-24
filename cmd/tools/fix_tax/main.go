package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"

	"github.com/classic-terra/core/v3/app"
	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/ingest"
	"github.com/classic-terra/indexer-go/internal/model"
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type DummyTx struct {
	Msgs []sdk.Msg
}

func (tx *DummyTx) GetMsgs() []sdk.Msg   { return tx.Msgs }
func (tx *DummyTx) ValidateBasic() error { return nil }

func main() {
	// Load Config
	cfg, err := config.LoadConfig(".")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Connect to DBs
	ch, err := db.NewClickHouse(cfg.Database.ClickHouseAddr, cfg.Database.ClickHouseDB, cfg.Database.ClickHouseUser, cfg.Database.ClickHousePassword)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}

	pg, err := db.NewPostgres(cfg.Database.PostgresConn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}

	// Connect to gRPC
	var grpcConn *grpc.ClientConn
	if cfg.Node.GRPC != "" {
		grpcURL := cfg.Node.GRPC
		secure := false
		if strings.HasPrefix(grpcURL, "https://") || strings.HasSuffix(grpcURL, ":443") {
			secure = true
		}
		grpcURL = strings.TrimPrefix(grpcURL, "https://")
		grpcURL = strings.TrimPrefix(grpcURL, "http://")

		var creds credentials.TransportCredentials
		if secure {
			creds = credentials.NewTLS(&tls.Config{})
		} else {
			creds = insecure.NewCredentials()
		}

		grpcConn, err = grpc.Dial(grpcURL, grpc.WithTransportCredentials(creds))
		if err != nil {
			log.Fatalf("Failed to dial gRPC: %v", err)
		}
	} else {
		log.Fatal("gRPC URL is required for tax calculation")
	}

	taxCalc := ingest.NewTaxCalculator(grpcConn)
	cdc := app.MakeEncodingConfig().Marshaler

	// 1. Fetch Denoms Map
	denoms := make(map[string]uint16)
	rows, err := pg.Pool.Query(context.Background(), "SELECT id, denom FROM denoms")
	if err != nil {
		log.Fatalf("Failed to fetch denoms: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uint16
		var denom string
		if err := rows.Scan(&id, &denom); err == nil {
			denoms[denom] = id
		}
	}
	log.Println("Loaded denoms map")

	// 1b. Fetch Msg Types Map
	msgTypes := make(map[uint16]string)
	rowsMsg, err := pg.Pool.Query(context.Background(), "SELECT id, msg_type FROM msg_types")
	if err != nil {
		log.Fatalf("Failed to fetch msg_types: %v", err)
	}
	defer rowsMsg.Close()
	for rowsMsg.Next() {
		var id uint16
		var typeName string
		if err := rowsMsg.Scan(&id, &typeName); err == nil {
			msgTypes[id] = typeName
		}
	}
	log.Println("Loaded msg_types map")

	// 2. Iterate Txs
	// We iterate by height to be efficient
	startHeight := int64(0)
	batchSize := 1000

	log.Println("Starting tax backfill...")

	for {
		var txs []model.Tx
		query := fmt.Sprintf(`
			SELECT * FROM txs 
			WHERE height > %d 
			ORDER BY height, index_in_block 
			LIMIT %d
		`, startHeight, batchSize)

		if err := ch.Conn.Select(context.Background(), &txs, query); err != nil {
			log.Fatalf("Failed to query txs: %v", err)
		}

		if len(txs) == 0 {
			break
		}

		var txsToUpdate []model.Tx
		var txHashesToDelete []string

		for i := range txs {
			tx := &txs[i]
			startHeight = int64(tx.Height) // Update cursor

			// Reconstruct Tx
			dummyTx, err := reconstructTx(tx.MsgsJSON, tx.MsgTypeIDs, msgTypes, cdc)
			if err != nil {
				// log.Printf("Failed to reconstruct tx %s: %v", tx.TxHash, err)
				continue
			}

			calculatedTax, err := taxCalc.CalculateTax(context.Background(), int64(tx.Height), dummyTx, cdc)
			if err != nil {
				log.Printf("Failed to calculate tax for tx %s: %v", tx.TxHash, err)
				continue
			}

			// Debug log for first few
			if i < 5 && len(calculatedTax) > 0 {
				log.Printf("Tx %s: Calculated Tax: %v", tx.TxHash, calculatedTax)
			}

			if len(calculatedTax) > 0 {
				var taxAmounts []int64
				var taxDenomIDs []uint16

				for _, coin := range calculatedTax {
					taxAmounts = append(taxAmounts, coin.Amount.Int64())

					id, ok := denoms[coin.Denom]
					if !ok {
						// Insert new denom
						var newID uint16
						err := pg.Pool.QueryRow(context.Background(), "INSERT INTO denoms (denom) VALUES ($1) ON CONFLICT (denom) DO UPDATE SET denom=EXCLUDED.denom RETURNING id", coin.Denom).Scan(&newID)
						if err == nil {
							denoms[coin.Denom] = newID
							id = newID
						} else {
							log.Printf("Failed to insert denom %s: %v", coin.Denom, err)
							continue
						}
					}
					taxDenomIDs = append(taxDenomIDs, id)
				}

				// Update fields
				tx.TaxAmounts = taxAmounts
				tx.TaxDenomIDs = taxDenomIDs

				txsToUpdate = append(txsToUpdate, *tx)
				txHashesToDelete = append(txHashesToDelete, fmt.Sprintf("'%s'", tx.TxHash))
			}
		}

		if len(txsToUpdate) > 0 {
			log.Printf("Updating %d txs in batch (Height %d)...", len(txsToUpdate), startHeight)

			// 1. Delete
			// Construct IN clause
			inClause := strings.Join(txHashesToDelete, ",")
			deleteQuery := fmt.Sprintf("ALTER TABLE txs DELETE WHERE tx_hash IN (%s)", inClause)

			if err := ch.Conn.Exec(context.Background(), deleteQuery); err != nil {
				log.Printf("Failed to delete txs: %v", err)
				log.Fatal("Stopping due to delete failure")
			}

			// 2. Insert
			batch, err := ch.Conn.PrepareBatch(context.Background(), "INSERT INTO txs")
			if err != nil {
				log.Fatalf("Failed to prepare batch: %v", err)
			}
			for _, t := range txsToUpdate {
				if err := batch.AppendStruct(&t); err != nil {
					log.Printf("Failed to append struct: %v", err)
				}
			}
			if err := batch.Send(); err != nil {
				log.Fatalf("Failed to send batch: %v", err)
			}
		} else {
			log.Printf("Processed up to height %d (No updates)", startHeight)
		}
	}
}

func reconstructTx(msgsJSON []string, msgTypeIDs []uint16, msgTypes map[uint16]string, cdc codec.Codec) (sdk.Tx, error) {
	var msgs []sdk.Msg
	if len(msgsJSON) != len(msgTypeIDs) {
		return nil, fmt.Errorf("msgsJSON length %d does not match msgTypeIDs length %d", len(msgsJSON), len(msgTypeIDs))
	}

	for i, jsonStr := range msgsJSON {
		typeID := msgTypeIDs[i]
		typeName, ok := msgTypes[typeID]
		if !ok {
			return nil, fmt.Errorf("unknown msg type id %d", typeID)
		}

		protoCodec, ok := cdc.(*codec.ProtoCodec)
		if !ok {
			return nil, fmt.Errorf("codec is not ProtoCodec")
		}

		msgType, err := protoCodec.InterfaceRegistry().Resolve(typeName)
		if err != nil {
			// Try prepending /
			msgType, err = protoCodec.InterfaceRegistry().Resolve("/" + typeName)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve type %s: %v", typeName, err)
			}
		}

		msg, ok := msgType.(sdk.Msg)
		if !ok {
			return nil, fmt.Errorf("type %s is not sdk.Msg", typeName)
		}

		err = cdc.UnmarshalJSON([]byte(jsonStr), msg)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json for %s: %v", typeName, err)
		}

		msgs = append(msgs, msg)
	}
	return &DummyTx{Msgs: msgs}, nil
}
