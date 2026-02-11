package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	Pool *pgxpool.Pool
}

func NewPostgres(connString string) (*Postgres, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &Postgres{Pool: pool}, nil
}

// ============================================================
// Blocks (migrated from ClickHouse)
// ============================================================

func (pg *Postgres) GetMaxHeight(ctx context.Context) (int64, error) {
	var height *int64
	err := pg.Pool.QueryRow(ctx, "SELECT max(height) FROM blocks").Scan(&height)
	if err != nil || height == nil {
		return 0, nil
	}
	return *height, nil
}

func (pg *Postgres) BlockExists(ctx context.Context, height int64) (bool, error) {
	var exists bool
	err := pg.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM blocks WHERE height = $1)", height).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// PgBlock mirrors model.Block but with db-friendly scanning.
type PgBlock struct {
	Height          uint64
	BlockHash       string
	BlockTime       time.Time
	ProposerAddress string
	TxCount         uint32
}

func (pg *Postgres) GetLatestBlock(ctx context.Context) (*PgBlock, error) {
	var b PgBlock
	err := pg.Pool.QueryRow(ctx,
		"SELECT height, block_hash, block_time, proposer_address, tx_count FROM blocks ORDER BY height DESC LIMIT 1",
	).Scan(&b.Height, &b.BlockHash, &b.BlockTime, &b.ProposerAddress, &b.TxCount)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (pg *Postgres) GetBlock(ctx context.Context, height uint64) (*PgBlock, error) {
	var b PgBlock
	err := pg.Pool.QueryRow(ctx,
		"SELECT height, block_hash, block_time, proposer_address, tx_count FROM blocks WHERE height = $1",
		height,
	).Scan(&b.Height, &b.BlockHash, &b.BlockTime, &b.ProposerAddress, &b.TxCount)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (pg *Postgres) CountBlocksInRange(ctx context.Context, start, end int64) (int64, error) {
	var count int64
	err := pg.Pool.QueryRow(ctx,
		"SELECT count(*) FROM blocks WHERE height >= $1 AND height < $2", start, end,
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (pg *Postgres) GetMaxHeightInRange(ctx context.Context, start, end int64) (int64, error) {
	var height *int64
	err := pg.Pool.QueryRow(ctx,
		"SELECT max(height) FROM blocks WHERE height >= $1 AND height < $2", start, end,
	).Scan(&height)
	if err != nil || height == nil {
		return 0, nil
	}
	return *height, nil
}

func (pg *Postgres) GetExistingHeightsInRange(ctx context.Context, start, end int64) (map[int64]bool, error) {
	result := make(map[int64]bool)
	rows, err := pg.Pool.Query(ctx,
		"SELECT height FROM blocks WHERE height >= $1 AND height <= $2", start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var h int64
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		result[h] = true
	}
	return result, nil
}

func (pg *Postgres) FindGapsInRange(ctx context.Context, start, end int64, limit int) ([]int64, error) {
	existing, err := pg.GetExistingHeightsInRange(ctx, start, end)
	if err != nil {
		return nil, err
	}
	var gaps []int64
	for h := start; h <= end && len(gaps) < limit; h++ {
		if !existing[h] {
			gaps = append(gaps, h)
		}
	}
	return gaps, nil
}

// InsertBlocks bulk-inserts blocks into PostgreSQL using COPY.
func (pg *Postgres) InsertBlocks(ctx context.Context, blocks []PgBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	// Use a transaction with ON CONFLICT DO NOTHING for idempotent inserts.
	batch := &pgx.Batch{}
	for _, b := range blocks {
		batch.Queue(
			`INSERT INTO blocks (height, block_hash, block_time, proposer_address, tx_count)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (height) DO NOTHING`,
			b.Height, b.BlockHash, b.BlockTime, b.ProposerAddress, b.TxCount,
		)
	}
	br := pg.Pool.SendBatch(ctx, batch)
	defer br.Close()
	for range blocks {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert block: %w", err)
		}
	}
	return nil
}

// ============================================================
// Oracle Prices (migrated from ClickHouse)
// ============================================================

type PgOraclePrice struct {
	BlockTime time.Time
	Height    uint64
	Denom     string
	Price     float64
	Currency  string
}

func (pg *Postgres) InsertOraclePrices(ctx context.Context, prices []PgOraclePrice) error {
	if len(prices) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, p := range prices {
		batch.Queue(
			`INSERT INTO oracle_prices (block_time, height, denom, price, currency)
			 VALUES ($1, $2, $3, $4, $5)`,
			p.BlockTime, p.Height, p.Denom, p.Price, p.Currency,
		)
	}
	br := pg.Pool.SendBatch(ctx, batch)
	defer br.Close()
	for range prices {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert oracle price: %w", err)
		}
	}
	return nil
}

func (pg *Postgres) GetPriceHistory(ctx context.Context, denom string, interval string, limit int) ([]map[string]interface{}, error) {
	var truncExpr string
	var duration string

	switch interval {
	case "1m":
		truncExpr = "date_trunc('minute', block_time)"
		duration = "24 hours"
	case "5m":
		truncExpr = "to_timestamp(floor(extract(epoch from block_time) / 300) * 300)"
		duration = "24 hours"
	case "15m":
		truncExpr = "to_timestamp(floor(extract(epoch from block_time) / 900) * 900)"
		duration = "3 days"
	case "30m":
		truncExpr = "to_timestamp(floor(extract(epoch from block_time) / 1800) * 1800)"
		duration = "5 days"
	case "1h":
		truncExpr = "date_trunc('hour', block_time)"
		duration = "7 days"
	case "1d":
		truncExpr = "date_trunc('day', block_time)"
		duration = "30 days"
	default:
		truncExpr = "date_trunc('day', block_time)"
		duration = "30 days"
	}

	sql := fmt.Sprintf(`
		SELECT
			EXTRACT(EPOCH FROM %s)::bigint * 1000 AS datetime,
			avg(price) AS price
		FROM oracle_prices
		WHERE denom = $1 AND block_time >= NOW() - INTERVAL '%s'
		GROUP BY 1
		ORDER BY 1 DESC
		LIMIT $2
	`, truncExpr, duration)

	rows, err := pg.Pool.Query(ctx, sql, denom, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var ts int64
		var price float64
		if err := rows.Scan(&ts, &price); err != nil {
			return nil, err
		}
		result = append(result, map[string]interface{}{
			"denom":    denom,
			"datetime": ts,
			"price":    price,
		})
	}
	return result, nil
}

func (pg *Postgres) GetPrice24hAgo(ctx context.Context, denom string) (float64, error) {
	var price float64
	err := pg.Pool.QueryRow(ctx, `
		SELECT price FROM oracle_prices
		WHERE denom = $1
		  AND block_time <= NOW() - INTERVAL '24 hours'
		  AND block_time >= NOW() - INTERVAL '48 hours'
		ORDER BY block_time DESC
		LIMIT 1
	`, denom).Scan(&price)
	if err != nil {
		return 0, err
	}
	return price, nil
}

func (pg *Postgres) GetAllPrices24hAgo(ctx context.Context) (map[string]float64, error) {
	rows, err := pg.Pool.Query(ctx, `
		SELECT DISTINCT ON (denom) denom, price
		FROM oracle_prices
		WHERE block_time <= NOW() - INTERVAL '24 hours'
		  AND block_time >= NOW() - INTERVAL '48 hours'
		ORDER BY denom, block_time DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]float64)
	for rows.Next() {
		var denom string
		var price float64
		if err := rows.Scan(&denom, &price); err != nil {
			return nil, err
		}
		result[denom] = price
	}
	return result, nil
}

// ============================================================
// Account Txs (dual-write: ClickHouse + PostgreSQL)
// ============================================================

type PgAccountTx struct {
	AddressID    uint64
	Height       uint64
	IndexInBlock uint16
	BlockTime    time.Time
	TxHash       string
	Direction    int8
	MainDenomID  uint16
	MainAmount   int64
	IsBlockEvent bool
	EventScope   int8
}

func (pg *Postgres) InsertAccountTxs(ctx context.Context, txs []PgAccountTx) error {
	if len(txs) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, t := range txs {
		batch.Queue(
			`INSERT INTO account_txs
			  (address_id, height, index_in_block, block_time, tx_hash,
			   direction, main_denom_id, main_amount, is_block_event, event_scope)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			 ON CONFLICT (address_id, height, index_in_block, is_block_event) DO NOTHING`,
			t.AddressID, t.Height, t.IndexInBlock, t.BlockTime, t.TxHash,
			t.Direction, t.MainDenomID, t.MainAmount, t.IsBlockEvent, t.EventScope,
		)
	}
	br := pg.Pool.SendBatch(ctx, batch)
	defer br.Close()
	for range txs {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert account_tx: %w", err)
		}
	}
	return nil
}

func (pg *Postgres) GetAccountActivity(ctx context.Context, addressID uint64, offset uint64, limit int) ([]PgAccountTx, error) {
	var args []interface{}
	sql := `SELECT address_id, height, index_in_block, block_time, tx_hash,
	               direction, main_denom_id, main_amount, is_block_event, event_scope
	        FROM account_txs
	        WHERE address_id = $1`
	args = append(args, addressID)

	argIdx := 2
	if offset > 0 {
		height := offset / 100000
		index := offset % 100000
		sql += fmt.Sprintf(
			" AND (height < $%d OR (height = $%d AND index_in_block < $%d))",
			argIdx, argIdx+1, argIdx+2)
		args = append(args, height, height, index)
		argIdx += 3
	}

	sql += fmt.Sprintf(" ORDER BY height DESC, index_in_block DESC LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := pg.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PgAccountTx
	for rows.Next() {
		var t PgAccountTx
		if err := rows.Scan(
			&t.AddressID, &t.Height, &t.IndexInBlock, &t.BlockTime, &t.TxHash,
			&t.Direction, &t.MainDenomID, &t.MainAmount, &t.IsBlockEvent, &t.EventScope,
		); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, nil
}

// GetDayTimeline returns a list of distinct days that have blocks, useful for dashboard stubs.
func (pg *Postgres) GetDayTimeline(ctx context.Context) ([]uint64, error) {
	rows, err := pg.Pool.Query(ctx, `
		SELECT EXTRACT(EPOCH FROM date_trunc('day', block_time))::bigint * 1000 AS datetime
		FROM blocks
		GROUP BY 1
		ORDER BY 1 ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []uint64
	for rows.Next() {
		var ts uint64
		if err := rows.Scan(&ts); err != nil {
			return nil, err
		}
		result = append(result, ts)
	}
	return result, nil
}
