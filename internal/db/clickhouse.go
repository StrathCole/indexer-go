package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouse struct {
	Conn driver.Conn
}

func NewClickHouse(addr string, database string, username string, password string) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Debug: false,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:     time.Second * 30,
		MaxOpenConns:    20, // Increased from 5 to support parallel backfill
		MaxIdleConns:    10, // Increased from 5
		ConnMaxLifetime: time.Duration(10) * time.Minute,
	})

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(context.Background()); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}

	return &ClickHouse{Conn: conn}, nil
}

func (ch *ClickHouse) GetMaxHeight(ctx context.Context) (int64, error) {
	var height uint64
	err := ch.Conn.QueryRow(ctx, "SELECT max(height) FROM blocks").Scan(&height)
	if err != nil {
		// If table is empty or doesn't exist, it might return 0 or error.
		// We should handle it gracefully.
		return 0, nil
	}
	return int64(height), nil
}

func (ch *ClickHouse) BlockExists(ctx context.Context, height int64) (bool, error) {
	var count uint64
	err := ch.Conn.QueryRow(ctx, "SELECT count() FROM blocks WHERE height = $1", height).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (ch *ClickHouse) GetPriceHistory(ctx context.Context, denom string, interval string) ([]map[string]interface{}, error) {
	var chInterval string
	switch interval {
	case "1m":
		chInterval = "1 minute"
	case "5m":
		chInterval = "5 minute"
	case "15m":
		chInterval = "15 minute"
	case "30m":
		chInterval = "30 minute"
	case "1h":
		chInterval = "1 hour"
	case "1d":
		chInterval = "1 day"
	default:
		chInterval = "1 hour"
	}

	// Assuming table name is 'oracle_prices'
	query := fmt.Sprintf(`
		SELECT 
			toStartOfInterval(block_time, INTERVAL %s) as time,
			avg(price) as price
		FROM oracle_prices
		WHERE denom = $1
		GROUP BY time
		ORDER BY time ASC
	`, chInterval)

	rows, err := ch.Conn.Query(ctx, query, denom)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var t time.Time
		var price float64
		if err := rows.Scan(&t, &price); err != nil {
			return nil, err
		}
		result = append(result, map[string]interface{}{
			"datetime": t.Unix() * 1000,
			"price":    price,
			"denom":    denom,
		})
	}
	return result, nil
}

func (ch *ClickHouse) FindNextGap(ctx context.Context, startHeight, endHeight int64) (int64, error) {
	// 1. Check if startHeight exists
	exists, err := ch.BlockExists(ctx, startHeight)
	if err != nil {
		return 0, err
	}
	if !exists {
		return startHeight, nil
	}

	// 2. Find the first gap using array functions (robust across CH versions)
	// We collect heights in range, sort them, calculate differences, and find the first diff > 1
	query := `
		SELECT arrayElement(sorted_heights, i-1) + 1
		FROM (
			SELECT 
				arraySort(groupArray(height)) as sorted_heights,
				arrayDifference(sorted_heights) as diffs,
				arrayFirstIndex(x -> x > 1, diffs) as i
			FROM (
				SELECT height FROM blocks WHERE height >= $1 AND height < $2
			)
		)
		WHERE i > 0
	`

	rows, err := ch.Conn.Query(ctx, query, startHeight, endHeight)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if rows.Next() {
		var gapStart uint64
		if err := rows.Scan(&gapStart); err != nil {
			return 0, err
		}
		return int64(gapStart), nil
	}

	return 0, nil
}

func (ch *ClickHouse) CountBlocksInRange(ctx context.Context, start, end int64) (int64, error) {
	var count uint64
	err := ch.Conn.QueryRow(ctx, "SELECT count() FROM blocks WHERE height >= $1 AND height < $2", start, end).Scan(&count)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

func (ch *ClickHouse) GetMaxHeightInRange(ctx context.Context, start, end int64) (int64, error) {
	var height uint64
	err := ch.Conn.QueryRow(ctx, "SELECT max(height) FROM blocks WHERE height >= $1 AND height < $2", start, end).Scan(&height)
	if err != nil {
		return 0, err
	}
	return int64(height), nil
}

// GetExistingHeightsInRange returns a set of existing block heights in the given range
func (ch *ClickHouse) GetExistingHeightsInRange(ctx context.Context, start, end int64) (map[int64]bool, error) {
	result := make(map[int64]bool)

	rows, err := ch.Conn.Query(ctx, "SELECT height FROM blocks WHERE height >= $1 AND height <= $2", start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var height uint64
		if err := rows.Scan(&height); err != nil {
			return nil, err
		}
		result[int64(height)] = true
	}

	return result, nil
}

// FindGapsInRange returns a slice of missing block heights in the given range
func (ch *ClickHouse) FindGapsInRange(ctx context.Context, start, end int64, limit int) ([]int64, error) {
	// Get existing heights
	existing, err := ch.GetExistingHeightsInRange(ctx, start, end)
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
