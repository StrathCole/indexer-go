package ingest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/classic-terra/indexer-go/internal/model"
)

type ReindexOptions struct {
	DeleteTimeout      time.Duration
	DeletePollInterval time.Duration
	DryRun             bool
}

type ReindexSummary struct {
	Height           uint64
	TxCount          int
	EventCount       int
	AccountTxCount   int
	OraclePriceCount int
	ValidatorCount   int
	BlockRewardCount int
}

var clickHouseHeightTables = []string{
	"tx_event_lookup",
	"events",
	"txs",
	"account_txs",
	"oracle_prices",
	"validator_returns",
	"block_rewards",
	"blocks",
}

func (s *Service) ReindexBlock(ctx context.Context, height int64, opts ReindexOptions) (ReindexSummary, error) {
	if height <= 0 {
		return ReindexSummary{}, fmt.Errorf("height must be > 0")
	}
	if opts.DeleteTimeout <= 0 {
		opts.DeleteTimeout = 15 * time.Minute
	}
	if opts.DeletePollInterval <= 0 {
		opts.DeletePollInterval = 2 * time.Second
	}

	block, txs, events, accountTxs, oraclePrices, validatorReturns, blockRewards, err := s.FetchAndConvertBlock(height)
	if err != nil {
		return ReindexSummary{}, fmt.Errorf("fetch and convert height %d: %w", height, err)
	}
	if block.Height != uint64(height) {
		return ReindexSummary{}, fmt.Errorf("fetched height mismatch: requested %d, got %d", height, block.Height)
	}

	summary := ReindexSummary{
		Height:           block.Height,
		TxCount:          len(txs),
		EventCount:       len(events),
		AccountTxCount:   len(accountTxs),
		OraclePriceCount: len(oraclePrices),
		ValidatorCount:   len(validatorReturns),
		BlockRewardCount: len(blockRewards),
	}
	if opts.DryRun {
		return summary, nil
	}

	affectedAddressIDs, affectedDays, err := s.reindexAggregateInputs(ctx, block.Height, block.BlockTime, accountTxs)
	if err != nil {
		return summary, err
	}

	if err := s.deleteBlockData(ctx, block.Height, opts.DeleteTimeout, opts.DeletePollInterval); err != nil {
		return summary, err
	}

	if err := s.BatchInsert(ctx, []model.Block{block}, txs, events, accountTxs, oraclePrices, validatorReturns, blockRewards); err != nil {
		return summary, fmt.Errorf("insert reindexed height %d: %w", height, err)
	}

	if err := s.refreshReindexAggregates(ctx, affectedAddressIDs, affectedDays, opts.DeleteTimeout, opts.DeletePollInterval); err != nil {
		return summary, err
	}

	return summary, nil
}

func (s *Service) deleteBlockData(ctx context.Context, height uint64, timeout time.Duration, pollInterval time.Duration) error {
	for _, table := range clickHouseHeightTables {
		exists, err := s.clickHouseTableExists(ctx, table)
		if err != nil {
			return fmt.Errorf("check ClickHouse table %s: %w", table, err)
		}
		if !exists {
			continue
		}
		if err := s.deleteClickHouseHeight(ctx, table, height, timeout, pollInterval); err != nil {
			return err
		}
	}

	if err := s.pg.DeleteBlockData(ctx, height); err != nil {
		return err
	}
	return nil
}

func (s *Service) deleteClickHouseHeight(ctx context.Context, table string, height uint64, timeout time.Duration, pollInterval time.Duration) error {
	if err := s.ch.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DELETE WHERE height = %d", table, height)); err != nil {
		return fmt.Errorf("delete ClickHouse %s height %d: %w", table, height, err)
	}

	deadline := time.Now().Add(timeout)
	for {
		remaining, err := s.countClickHouseHeightRows(ctx, table, height)
		if err != nil {
			return fmt.Errorf("count ClickHouse %s height %d after delete: %w", table, height, err)
		}
		if remaining == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for ClickHouse delete on %s height %d; %d rows still visible", table, height, remaining)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (s *Service) countClickHouseHeightRows(ctx context.Context, table string, height uint64) (uint64, error) {
	var count uint64
	if err := s.ch.Conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE height = %d", table, height)).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Service) clickHouseTableExists(ctx context.Context, table string) (bool, error) {
	var exists uint8
	if err := s.ch.Conn.QueryRow(ctx, "EXISTS TABLE "+strings.TrimSpace(table)).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (s *Service) reindexAggregateInputs(ctx context.Context, height uint64, blockTime time.Time, accountTxs []model.AccountTx) ([]uint64, []string, error) {
	addressIDs := make(map[uint64]struct{})
	days := map[string]struct{}{sqlDay(blockTime): {}}

	for _, at := range accountTxs {
		addressIDs[at.AddressID] = struct{}{}
		days[sqlDay(at.BlockTime)] = struct{}{}
	}

	exists, err := s.clickHouseTableExists(ctx, "account_txs")
	if err != nil {
		return nil, nil, fmt.Errorf("check ClickHouse account_txs: %w", err)
	}
	if exists {
		var existingAddressIDs []uint64
		if err := s.ch.Conn.Select(ctx, &existingAddressIDs, fmt.Sprintf("SELECT DISTINCT address_id FROM account_txs WHERE height = %d", height)); err != nil {
			return nil, nil, fmt.Errorf("load existing account_txs address ids for height %d: %w", height, err)
		}
		for _, id := range existingAddressIDs {
			addressIDs[id] = struct{}{}
		}

		var existingDays []string
		if err := s.ch.Conn.Select(ctx, &existingDays, fmt.Sprintf("SELECT DISTINCT toString(toDate(block_time)) FROM account_txs WHERE height = %d", height)); err != nil {
			return nil, nil, fmt.Errorf("load existing account_txs days for height %d: %w", height, err)
		}
		for _, day := range existingDays {
			if day != "" {
				days[day] = struct{}{}
			}
		}
	}

	return sortedUint64Keys(addressIDs), sortedStringKeys(days), nil
}

func (s *Service) refreshReindexAggregates(ctx context.Context, addressIDs []uint64, days []string, timeout time.Duration, pollInterval time.Duration) error {
	registeredDaySet := make(map[string]struct{}, len(days))
	for _, day := range days {
		registeredDaySet[day] = struct{}{}
	}

	if len(addressIDs) > 0 {
		oldFirstSeenDays, err := s.collectAddressFirstSeenDays(ctx, addressIDs)
		if err != nil {
			return err
		}
		for _, day := range oldFirstSeenDays {
			registeredDaySet[day] = struct{}{}
		}
	}

	if len(days) > 0 {
		if err := s.refreshDailyActiveTx(ctx, days, timeout, pollInterval); err != nil {
			return err
		}
	}

	if len(addressIDs) > 0 {
		if err := s.refreshAddressFirstSeenTx(ctx, addressIDs, timeout, pollInterval); err != nil {
			return err
		}
	}

	if len(addressIDs) > 0 {
		newFirstSeenDays, err := s.collectAddressFirstSeenDays(ctx, addressIDs)
		if err != nil {
			return err
		}
		for _, day := range newFirstSeenDays {
			registeredDaySet[day] = struct{}{}
		}
	}

	registeredDays := sortedStringKeys(registeredDaySet)
	if len(registeredDays) > 0 {
		if err := s.refreshRegisteredAccountsDailyTx(ctx, registeredDays, timeout, pollInterval); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) refreshDailyActiveTx(ctx context.Context, days []string, timeout time.Duration, pollInterval time.Duration) error {
	exists, err := s.clickHouseTableExists(ctx, "account_txs_daily_active_tx")
	if err != nil {
		return fmt.Errorf("check ClickHouse account_txs_daily_active_tx: %w", err)
	}
	if !exists {
		return nil
	}

	for _, day := range days {
		where := fmt.Sprintf("day = toDate('%s')", day)
		if err := s.deleteClickHouseWhere(ctx, "account_txs_daily_active_tx", where, timeout, pollInterval); err != nil {
			return err
		}

		insertSQL := fmt.Sprintf(`
INSERT INTO account_txs_daily_active_tx
SELECT toDate(block_time) AS day, uniqCombined64State(address_id) AS active_state
FROM account_txs
WHERE is_block_event = 0 AND toDate(block_time) = toDate('%s')
GROUP BY day`, day)
		if err := s.ch.Conn.Exec(ctx, insertSQL); err != nil {
			return fmt.Errorf("rebuild account_txs_daily_active_tx for %s: %w", day, err)
		}
	}
	return nil
}

func (s *Service) refreshAddressFirstSeenTx(ctx context.Context, addressIDs []uint64, timeout time.Duration, pollInterval time.Duration) error {
	exists, err := s.clickHouseTableExists(ctx, "address_first_seen_tx")
	if err != nil {
		return fmt.Errorf("check ClickHouse address_first_seen_tx: %w", err)
	}
	if !exists {
		return nil
	}

	for _, chunk := range chunkUint64s(addressIDs, 5000) {
		ids := joinUint64s(chunk)
		if err := s.deleteClickHouseWhere(ctx, "address_first_seen_tx", "address_id IN ("+ids+")", timeout, pollInterval); err != nil {
			return err
		}

		insertSQL := fmt.Sprintf(`
INSERT INTO address_first_seen_tx
SELECT address_id, minState(block_time) AS first_seen_state
FROM account_txs
WHERE is_block_event = 0 AND address_id IN (%s)
GROUP BY address_id`, ids)
		if err := s.ch.Conn.Exec(ctx, insertSQL); err != nil {
			return fmt.Errorf("rebuild address_first_seen_tx: %w", err)
		}
	}
	return nil
}

func (s *Service) collectAddressFirstSeenDays(ctx context.Context, addressIDs []uint64) ([]string, error) {
	exists, err := s.clickHouseTableExists(ctx, "address_first_seen_tx")
	if err != nil {
		return nil, fmt.Errorf("check ClickHouse address_first_seen_tx: %w", err)
	}
	if !exists {
		return nil, nil
	}

	days := make(map[string]struct{})
	for _, chunk := range chunkUint64s(addressIDs, 5000) {
		var chunkDays []string
		query := fmt.Sprintf(`
SELECT DISTINCT toString(toDate(first_seen))
FROM (
	SELECT address_id, minMerge(first_seen_state) AS first_seen
	FROM address_first_seen_tx
	WHERE address_id IN (%s)
	GROUP BY address_id
)`, joinUint64s(chunk))
		if err := s.ch.Conn.Select(ctx, &chunkDays, query); err != nil {
			return nil, fmt.Errorf("collect address_first_seen_tx days: %w", err)
		}
		for _, day := range chunkDays {
			if day != "" {
				days[day] = struct{}{}
			}
		}
	}

	return sortedStringKeys(days), nil
}

func (s *Service) refreshRegisteredAccountsDailyTx(ctx context.Context, days []string, timeout time.Duration, pollInterval time.Duration) error {
	exists, err := s.clickHouseTableExists(ctx, "registered_accounts_daily_tx")
	if err != nil {
		return fmt.Errorf("check ClickHouse registered_accounts_daily_tx: %w", err)
	}
	if !exists {
		return nil
	}

	for _, day := range days {
		where := fmt.Sprintf("day = toDate('%s')", day)
		if err := s.deleteClickHouseWhere(ctx, "registered_accounts_daily_tx", where, timeout, pollInterval); err != nil {
			return err
		}

		insertSQL := fmt.Sprintf(`
INSERT INTO registered_accounts_daily_tx
SELECT first_seen_day AS day, count() AS value
FROM (
	SELECT address_id, toDate(minMerge(first_seen_state)) AS first_seen_day
	FROM address_first_seen_tx
	GROUP BY address_id
)
WHERE first_seen_day = toDate('%s')
GROUP BY first_seen_day`, day)
		if err := s.ch.Conn.Exec(ctx, insertSQL); err != nil {
			return fmt.Errorf("rebuild registered_accounts_daily_tx for %s: %w", day, err)
		}
	}
	return nil
}

func (s *Service) deleteClickHouseWhere(ctx context.Context, table string, where string, timeout time.Duration, pollInterval time.Duration) error {
	if err := s.ch.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", table, where)); err != nil {
		return fmt.Errorf("delete ClickHouse %s where %s: %w", table, where, err)
	}

	deadline := time.Now().Add(timeout)
	for {
		var remaining uint64
		if err := s.ch.Conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE %s", table, where)).Scan(&remaining); err != nil {
			return fmt.Errorf("count ClickHouse %s where %s after delete: %w", table, where, err)
		}
		if remaining == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for ClickHouse delete on %s where %s; %d rows still visible", table, where, remaining)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func sqlDay(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}

func sortedUint64Keys(values map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(values))
	for v := range values {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedStringKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for v := range values {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func chunkUint64s(values []uint64, size int) [][]uint64 {
	if size <= 0 || len(values) == 0 {
		return nil
	}
	var out [][]uint64
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		out = append(out, values[start:end])
	}
	return out
}

func joinUint64s(values []uint64) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}
