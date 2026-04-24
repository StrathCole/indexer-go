package ingest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/classic-terra/indexer-go/internal/model"
)

type ReindexOptions struct {
	DeleteTimeout      time.Duration
	DeletePollInterval time.Duration
	ProgressInterval   time.Duration
	FetchWorkers       int
	SkipAggregates     bool
	DryRun             bool
	Progress           func(format string, args ...any)
}

type ReindexSummary struct {
	Height           uint64
	TxCount          int
	EventCount       int
	TxEventCount     int
	BlockEventCount  int
	AccountTxCount   int
	OraclePriceCount int
	ValidatorCount   int
	BlockRewardCount int
}

type reindexBlockPayload struct {
	height           uint64
	block            model.Block
	txs              []model.Tx
	events           []model.Event
	accountTxs       []model.AccountTx
	oraclePrices     []model.OraclePrice
	validatorReturns []model.ValidatorReturn
	blockRewards     []model.BlockReward
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

var clickHouseLegacyOptionalTables = map[string]struct{}{
	"blocks":        {},
	"oracle_prices": {},
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
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = 10 * time.Second
	}
	if opts.FetchWorkers <= 0 {
		opts.FetchWorkers = 1
	}

	opts.progressf("height %d: fetching and converting block", height)
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
		TxEventCount:     countEventsByScope(events, "tx"),
		BlockEventCount:  countNonTxEvents(events),
		AccountTxCount:   len(accountTxs),
		OraclePriceCount: len(oraclePrices),
		ValidatorCount:   len(validatorReturns),
		BlockRewardCount: len(blockRewards),
	}
	opts.progressf("height %d: fetched txs=%d events=%d block_events=%d tx_events=%d account_txs=%d", height, summary.TxCount, summary.EventCount, summary.BlockEventCount, summary.TxEventCount, summary.AccountTxCount)
	if opts.DryRun {
		return summary, nil
	}

	var affectedAddressIDs []uint64
	var affectedDays []string
	if !opts.SkipAggregates {
		opts.progressf("height %d: loading aggregate refresh inputs", height)
		affectedAddressIDs, affectedDays, err = s.reindexAggregateInputs(ctx, block.Height, block.BlockTime, accountTxs)
		if err != nil {
			return summary, err
		}
		opts.progressf("height %d: aggregate refresh inputs loaded addresses=%d days=%d", height, len(affectedAddressIDs), len(affectedDays))
	}

	opts.progressf("height %d: deleting old rows", height)
	if err := s.deleteBlockData(ctx, block.Height, opts); err != nil {
		return summary, err
	}

	opts.progressf("height %d: inserting fresh rows", height)
	if err := s.BatchInsert(ctx, []model.Block{block}, txs, events, accountTxs, oraclePrices, validatorReturns, blockRewards); err != nil {
		return summary, fmt.Errorf("insert reindexed height %d: %w", height, err)
	}

	if !opts.SkipAggregates {
		opts.progressf("height %d: refreshing aggregates", height)
		if err := s.refreshReindexAggregates(ctx, affectedAddressIDs, affectedDays, opts); err != nil {
			return summary, err
		}
	}
	opts.progressf("height %d: complete", height)

	return summary, nil
}

func (s *Service) ReindexBlocks(ctx context.Context, heights []int64, opts ReindexOptions) ([]ReindexSummary, error) {
	if len(heights) == 0 {
		return nil, fmt.Errorf("at least one height is required")
	}
	if opts.DeleteTimeout <= 0 {
		opts.DeleteTimeout = 15 * time.Minute
	}
	if opts.DeletePollInterval <= 0 {
		opts.DeletePollInterval = 2 * time.Second
	}
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = 10 * time.Second
	}
	if opts.FetchWorkers <= 0 {
		opts.FetchWorkers = 6
	}
	if opts.FetchWorkers > len(heights) {
		opts.FetchWorkers = len(heights)
	}

	for _, h := range heights {
		if h <= 0 {
			return nil, fmt.Errorf("height must be > 0: %d", h)
		}
	}

	type fetchJob struct {
		idx    int
		height int64
	}
	type fetchResult struct {
		idx     int
		summary ReindexSummary
		payload reindexBlockPayload
		err     error
	}

	jobs := make(chan fetchJob, len(heights))
	results := make(chan fetchResult, len(heights))

	for idx, h := range heights {
		jobs <- fetchJob{idx: idx, height: h}
	}
	close(jobs)

	var wg sync.WaitGroup
	for worker := 0; worker < opts.FetchWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				opts.progressf("height %d: fetching and converting block", job.height)
				block, txs, events, accountTxs, oraclePrices, validatorReturns, blockRewards, err := s.FetchAndConvertBlock(job.height)
				if err != nil {
					results <- fetchResult{idx: job.idx, err: fmt.Errorf("fetch and convert height %d: %w", job.height, err)}
					continue
				}
				if block.Height != uint64(job.height) {
					results <- fetchResult{idx: job.idx, err: fmt.Errorf("fetched height mismatch: requested %d, got %d", job.height, block.Height)}
					continue
				}

				summary := ReindexSummary{
					Height:           block.Height,
					TxCount:          len(txs),
					EventCount:       len(events),
					TxEventCount:     countEventsByScope(events, "tx"),
					BlockEventCount:  countNonTxEvents(events),
					AccountTxCount:   len(accountTxs),
					OraclePriceCount: len(oraclePrices),
					ValidatorCount:   len(validatorReturns),
					BlockRewardCount: len(blockRewards),
				}
				opts.progressf("height %d: fetched txs=%d events=%d block_events=%d tx_events=%d account_txs=%d", job.height, summary.TxCount, summary.EventCount, summary.BlockEventCount, summary.TxEventCount, summary.AccountTxCount)

				results <- fetchResult{
					idx:     job.idx,
					summary: summary,
					payload: reindexBlockPayload{
						height:           block.Height,
						block:            block,
						txs:              txs,
						events:           events,
						accountTxs:       accountTxs,
						oraclePrices:     oraclePrices,
						validatorReturns: validatorReturns,
						blockRewards:     blockRewards,
					},
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	summariesByIdx := make([]ReindexSummary, len(heights))
	payloadsByIdx := make([]reindexBlockPayload, len(heights))
	var fetchErr error
	for result := range results {
		if result.err != nil {
			if fetchErr == nil {
				fetchErr = result.err
			}
			continue
		}
		summariesByIdx[result.idx] = result.summary
		payloadsByIdx[result.idx] = result.payload
	}
	if fetchErr != nil {
		return nil, fetchErr
	}

	summaries := make([]ReindexSummary, 0, len(heights))
	payloads := make([]reindexBlockPayload, 0, len(heights))
	for i := range heights {
		summaries = append(summaries, summariesByIdx[i])
		if !opts.DryRun {
			payloads = append(payloads, payloadsByIdx[i])
		}
	}

	if opts.DryRun {
		return summaries, nil
	}

	heightSet := make(map[uint64]struct{}, len(payloads))
	affectedDaySet := make(map[string]struct{})
	affectedAddressSet := make(map[uint64]struct{})

	allBlocks := make([]model.Block, 0, len(payloads))
	allTxs := make([]model.Tx, 0)
	allEvents := make([]model.Event, 0)
	allAccountTxs := make([]model.AccountTx, 0)
	allOraclePrices := make([]model.OraclePrice, 0)
	allValidatorReturns := make([]model.ValidatorReturn, 0)
	allBlockRewards := make([]model.BlockReward, 0)

	for _, payload := range payloads {
		heightSet[payload.height] = struct{}{}
		affectedDaySet[sqlDay(payload.block.BlockTime)] = struct{}{}
		for _, at := range payload.accountTxs {
			affectedAddressSet[at.AddressID] = struct{}{}
			affectedDaySet[sqlDay(at.BlockTime)] = struct{}{}
		}

		allBlocks = append(allBlocks, payload.block)
		allTxs = append(allTxs, payload.txs...)
		allEvents = append(allEvents, payload.events...)
		allAccountTxs = append(allAccountTxs, payload.accountTxs...)
		allOraclePrices = append(allOraclePrices, payload.oraclePrices...)
		allValidatorReturns = append(allValidatorReturns, payload.validatorReturns...)
		allBlockRewards = append(allBlockRewards, payload.blockRewards...)
	}

	batchHeights := sortedUint64Keys(heightSet)

	if !opts.SkipAggregates {
		existingAddressIDs, existingDays, err := s.reindexExistingAggregateInputsBatch(ctx, batchHeights)
		if err != nil {
			return nil, err
		}
		for _, id := range existingAddressIDs {
			affectedAddressSet[id] = struct{}{}
		}
		for _, day := range existingDays {
			if day != "" {
				affectedDaySet[day] = struct{}{}
			}
		}
	}

	affectedDays := sortedStringKeys(affectedDaySet)
	affectedAddressIDs := sortedUint64Keys(affectedAddressSet)

	opts.progressf("batch: deleting old rows for %d heights", len(batchHeights))
	if err := s.deleteBlockDataBatch(ctx, batchHeights, opts); err != nil {
		return nil, err
	}

	opts.progressf("batch: inserting fresh rows for %d heights", len(batchHeights))
	if err := s.BatchInsert(ctx, allBlocks, allTxs, allEvents, allAccountTxs, allOraclePrices, allValidatorReturns, allBlockRewards); err != nil {
		return nil, fmt.Errorf("insert reindexed heights batch: %w", err)
	}

	if !opts.SkipAggregates {
		opts.progressf("batch: refreshing aggregates for addresses=%d days=%d", len(affectedAddressIDs), len(affectedDays))
		if err := s.refreshReindexAggregates(ctx, affectedAddressIDs, affectedDays, opts); err != nil {
			return nil, err
		}
	}

	for _, summary := range summaries {
		opts.progressf("height %d: complete", summary.Height)
	}

	return summaries, nil
}

func (opts ReindexOptions) progressf(format string, args ...any) {
	if opts.Progress != nil {
		opts.Progress(format, args...)
	}
}

func countEventsByScope(events []model.Event, scope string) int {
	var count int
	for _, event := range events {
		if event.Scope == scope {
			count++
		}
	}
	return count
}

func countNonTxEvents(events []model.Event) int {
	var count int
	for _, event := range events {
		if event.Scope != "tx" {
			count++
		}
	}
	return count
}

func (s *Service) deleteBlockData(ctx context.Context, height uint64, opts ReindexOptions) error {
	return s.deleteBlockDataBatch(ctx, []uint64{height}, opts)
}

func (s *Service) deleteBlockDataBatch(ctx context.Context, heights []uint64, opts ReindexOptions) error {
	if len(heights) == 0 {
		return nil
	}
	heightFilter := "height IN (" + joinUint64s(heights) + ")"

	for _, table := range clickHouseHeightTables {
		opts.progressf("heights=%d: checking ClickHouse table %s", len(heights), table)
		exists, err := s.clickHouseTableExists(ctx, table)
		if err != nil {
			return fmt.Errorf("check ClickHouse table %s: %w", table, err)
		}
		if !exists {
			if _, legacyOptional := clickHouseLegacyOptionalTables[table]; legacyOptional {
				continue
			}
			opts.progressf("heights=%d: ClickHouse table %s missing, skipping", len(heights), table)
			continue
		}
		if err := s.deleteClickHouseWhere(ctx, table, heightFilter, opts); err != nil {
			return err
		}
	}

	opts.progressf("heights=%d: deleting PostgreSQL rows", len(heights))
	if err := s.pg.DeleteBlockDataBatch(ctx, heights); err != nil {
		return err
	}
	opts.progressf("heights=%d: PostgreSQL rows deleted", len(heights))
	return nil
}

func (s *Service) deleteClickHouseHeight(ctx context.Context, table string, height uint64, opts ReindexOptions) error {
	opts.progressf("height %d: deleting ClickHouse %s rows", height, table)
	if err := s.ch.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DELETE WHERE height = %d", table, height)); err != nil {
		return fmt.Errorf("delete ClickHouse %s height %d: %w", table, height, err)
	}

	deadline := time.Now().Add(opts.DeleteTimeout)
	lastProgress := time.Now()
	for {
		remaining, err := s.countClickHouseHeightRows(ctx, table, height)
		if err != nil {
			return fmt.Errorf("count ClickHouse %s height %d after delete: %w", table, height, err)
		}
		if remaining == 0 {
			opts.progressf("height %d: ClickHouse %s delete complete", height, table)
			return nil
		}
		if time.Since(lastProgress) >= opts.ProgressInterval {
			opts.progressf("height %d: waiting for ClickHouse %s delete, remaining_rows=%d", height, table, remaining)
			lastProgress = time.Now()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for ClickHouse delete on %s height %d; %d rows still visible", table, height, remaining)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(opts.DeletePollInterval):
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
		existingAddressIDs, err := s.queryClickHouseUint64Column(ctx, fmt.Sprintf("SELECT DISTINCT address_id FROM account_txs WHERE height = %d", height))
		if err != nil {
			return nil, nil, fmt.Errorf("load existing account_txs address ids for height %d: %w", height, err)
		}
		for _, id := range existingAddressIDs {
			addressIDs[id] = struct{}{}
		}

		existingDays, err := s.queryClickHouseStringColumn(ctx, fmt.Sprintf("SELECT DISTINCT toString(toDate(block_time)) FROM account_txs WHERE height = %d", height))
		if err != nil {
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

func (s *Service) reindexExistingAggregateInputsBatch(ctx context.Context, heights []uint64) ([]uint64, []string, error) {
	if len(heights) == 0 {
		return nil, nil, nil
	}

	exists, err := s.clickHouseTableExists(ctx, "account_txs")
	if err != nil {
		return nil, nil, fmt.Errorf("check ClickHouse account_txs: %w", err)
	}
	if !exists {
		return nil, nil, nil
	}

	heightFilter := "height IN (" + joinUint64s(heights) + ")"

	addressIDs, err := s.queryClickHouseUint64Column(ctx, "SELECT DISTINCT address_id FROM account_txs WHERE "+heightFilter)
	if err != nil {
		return nil, nil, fmt.Errorf("load existing account_txs address ids for heights batch: %w", err)
	}

	days, err := s.queryClickHouseStringColumn(ctx, "SELECT DISTINCT toString(toDate(block_time)) FROM account_txs WHERE "+heightFilter)
	if err != nil {
		return nil, nil, fmt.Errorf("load existing account_txs days for heights batch: %w", err)
	}

	return addressIDs, days, nil
}

func (s *Service) refreshReindexAggregates(ctx context.Context, addressIDs []uint64, days []string, opts ReindexOptions) error {
	oldFirstSeenDayCounts := make(map[string]uint64)

	if len(addressIDs) > 0 {
		opts.progressf("aggregates: collecting old first-seen day counts for %d addresses", len(addressIDs))
		counts, err := s.collectAddressFirstSeenDayCounts(ctx, addressIDs)
		if err != nil {
			return err
		}
		oldFirstSeenDayCounts = counts
	}

	if len(days) > 0 {
		if err := s.refreshDailyActiveTx(ctx, days, opts); err != nil {
			return err
		}
	}

	if len(addressIDs) > 0 {
		if err := s.refreshAddressFirstSeenTx(ctx, addressIDs, opts); err != nil {
			return err
		}
	}

	if len(addressIDs) > 0 {
		opts.progressf("aggregates: collecting new first-seen day counts for %d addresses", len(addressIDs))
		newFirstSeenDayCounts, err := s.collectAddressFirstSeenDayCounts(ctx, addressIDs)
		if err != nil {
			return err
		}
		if err := s.refreshRegisteredAccountsDailyTx(ctx, oldFirstSeenDayCounts, newFirstSeenDayCounts, opts); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) refreshDailyActiveTx(ctx context.Context, days []string, opts ReindexOptions) error {
	exists, err := s.clickHouseTableExists(ctx, "account_txs_daily_active_tx")
	if err != nil {
		return fmt.Errorf("check ClickHouse account_txs_daily_active_tx: %w", err)
	}
	if !exists {
		return nil
	}

	for _, day := range days {
		opts.progressf("aggregates: refreshing account_txs_daily_active_tx for %s", day)
		where := fmt.Sprintf("day = toDate('%s')", day)
		if err := s.deleteClickHouseWhere(ctx, "account_txs_daily_active_tx", where, opts); err != nil {
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

func (s *Service) refreshAddressFirstSeenTx(ctx context.Context, addressIDs []uint64, opts ReindexOptions) error {
	exists, err := s.clickHouseTableExists(ctx, "address_first_seen_tx")
	if err != nil {
		return fmt.Errorf("check ClickHouse address_first_seen_tx: %w", err)
	}
	if !exists {
		return nil
	}

	chunks := chunkUint64s(addressIDs, 5000)
	for i, chunk := range chunks {
		opts.progressf("aggregates: refreshing address_first_seen_tx chunk %d/%d addresses=%d", i+1, len(chunks), len(chunk))
		ids := joinUint64s(chunk)
		if err := s.deleteClickHouseWhere(ctx, "address_first_seen_tx", "address_id IN ("+ids+")", opts); err != nil {
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

func (s *Service) collectAddressFirstSeenDayCounts(ctx context.Context, addressIDs []uint64) (map[string]uint64, error) {
	exists, err := s.clickHouseTableExists(ctx, "address_first_seen_tx")
	if err != nil {
		return nil, fmt.Errorf("check ClickHouse address_first_seen_tx: %w", err)
	}
	if !exists {
		return map[string]uint64{}, nil
	}
	if len(addressIDs) == 0 {
		return map[string]uint64{}, nil
	}

	counts := make(map[string]uint64)
	for _, chunk := range chunkUint64s(addressIDs, 5000) {
		query := fmt.Sprintf(`
SELECT day, count()
FROM (
	SELECT toString(toDate(minMerge(first_seen_state))) AS day
	FROM address_first_seen_tx
	WHERE address_id IN (%s)
	GROUP BY address_id
) AS grouped
GROUP BY day`, joinUint64s(chunk))

		rows, err := s.ch.Conn.Query(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("collect address_first_seen_tx day counts: %w", err)
		}
		for rows.Next() {
			var day string
			var count uint64
			if err := rows.Scan(&day, &count); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan address_first_seen_tx day counts: %w", err)
			}
			if day != "" {
				counts[day] += count
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate address_first_seen_tx day counts: %w", err)
		}
		rows.Close()
	}

	return counts, nil
}

func (s *Service) queryClickHouseUint64Column(ctx context.Context, query string) ([]uint64, error) {
	rows, err := s.ch.Conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []uint64
	for rows.Next() {
		var value uint64
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Service) queryClickHouseStringColumn(ctx context.Context, query string) ([]string, error) {
	rows, err := s.ch.Conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Service) refreshRegisteredAccountsDailyTx(ctx context.Context, oldCounts map[string]uint64, newCounts map[string]uint64, opts ReindexOptions) error {
	exists, err := s.clickHouseTableExists(ctx, "registered_accounts_daily_tx")
	if err != nil {
		return fmt.Errorf("check ClickHouse registered_accounts_daily_tx: %w", err)
	}
	if !exists {
		return nil
	}

	daySet := make(map[string]struct{}, len(oldCounts)+len(newCounts))
	for day := range oldCounts {
		daySet[day] = struct{}{}
	}
	for day := range newCounts {
		daySet[day] = struct{}{}
	}
	days := sortedStringKeys(daySet)

	for _, day := range days {
		oldCount := int64(oldCounts[day])
		newCount := int64(newCounts[day])
		delta := newCount - oldCount
		if delta == 0 {
			continue
		}

		opts.progressf("aggregates: adjusting registered_accounts_daily_tx for %s delta=%d", day, delta)

		var current uint64
		currentQuery := fmt.Sprintf("SELECT toUInt64(ifNull(sum(value), 0)) FROM registered_accounts_daily_tx WHERE day = toDate('%s')", day)
		if err := s.ch.Conn.QueryRow(ctx, currentQuery).Scan(&current); err != nil {
			return fmt.Errorf("load registered_accounts_daily_tx current value for %s: %w", day, err)
		}

		updated := int64(current) + delta
		if updated < 0 {
			updated = 0
		}

		where := fmt.Sprintf("day = toDate('%s')", day)
		if err := s.deleteClickHouseWhere(ctx, "registered_accounts_daily_tx", where, opts); err != nil {
			return err
		}
		if updated == 0 {
			continue
		}

		insertSQL := fmt.Sprintf("INSERT INTO registered_accounts_daily_tx (day, value) VALUES (toDate('%s'), %d)", day, updated)
		if err := s.ch.Conn.Exec(ctx, insertSQL); err != nil {
			return fmt.Errorf("adjust registered_accounts_daily_tx for %s: %w", day, err)
		}
	}
	return nil
}

func (s *Service) deleteClickHouseWhere(ctx context.Context, table string, where string, opts ReindexOptions) error {
	opts.progressf("ClickHouse %s: deleting rows where %s", table, where)
	if err := s.ch.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", table, where)); err != nil {
		return fmt.Errorf("delete ClickHouse %s where %s: %w", table, where, err)
	}

	deadline := time.Now().Add(opts.DeleteTimeout)
	lastProgress := time.Now()
	for {
		var remaining uint64
		if err := s.ch.Conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE %s", table, where)).Scan(&remaining); err != nil {
			return fmt.Errorf("count ClickHouse %s where %s after delete: %w", table, where, err)
		}
		if remaining == 0 {
			opts.progressf("ClickHouse %s: delete complete where %s", table, where)
			return nil
		}
		if time.Since(lastProgress) >= opts.ProgressInterval {
			opts.progressf("ClickHouse %s: waiting for delete, remaining_rows=%d where %s", table, remaining, where)
			lastProgress = time.Now()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for ClickHouse delete on %s where %s; %d rows still visible", table, where, remaining)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(opts.DeletePollInterval):
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
