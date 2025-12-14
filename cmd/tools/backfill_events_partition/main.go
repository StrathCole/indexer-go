package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/model"

	"github.com/classic-terra/core/v3/app"
	sdk "github.com/cosmos/cosmos-sdk/types"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	tmtypes "github.com/cometbft/cometbft/types"
)

type blockPayload struct {
	height uint64
	events []model.Event
	err    error
}

func fetchBlockAndResults(ctx context.Context, rpc *rpchttp.HTTP, height int64) (*coretypes.ResultBlock, *coretypes.ResultBlockResults, error) {
	maxRetries := 6
	retryDelay := 500 * time.Millisecond

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		block, err := rpc.Block(ctx, &height)
		if err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(retryDelay):
			}
			continue
		}

		results, err := rpc.BlockResults(ctx, &height)
		if err == nil {
			return block, results, nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(retryDelay):
		}
	}
	return nil, nil, lastErr
}

func extractEventsFromBlock(height uint64, blockTime time.Time, blockTxs tmtypes.Txs, results *coretypes.ResultBlockResults, txDecoder sdk.TxDecoder) []model.Event {
	var out []model.Event

	for i, event := range results.BeginBlockEvents {
		for _, attr := range event.Attributes {
			out = append(out, model.Event{
				Height:     height,
				BlockTime:  blockTime,
				Scope:      "begin_block",
				TxIndex:    -1,
				EventIndex: uint16(i),
				EventType:  event.Type,
				AttrKey:    string(attr.Key),
				AttrValue:  string(attr.Value),
				TxHash:     "",
			})
		}
	}

	for i, event := range results.EndBlockEvents {
		for _, attr := range event.Attributes {
			out = append(out, model.Event{
				Height:     height,
				BlockTime:  blockTime,
				Scope:      "end_block",
				TxIndex:    -1,
				EventIndex: uint16(i),
				EventType:  event.Type,
				AttrKey:    string(attr.Key),
				AttrValue:  string(attr.Value),
				TxHash:     "",
			})
		}
	}

	// Match ingest behavior: tx events are only included if the tx can be decoded.
	for txIndex, txBytes := range blockTxs {
		if txIndex >= len(results.TxsResults) {
			continue
		}
		if _, err := txDecoder(txBytes); err != nil {
			continue
		}

		res := results.TxsResults[txIndex]
		txHash := fmt.Sprintf("%X", tmtypes.Tx(txBytes).Hash())

		for eventIndex, event := range res.Events {
			for _, attr := range event.Attributes {
				out = append(out, model.Event{
					Height:     height,
					BlockTime:  blockTime,
					Scope:      "tx",
					TxIndex:    int16(txIndex),
					EventIndex: uint16(eventIndex),
					EventType:  event.Type,
					AttrKey:    string(attr.Key),
					AttrValue:  string(attr.Value),
					TxHash:     txHash,
				})
			}
		}
	}

	return out
}

func insertEvents(ctx context.Context, ch *db.ClickHouse, events []model.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := ch.Conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		return fmt.Errorf("prepare events batch: %w", err)
	}

	for _, e := range events {
		if err := batch.Append(
			e.Height,
			e.BlockTime,
			e.Scope,
			e.TxIndex,
			e.EventIndex,
			e.EventType,
			e.AttrKey,
			e.AttrValue,
			e.TxHash,
		); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send events batch: %w", err)
	}

	return nil
}

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	partition := flag.Int("partition", 0, "Partition in YYYYMM (e.g. 202311)")
	startOverride := flag.Int64("start", 0, "Optional start height override")
	endOverride := flag.Int64("end", 0, "Optional end height override")
	workers := flag.Int("workers", 0, "Number of RPC workers (0 = auto)")
	flushEvents := flag.Int("flush", 200000, "Flush to ClickHouse after this many events")
	rpcTimeout := flag.Duration("rpc-timeout", 20*time.Second, "Timeout per RPC request")
	deleteFirst := flag.Bool("delete-first", false, "Drop the existing events partition first")
	dryRun := flag.Bool("dry-run", false, "Compute range and counts but do not write")
	flag.Parse()

	if *partition == 0 {
		log.Fatalf("--partition is required (e.g. --partition 202311)")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ch, err := db.NewClickHouse(
		cfg.Database.ClickHouseAddr,
		cfg.Database.ClickHouseDB,
		cfg.Database.ClickHouseUser,
		cfg.Database.ClickHousePassword,
	)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}

	ctx := context.Background()

	var minH, maxH uint64
	var blockCount uint64
	if err := ch.Conn.QueryRow(ctx,
		"SELECT min(height), max(height), count() FROM blocks WHERE toYYYYMM(block_time) = ?",
		*partition,
	).Scan(&minH, &maxH, &blockCount); err != nil {
		log.Fatalf("Failed to query blocks partition %d: %v", *partition, err)
	}
	if blockCount == 0 {
		log.Fatalf("No blocks found for partition %d; cannot derive height range", *partition)
	}

	startH := int64(minH)
	endH := int64(maxH)
	if *startOverride > 0 {
		startH = *startOverride
	}
	if *endOverride > 0 {
		endH = *endOverride
	}
	if startH > endH {
		log.Fatalf("Invalid height range: start=%d end=%d", startH, endH)
	}

	var existingEvents uint64
	if err := ch.Conn.QueryRow(ctx,
		"SELECT count() FROM events WHERE toYYYYMM(block_time) = ?",
		*partition,
	).Scan(&existingEvents); err != nil {
		log.Fatalf("Failed to count existing events for partition %d: %v", *partition, err)
	}

	log.Printf("Partition %d: blocks=%d, height range=%d..%d, existing events=%d", *partition, blockCount, startH, endH, existingEvents)

	if *dryRun {
		log.Printf("Dry-run enabled: exiting without changes")
		return
	}

	if *deleteFirst {
		log.Printf("Dropping events partition %d...", *partition)
		if err := ch.Conn.Exec(ctx, "ALTER TABLE events DROP PARTITION ?", *partition); err != nil {
			log.Fatalf("Failed to drop events partition %d: %v", *partition, err)
		}
	}

	rpc, err := rpchttp.New(cfg.Node.RPC, "/websocket")
	if err != nil {
		log.Fatalf("Failed to create RPC client: %v", err)
	}
	if err := rpc.Start(); err != nil {
		log.Fatalf("Failed to start RPC client: %v", err)
	}
	defer rpc.Stop()

	encCfg := app.MakeEncodingConfig()
	txDecoder := encCfg.TxConfig.TxDecoder()

	w := *workers
	if w <= 0 {
		w = runtime.GOMAXPROCS(0)
		if w > 8 {
			w = 8
		}
		if w < 1 {
			w = 1
		}
	}

	jobs := make(chan int64, w*2)
	results := make(chan blockPayload, w*2)

	var processed uint64
	var failed uint64

	var wg sync.WaitGroup
	for i := 0; i < w; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range jobs {
				rpcCtx, cancel := context.WithTimeout(ctx, *rpcTimeout)
				block, res, err := fetchBlockAndResults(rpcCtx, rpc, h)
				cancel()

				if err != nil {
					results <- blockPayload{height: uint64(h), err: err}
					continue
				}

				evs := extractEventsFromBlock(uint64(h), block.Block.Time, block.Block.Txs, res, txDecoder)
				results <- blockPayload{height: uint64(h), events: evs}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	go func() {
		for h := startH; h <= endH; h++ {
			jobs <- h
		}
		close(jobs)
	}()

	bufCap := *flushEvents
	if bufCap < 1000 {
		bufCap = 1000
	}
	buf := make([]model.Event, 0, bufCap)

	lastLog := time.Now()

	for r := range results {
		if r.err != nil {
			atomic.AddUint64(&failed, 1)
			if atomic.LoadUint64(&failed) <= 20 {
				log.Printf("Failed height %d: %v", r.height, r.err)
			}
			continue
		}

		atomic.AddUint64(&processed, 1)
		buf = append(buf, r.events...)

		if len(buf) >= bufCap {
			if err := insertEvents(ctx, ch, buf); err != nil {
				log.Fatalf("Insert failed (buffered): %v", err)
			}
			buf = buf[:0]
		}

		if time.Since(lastLog) > 10*time.Second {
			p := atomic.LoadUint64(&processed)
			f := atomic.LoadUint64(&failed)
			log.Printf("Progress: processed=%d heights, failed=%d, bufferedEvents=%d", p, f, len(buf))
			lastLog = time.Now()
		}
	}

	if len(buf) > 0 {
		if err := insertEvents(ctx, ch, buf); err != nil {
			log.Fatalf("Insert failed (final flush): %v", err)
		}
	}

	p := atomic.LoadUint64(&processed)
	f := atomic.LoadUint64(&failed)
	log.Printf("Done. processed=%d heights, failed=%d", p, f)

	var finalEvents uint64
	if err := ch.Conn.QueryRow(ctx,
		"SELECT count() FROM events WHERE toYYYYMM(block_time) = ?",
		*partition,
	).Scan(&finalEvents); err != nil {
		log.Fatalf("Failed to count events after backfill: %v", err)
	}
	log.Printf("Partition %d now has %d events", *partition, finalEvents)
}
