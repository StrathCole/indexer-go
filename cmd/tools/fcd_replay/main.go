package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/classic-terra/indexer-go/internal/config"
	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/ingest"
	"github.com/classic-terra/indexer-go/internal/model"
)

func main() {
	configPath := flag.String("config", ".", "Path to config directory")
	fcdBaseURL := flag.String("fcd-base-url", "https://terra-classic-fcd.publicnode.com", "FCD base URL or /v1 base URL")
	height := flag.Int64("height", 0, "Single block height to replay")
	fromHeight := flag.Int64("from-height", 0, "Inclusive start height for replay range")
	toHeight := flag.Int64("to-height", 0, "Inclusive end height for replay range")
	dryRun := flag.Bool("dry-run", false, "Fetch and convert blocks without inserting data")
	continueOnError := flag.Bool("continue-on-error", true, "Deprecated compatibility flag; failed heights always stop replay")
	progressEvery := flag.Int("progress-every", 100, "Log a progress line every N processed heights")
	requestTimeout := flag.Duration("request-timeout", 30*time.Second, "Per-block fetch and insert timeout")
	maxRetries := flag.Int("max-retries", 5, "Maximum retries for transient failures per height")
	retryBackoff := flag.Duration("retry-backoff", time.Second, "Initial retry backoff; it doubles after each failed attempt")
	flag.Parse()

	start, end, err := collectTargetRange(*height, *fromHeight, *toHeight)
	if err != nil {
		log.Fatalf("Invalid target range: %v", err)
	}
	if *progressEvery <= 0 {
		log.Fatalf("--progress-every must be > 0")
	}
	if *requestTimeout <= 0 {
		log.Fatalf("--request-timeout must be > 0")
	}
	if *maxRetries < 0 {
		log.Fatalf("--max-retries must be >= 0")
	}
	if *retryBackoff <= 0 {
		log.Fatalf("--retry-backoff must be > 0")
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

	pg, err := db.NewPostgres(cfg.Database.PostgresConn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}

	svc, err := ingest.NewService(
		ch,
		pg,
		cfg.Node.RPC,
		cfg.Node.GRPC,
		0,
		0,
		1,
		1,
		0,
		0,
		0,
		false,
	)
	if err != nil {
		log.Fatalf("Failed to initialize ingest service: %v", err)
	}

	total := end - start + 1
	log.Printf("FCD replay range %d..%d (%d blocks), dry_run=%t", start, end, total, *dryRun)
	log.Printf("Safety rule: any height with existing block marker or tx rows is skipped and never rewritten")

	var replayed int64
	var skipped int64
	var failed int64
	var txsInserted int64
	var eventsInserted int64
	var accountTxsInserted int64

	for offset := int64(0); offset < total; offset++ {
		currentHeight := start + offset
		outcome, err := replayHeightWithRetry(
			context.Background(),
			svc,
			*fcdBaseURL,
			currentHeight,
			*dryRun,
			*requestTimeout,
			*maxRetries,
			*retryBackoff,
			func(retry int, delay time.Duration, retryErr error) {
				log.Printf(
					"Height %d: retry %d/%d in %s after %v",
					currentHeight,
					retry,
					*maxRetries,
					delay,
					retryErr,
				)
			},
		)
		if err != nil {
			stage := replayErrorStage(err)
			if *continueOnError {
				log.Printf("Height %d: %s failed; stopping replay so this height is not skipped", currentHeight, stage)
			}
			log.Fatalf("Height %d: %s failed after %d retries: %v", currentHeight, stage, *maxRetries, err)
		}

		if outcome.skipped {
			skipped++
			if shouldLogProgress(offset+1, total, *progressEvery) {
				log.Printf("Progress %d/%d: skipped=%d replayed=%d failed=%d", offset+1, total, skipped, replayed, failed)
			}
			continue
		}
		replayed++
		txsInserted += outcome.txs
		eventsInserted += outcome.events
		accountTxsInserted += outcome.accountTxs

		if shouldLogProgress(offset+1, total, *progressEvery) {
			log.Printf(
				"Progress %d/%d: skipped=%d replayed=%d failed=%d txs=%d events=%d account_txs=%d",
				offset+1,
				total,
				skipped,
				replayed,
				failed,
				txsInserted,
				eventsInserted,
				accountTxsInserted,
			)
		}
	}

	log.Printf(
		"Finished FCD replay. range=%d..%d replayed=%d skipped=%d failed=%d dry_run=%t txs=%d events=%d account_txs=%d",
		start,
		end,
		replayed,
		skipped,
		failed,
		*dryRun,
		txsInserted,
		eventsInserted,
		accountTxsInserted,
	)
}

func collectTargetRange(singleHeight, fromHeight, toHeight int64) (int64, int64, error) {
	if singleHeight < 0 || fromHeight < 0 || toHeight < 0 {
		return 0, 0, fmt.Errorf("heights must be non-negative")
	}
	if singleHeight > 0 && (fromHeight > 0 || toHeight > 0) {
		return 0, 0, fmt.Errorf("use either --height or --from-height/--to-height")
	}
	if singleHeight > 0 {
		return singleHeight, singleHeight, nil
	}
	if fromHeight <= 0 || toHeight <= 0 {
		return 0, 0, fmt.Errorf("provide --height or both --from-height and --to-height")
	}
	if fromHeight > toHeight {
		return 0, 0, fmt.Errorf("--from-height %d is greater than --to-height %d", fromHeight, toHeight)
	}
	return fromHeight, toHeight, nil
}

func shouldLogProgress(processed, total int64, every int) bool {
	if processed == total {
		return true
	}
	return processed%int64(every) == 0
}

type replayStage string

const (
	replayStageExistingData  replayStage = "existing-data check"
	replayStageFetchConvert  replayStage = "fetch/convert"
	replayStageInsert        replayStage = "insert"
	replayStagePartialInsert replayStage = "partial insert"
	maxRetryBackoff                      = 30 * time.Second
)

type replayError struct {
	stage replayStage
	err   error
}

func (e *replayError) Error() string {
	return e.err.Error()
}

func (e *replayError) Unwrap() error {
	return e.err
}

type replayOutcome struct {
	skipped    bool
	txs        int64
	events     int64
	accountTxs int64
}

func replayHeightWithRetry(
	parentCtx context.Context,
	svc *ingest.Service,
	fcdBaseURL string,
	height int64,
	dryRun bool,
	requestTimeout time.Duration,
	maxRetries int,
	retryBackoff time.Duration,
	onRetry func(retry int, delay time.Duration, err error),
) (replayOutcome, error) {
	var outcome replayOutcome
	insertAttempted := false
	err := retryWithExponentialBackoff(parentCtx, maxRetries, retryBackoff, func() error {
		attemptCtx, cancel := context.WithTimeout(parentCtx, requestTimeout)
		defer cancel()

		nextOutcome, err := replayHeight(attemptCtx, svc, fcdBaseURL, height, dryRun)
		if err != nil {
			if replayErrorHasStage(err, replayStageInsert) {
				insertAttempted = true
			}
			return err
		}
		if insertAttempted && nextOutcome.skipped {
			return &replayError{
				stage: replayStagePartialInsert,
				err:   fmt.Errorf("height %d has stored rows after a previous insert failure", height),
			}
		}
		outcome = nextOutcome
		return nil
	}, onRetry)
	return outcome, err
}

func replayHeight(
	ctx context.Context,
	svc *ingest.Service,
	fcdBaseURL string,
	height int64,
	dryRun bool,
) (replayOutcome, error) {
	exists, err := svc.HeightHasStoredData(ctx, height)
	if err != nil {
		return replayOutcome{}, &replayError{stage: replayStageExistingData, err: err}
	}
	if exists {
		return replayOutcome{skipped: true}, nil
	}

	block, txs, events, accountTxs, err := svc.FetchAndConvertFCDBlock(ctx, fcdBaseURL, height)
	if err != nil {
		return replayOutcome{}, &replayError{stage: replayStageFetchConvert, err: err}
	}

	if !dryRun {
		if err := svc.BatchInsert(ctx, []model.Block{block}, txs, events, accountTxs, nil, nil, nil); err != nil {
			return replayOutcome{}, &replayError{stage: replayStageInsert, err: err}
		}
	}

	return replayOutcome{
		txs:        int64(len(txs)),
		events:     int64(len(events)),
		accountTxs: int64(len(accountTxs)),
	}, nil
}

func retryWithExponentialBackoff(
	ctx context.Context,
	maxRetries int,
	initialBackoff time.Duration,
	operation func() error,
	onRetry func(retry int, delay time.Duration, err error),
) error {
	if maxRetries < 0 {
		return fmt.Errorf("max retries must be >= 0")
	}
	if initialBackoff <= 0 {
		return fmt.Errorf("initial backoff must be > 0")
	}

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := operation()
		if err == nil {
			return nil
		}
		if attempt >= maxRetries || !isRetryableReplayError(err) {
			return err
		}

		delay := retryDelay(initialBackoff, attempt)
		if onRetry != nil {
			onRetry(attempt+1, delay, err)
		}
		if err := waitForRetry(ctx, delay); err != nil {
			return err
		}
	}
}

func isRetryableReplayError(err error) bool {
	var replayErr *replayError
	if !errors.As(err, &replayErr) {
		return errors.Is(err, context.DeadlineExceeded)
	}
	if errors.Is(err, context.Canceled) {
		return false
	}

	switch replayErr.stage {
	case replayStageExistingData, replayStageInsert:
		return true
	case replayStageFetchConvert:
		return ingest.IsRetryableFCDReplayError(err)
	default:
		return false
	}
}

func replayErrorHasStage(err error, stage replayStage) bool {
	var replayErr *replayError
	return errors.As(err, &replayErr) && replayErr.stage == stage
}

func replayErrorStage(err error) string {
	var replayErr *replayError
	if errors.As(err, &replayErr) {
		return string(replayErr.stage)
	}
	return "replay"
}

func retryDelay(initialBackoff time.Duration, retry int) time.Duration {
	if retry <= 0 {
		if initialBackoff > maxRetryBackoff {
			return maxRetryBackoff
		}
		return initialBackoff
	}

	delay := initialBackoff
	for i := 0; i < retry; i++ {
		if delay >= maxRetryBackoff/2 {
			return maxRetryBackoff
		}
		delay *= 2
	}
	if delay > maxRetryBackoff {
		return maxRetryBackoff
	}
	return delay
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
