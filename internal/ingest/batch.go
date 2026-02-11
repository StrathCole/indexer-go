package ingest

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/classic-terra/indexer-go/internal/db"
	"github.com/classic-terra/indexer-go/internal/model"
)

func (s *Service) BatchInsert(
	ctx context.Context,
	blocks []model.Block,
	txs []model.Tx,
	events []model.Event,
	accountTxs []model.AccountTx,
	oraclePrices []model.OraclePrice,
	validatorReturns []model.ValidatorReturn,
	blockRewards []model.BlockReward,
) error {
	if len(txs) > 0 {
		batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO txs")
		if err != nil {
			return fmt.Errorf("failed to prepare txs batch: %w", err)
		}
		for _, t := range txs {
			err := batch.Append(
				t.Height,
				t.IndexInBlock,
				t.BlockTime,
				t.TxHash,
				t.Codespace,
				t.Code,
				t.GasWanted,
				t.GasUsed,
				t.FeeAmounts,
				t.FeeDenomIDs,
				t.TaxAmounts,
				t.TaxDenomIDs,
				t.MsgTypeIDs,
				t.MsgsJSON,
				t.SignaturesJSON,
				t.Memo,
				t.RawLog,
				t.LogsJSON,
			)
			if err != nil {
				return fmt.Errorf("failed to append tx: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send txs batch: %w", err)
		}
	}

	if len(events) > 0 {
		batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO events")
		if err != nil {
			return fmt.Errorf("failed to prepare events batch: %w", err)
		}
		for _, e := range events {
			err := batch.Append(
				e.Height,
				e.BlockTime,
				e.Scope,
				e.TxIndex,
				e.EventIndex,
				e.EventType,
				e.AttrKey,
				e.AttrValue,
				e.TxHash,
			)
			if err != nil {
				return fmt.Errorf("failed to append event: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send events batch: %w", err)
		}
	}

	if len(accountTxs) > 0 {
		// Dual-write: ClickHouse (for dashboard aggregations) + PostgreSQL (for account history lookups)
		batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO account_txs")
		if err != nil {
			return fmt.Errorf("failed to prepare account_txs batch: %w", err)
		}
		for _, at := range accountTxs {
			err := batch.Append(
				at.AddressID,
				at.Height,
				at.IndexInBlock,
				at.BlockTime,
				at.TxHash,
				at.Direction,
				at.MainDenomID,
				at.MainAmount,
				at.IsBlockEvent,
				at.EventScope,
			)
			if err != nil {
				return fmt.Errorf("failed to append account_tx: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send account_txs batch: %w", err)
		}

		// PostgreSQL dual-write for fast account-history lookups
		pgAccountTxs := make([]db.PgAccountTx, len(accountTxs))
		for i, at := range accountTxs {
			pgAccountTxs[i] = db.PgAccountTx{
				AddressID:    at.AddressID,
				Height:       at.Height,
				IndexInBlock: at.IndexInBlock,
				BlockTime:    at.BlockTime,
				TxHash:       at.TxHash,
				Direction:    at.Direction,
				MainDenomID:  at.MainDenomID,
				MainAmount:   at.MainAmount,
				IsBlockEvent: at.IsBlockEvent,
				EventScope:   at.EventScope,
			}
		}
		if err := s.pg.InsertAccountTxs(ctx, pgAccountTxs); err != nil {
			log.Printf("Warning: failed to insert account_txs into PostgreSQL: %v", err)
		}
	}

	// Oracle prices → PostgreSQL (migrated from ClickHouse)
	if len(oraclePrices) > 0 {
		pgPrices := make([]db.PgOraclePrice, len(oraclePrices))
		for i, op := range oraclePrices {
			pgPrices[i] = db.PgOraclePrice{
				BlockTime: op.BlockTime,
				Height:    op.Height,
				Denom:     op.Denom,
				Price:     op.Price,
				Currency:  op.Currency,
			}
		}
		if err := s.pg.InsertOraclePrices(ctx, pgPrices); err != nil {
			return fmt.Errorf("failed to insert oracle_prices into PostgreSQL: %w", err)
		}
	}

	if len(validatorReturns) > 0 {
		batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO validator_returns")
		if err != nil {
			return fmt.Errorf("failed to prepare validator_returns batch: %w", err)
		}
		for _, vr := range validatorReturns {
			err := batch.Append(
				vr.BlockTime,
				vr.Height,
				vr.OperatorAddress,
				vr.Commission,
				vr.Reward,
			)
			if err != nil {
				return fmt.Errorf("failed to append validator_return: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send validator_returns batch: %w", err)
		}
	}

	if len(blockRewards) > 0 {
		batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO block_rewards")
		if err != nil {
			return fmt.Errorf("failed to prepare block_rewards batch: %w", err)
		}
		for _, br := range blockRewards {
			err := batch.Append(
				br.BlockTime,
				br.Height,
				br.TotalReward,
				br.TotalCommission,
			)
			if err != nil {
				return fmt.Errorf("failed to append block_reward: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send block_rewards batch: %w", err)
		}
	}

	// Blocks → PostgreSQL (migrated from ClickHouse)
	if len(blocks) > 0 {
		pgBlocks := make([]db.PgBlock, len(blocks))
		for i, b := range blocks {
			pgBlocks[i] = db.PgBlock{
				Height:          b.Height,
				BlockHash:       b.BlockHash,
				BlockTime:       b.BlockTime,
				ProposerAddress: b.ProposerAddress,
				TxCount:         b.TxCount,
			}
		}
		if err := s.pg.InsertBlocks(ctx, pgBlocks); err != nil {
			return fmt.Errorf("failed to insert blocks into PostgreSQL: %w", err)
		}
	}

	return nil
}

func (s *Service) insertRegisteredAccountsDailyTx(ctx context.Context, blockTime time.Time, count uint64) error {
	if count == 0 {
		return nil
	}

	u := blockTime.UTC()
	day := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)

	batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO registered_accounts_daily_tx")
	if err != nil {
		return fmt.Errorf("failed to prepare registered_accounts_daily_tx batch: %w", err)
	}
	if err := batch.Append(day, count); err != nil {
		return fmt.Errorf("failed to append registered_accounts_daily_tx row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send registered_accounts_daily_tx batch: %w", err)
	}
	return nil
}
