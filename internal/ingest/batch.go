package ingest

import (
	"context"
	"fmt"

	"github.com/classic-terra/indexer-go/internal/model"
)

func (s *Service) BatchInsert(
	ctx context.Context,
	blocks []model.Block,
	txs []model.Tx,
	events []model.Event,
	accountTxs []model.AccountTx,
	oraclePrices []model.OraclePrice,
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
			)
			if err != nil {
				return fmt.Errorf("failed to append account_tx: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send account_txs batch: %w", err)
		}
	}

	if len(oraclePrices) > 0 {
		batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO oracle_prices")
		if err != nil {
			return fmt.Errorf("failed to prepare oracle_prices batch: %w", err)
		}
		for _, op := range oraclePrices {
			err := batch.Append(
				op.BlockTime,
				op.Height,
				op.Denom,
				op.Price,
				op.Currency,
			)
			if err != nil {
				return fmt.Errorf("failed to append oracle_price: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send oracle_prices batch: %w", err)
		}
	}

	if len(blocks) > 0 {
		batch, err := s.ch.Conn.PrepareBatch(ctx, "INSERT INTO blocks")
		if err != nil {
			return fmt.Errorf("failed to prepare blocks batch: %w", err)
		}
		for _, b := range blocks {
			err := batch.Append(
				b.Height,
				b.BlockHash,
				b.BlockTime,
				b.ProposerAddress,
				b.TxCount,
			)
			if err != nil {
				return fmt.Errorf("failed to append block: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send blocks batch: %w", err)
		}
	}

	return nil
}
