package model

import (
	"time"
)

// Block represents a block in ClickHouse
type Block struct {
	Height          uint64    `ch:"height"`
	BlockHash       string    `ch:"block_hash"`
	BlockTime       time.Time `ch:"block_time"`
	ProposerAddress string    `ch:"proposer_address"`
	TxCount         uint32    `ch:"tx_count"`
}

// Tx represents a transaction in ClickHouse
type Tx struct {
	Height       uint64    `ch:"height"`
	IndexInBlock uint16    `ch:"index_in_block"`
	BlockTime    time.Time `ch:"block_time"`
	TxHash       string    `ch:"tx_hash"`
	Codespace    string    `ch:"codespace"`
	Code         uint32    `ch:"code"`
	GasWanted    uint64    `ch:"gas_wanted"`
	GasUsed      uint64    `ch:"gas_used"`
	FeeAmounts   []int64   `ch:"fee_amounts"`
	FeeDenomIDs  []uint16  `ch:"fee_denom_ids"`
	TaxAmounts   []int64   `ch:"tax_amounts"`
	TaxDenomIDs  []uint16  `ch:"tax_denom_ids"`
	MsgTypeIDs   []uint16  `ch:"msg_type_ids"`
	MsgsJSON     []string  `ch:"msgs_json"`
	Memo         string    `ch:"memo"`
	RawLog       string    `ch:"raw_log"`
	LogsJSON     string    `ch:"logs_json"`
}

// Event represents an event in ClickHouse
type Event struct {
	Height     uint64    `ch:"height"`
	BlockTime  time.Time `ch:"block_time"`
	Scope      string    `ch:"scope"` // 'block' or 'tx'
	TxIndex    int16     `ch:"tx_index"`
	EventIndex uint16    `ch:"event_index"`
	EventType  string    `ch:"event_type"`
	AttrKey    string    `ch:"attr_key"`
	AttrValue  string    `ch:"attr_value"`
	TxHash     string    `ch:"tx_hash"`
}

// AccountTx represents an account transaction in ClickHouse
type AccountTx struct {
	AddressID    uint64    `ch:"address_id"`
	Height       uint64    `ch:"height"`
	IndexInBlock uint16    `ch:"index_in_block"`
	BlockTime    time.Time `ch:"block_time"`
	TxHash       string    `ch:"tx_hash"`
	Direction    int8      `ch:"direction"`
	MainDenomID  uint16    `ch:"main_denom_id"`
	MainAmount   int64     `ch:"main_amount"`
}

// OraclePrice represents an oracle price in ClickHouse
type OraclePrice struct {
	BlockTime time.Time `ch:"block_time"`
	Height    uint64    `ch:"height"`
	Denom     string    `ch:"denom"`
	Price     float64   `ch:"price"`
	Currency  string    `ch:"currency"`
}
