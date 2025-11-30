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
	SignaturesJSON []string `ch:"signatures_json"`
	Memo         string    `ch:"memo"`
	RawLog       string    `ch:"raw_log"`
	LogsJSON     string    `ch:"logs_json"`
}

// Event represents an event in ClickHouse
type Event struct {
	Height     uint64    `ch:"height"`
	BlockTime  time.Time `ch:"block_time"`
	Scope      string    `ch:"scope"` // 'block', 'tx', 'begin_block', 'end_block'
	TxIndex    int16     `ch:"tx_index"`
	EventIndex uint16    `ch:"event_index"`
	EventType  string    `ch:"event_type"`
	AttrKey    string    `ch:"attr_key"`
	AttrValue  string    `ch:"attr_value"`
	TxHash     string    `ch:"tx_hash"`
}

// AccountTx represents an account activity record in ClickHouse
// Used for both transactions and block events
type AccountTx struct {
	AddressID    uint64    `ch:"address_id"`
	Height       uint64    `ch:"height"`
	IndexInBlock uint16    `ch:"index_in_block"`
	BlockTime    time.Time `ch:"block_time"`
	TxHash       string    `ch:"tx_hash"`
	Direction    int8      `ch:"direction"` // 0: unknown, 1: in, 2: out
	MainDenomID  uint16    `ch:"main_denom_id"`
	MainAmount   int64     `ch:"main_amount"`
	IsBlockEvent bool      `ch:"is_block_event"` // true for begin_block/end_block events
	EventScope   int8      `ch:"event_scope"`    // 0: tx, 1: begin_block, 2: end_block
}

// EventScope constants
const (
	EventScopeTx         int8 = 0
	EventScopeBeginBlock int8 = 1
	EventScopeEndBlock   int8 = 2
)

// OraclePrice represents an oracle price in ClickHouse
type OraclePrice struct {
	BlockTime time.Time `ch:"block_time"`
	Height    uint64    `ch:"height"`
	Denom     string    `ch:"denom"`
	Price     float64   `ch:"price"`
	Currency  string    `ch:"currency"`
}

// ValidatorReturn represents a validator return record in ClickHouse
type ValidatorReturn struct {
	BlockTime       time.Time          `ch:"block_time"`
	Height          uint64             `ch:"height"`
	OperatorAddress string             `ch:"operator_address"`
	Commission      map[string]float64 `ch:"commission"`
	Reward          map[string]float64 `ch:"reward"`
}

// BlockReward represents a block reward record in ClickHouse
type BlockReward struct {
	BlockTime       time.Time          `ch:"block_time"`
	Height          uint64             `ch:"height"`
	TotalReward     map[string]float64 `ch:"total_reward"`
	TotalCommission map[string]float64 `ch:"total_commission"`
}
