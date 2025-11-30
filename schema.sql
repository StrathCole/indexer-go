-- ClickHouse Schema

CREATE TABLE IF NOT EXISTS blocks (
    height          UInt64,
    block_hash      FixedString(64),
    block_time      DateTime64(3),
    proposer_address String,
    tx_count        UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (height);

CREATE TABLE IF NOT EXISTS txs (
    height          UInt64,
    index_in_block  UInt16,
    block_time      DateTime64(3),
    tx_hash         FixedString(64),
    codespace       LowCardinality(String),
    code            UInt32,
    gas_wanted      UInt64,
    gas_used        UInt64,
    fee_amounts     Array(Int64),
    fee_denom_ids   Array(UInt16),
    tax_amounts     Array(Int64),
    tax_denom_ids   Array(UInt16),
    msg_type_ids    Array(UInt16),
    msgs_json       Array(String),
    signatures_json Array(String),
    memo            String,
    -- Additional fields for FCD compatibility
    raw_log         String, -- or compressed
    logs_json       String  -- reconstructed logs
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (height, index_in_block);

CREATE TABLE IF NOT EXISTS events (
    height          UInt64,
    block_time      DateTime64(3),
    scope           Enum8('block' = 0, 'tx' = 1, 'begin_block' = 2, 'end_block' = 3),
    tx_index        Int16,                 -- -1 for block-level events
    event_index     UInt16,
    event_type      LowCardinality(String),
    attr_key        LowCardinality(String),
    attr_value      String,
    tx_hash         FixedString(64) DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (event_type, attr_key, height, tx_index, event_index);

CREATE TABLE IF NOT EXISTS account_txs (
    address_id      UInt64,
    height          UInt64,
    index_in_block  UInt16,
    block_time      DateTime64(3),
    tx_hash         FixedString(64),
    direction       Int8, -- 0: unknown, 1: in, 2: out
    main_denom_id   UInt16,
    main_amount     Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (address_id, height, index_in_block);

-- Account-Block relations for addresses found in begin_block and end_block events
CREATE TABLE IF NOT EXISTS account_blocks (
    address_id      UInt64,
    height          UInt64,
    block_time      DateTime64(3),
    scope           Enum8('begin_block' = 0, 'end_block' = 1)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (address_id, height);

-- New table for Oracle Prices (Time Series)
CREATE TABLE IF NOT EXISTS oracle_prices (
    block_time      DateTime64(3),
    height          UInt64,
    denom           LowCardinality(String),
    price           Float64,
    currency        LowCardinality(String) -- e.g. 'uusd'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (denom, block_time);

-- Validator Returns (Historical) - Replaces ValidatorReturnInfoEntity
CREATE TABLE IF NOT EXISTS validator_returns (
    block_time      DateTime64(3),
    height          UInt64,
    operator_address String,
    commission      Map(String, Float64),
    reward          Map(String, Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (operator_address, block_time);

-- Block Rewards (Historical) - Replaces BlockRewardEntity
CREATE TABLE IF NOT EXISTS block_rewards (
    block_time      DateTime64(3),
    height          UInt64,
    total_reward    Map(String, Float64),
    total_commission Map(String, Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (block_time);
