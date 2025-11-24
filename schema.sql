-- ClickHouse Schema

CREATE TABLE IF NOT EXISTS blocks (
    height          UInt64,
    block_hash      FixedString(64),
    block_time      DateTime,
    proposer_address String,
    tx_count        UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (height);

CREATE TABLE IF NOT EXISTS txs (
    height          UInt64,
    index_in_block  UInt16,
    block_time      DateTime,
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
    block_time      DateTime,
    scope           Enum8('block' = 0, 'tx' = 1),
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
    block_time      DateTime,
    tx_hash         FixedString(64),
    direction       Int8, -- 0: unknown, 1: in, 2: out
    main_denom_id   UInt16,
    main_amount     Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (address_id, height, index_in_block);

-- New table for Oracle Prices (Time Series)
CREATE TABLE IF NOT EXISTS oracle_prices (
    block_time      DateTime,
    height          UInt64,
    denom           LowCardinality(String),
    price           Float64,
    currency        LowCardinality(String) -- e.g. 'uusd'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (denom, block_time);
