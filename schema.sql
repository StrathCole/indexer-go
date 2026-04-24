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
    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
    tx_bytes        String,
    codespace       LowCardinality(String),
    code            UInt32,
    tx_response_data String,
    tx_response_info String,
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
    INDEX idx_events_height height TYPE minmax GRANULARITY 1,
    block_time      DateTime64(3),
    scope           Enum8('block' = 0, 'tx' = 1, 'begin_block' = 2, 'end_block' = 3),
    tx_index        Int16,                 -- -1 for block-level events
    event_index     UInt16,
    event_type      LowCardinality(String),
    INDEX idx_events_event_type event_type TYPE bloom_filter(0.01) GRANULARITY 4,
    attr_key        LowCardinality(String),
    INDEX idx_events_attr_key attr_key TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_events_type_key (event_type, attr_key) TYPE bloom_filter(0.01) GRANULARITY 4,
    attr_value      String,
    attr_index      Bool DEFAULT false,
    tx_hash         FixedString(64) DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (height, scope, tx_index, event_index, event_type, attr_key);

-- Search-oriented tx event lookup used by the local /cosmos/tx/v1beta1/txs implementation.
-- Existing deployments should backfill once:
-- INSERT INTO tx_event_lookup SELECT event_type, attr_key, attr_value, height, toUInt16(tx_index), tx_hash FROM events WHERE scope = 'tx' AND tx_index >= 0;
CREATE TABLE IF NOT EXISTS tx_event_lookup (
    event_type      LowCardinality(String),
    attr_key        LowCardinality(String),
    attr_value      String,
    height          UInt64,
    index_in_block  UInt16,
    tx_hash         FixedString(64)
)
ENGINE = MergeTree
ORDER BY (event_type, attr_key, attr_value, height, index_in_block);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tx_event_lookup
TO tx_event_lookup
AS
SELECT
    event_type,
    attr_key,
    attr_value,
    height,
    toUInt16(tx_index) AS index_in_block,
    tx_hash
FROM events
WHERE scope = 'tx' AND tx_index >= 0;

-- Account activity tracking for both transactions and block events
CREATE TABLE IF NOT EXISTS account_txs (
    address_id      UInt64,
    height          UInt64,
    index_in_block  UInt16,
    block_time      DateTime64(3),
    tx_hash         FixedString(64),
    direction       Int8, -- 0: unknown, 1: in, 2: out
    main_denom_id   UInt16,
    main_amount     Int64,
    is_block_event  Bool DEFAULT false,
    event_scope     Int8 DEFAULT 0 -- 0: tx, 1: begin_block, 2: end_block
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (address_id, height, index_in_block, is_block_event);

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

-- Dashboard pre-aggregations
-- Daily unique active accounts from transactions only (used by /v1/dashboard/active_accounts).
-- This intentionally excludes begin_block/end_block derived rows in account_txs (is_block_event=1),
-- which can touch large numbers of accounts and would inflate “active accounts” beyond fcd-classic semantics.
-- NOTE: For existing deployments, this will only populate for new inserts unless you backfill.
CREATE TABLE IF NOT EXISTS account_txs_daily_active_tx (
    day Date,
    active_state AggregateFunction(uniqCombined64, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_account_txs_daily_active_tx
TO account_txs_daily_active_tx
AS
SELECT
    toDate(block_time) AS day,
    uniqCombined64State(address_id) AS active_state
FROM account_txs
WHERE is_block_event = 0
GROUP BY day;

-- First seen timestamp per address_id from transactions only.
-- This intentionally excludes begin_block/end_block derived rows (is_block_event=1).
CREATE TABLE IF NOT EXISTS address_first_seen_tx (
    address_id UInt64,
    first_seen_state AggregateFunction(min, DateTime64(3))
)
ENGINE = AggregatingMergeTree
ORDER BY (address_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_address_first_seen_tx
TO address_first_seen_tx
AS
SELECT
    address_id,
    minState(block_time) AS first_seen_state
FROM account_txs
WHERE is_block_event = 0
GROUP BY address_id;

-- Optional derived table: daily new accounts from transactions only.
-- This is maintained incrementally by ingest (counting newly inserted addresses during tx processing).
CREATE TABLE IF NOT EXISTS registered_accounts_daily_tx (
    day Date,
    value UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day);
