-- Postgres Schema

CREATE TABLE IF NOT EXISTS addresses (
    id        BIGSERIAL PRIMARY KEY,
    address   TEXT UNIQUE NOT NULL,
    type      TEXT NOT NULL,  -- e.g. 'account', 'contract', 'validator', 'module'
    last_seen_height BIGINT NOT NULL DEFAULT 0,
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT 'epoch'
);

CREATE INDEX IF NOT EXISTS idx_addresses_address ON addresses(address);
CREATE INDEX IF NOT EXISTS idx_addresses_type    ON addresses(type);
CREATE INDEX IF NOT EXISTS idx_addresses_last_seen_at ON addresses(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_addresses_last_seen_height ON addresses(last_seen_height);

CREATE TABLE IF NOT EXISTS denoms (
    id    SMALLSERIAL PRIMARY KEY,
    denom TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS msg_types (
    id        SMALLSERIAL PRIMARY KEY,
    msg_type  TEXT UNIQUE NOT NULL
);

-- Validator Info (Snapshot / Latest State)
CREATE TABLE IF NOT EXISTS validator_info (
    operator_address    TEXT PRIMARY KEY,
    consensus_pubkey    TEXT,
    account_address     TEXT,
    moniker             TEXT,
    identity            TEXT,
    website             TEXT,
    details             TEXT,
    profile_icon        TEXT,
    tokens              TEXT,
    delegator_shares    TEXT,
    voting_power_amount TEXT,
    voting_power_weight TEXT,
    commission_rate     TEXT,
    commission_max_rate TEXT,
    commission_max_change_rate TEXT,
    commission_update_time TEXT,
    status              TEXT,
    uptime              FLOAT,
    self_delegation_amount TEXT,
    self_delegation_weight TEXT,
    reward_pool_total   TEXT,
    reward_pool_denoms  JSONB, -- Array of {denom, amount}
    staking_return      TEXT,
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- General Info (Snapshot / History)
-- FCD stores history of this, so maybe we should keep history.
CREATE TABLE IF NOT EXISTS general_info (
    id                  BIGSERIAL PRIMARY KEY,
    datetime            TIMESTAMP NOT NULL,
    tax_rate            TEXT,
    issuances           JSONB, -- Map of denom -> amount
    community_pool      JSONB, -- Map of denom -> amount
    bonded_tokens       TEXT,
    not_bonded_tokens   TEXT,
    staking_ratio       TEXT,
    tax_caps            JSONB  -- Array of {denom, taxCap}
);

CREATE INDEX IF NOT EXISTS idx_general_info_datetime ON general_info(datetime);

-- Rich List (Snapshot)
CREATE TABLE IF NOT EXISTS rich_list (
    id          BIGSERIAL PRIMARY KEY,
    denom       TEXT NOT NULL,
    account     TEXT NOT NULL,
    amount      TEXT NOT NULL,
    percentage  FLOAT NOT NULL,
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- Staging table for atomic richlist rebuilds
-- Build into rich_list_build, then swap table names in a short transaction.
CREATE TABLE IF NOT EXISTS rich_list_build (LIKE rich_list INCLUDING ALL);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rich_list_denom_account ON rich_list(denom, account);
CREATE INDEX IF NOT EXISTS idx_rich_list_account ON rich_list(account);

CREATE INDEX IF NOT EXISTS idx_rich_list_denom_amount ON rich_list(denom, amount DESC);

-- Rich List metadata (single-row table)
CREATE TABLE IF NOT EXISTS rich_list_meta (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_full_rebuild     TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    last_incremental_run  TIMESTAMPTZ NOT NULL DEFAULT 'epoch'
);

INSERT INTO rich_list_meta (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- ============================================================
-- Migrated from ClickHouse for faster point-lookup access
-- ============================================================

-- Blocks (migrated from ClickHouse)
CREATE TABLE IF NOT EXISTS blocks (
    height           BIGINT PRIMARY KEY,
    block_hash       CHAR(64) NOT NULL,
    block_time       TIMESTAMPTZ NOT NULL,
    proposer_address TEXT NOT NULL,
    tx_count         INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_blocks_block_time ON blocks(block_time);

-- Oracle Prices (migrated from ClickHouse)
CREATE TABLE IF NOT EXISTS oracle_prices (
    id               BIGSERIAL PRIMARY KEY,
    block_time       TIMESTAMPTZ NOT NULL,
    height           BIGINT NOT NULL,
    denom            TEXT NOT NULL,
    price            DOUBLE PRECISION NOT NULL,
    currency         TEXT NOT NULL DEFAULT 'uusd'
);

CREATE INDEX IF NOT EXISTS idx_oracle_prices_denom_time ON oracle_prices(denom, block_time DESC);
CREATE INDEX IF NOT EXISTS idx_oracle_prices_time ON oracle_prices(block_time);

-- Account Txs (dual-written: ClickHouse for dashboard aggregations,
--              PostgreSQL for fast account-history lookups)
CREATE TABLE IF NOT EXISTS account_txs (
    address_id       BIGINT NOT NULL,
    height           BIGINT NOT NULL,
    index_in_block   SMALLINT NOT NULL,
    block_time       TIMESTAMPTZ NOT NULL,
    tx_hash          CHAR(64) NOT NULL,
    direction        SMALLINT NOT NULL DEFAULT 0,
    main_denom_id    SMALLINT NOT NULL DEFAULT 0,
    main_amount      BIGINT NOT NULL DEFAULT 0,
    is_block_event   BOOLEAN NOT NULL DEFAULT false,
    event_scope      SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (address_id, height, index_in_block, is_block_event)
);

-- Main query: account history, paginated descending
CREATE INDEX IF NOT EXISTS idx_account_txs_addr_height
    ON account_txs(address_id, height DESC, index_in_block DESC);

CREATE INDEX IF NOT EXISTS idx_account_txs_time ON account_txs(block_time);
CREATE INDEX IF NOT EXISTS idx_account_txs_hash ON account_txs(tx_hash);
