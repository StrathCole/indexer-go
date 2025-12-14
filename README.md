# Terra Classic Indexer (Go)

This is a high-performance replacement for the legacy FCD (used by e.g. Finder) indexer, written in Go.
It uses ClickHouse for storing historical data (blocks, txs, events) and PostgreSQL for metadata.

## Architecture

- **indexer-ingest**: Connects to a Terra Classic node (RPC/gRPC), fetches blocks, processes them, and inserts data into ClickHouse.
- **indexer-api**: Serves REST APIs compatible with FCD, querying ClickHouse for history and the Node RPC for live state.

## Swagger

The API serves Swagger UI at `/swagger/` and the raw Swagger 2.0 spec at `/swagger/doc.json`.
If `node.lcd` is configured, the spec is merged with the node's LCD swagger (`/swagger/swagger.yaml`) for a more comprehensive doc set.

## Prerequisites

- Go 1.24+
- ClickHouse (v22 or later recommended)
- PostgreSQL (v14 or later recommended)
- Terra Classic Node (RPC/gRPC access)
- `psql` command-line tool for PostgreSQL.
- `clickhouse-client` command-line tool for ClickHouse.

## Database Setup

This guide explains how to set up the PostgreSQL and ClickHouse databases required for the `indexer-go` service.

### 1. PostgreSQL Setup

PostgreSQL is used for storing relational data such as address mappings, validator information, and the richlist.

#### Create Database and User

First, access your PostgreSQL server:

```bash
sudo -u postgres psql
```

Run the following SQL commands to create the database and user:

```sql
CREATE DATABASE fcd;
CREATE USER fcd_user WITH ENCRYPTED PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE fcd TO fcd_user;
\c fcd
GRANT ALL ON SCHEMA public TO fcd_user;
```

*(Replace `password` with a secure password)*

#### Apply Postgres Schema

Exit the `psql` shell and run the following command to apply the schema from `schema_postgres.sql`:

```bash
psql -h localhost -U fcd_user -d fcd -f schema_postgres.sql
```

You will be prompted for the password you set earlier.

### 2. ClickHouse Setup

ClickHouse is used for storing high-volume time-series data such as blocks, transactions, events, and oracle prices.

#### Create Database

Access the ClickHouse client:

```bash
clickhouse-client
```

Run the following command to create the database:

```sql
CREATE DATABASE IF NOT EXISTS fcd;
```

*(Note: The application defaults to using the `fcd` database, but you can create a specific one like `fcd_terra_classic` and update the configuration)*

#### Create User

It is recommended to create a dedicated user for the indexer.

```sql
CREATE USER IF NOT EXISTS fcd_user IDENTIFIED WITH sha256_password BY 'password';
GRANT ALL ON fcd.* TO fcd_user;
```

*(Replace `password` with a secure password)*

#### Apply ClickHouse Schema

You can apply the schema directly using the `clickhouse-client` and the `schema.sql` file. If you created a user, include the credentials:

```bash
clickhouse-client --user fcd_user --password password --database=fcd --multiquery < schema.sql
```

If you already have an existing `txs` table, make sure it has a data-skipping index on `tx_hash` (used by the single-tx endpoint `/v1/txs/{hash}`). You can add it and materialize it like this:

```sql
ALTER TABLE txs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE txs MATERIALIZE INDEX idx_tx_hash;
```

For fast block-by-height endpoints (which query block-level events by height), it also helps to have a skipping index on `events.height`:

```sql
ALTER TABLE events ADD INDEX IF NOT EXISTS idx_events_height height TYPE minmax GRANULARITY 1;
ALTER TABLE events MATERIALIZE INDEX idx_events_height;
```

If your `events` table is still ordered by `(event_type, attr_key, height, ...)`, block-by-height queries can still be slow even with `idx_events_height`. The most effective fix is to rebuild `events` with a height-first primary key.

Migration options:

1) Small/medium tables: one-time migration (safe rename swap)

```sql
-- 1) Create a new events table with height-first ORDER BY
DROP TABLE IF EXISTS events_v2;
CREATE TABLE events_v2 (
	height          UInt64,
	INDEX idx_events_height height TYPE minmax GRANULARITY 1,
	block_time      DateTime64(3),
	scope           Enum8('block' = 0, 'tx' = 1, 'begin_block' = 2, 'end_block' = 3),
	tx_index        Int16,
	event_index     UInt16,
	event_type      LowCardinality(String),
	INDEX idx_events_event_type event_type TYPE bloom_filter(0.01) GRANULARITY 4,
	attr_key        LowCardinality(String),
	INDEX idx_events_attr_key attr_key TYPE bloom_filter(0.01) GRANULARITY 4,
	INDEX idx_events_type_key (event_type, attr_key) TYPE bloom_filter(0.01) GRANULARITY 4,
	attr_value      String,
	tx_hash         FixedString(64) DEFAULT ''
) ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (height, scope, tx_index, event_index, event_type, attr_key);

-- 2) Backfill
INSERT INTO events_v2 SELECT * FROM events;

-- 3) Atomic swap
RENAME TABLE events TO events_old, events_v2 TO events;

-- 4) (Optional) Drop the old table after verification
-- DROP TABLE events_old;
```

2) Very large tables (disk constrained): partition-by-partition copy + drop

This avoids needing enough disk to hold a full second copy of `events`. You only need headroom for one partition at a time.

```sql
-- Create the destination table once (same DDL as above)
DROP TABLE IF EXISTS events_v2;
CREATE TABLE events_v2 (
	height          UInt64,
	INDEX idx_events_height height TYPE minmax GRANULARITY 1,
	block_time      DateTime64(3),
	scope           Enum8('block' = 0, 'tx' = 1, 'begin_block' = 2, 'end_block' = 3),
	tx_index        Int16,
	event_index     UInt16,
	event_type      LowCardinality(String),
	INDEX idx_events_event_type event_type TYPE bloom_filter(0.01) GRANULARITY 4,
	attr_key        LowCardinality(String),
	INDEX idx_events_attr_key attr_key TYPE bloom_filter(0.01) GRANULARITY 4,
	INDEX idx_events_type_key (event_type, attr_key) TYPE bloom_filter(0.01) GRANULARITY 4,
	attr_value      String,
	tx_hash         FixedString(64) DEFAULT ''
) ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (height, scope, tx_index, event_index, event_type, attr_key);

-- Get partitions present
SELECT DISTINCT toYYYYMM(block_time) AS p FROM events ORDER BY p ASC;

-- For each partition p, copy it and then drop it from the old table (repeat per p)
INSERT INTO events_v2
SELECT * FROM events
WHERE toYYYYMM(block_time) = 202401;

ALTER TABLE events DROP PARTITION 202401;

-- After all partitions are migrated:
RENAME TABLE events TO events_old, events_v2 TO events;
-- DROP TABLE events_old;
```

### Verification

To verify the setup, you can check if the tables exist.

**PostgreSQL:**

```bash
psql -h localhost -U fcd_user -d fcd -c "\dt"
```

Should list: `addresses`, `denoms`, `general_info`, `msg_types`, `rich_list`, `validator_info`.

**ClickHouse:**

```bash
clickhouse-client --query "SHOW TABLES"
```

Should list: `account_txs`, `blocks`, `events`, `oracle_prices`, `txs`.

## Configuration

After setting up the databases, update your `config.toml` or environment variables to point to the correct instances.

**config.toml:**

```toml
[database]
clickhouse_addr = "localhost:9000"
clickhouse_db = "fcd"
clickhouse_user = "fcd_user"
clickhouse_password = "password"
postgres_conn = "postgres://fcd_user:password@localhost:5432/fcd"
```

**Environment Variables:**

```bash
export FCD_DATABASE_CLICKHOUSE_ADDR="localhost:9000"
export FCD_DATABASE_CLICKHOUSE_USER="fcd_user"
export FCD_DATABASE_CLICKHOUSE_PASSWORD="password"
export FCD_DATABASE_POSTGRES_CONN="postgres://fcd_user:password@localhost:5432/fcd"
```

### Backfilling and Range Indexing

If you need to start indexing from a specific height (e.g., if the node has pruned older blocks) or want to index a specific range, you can configure `start_height` and `end_height`.

**config.toml:**

```toml
[ingest]
start_height = 25616144  # Start indexing from this height
end_height = 0           # 0 means run indefinitely
```

**Environment Variables:**

```bash
export FCD_INGEST_START_HEIGHT=25616144
export FCD_INGEST_END_HEIGHT=0
```

## Running

The application consists of two services:

1. **Ingest Service**: Connects to the Terra node (RPC/LCD) and indexes blocks into the databases.
2. **API Service**: Serves the REST API compatible with FCD.

### Build

Build the binaries using `make`:

```bash
make build
```

This will create `indexer-ingest` and `indexer-api` in the `build/` directory.

### Run Ingest Service

Ensure your `config.toml` is in the current directory or configured via environment variables.

```bash
make run-ingest
```

### Run API Service

```bash
make run-api
```

The API will be available at `http://localhost:3000` (or your configured `listen_addr`).

## API Endpoints

- `GET /v1/blocks/latest`
- `GET /v1/blocks/{height}`
- `GET /v1/txs?account={address}&limit=10&offset=0`
- `GET /v1/txs/{hash}`
- `GET /v1/bank/{account}` (Proxies to LCD)
- `GET /v1/dashboard` (Aggregates LCD data)

## Development

The project uses `classic-core` v3.5.1 as a dependency.
Ensure you have the correct replacements in `go.mod` if working locally with `classic-core`.
