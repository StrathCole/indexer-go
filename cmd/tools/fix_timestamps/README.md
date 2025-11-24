# Timestamp Fixer Tool

This tool migrates the ClickHouse schema to use `DateTime64(3)` (millisecond precision) and fixes existing data by fetching correct timestamps from the RPC node.

## Usage

1. Build the tool:

    ```bash
    go build -o fix_timestamps main.go
    ```

2. Run schema migration (ALTER TABLE):

    ```bash
    ./fix_timestamps --config ../../../ --migrate
    ```

    Note: This will execute `ALTER TABLE ... MODIFY COLUMN ...` for all relevant tables. This operation might take time depending on data size.

3. Run data fix:

    ```bash
    ./fix_timestamps --config ../../../ --fix --limit 1000
    ```

    This will:
    - Find blocks where the timestamp has 0 milliseconds (e.g. `...12.000`).
    - Fetch the block from RPC.
    - If RPC timestamp has milliseconds, update the database.
    - It processes blocks in batches (default limit 1000). Run it repeatedly until no more blocks are found.

## Configuration

The tool uses the same `config.toml` as the indexer. Point to the directory containing `config.toml` using the `--config` flag.
