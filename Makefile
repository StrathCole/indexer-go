.PHONY: all build build-ingest build-api build-tools clean run-ingest run-api test lint

all: build

build: build-ingest build-api build-tools

build-ingest:
	@mkdir -p build
	go build -o build/indexer-ingest ./cmd/indexer-ingest

build-api:
	@mkdir -p build
	go build -o build/indexer-api ./cmd/indexer-api

build-tools:
	@mkdir -p build
	go build -o build/fix_tax ./cmd/tools/fix_tax
	go build -o build/fix_timestamps ./cmd/tools/fix_timestamps
	go build -o build/backfill_account_blocks ./cmd/tools/backfill_account_blocks
	go build -o build/backfill_dashboard_aggregates ./cmd/tools/backfill_dashboard_aggregates
	go build -o build/repair_missing_tx_data ./cmd/tools/repair_missing_tx_data
	go build -o build/reindex_blocks ./cmd/tools/reindex_blocks
	go build -o build/backfill_tx_search ./cmd/tools/backfill_tx_search
	go build -o build/migrate_ch_to_pg ./cmd/tools/migrate_ch_to_pg

clean:
	rm -rf build

run-ingest: build-ingest
	./build/indexer-ingest

run-api: build-api
	./build/indexer-api

test:
	go test ./...

lint:
	golangci-lint run
