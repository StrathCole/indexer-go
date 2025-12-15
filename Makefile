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
