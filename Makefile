.PHONY: all build build-ingest build-api clean run-ingest run-api test lint

all: build

build: build-ingest build-api

build-ingest:
	@mkdir -p build
	go build -o build/indexer-ingest ./cmd/indexer-ingest

build-api:
	@mkdir -p build
	go build -o build/indexer-api ./cmd/indexer-api

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
