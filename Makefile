.PHONY: build test vet lint clean proto python-test python-lint all

all: build vet test

# Go targets
build:
	go build ./...

test:
	go test ./... -v

vet:
	go vet ./...

lint:
	golangci-lint run ./...

clean:
	go clean ./...
	rm -rf bin/

proto:
	./tools/proto-gen.sh

# Python targets
python-test:
	cd python && python -m pytest tests/ -v

python-lint:
	cd python && ruff check src/ tests/

# Build specific binaries
bin/%:
	go build -o bin/$* ./cmd/$*
