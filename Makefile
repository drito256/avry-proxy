.PHONY: build test lint clean run-proxy run-worker

BINARY_DIR := bin

build:
	@mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_DIR)/avry-proxy ./cmd/proxy
	go build -o $(BINARY_DIR)/backend-worker ./cmd/worker

test:
	go test -v -race ./...

clean:
	rm -rf $(BINARY_DIR)
