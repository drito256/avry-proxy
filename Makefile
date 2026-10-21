.PHONY: build test bench lint clean

BINARY_DIR := bin

build:
	@mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_DIR)/avry-proxy ./cmd/proxy
	go build -o $(BINARY_DIR)/backend-worker ./cmd/worker
	go build -o $(BINARY_DIR)/bench ./cmd/bench

test:
	go test -v -race ./...

bench:
	./benchmarks/run_experiment.sh

clean:
	rm -rf $(BINARY_DIR)
