# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

SHELL := /bin/bash
BIN_DIR := bin
LIB_DIR := lib

.PHONY: all build fmt test test-race test-containers lint check tidy clean wasm bindings-c bindings-python demo

all: build test

build:
	@mkdir -p $(BIN_DIR)
	go build -v -ldflags="-s -w" -o $(BIN_DIR)/xtable ./cmd/xtable
	go build -v -ldflags="-s -w" -o $(BIN_DIR)/xtable-service ./cmd/xtable-service
	@echo "✓ Binaries built in $(BIN_DIR)/"

fmt:
	gofmt -w .

test:
	go test -short -v ./...

test-race:
	go test -short -race -v ./...

test-containers:
	go test -race -v -count=1 ./test/...

lint:
	golangci-lint run ./...

# Deliberately does NOT depend on `fmt`: reformatting first would make the
# gofmt stage unable to ever fail.
check:
	@echo "==> gofmt"
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	@echo "==> go vet"
	go vet ./...
	@echo "==> go vet (js/wasm)"
	GOOS=js GOARCH=wasm go vet ./cmd/xtable-wasm
	@echo "==> go test -short"
	go test -short ./...
	@echo "==> golangci-lint"
	golangci-lint run ./...
	@echo "✓ All checks passed"

tidy:
	go mod tidy
	git diff --exit-code go.mod go.sum

wasm:
	@mkdir -p $(BIN_DIR)
	GOOS=js GOARCH=wasm go build -v -ldflags="-s -w" -o $(BIN_DIR)/xtable.wasm ./cmd/xtable-wasm
	@echo "✓ WebAssembly built: $(BIN_DIR)/xtable.wasm"
	@echo "  ⚠ experimental: never executed in a browser, no test coverage, S3/Glue/Iceberg-REST unreachable"

bindings-c:
	@mkdir -p $(LIB_DIR)
	CGO_ENABLED=1 go build -buildmode=c-shared -o $(LIB_DIR)/libxtable.dylib ./bindings/c || \
	CGO_ENABLED=1 go build -buildmode=c-shared -o $(LIB_DIR)/libxtable.so ./bindings/c
	@echo "✓ C-shared library built in $(LIB_DIR)/"

demo: build
	go run ./demo/gen_sample.go
	./bin/xtable sync --config ./demo/my_dataset.yaml
	./bin/xtable inspect --path ./demo/sample_delta_table --format iceberg

clean:
	rm -rf $(BIN_DIR) $(LIB_DIR) demo/sample_delta_table

bindings-python: bindings-c
	@echo "Building Python package with bundled native library..."
	@if [ -f $(LIB_DIR)/libxtable.dylib ]; then \
		cp $(LIB_DIR)/libxtable.dylib bindings/python/pyxtable/; \
		echo "Copied libxtable.dylib for macOS"; \
	else \
		cp $(LIB_DIR)/libxtable.so bindings/python/pyxtable/; \
		echo "Copied libxtable.so for Linux"; \
	fi
	@cd bindings/python && python -m build 2>/dev/null || echo "Note: python -m build install required: pip install build"
	@echo "✓ Python package built in bindings/python/dist/"
	@echo "Note: Wheels are platform-specific (libxtable.dylib/.so cannot be bundled together)"

bindings-python-clean:
	@echo "Cleaning Python package build artifacts..."
	@rm -f bindings/python/pyxtable/libxtable.*
	@rm -rf bindings/python/build bindings/python/dist
	@rm -rf bindings/python/pyxtable.egg-info
	@echo "✓ Python artifacts cleaned"
