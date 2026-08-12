<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache XTable (Go) Agent Guide

This document defines repository-specific instructions, architecture rules, and validation workflows for AI coding agents working on **Apache XTable in Go ([`incubator-xtable-go`](file:///Users/slachiewicz/oss/incubator-xtable-go))**.

---

## 1. Project Mission & Invariants

Apache XTable (Go) provides **omni-directional, zero-copy metadata translation** across open lakehouse table formats (**Apache Iceberg**, **Delta Lake**, **Apache Hudi**, **Apache Paimon**, and raw **Parquet**), as well as catalog synchronization (**AWS Glue**, **Hive Metastore**, **Iceberg REST**).

### Core Invariants:
1. **Zero Data File Rewrites**: XTable translates and generates *metadata only*. It MUST NEVER alter, rewrite, or move physical Parquet/ORC data files.
2. **Schema & Field ID Integrity**: Field IDs, nullability, and data types MUST be accurately preserved across format translations (crucial for Iceberg time travel and schema evolution).
3. **No JVM / Hadoop Dependencies**: All code in this repository MUST be pure Go. Never introduce Java, Scala, Spark, or Hadoop XML configuration dependencies.
4. **Embedded Sync Continuity**: All target converters MUST embed and read `TableSyncMetadata` (`xtable_last_instant_synced`, `xtable_source_format`) to guarantee incremental synchronization safety.

---

## 2. Repository Layout & Architecture

```
.
├── cmd/
│   ├── xtable/               # Main CLI tool (sync, inspect, version)
│   └── xtable-service/       # REST Service & continuous daemon
├── pkg/
│   ├── model/                # Canonical pivot domain model
│   │   ├── types.go          # Canonical lakehouse types (TypeInt, TypeString, etc.)
│   │   ├── schema.go         # Schema tree, Field, PartitionField, transforms
│   │   ├── stats.go          # Range, ColumnStat (min/max, nulls, NaNs), PartitionValue
│   │   ├── datafile.go       # DataFile, DeletionVector, PartitionFileGroup
│   │   ├── table.go          # Table metadata descriptor & layout strategy
│   │   ├── snapshot.go       # Snapshot state
│   │   ├── changes.go        # TableChange & IncrementalTableChanges
│   │   ├── diff.go           # FilesDiff calculation
│   │   └── metadata.go       # TableSyncMetadata properties
│   ├── spi/                  # Service Provider Interfaces
│   │   ├── source.go         # ConversionSource interface
│   │   ├── target.go         # ConversionTarget interface
│   │   └── sync.go           # SyncMode, SyncStatusCode, SyncResult
│   ├── io/                   # Unified storage abstraction
│   │   ├── storage.go        # Storage interface, NewStorageForPath, JoinPath
│   │   ├── local.go          # Local filesystem implementation
│   │   ├── memory.go         # Thread-safe in-memory virtual storage
│   │   └── s3.go             # Native AWS S3 cloud storage (aws-sdk-go-v2)
│   ├── formats/              # Table format adapters
│   │   ├── delta/            # Delta Lake (JSON actions, schema mapping, source/target)
│   │   ├── iceberg/          # Apache Iceberg (metadata v2/v3, manifest/schema mapping)
│   │   ├── hudi/             # Apache Hudi (.hoodie timeline, properties, source/target)
│   │   ├── parquet/          # Raw Parquet directory crawler & stats extractor
│   │   └── paimon/           # Apache Paimon snapshot & manifest reader
│   ├── catalog/              # Catalog sync clients (Glue, HMS, REST)
│   └── conversion/           # Orchestrator & Controllers
│       ├── controller.go     # ConversionController
│       └── config.go         # DatasetConfig & Config parsing
├── demo/                     # Sample datasets and demonstration generators
├── test/                     # End-to-end integration test suites
├── go.mod
├── go.sum
└── AGENTS.md
```

---

## 3. Development, Build & Testing Workflows

### Prerequisites
- Go 1.22+ (tested with Go 1.26+)
- `golangci-lint` (for code quality audits)

### Common Commands
```bash
# Run all unit and package tests
go test -v ./...

# Run tests with race detection
go test -race ./...

# Build the CLI binary
go build -o bin/xtable ./cmd/xtable

# Run linter
golangci-lint run ./...

# Tidy dependencies
go mod tidy
```

### Targeted Testing While Iterating
```bash
# Test specific format adapter
go test -v ./pkg/formats/delta
go test -v ./pkg/formats/iceberg
go test -v ./pkg/formats/hudi
go test -v ./pkg/formats/parquet

# Test core model, storage, or conversion controller
go test -v ./pkg/model
go test -v ./pkg/io
go test -v ./pkg/conversion
go test -v ./pkg/catalog
go test -v ./pkg/daemon
```

---

## 4. Coding Standards & Agent Guidelines

1. **Table-Driven Tests**: All new functionality MUST include table-driven unit tests with named subtests (`t.Run`), `t.Parallel()`, and `testify` (`require`/`assert`).
2. **Error Handling**:
   - Wrap errors with contextual information using `fmt.Errorf("...: %w", err)`.
   - Use sentinel errors (`io.ErrNotFound`, `io.ErrAlreadyExists`) and `errors.Is`/`errors.As`.
   - Never panic or ignore returned errors.
3. **Naming Conventions**:
   - Follow standard Go naming conventions (MixedCaps, short package names, no `Get` prefixes on getters).
   - Receiver names: 1–2 letter abbreviations (e.g. `func (s *Source)`, `func (t *Target)`).
   - Compile-time interface checks: Use `var _ spi.ConversionSource = (*Source)(nil)` and `var _ spi.ConversionTarget = (*Target)(nil)`.
4. **Concurrency Safety**:
   - Use `context.Context` for all I/O and conversion operations.
   - Avoid package-level mutable state. Protect shared resources with `sync.RWMutex`.
5. **Zero Allocation in Hot Paths**:
   - Preallocate slices when capacity is known (`make([]*DataFile, 0, len(files))`).
   - Reuse buffers where appropriate.

---

## 5. Skills & Context Integration

Agents working in this codebase should activate and leverage the following Go skills available in `~/.gemini/config/skills/`:
- `golang-testing`: Writing robust table-driven unit and integration tests.
- `golang-code-style`: Effective Go style and review rules.
- `golang-concurrency`: Context management, goroutine lifecycle, and channel patterns.
- `golang-error-handling`: Error chaining and wrapping conventions.
- `golang-performance`: Zero-allocation hot paths and profiling.
- `golang-spf13-cobra` & `golang-spf13-viper`: CLI and config management.

---

## 6. Persistent Memory (ICM) Rules

When working on this repository, agents MUST invoke `icm store`:
1. When resolving a difficult bug or test failure (`-t errors-resolved`).
2. When making an architectural or format design decision (`-t decisions-xtable-go`).
3. When completing a milestone or significant task (`-t context-xtable-go`).
