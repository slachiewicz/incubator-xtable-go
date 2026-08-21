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

# polytable Technical Specification

> **Not an Apache Software Foundation project.** `polytable` is an independent, unofficial Go port
> of Apache XTable (incubating), not affiliated with or endorsed by the ASF.

**Version:** 0.1.0-SNAPSHOT  
**Status:** Active Draft / Implementation  
**Module:** `github.com/slachiewicz/polytable`

---

## 1. Executive Summary & Mission

**polytable** is a pure Go, omni-directional, zero-copy metadata translation engine and synchronization daemon for open lakehouse table formats. It enables seamless interoperability across **Apache Iceberg**, **Delta Lake**, **Apache Hudi**, **Apache Paimon**, and unmanaged **Parquet** datasets, as well as catalog synchronization (**AWS Glue**, **Iceberg REST**). Hive Metastore is a declared
parity target but is **not implemented**; `CatalogTypeHMS` exists as a constant only.

### Primary Motivations for the Go-Native Implementation:
1. **Zero JVM / Hadoop Dependency**: Operates as a single, self-contained static binary with no Java, Scala, Spark, or Hadoop XML configuration requirements. The CLI measures 13.9 MiB stripped; see section 9.2.
2. **Ultra-Low Latency & Fast Cold Starts**: 6.6 ms measured start-to-exit and 15.9 MiB idle RSS, enabling serverless execution (AWS Lambda, Google Cloud Run), Kubernetes sidecars, and edge deployments. Earlier revisions claimed "<5ms" and "~5MB" here; neither was measured, and section 9.2 supersedes both.
3. **Cross-Language Embeddability**: Compilable into C-shared dynamic/static libraries (`.so`, `.dylib`, `.dll`, `.a`) for direct native embedding in Rust (DataFusion), C++ (DuckDB, Velox), Python (Polars/PyArrow), and Node.js.
4. **Native Cloud I/O**: Direct integration with modern cloud SDKs (`aws-sdk-go-v2`, `cloud.google.com/go/storage`, `azblob`) utilizing cloud-native credential providers (IAM Roles, IRSA, Workload Identity).

---

## 2. Core Architectural Invariants

All components within this repository MUST strictly adhere to the following invariants:

| # | Invariant | Description |
| :--- | :--- | :--- |
| **INV-1** | **Zero Data File Rewrites** | polytable translates and generates *metadata only*. It MUST NEVER alter, rewrite, or move physical Parquet/ORC data files. |
| **INV-2** | **Schema & Field ID Integrity** | Hierarchical field IDs, nullability, data types, and comments MUST be accurately preserved across all format transformations. |
| **INV-3** | **Pure Go Runtime** | Zero JVM, Hadoop, Spark, or CGO runtime dependencies in the core libraries. |
| **INV-4** | **Embedded Sync Continuity** | Target converters MUST embed and read `TableSyncMetadata` (`xtable_last_instant_synced`, `xtable_source_format`) to guarantee idempotent and incremental sync safety. |
| **INV-5** | **Zero-Copy Performance** | Slices and memory buffers in hot conversion paths are preallocated to minimize heap allocations and GC overhead. |

---

## 3. System Architecture & Component Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Client / Consumer Layer                           │
│  CLI (cmd/polytable)  │  REST Service (cmd/polytable-service) │   C-Shared  │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                    Conversion Controller (pkg/conversion)                   │
│   • Sync Mode Selector (Full vs Incremental)                                │
│   • Target Commit Orchestration & Lineage Tracking                          │
└───────────────────┬───────────────────────────────────┬─────────────────────┘
                    │                                   │
┌───────────────────▼───────────────────┐   ┌───────────▼─────────────────────┐
│  Canonical Domain Model (pkg/model)   │   │  Storage Abstraction (pkg/io)   │
│   • Schema Tree & Field ID System     │   │   • Local Filesystem (Local)    │
│   • DataFiles, Stats & Partitioning   │   │   • In-Memory Virtual (Memory)  │
│   • Snapshots & Incremental Changes   │   │   • Native AWS S3 (S3Storage)   │
│   • Deletion Vectors (Roaring/Bitmap) │   │   • URI-Safe Path Router        │
└───────────────────┬───────────────────┘   └───────────┬─────────────────────┘
                    │                                   │
┌───────────────────▼───────────────────────────────────▼─────────────────────┐
│                    Table Format Adapters (pkg/formats)                      │
│   ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐   │
│   │  Delta Lake   │ │Apache Iceberg │ │  Apache Hudi  │ │  Raw Parquet  │   │
│   │(JSON Actions) │ │ (v2/v3 Specs) │ │(.hoodie log)  │ │ (Footer/Hive) │   │
│   └───────────────┘ └───────────────┘ └───────────────┘ └───────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Canonical Domain Model Specification (`pkg/model`)

The canonical domain model acts as the unified pivot representation for all table formats.

### 4.1 Lakehouse Type System (`pkg/model/types.go`)
- **Primitives**: `BOOLEAN`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `STRING`, `BYTES`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `UUID`, `FIXED`.
- **Complex Types**:
  - `DECIMAL`: Stores `precision` and `scale` metadata attributes.
  - `RECORD`: Named struct containing an ordered list of `Field` objects.
  - `LIST`: Contains `ElementSchema` with element nullability.
  - `MAP`: Contains `KeySchema` and `ValueSchema` with value nullability.

### 4.2 Hierarchical Schema & Field IDs (`pkg/model/schema.go`)
- Every field carries:
  - `Name`: String field identifier.
  - `FieldID`: Unique integer ID (crucial for Iceberg schema evolution and column renames).
  - `Schema`: Type descriptor with `IsNullable` and `Comment`.
- Methods:
  - `AllFields()`: Flattens hierarchical struct fields for index lookup.
  - `FieldByPath(path)`: Looks up nested fields using dot notation (`parent.child`).

### 4.3 Partitioning & Column Statistics (`pkg/model/stats.go`)
- **Partition Transforms**: `VALUE` (identity), `YEAR`, `MONTH`, `DAY`, `HOUR`.
- **Column Statistics (`ColumnStat`)**:
  - `Range`: Generic min/max bound pair (`MinValue`, `MaxValue`).
  - `NumNulls`: Total null record count.
  - `NumNaNs`: Total NaN float/double count.

### 4.4 Data Files & Deletion Vectors (`pkg/model/datafile.go`)
- `DataFile`:
  - `PhysicalPath`: Absolute URI or relative storage path.
  - `FileFormat`: `APACHE_PARQUET` or `APACHE_ORC`.
  - `FileSizeBytes`: File length in bytes.
  - `RecordCount`: Total rows in physical file.
  - `PartitionValues`: List of partition key/value bindings.
  - `ColumnStats`: Array of per-column chunk statistics.
  - `DeletionVector`: Optional pointer to row deletion metadata.
- `DeletionVector`:
  - `StoragePath`: Path to external Roaring Bitmap or Position Delete file.
  - `Offset`: Byte offset where bitmap starts.
  - `SizeInBytes`: Binary size of deletion vector.
  - `Cardinality`: Exact count of deleted rows.

### 4.5 Embedded Sync Continuity Metadata (`pkg/model/metadata.go`)
Every target table embeds metadata in table properties:
- `xtable_last_instant_synced`: Unix millisecond timestamp of the last synchronized commit.
- `xtable_source_format`: The source format identifier (`DELTA`, `ICEBERG`, `HUDI`, `PARQUET`).

---

## 5. Storage Abstraction Specification (`pkg/io`)

### 5.1 Storage Interface (`pkg/io/storage.go`)
```go
type Storage interface {
    Read(ctx context.Context, path string) ([]byte, error)
    Write(ctx context.Context, path string, data []byte) error
    List(ctx context.Context, prefix string) ([]FileInfo, error)
    Exists(ctx context.Context, path string) (bool, error)
    Delete(ctx context.Context, path string) error
    Close() error
}
```

### 5.2 Storage Implementations
1. **`LocalStorage` (`pkg/io/local.go`)**:
   - Atomic writes via temporary file creation and OS atomic `rename()`.
   - Automatic parent directory hierarchy creation.
2. **`MemoryStorage` (`pkg/io/memory.go`)**:
   - Fully thread-safe in-memory virtual filesystem using `sync.RWMutex`.
   - Used for zero-disk microsecond unit tests and WASM environments.
3. **`S3Storage` (`pkg/io/s3.go`)**:
   - Zero-JVM native AWS S3 client using `aws-sdk-go-v2`.
   - Supports `s3://` and `s3a://` URI schemes with automatic IAM role/credential discovery.
4. **Dynamic Storage Router (`NewStorageForPath`)**:
   - Automatically selects storage driver (`S3Storage`, `LocalStorage`, `MemoryStorage`) based on URI scheme.
   - `JoinPath()`: URI scheme-preserving path joining.

---

## 6. Table Format Adapters (`pkg/formats`)

### 6.1 Delta Lake Adapter (`pkg/formats/delta`)
- **Protocol**: Protocol action reader/writer (`minReaderVersion: 1`, `minWriterVersion: 2`).
- **Log Management**: Sequential atomic `(N+1).json` commits inside `_delta_log/`.
- **Schema Translation**: Bidirectional conversion between `model.Schema` and Spark StructType JSON.
- **Actions**: Protocol, MetaData, AddFile, RemoveFile, CommitInfo.
- **Deletion Vectors**: Reads and writes Roaring Bitmap descriptors (`storageType: "u"`).
- **Column Statistics**: Reads `add.stats` into `model.ColumnStat`; writes `minValues`/`maxValues`/`nullCount` back out, dropping non-finite bounds (NaN, ±Inf) per column instead of discarding the file's whole stats string.
- **Incremental Sync**: Single-pass log walk (one read per commit, schema rebuilt only on `metaData` actions). Sync instants are derived from version order — strictly increasing even for same-millisecond commits, backwards clock skew, or commits without a `commitInfo` action — so `instant > fromInstant` selects exactly the versions after the last synced one.

### 6.2 Apache Iceberg Adapter (`pkg/formats/iceberg`)
- **Metadata Specification**: Iceberg Table Metadata v2/v3 (`metadata/v{N}.metadata.json`).
- **Manifests**: Manifest list files (`snap-<snapshot_id>-<uuid>.json`) and manifest files (`<uuid>-m0.json`).
- **Version Hinting**: Atomic `metadata/version-hint.text` updates.
- **Schema Mapping**: Deterministic assignment and preservation of Iceberg field IDs.
- **Column Statistics**: Manifest `lower_bounds`/`upper_bounds`/`value_counts`/`null_value_counts` map to and from `model.ColumnStat`, keyed by field ID so names survive renames. Bounds are base64 of Iceberg's single-value binary serialization; float/double zero bounds are widened (`-0.0` lower, `0.0` upper) to respect Iceberg's total ordering. Decimal and nested-column bounds are omitted rather than encoded wrong.

### 6.3 Apache Hudi Adapter (`pkg/formats/hudi`)
- **Table Properties**: Reader and serializer for `.hoodie/hoodie.properties` (`COPY_ON_WRITE` / `MERGE_ON_READ`).
- **Timeline Engine**: Parses and writes commit files (`.hoodie/<instant>.commit`) with `HoodieCommitMetadata` and `HoodieWriteStat` payloads.
- **Schema Mapping**: Bidirectional mapping between `model.Schema` and Avro Schema JSON (`.hoodie/.schema/<instant>.avsc`).

### 6.4 Raw Parquet Crawler (`pkg/formats/parquet`)
- **Directory Crawler**: Discovers unmanaged Parquet files across nested folder structures.
- **Hive Partition Extractor**: Parses `key=value` directory segments into partition fields and values.
- **Footer Reader**: Extracts table schema, row counts, and column chunk statistics directly from Parquet footers, aggregating row-group min/max/null-count statistics per column into `model.ColumnStat`.

---

## 7. Conversion Orchestration Specification (`pkg/conversion`)

### 7.1 Sync Mode Selection Algorithm
1. **Inspect Target Table**: Query `target.GetTableMetadata(ctx)`.
2. **Evaluate Incremental Safety**:
   - If `lastInstantSynced > 0` AND `config.SyncMode != FULL`:
   - Query `source.IsIncrementalSyncSafeFrom(ctx, lastInstantSynced)`.
   - If `true` $\longrightarrow$ **Incremental Sync**:
     - Call `source.GetChangesSince(ctx, lastInstantSynced)`.
     - Call `target.CommitChanges(ctx, changes)`.
3. **Fallback to Full Snapshot**:
   - If target does not exist or incremental sync is unsafe:
     - Call `source.GetCurrentSnapshot(ctx)`.
     - Call `target.CommitSnapshot(ctx, snapshot)`.

---

## 8. CLI & Service Interfaces

### 8.1 CLI Commands (`cmd/polytable`)
- `polytable sync --datasetConfig <path>`: Runs synchronization across configured datasets.
- `polytable inspect --basePath <path> --format <DELTA|ICEBERG|HUDI|PARQUET>`: Pretty-prints schema, partition spec, commit time, and data file count.
- `polytable version`: Displays binary version and commit hash.

### 8.2 REST Service Specification (`spec/rest-service-open-api.yaml`)
- `POST /v1/conversion/table`: Initiates table conversion (sync or async via `Prefer: respond-async`).
- `GET /v1/conversion/table/{conversionId}`: Polls async conversion job progress.
- `POST /v1/conversion/inspect`: Discovers schema and metadata for any table path.
- `GET /v1/health`: Returns service health status and version.

---

## 9. Performance Benchmarks & Targets

All figures below were measured; none is a projection. Reproduce with `make bench COUNT=10` and
`make bench-footprint`. Absolute timings are hardware-specific — these come from an Apple M1
(darwin/arm64, Go 1.27.0) — so treat the **shape** of the curve as the durable result and re-measure
before quoting a number anywhere it matters. The toolchain is part of the measurement: Go 1.27 backs
`encoding/json` with the v2 implementation, which cut allocations here by 40% (geomean, p=0.002)
against this same tree built with Go 1.26.6.

### 9.1 Snapshot sync

`BenchmarkSnapshotSync`, full Delta → Iceberg snapshot sync against in-memory storage, so the figure
isolates translation cost from object-store latency. A network backend adds its own round trips.

| Data files | Time per sync | Allocated | Allocations |
| ---: | ---: | ---: | ---: |
| 10 | 0.067 ms | 53 KiB | 554 |
| 100 | 0.51 ms | 461 KiB | 3,905 |
| 1,000 | 5.23 ms | 4.3 MiB | 37,089 |
| 10,000 | 52.5 ms | 46.1 MiB | 367,978 |

Timings are medians of `-count=10`. The two small rows are the noisy ones — ±30% at 10 files and
±51% at 100, where a single sync is short enough that scheduler jitter dominates — while the 1,000
and 10,000-file rows hold to ±4% and ±3%. Allocation counts do not vary at all. Prefer the large
rows for any claim that matters.

Cost is linear in file count: each 10x more files costs roughly 10x the time and memory. **The
"< 50 ms" target in earlier revisions holds up to a few thousand files and is still missed at 10,000
(52.5 ms), though by a far smaller margin than the 58 ms measured on Go 1.26.5.** State the file
count alongside any latency claim; a bare "2.5 ms sync" is meaningless.

`BenchmarkSnapshotRead` isolates the read half — 33.5 ms of the 52.5 ms at 10,000 files, so roughly
64% of a full sync is spent parsing the source table's log rather than writing the target's metadata.
That is still where optimisation effort belongs, but the read half is no longer as dominant as it
was: it was 83% before Go 1.27, and the json v2 rewrite took more out of parsing than out of writing.

### 9.2 Footprint

| Property | Measured | Method |
| :--- | :--- | :--- |
| `cmd/polytable`, `-ldflags="-s -w"` | 13.9 MiB | `go build`, darwin/arm64 |
| `cmd/polytable-service`, same flags | 14.4 MiB | same |
| `cmd/polytable-wasm`, same flags | 18.4 MiB | after excluding the AWS SDK from `js` builds; was 25.6 MiB |
| `polytable version`, process start to exit | median 6.6 ms (min 6.3, max 7.3, n=30) | wall clock around `fork`/`exec`; an upper bound, it includes the harness |
| `polytable-service` idle RSS | 15.9 MiB, unchanged after serving requests | `ps -o rss=` two seconds after the health endpoint answered |

The three binary sizes are each larger than the figure this section carried previously, by 0.4 MiB
for the two native binaries and 0.8 MiB for the WebAssembly artifact; idle RSS and startup time both
moved slightly the other way, to 15.9 MiB and 6.6 ms. Growth is the expected direction — json v2
and the size-specialized allocation routines are both more code — but the comparison is against a
recorded number, not a controlled one: the tree moved between the two measurements as well as the
toolchain, so do not read the delta as attributable to Go 1.27 alone.

### 9.3 Comparison with Java Apache XTable

None has been run. A meaningful comparison needs both implementations exercised on identical tables
and hardware; until someone does that, this specification makes no cross-implementation claim.
Earlier revisions published such a column, and its figures were not measured.

### 9.4 Size problems, resolved

- ~~The WebAssembly target links the entire AWS SDK~~ — **fixed.** `pkg/io/s3.go` and the Glue
  implementations in `pkg/catalog` now carry `//go:build !js`, with `js` counterparts returning
  `ErrS3Unsupported` and `ErrGlueUnsupported`. `GOOS=js GOARCH=wasm go list -deps ./cmd/polytable-wasm`
  now reports **zero** `aws-sdk-go-v2` and `smithy` packages, down from 103, and the artifact fell
  from 25.6 MiB to **18.4 MiB**. An `s3://` path in a browser now fails with a stated reason rather
  than dragging in an SDK that could never have worked there.
- ~~Release artifacts are unstripped~~ — **fixed.** The release workflow, `build-artifacts.yml` and
  the `Makefile` all build with `-ldflags="-s -w"`, cutting roughly 6 MiB from each of the eight
  published binaries — `cmd/polytable` measures 20.2 MiB unstripped against 13.9 MiB stripped. The
  `c-shared` build is deliberately left unstripped: its dynamic symbol table is the interface.

---

## 10. Roadmap & Future Extensions

### 10.1 Delivered

Phases 4 and 5 have shipped. This section previously listed them as future work.

| Capability | Where it lives |
| :--- | :--- |
| Catalog sync clients — AWS Glue, Iceberg REST | `pkg/catalog/{glue,rest}.go`, reached from `DatasetConfig.Catalogs` |
| Catalog conversion sources — resolve a table as `db.table` instead of a path | `pkg/catalog/{glue,rest}_conversion.go`, reached from `DatasetConfig.SourceCatalog` |
| Catalog partition synchronization | `pkg/catalog/partition.go` + `glue_partition.go`, applied automatically for Hive-style catalogs |
| Continuous daemon and REST service | `pkg/daemon`, `cmd/polytable-service`, contract in `spec/rest-service-open-api.yaml` |
| C-shared libraries for Python, Rust, DuckDB, C++ | `bindings/c`, built with `make bindings-c` |
| Python SDK | `bindings/python` (`polytable`) |
| WebAssembly — ⚠️ **experimental, untested** | `cmd/polytable-wasm`, `GOOS=js GOARCH=wasm`. Compile-checked only; never executed in a browser or under Node.js, and no test covers it. Only local and in-memory paths can work — S3, Glue and Iceberg REST are unreachable from a browser sandbox. |
| Paimon adapter | `pkg/formats/paimon` — source and target |
| Parquet target | `pkg/formats/parquet/target.go` |

### 10.2 Outstanding

| Gap | Notes |
| :--- | :--- |
| **Hive Metastore** | `CatalogTypeHMS` is a declared constant that returns `ErrCatalogNotImplemented`. Java's `xtable-hive-metastore` carries a sync client, schema extractor, partition sync operations and three per-format table builders; a create-tables-only port would misrepresent itself as supported. Requires a Thrift dependency and an HMS container for integration testing. |
| **Catalog partition sync for HMS** | Follows HMS itself. The Glue implementation is in place and the `PartitionSyncOperations` contract is catalog-agnostic. |
| ~~Performance harness~~ | **Done.** `make bench` plus `make bench-footprint`; §9 carries measured figures only, and `.github/workflows/bench.yml` compares each pull request against its merge base with `benchstat`. |
| **Released versions** | No tags exist. The module path and repository name were settled only recently, and Go module tags are immutable, so tagging is deliberately deferred. |

### 10.3 Deliberately not planned

| Item | Reason |
| :--- | :--- |
| Decoding deletion-vector bitmaps | Would require reading data files, violating INV-1. Deletion vectors are translated as descriptors — path, offset, size, cardinality — and the bitmap payload is passed through untouched. |
| Renaming stuttering identifiers (`delta.DeltaCommit`, `catalog.CatalogType`) | A breaking public API change; `revive`'s stuttering check is disabled deliberately rather than obeyed. |
