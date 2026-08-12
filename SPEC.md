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

# Apache XTable (Go) Technical Specification

**Version:** 0.1.0-SNAPSHOT  
**Status:** Active Draft / Implementation  
**Module:** `github.com/apache/incubator-xtable-go`

---

## 1. Executive Summary & Mission

**Apache XTable (Go)** is a pure Go, omni-directional, zero-copy metadata translation engine and synchronization daemon for open lakehouse table formats. It enables seamless interoperability across **Apache Iceberg**, **Delta Lake**, **Apache Hudi**, **Apache Paimon**, and unmanaged **Parquet** datasets, as well as catalog synchronization (**AWS Glue**, **Iceberg REST**). Hive Metastore is a declared
parity target but is **not implemented**; `CatalogTypeHMS` exists as a constant only.

### Primary Motivations for the Go-Native Implementation:
1. **Zero JVM / Hadoop Dependency**: Operates as a single, self-contained static binary (~5MB) with no Java, Scala, Spark, or Hadoop XML configuration requirements.
2. **Ultra-Low Latency & Fast Cold Starts**: <5ms startup time and <50MB memory footprint, enabling serverless execution (AWS Lambda, Google Cloud Run), Kubernetes sidecars, and edge deployments.
3. **Cross-Language Embeddability**: Compilable into C-shared dynamic/static libraries (`.so`, `.dylib`, `.dll`, `.a`) for direct native embedding in Rust (DataFusion), C++ (DuckDB, Velox), Python (Polars/PyArrow), and Node.js.
4. **Native Cloud I/O**: Direct integration with modern cloud SDKs (`aws-sdk-go-v2`, `cloud.google.com/go/storage`, `azblob`) utilizing cloud-native credential providers (IAM Roles, IRSA, Workload Identity).

---

## 2. Core Architectural Invariants

All components within this repository MUST strictly adhere to the following invariants:

| # | Invariant | Description |
| :--- | :--- | :--- |
| **INV-1** | **Zero Data File Rewrites** | XTable translates and generates *metadata only*. It MUST NEVER alter, rewrite, or move physical Parquet/ORC data files. |
| **INV-2** | **Schema & Field ID Integrity** | Hierarchical field IDs, nullability, data types, and comments MUST be accurately preserved across all format transformations. |
| **INV-3** | **Pure Go Runtime** | Zero JVM, Hadoop, Spark, or CGO runtime dependencies in the core libraries. |
| **INV-4** | **Embedded Sync Continuity** | Target converters MUST embed and read `TableSyncMetadata` (`xtable_last_instant_synced`, `xtable_source_format`) to guarantee idempotent and incremental sync safety. |
| **INV-5** | **Zero-Copy Performance** | Slices and memory buffers in hot conversion paths are preallocated to minimize heap allocations and GC overhead. |

---

## 3. System Architecture & Component Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Client / Consumer Layer                           │
│   CLI (cmd/xtable)    │   REST Service (cmd/xtable-service)   │   C-Shared  │
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

### 6.2 Apache Iceberg Adapter (`pkg/formats/iceberg`)
- **Metadata Specification**: Iceberg Table Metadata v2/v3 (`metadata/v{N}.metadata.json`).
- **Manifests**: Manifest list files (`snap-<snapshot_id>-<uuid>.json`) and manifest files (`<uuid>-m0.json`).
- **Version Hinting**: Atomic `metadata/version-hint.text` updates.
- **Schema Mapping**: Deterministic assignment and preservation of Iceberg field IDs.

### 6.3 Apache Hudi Adapter (`pkg/formats/hudi`)
- **Table Properties**: Reader and serializer for `.hoodie/hoodie.properties` (`COPY_ON_WRITE` / `MERGE_ON_READ`).
- **Timeline Engine**: Parses and writes commit files (`.hoodie/<instant>.commit`) with `HoodieCommitMetadata` and `HoodieWriteStat` payloads.
- **Schema Mapping**: Bidirectional mapping between `model.Schema` and Avro Schema JSON (`.hoodie/.schema/<instant>.avsc`).

### 6.4 Raw Parquet Crawler (`pkg/formats/parquet`)
- **Directory Crawler**: Discovers unmanaged Parquet files across nested folder structures.
- **Hive Partition Extractor**: Parses `key=value` directory segments into partition fields and values.
- **Footer Reader**: Extracts table schema, row counts, and column chunk statistics directly from Parquet footers.

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

### 8.1 CLI Commands (`cmd/xtable`)
- `xtable sync --datasetConfig <path>`: Runs synchronization across configured datasets.
- `xtable inspect --basePath <path> --format <DELTA|ICEBERG|HUDI|PARQUET>`: Pretty-prints schema, partition spec, commit time, and data file count.
- `xtable version`: Displays binary version and commit hash.

### 8.2 REST Service Specification (`spec/rest-service-open-api.yaml`)
- `POST /v1/conversion/table`: Initiates table conversion (sync or async via `Prefer: respond-async`).
- `GET /v1/conversion/table/{conversionId}`: Polls async conversion job progress.
- `POST /v1/conversion/inspect`: Discovers schema and metadata for any table path.
- `GET /v1/health`: Returns service health status and version.

---

## 9. Performance Benchmarks & Targets

| Operation | Java Apache XTable | `xtable-go` | Target |
| :--- | :--- | :--- | :--- |
| **Cold Start / Initialization** | 5,000 – 15,000 ms | **< 3 ms** | < 10 ms |
| **Binary / Distribution Size** | ~1,200 MB (bundled JAR) | **4.8 MB** (static binary) | < 15 MB |
| **Memory Footprint (Idle)** | 250 – 500 MB | **< 15 MB** | < 30 MB |
| **Single Table Snapshot Sync** | 2,000 – 8,000 ms | **2.5 ms** | < 50 ms |

---

## 10. Roadmap & Future Extensions

1. **Phase 4**: Catalog Synchronization Clients (**AWS Glue**, **Iceberg REST**) & Continuous Daemon (`cmd/xtable-service`). Hive Metastore remains outstanding.
2. **Phase 5 (Post-Parity Superpowers)**:
   - C-Shared Dynamic Libraries (`libxtable.so` / `libxtable.dylib`) for Python, Rust, DuckDB, and C++.
   - WebAssembly (`GOOS=js GOARCH=wasm` / WASI) for browser-based lakehouse inspection.
   - Paimon format reader/writer.
