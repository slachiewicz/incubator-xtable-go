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

# xtable-go

[![CI](https://github.com/slachiewicz/xtable-go/actions/workflows/ci.yml/badge.svg)](https://github.com/slachiewicz/xtable-go/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/slachiewicz/xtable-go/actions/workflows/integration.yml/badge.svg)](https://github.com/slachiewicz/xtable-go/actions/workflows/integration.yml)
[![Security & SAST](https://github.com/slachiewicz/xtable-go/actions/workflows/security.yml/badge.svg)](https://github.com/slachiewicz/xtable-go/actions/workflows/security.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/slachiewicz/xtable-go)](https://goreportcard.com/report/github.com/slachiewicz/xtable-go)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **Not an Apache Software Foundation project.** `xtable-go` is an independent, unofficial Go port of
> [Apache XTable (incubating)](https://github.com/apache/incubator-xtable). It is not affiliated with,
> endorsed by, or supported by the ASF, and it is not an Apache release. "Apache", "Apache XTable",
> "Apache Iceberg", "Apache Hudi" and "Apache Paimon" are trademarks of The Apache Software
> Foundation, used here only to identify the upstream projects this software derives from or
> interoperates with. For the official project, see [xtable.apache.org](https://xtable.apache.org).

**xtable-go** is a lightweight, ultra-high performance, **zero-JVM** lakehouse metadata translation engine written in pure Go. It facilitates **omni-directional, zero-copy interoperability** across open lakehouse table formats (**Delta Lake**, **Apache Iceberg**, **Apache Hudi**, **Apache Paimon**, and **Raw Parquet**) without rewriting underlying data files.

---

## 🌟 Why a Go port?

- ⚡ **Instant Execution**: Native static binary (~15MB) with **zero JVM boot latency** (<2ms execution).
- 🛡️ **Pure Go & Zero-JVM**: No Spark, Hadoop XML, Java, or Scala runtime dependencies required.
- 🔄 **Omni-Directional Sync**: Any format $\longleftrightarrow$ Any format (e.g., Delta $\to$ Iceberg & Hudi; Parquet $\to$ Delta $\to$ Iceberg; Hudi $\to$ Delta).
- 📦 **Deletion Vectors**: Full translation of deletion vector descriptors across modern lakehouse formats, preserving roaring bitmap payloads untouched.
- 🌐 **Ubiquitous Embeddability**:
  - **CLI Tool** (`xtable`)
  - **Continuous REST Daemon & Sidecar** (`xtable-service` with OpenAPI 3.0.3)
  - **Python SDK** (`pyxtable` via ctypes C ABI)
  - **C-Shared Dynamic Library** (`libxtable.so` / `libxtable.dylib`)
  - **WebAssembly Engine** (`xtable.wasm`) — ⚠️ **experimental**, see [WebAssembly status](#webassembly-status)
- ☁️ **Cloud Native Storage & Catalogs**: Native AWS S3 (`aws-sdk-go-v2`), AWS Glue Data Catalog, and Iceberg REST Catalog (Polaris, Unity, Nessie, Tabular).

---

## 📊 Format Support Matrix

| Format | Source (Reader) | Target (Writer) | Schema Evolution | Partitioning | Deletion Vectors |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Delta Lake** | ✅ | ✅ | ✅ (Field IDs) | ✅ | ✅ (Roaring Bitmap Descriptor) |
| **Apache Iceberg** (v2/v3) | ✅ | ✅ | ✅ (Field IDs) | ✅ (Transforms) | ✅ (Equality/Positional) |
| **Apache Hudi** | ✅ | ✅ | ✅ (Avro Schema) | ✅ | ✅ |
| **Raw Parquet** | ✅ | — | ✅ (Schema Crawler) | ✅ (Hive Style) | — |
| **Apache Paimon** | ✅ | Planned | ✅ (Type Mapping) | ✅ | — |

---

## 🚀 Quickstart

### 1. Installation

```bash
# Clone repository
git clone https://github.com/slachiewicz/xtable-go.git
cd xtable-go

# Build all binaries
make build
```

### 2. Run Interactive Demo

```bash
make demo
```

### 3. CLI Synchronization (`xtable sync`)

Create a dataset configuration file `dataset.yaml`:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
  - HUDI
datasets:
  - tableName: financial_events
    tableBasePath: s3://my-lakehouse-bucket/tables/financial_events
    syncMode: INCREMENTAL
```

Run synchronization:

```bash
./bin/xtable sync --config dataset.yaml
```

### 4. CLI Table Inspection (`xtable inspect`)

```bash
# Inspect Delta table
./bin/xtable inspect --path ./data/my_table --format DELTA

# Inspect Iceberg table
./bin/xtable inspect --path ./data/my_table --format ICEBERG

# Inspect raw Parquet directory
./bin/xtable inspect --path ./data/raw_parquet --format PARQUET
```

---

## 🛰️ Continuous REST Daemon (`xtable-service`)

Run `xtable-service` as a standalone REST server or Kubernetes sidecar:

```bash
./bin/xtable-service --port 8080 --daemon --interval 10s --config dataset.yaml
```

### OpenAPI REST API Endpoints:

- `POST /v1/conversion/table`: Trigger synchronous table translation.
- `POST /v1/conversion/table/async`: Trigger background asynchronous translation.
- `GET /v1/conversion/table/{id}`: Poll status of async translation task.
- `POST /v1/conversion/inspect`: Inspect table metadata and schema over HTTP.
- `GET /v1/health`: Liveness probe (`{"status":"UP","version":"0.1.0-SNAPSHOT"}`).

See full specification in [`spec/rest-service-open-api.yaml`](./spec/rest-service-open-api.yaml).

---

## 🐍 Python SDK (`pyxtable`)

Use Apache XTable natively in Python without JVM or PySpark:

```python
from pyxtable import sync, inspect, version

print(f"Using XTable Engine: {version()}")

# Perform zero-copy sync in Python
result = sync(
    source_format="DELTA",
    target_formats=["ICEBERG", "HUDI"],
    table_name="customers",
    table_base_path="/data/lakehouse/customers",
    sync_mode="INCREMENTAL"
)
print("Sync Result:", result)
```

---

## 🧪 Testing & Verification

```bash
# Run unit tests
make test

# Run tests with race detection
make test-race

# Run Docker container integration tests (MinIO S3 & Tabular Iceberg REST)
make test-containers

# Run linter
make lint
```

---

## 🏛️ Architecture & Specifications

For detailed architectural diagrams, domain models, and conversion invariants, refer to:
- 📖 [**Technical Specification (`SPEC.md`)**](./SPEC.md)
- 🤖 [**Agent & Contributor Guide (`AGENTS.md`)**](./AGENTS.md)
- 📋 [**REST Service OpenAPI Contract (`spec/rest-service-open-api.yaml`)**](./spec/rest-service-open-api.yaml)

---

## ⚖️ License & Disclaimer

`xtable-go` is licensed under the [Apache License, Version 2.0](LICENSE).

It is **not** an Apache Software Foundation project and carries no ASF endorsement. Portions derive
from [Apache XTable (incubating)](https://github.com/apache/incubator-xtable); see [`NOTICE`](NOTICE)
for the upstream attribution required by Apache-2.0 §4(d). The official Apache project is at
[xtable.apache.org](https://xtable.apache.org) — bug reports for it do not belong here.

## WebAssembly status

**The WebAssembly target is experimental. It has never been executed in a browser or under Node.js,
and it is not covered by any test.** The build is compile-checked only: `make check` runs
`GOOS=js GOARCH=wasm go vet ./cmd/xtable-wasm`, which type-checks the package but never runs it.

Two known limitations, both consequences of how the target is currently built:

- **Only local and in-memory paths can work.** `NewStorageForPath` selects the S3 backend for
  `s3://` and `s3a://` URIs, which needs AWS credentials and network access that a browser sandbox
  does not provide. Catalog synchronization (AWS Glue, Iceberg REST) is likewise unreachable.
- **The artifact is 17.6 MiB.** The AWS SDK is no longer linked: `pkg/io/s3.go` and the Glue files
  in `pkg/catalog` are built `!js`, with `js` stubs returning `ErrS3Unsupported` and
  `ErrGlueUnsupported`. That removed all 103 `aws-sdk-go-v2` and `smithy` packages and cut 8 MiB.
  The remainder is the Go runtime and the format adapters.

Treat `xtableInspect` and `xtableSync` as unvalidated. Report anything that works as much as
anything that does not.
