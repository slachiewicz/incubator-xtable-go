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

# Create your first interoperable table

This tutorial shows how to expose a Delta Lake table as an Apache Iceberg and an
Apache Hudi table with polytable, without copying or rewriting the data files.
polytable is a single native binary: no JVM, Spark, or Hadoop is involved at any
step. It is the polytable counterpart of the Apache XTable tutorial
[Creating your first interoperable table](https://xtable.apache.org/docs/how-to).

Every command in this guide was run as written; the output blocks are real.

## Prerequisites

- Go 1.26 or later and `make`, to build polytable.
- Python 3 with the [`deltalake`](https://pypi.org/project/deltalake/) package,
  to write the sample source table. If you already have a Delta Lake, Iceberg,
  Hudi, Paimon, or Parquet table, you can skip that step and point the config at
  your table instead.
- Optional: [DuckDB](https://duckdb.org), to query the converted table.

## Build polytable

```shell
git clone https://github.com/slachiewicz/polytable.git
cd polytable
make build
```

The binaries land in `bin/`. For a self-contained end-to-end demonstration with
a generated table, you can also run `make demo`.

## Create a source dataset

Write a small Delta Lake table, partitioned by city, using
[delta-rs](https://github.com/delta-io/delta-rs) (tested with `deltalake` 1.6.3):

```shell
pip install deltalake
```

```python
import pyarrow as pa
from deltalake import write_deltalake

records = pa.table({
    "id": [1, 2, 3, 4, 5, 6],
    "name": ["Aisha", "Emiko", "Michael", "Andile", "Bo", "Carmen"],
    "age": [25, 30, 35, 40, 28, 31],
    "city": ["NYC", "SFO", "ORD", "NYC", "SEA", "DFW"],
    "create_ts": ["2023-09-28 00:00:00", "2023-09-28 00:00:00",
                  "2023-09-28 00:00:00", "2023-10-28 00:00:00",
                  "2023-09-23 00:00:00", "2023-08-29 00:00:00"],
})

write_deltalake("/tmp/delta-dataset/people", records, partition_by=["city"])
```

## Configure the sync

Create a file named `my_config.yaml`:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
  - HUDI
datasets:
  - tableBasePath: /tmp/delta-dataset/people
    tableName: people
```

Useful optional keys, all defined in `pkg/conversion/config.go`:

- `tableDataPath`: for an Iceberg source whose data directory differs from
  `tableBasePath`.
- `syncMode`: `FULL` or `INCREMENTAL`. The `--mode` flag overrides it per run.
- `storage`: S3 connection settings; see
  [Sync a table in Amazon S3](#sync-a-table-in-amazon-s3).
- `catalogs`: catalogs to register the synced table in; see
  [Sync to the AWS Glue Data Catalog](glue-catalog.md) and
  [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md).

## Run the sync

```shell
./bin/polytable sync --datasetConfig my_config.yaml
```

```text
🚀 Starting polytable synchronization...

[1/1] Syncing Table 'people' (DELTA -> [ICEBERG HUDI])...
  ✅ [HUDI] Synced successfully in 632.666µs (lastInstant: 1787393828352)
  ✅ [ICEBERG] Synced successfully in 7.487125ms (lastInstant: 1787393828352)

✨ Finished all dataset syncs in 8.142541ms
```

The table directory now carries metadata for all three formats side by side —
`_delta_log/` (the source), `metadata/` (Iceberg), and `.hoodie/` (Hudi) — all
describing the same untouched Parquet files.

Running the same command again reports that there is nothing to do:

```text
[1/1] Syncing Table 'people' (DELTA -> [ICEBERG HUDI])...
  ⏭  [HUDI] No new commits to sync (in 325.875µs)
  ⏭  [ICEBERG] No new commits to sync (in 719.125µs)
```

For scripting, `--output json` writes a machine-readable result document to
stdout, and `--dry-run` computes the changes without committing them.

## Verify the target tables

Inspect the generated Iceberg metadata with polytable itself:

```shell
./bin/polytable inspect --basePath /tmp/delta-dataset/people --format ICEBERG
```

```text
📊 Table: people (Format: ICEBERG)
📍 Base Path: /tmp/delta-dataset/people
📁 Active Data Files: 5

📋 Schema:
  - id: LONG (nullable)
  - name: STRING (nullable)
  ...

🗂 Partition Fields:
  - city (transform: VALUE)
```

To confirm that a foreign engine can read the result, query it with DuckDB
(tested with v1.5.5):

```shell
duckdb -c "SELECT city, count(*) AS people
           FROM iceberg_scan('/tmp/delta-dataset/people')
           GROUP BY city ORDER BY city;"
```

```text
┌─────────┬────────┐
│  city   │ people │
├─────────┼────────┤
│ DFW     │      1 │
│ NYC     │      2 │
│ ORD     │      1 │
│ SEA     │      1 │
│ SFO     │      1 │
└─────────┴────────┘
```

## Sync a table in Amazon S3

For a table in S3, use an `s3://` base path and let credentials come from the
standard AWS credential chain (environment variables, shared config file, or
instance role):

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://my-bucket/tables/people
    tableName: people
```

For an S3-compatible store such as MinIO, add a `storage` block to the dataset:

```yaml
datasets:
  - tableBasePath: s3://warehouse/people
    tableName: people
    storage:
      region: us-east-1
      endpoint: http://localhost:9000
      usePathStyle: true
```

This surface is exercised against a real MinIO container by the integration
suite (`make test-containers` runs `test/dockertest_minio_matrix_test.go`).

## Storage support

polytable supports local paths, `file://`, `s3://`, `s3a://`, and the in-memory
scheme `mem://`. Google Cloud Storage (`gs://`) and Azure Data Lake Storage
(`abfss://`) are not supported: a path with an unsupported scheme fails with a
clear error instead of being misread as a local path. The upstream Java project
covers those stores; use it if you need them today.

## Next steps

- [Sync to the AWS Glue Data Catalog](glue-catalog.md), including discovering
  tables to sync from Glue instead of a config file.
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) such as Nessie,
  Polaris, or Unity Catalog.
