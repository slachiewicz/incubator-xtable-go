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

# Features and limitations

polytable is an independent Go port of
[Apache XTable (incubating)](https://github.com/apache/incubator-xtable). It is
not an Apache Software Foundation project. This page describes what polytable
does, what it deliberately does not do, and the limitations you can hit in
practice. For step-by-step instructions, see
[Create your first interoperable table](how-to.md).

## Format support

The following table shows what each format supports as a conversion source and
target.

| Format | Source (Reader) | Target (Writer) | Schema Evolution | Partitioning | Column Statistics | Deletion Vectors |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Delta Lake** | ✅ | ✅ | ✅ (Field IDs) | ✅ | ✅ (read + write) | ✅ (Roaring Bitmap Descriptor) |
| **Apache Iceberg** (v2/v3) | ✅ | ✅ | ✅ (Field IDs) | ✅ (Transforms) | ✅ (read + write) | — |
| **Apache Hudi** | ✅ | ✅ | ✅ (Avro Schema) | ✅ | — | — |
| **Raw Parquet** | ✅ | ✅ | ✅ (Schema Crawler) | ✅ (Hive Style) | ✅ (source) | — |
| **Apache Paimon** | ✅ | ✅ | ✅ (Type Mapping) | ✅ | — | — |

## Metadata only, never data

polytable translates and generates table metadata; it never rewrites, moves, or
decodes physical data files. Deletion vectors travel as descriptors with the
bitmap payload untouched. This bounds what the tool can cost you: a bad sync is
a bad metadata directory, not a damaged table.

## Interoperability with Java XTable

Tables synced by polytable and by upstream Apache XTable interoperate. Both
tools embed the same sync-continuity properties,
`xtable_last_instant_synced` and `xtable_source_format`, in the target table
metadata, so a table can move between them without a resync.

## Known issues and limitations

### Storage

polytable reads and writes plain local paths and the `file://`, `s3://`,
`s3a://`, `abfss://`, `abfs://`, `wasbs://`, `wasb://`, and `mem://` schemes.
See [Cloud storage](cloud-storage.md) for Azure configuration and credentials.
The `gs://` scheme has no storage backend and fails with an error that names
the supported schemes. This is deliberate: a `gs://` path still parses,
because foreign metadata can carry such paths, but polytable refuses to route
it to a backend that can't serve it.

The Azure backend is verified against a real ADLS Gen2 account: sync, read
back, hierarchical-namespace directory handling, shared key and
`DefaultAzureCredential`. The Azurite suite
(`test/dockertest_azurite_test.go`) runs on every build and covers shared key,
SAS, anonymous, and all four ABFS spellings.

OneLake is not verified beyond the endpoint accepting a token: no request
against a real Fabric workspace has succeeded. The blob endpoint polytable
derives for OneLake paths matches the pair Microsoft documents, and the
`endpoint` override covers the regional, private-link and
`api.onelake.fabric.microsoft.com` forms it does not derive.

### Catalogs

The AWS Glue Data Catalog and the Iceberg REST catalog are implemented; see
[Sync to the AWS Glue Data Catalog](glue-catalog.md) and
[Sync to an Iceberg REST catalog](iceberg-rest-catalog.md). The
`HIVE_METASTORE` catalog type is declared but not implemented: configuring it
fails with a not-implemented error rather than being silently ignored.

A catalog entry registers each target format under the same table name, so
registering multiple target formats overwrites the previous registration. Use
one target format per catalog-registered dataset.

### Delta Lake

The Delta source reads classic Parquet checkpoints, both single-part and
multi-part, and seeds every read path from checkpoint state plus the JSON
commits after it. polytable rejects v2 checkpoints (sidecar files, the
`v2Checkpoint` reader feature) with an error that says so.

Delta deletion vectors are translated as descriptors only; polytable never
opens the roaring bitmap payload.

### Apache Hudi

polytable reads and writes the 0.14-era table layout: the Hudi target stamps
the `hoodie.table.version` property with the value `6`, and the source parses
the 0.x timeline. Tables written with the Hudi 1.x timeline layout
(`hoodie.table.version` 8 or later) are refused with an error that says so —
confirmed against a table written by Hudi 1.2.0 — and reading the 1.x layout
is tracked as T37 in [the improvement plan](improvement-plan.md).

### WebAssembly

The WebAssembly target is experimental. It has never been executed in a
browser or under Node.js, and it is not covered by any test; the build is
compile-checked only. Only local and in-memory paths can work in that
environment, and catalog synchronization is unreachable. For details, see
[WebAssembly status](../README.md#webassembly-status).

## Report issues

Report bugs and feature requests for polytable in this repository's issue
tracker. Reports about the upstream Apache XTable project belong in the
Apache tracker, not here, and reports about polytable don't belong upstream.
