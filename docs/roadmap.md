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

# Roadmap

This page sets direction; the execution queue with acceptance criteria stays in
[the improvement plan](improvement-plan.md), and the upstream facts this
direction rests on are in [the upstream watch](upstream-watch.md), dated
2026-08-22. As of 2026-08-22 every item below is scheduled there as a numbered
task — T38–T52 — so this page states why the work matters and the plan states
what "done" means for it. Positioning in one sentence: upstream Java XTable is spending its
next two releases on JVM-toolchain migration (Delta Kernel, Spark 4/Scala
2.13/Java 17) while discussing a Spark-free runtime — converging on polytable's
founding premise — so polytable's window is to close format-version and
correctness gaps faster than upstream can shed its runtime weight.

## Now: track the engine baseline upstream is moving to

Upstream 0.5.0 (cutoff September 30, 2026) makes Hudi 1.x a headline; 0.6.0
(November 15, 2026) lands Spark 4 with Delta 4.0 and Iceberg 1.10+. Each item
here exists because that baseline makes it load-bearing:

- **Hudi 1.x read support** (T37, guard landed): honor `hoodie.timeline.path`,
  parse `{begin}_{completion}` completed-instant names, exclude the
  `.hoodie/metadata` table from listing, then lift the version-6 floor. The
  committed Hudi 1.2.0 fixture is the acceptance fixture. A version-9 *target*
  follows upstream #834's semantics once their implementation settles.
- **Delta v2 checkpoints**: classic checkpoints landed (T36); v2 (sidecars,
  `v2Checkpoint` reader feature) is currently a loud rejection and becomes a
  real gap as Delta 4.0 defaults to it. Fixture first: a Spark-written table
  with v2 checkpoints enabled.
- **Iceberg metadata resolution robustness**: upstream's most user-visible bug
  family (#431, #287, #504, #759 — version-hint assumed, version tokens
  overflowing int32 on Snowflake-written tables, catalog-managed tables
  unloadable by path). Verify polytable's reader against each: fall back to
  listing `metadata/*.metadata.json`, treat version tokens as opaque, take the
  metadata pointer from a configured catalog, and treat per-column stats maps
  as optional everywhere (#641/#667 NPE class).

## Now: Azure, end to end

A stated requirement rather than an extension, and the one direction on this
page that is not driven by upstream: polytable must work against Azure storage
and Azure catalogs, OneLake included. Nothing else on this page is blocked on
it, and it is blocked on nothing.

- **Azure storage backend** (T51): `abfss://` and ADLS Gen2, with the OneLake
  path shape on top of it. Today `NewStorageForPathWithOptions` refuses the
  scheme outright, which is the correct failure but a failure nonetheless.
  Credential coverage is the real scope — Entra ID workload identity, managed
  identity, service principal, SAS and account key — since a backend that only
  accepts one of them is not deployable.
- **OneLake and Fabric catalog** (T52): the read API is Iceberg-REST
  compatible, so `pkg/catalog/rest.go` is the entry point rather than a new
  client, but Entra ID authentication and Fabric's workspace/lakehouse
  identifier shape are new surface. Upstream tracks the same idea as #810 and
  has not built it; there is no reference implementation to follow.

## Next: incremental-sync correctness

Upstream's open-issue tail shows exactly where incremental sync rots. Each is a
scenario for the test matrix before (or instead of) an implementation change:

- Partition specs resolved per manifest spec-id, not table-current (#126).
- Expired or truncated source history must force a snapshot fallback — landed
  for Delta in T36; mirror the check for Iceberg expired snapshots (#147).
- Rollbacks and restores translated as rollbacks, not as bare file removals
  (#40).
- Optimistic-concurrency behavior when a foreign writer commits to the target
  between sync and commit (#124).
- Path-canonicalization consistency: `DiffFiles` keys on `PhysicalPath`, and
  #586 upstream shows phantom add/remove pairs when two code paths normalize
  the same file differently. One property test: snapshot path and incremental
  path must produce byte-identical paths for the same file.
- Foreign-metadata exclusion: a reader listing a shared table directory must
  skip every other format's metadata directories (#813, #814 — upstream tables
  self-corrupted on round-trip). Audit the Parquet and Hudi sources for this.

## Parity endgame

- **Deletion vectors beyond Delta descriptors** (T24): upstream's umbrella
  #339 (detect → model → read → write, per format) is a ready-made task
  decomposition, unstaffed there — including the snapshot-path hole (#640)
  polytable must not copy. No upstream reference implementation exists to
  follow; spec-first, like the rest of this repository.
- **Catalog fan-out**: upstream RFC-1 (XCatalogSync) syncs one table into many
  catalogs with per-target table identifiers — the design polytable needs
  anyway to fix the one-name-overwrites registration gap recorded in
  [features and limitations](features-and-limitations.md).
- **HMS**: keep the explicit not-implemented refusal until a consumer with a
  concrete deployment appears; the type exists for config parity.

## Testing roadmap

The bar is upstream's `ITConversionController` scenario list — matched in
method, exceeded in engine diversity, behind in scenarios (see
[how polytable is tested](testing.md)):

1. Widen the fixture matrix beyond insert-only: upserts and deletes,
   compaction/replace, a Delta column rename under column mapping (#711's
   field-id trap), time travel, out-of-sync incremental.
2. Version-diverse fixtures, freely added: delta-rs old and new protocol
   versions, a Spark-written Delta table with v2 checkpoints, pyiceberg and
   Spark-written Iceberg, Hudi 0.14 and 1.2 (the latter committed).
3. Round-trip pair testing (A→B→A equivalence): still open upstream after two
   attempts (#24, #113, dead #252) — a place to lead rather than follow.
4. The Java interop nightly (T30): pin the upstream 0.5.0 jar when it ships;
   re-pin at 0.6.0. Its fixtures also become the Spark-written half of item 2.
5. Re-sweep upstream at each of their cutoffs and refresh
   [the upstream watch](upstream-watch.md).

## Small, high-leverage items

- Source-format auto-detection from the table directory (#830 upstream): one
  probe over `_delta_log/`, `metadata/`, `.hoodie/` — removes the most common
  CLI friction.
- Diff upstream PR #715's REST-spec change against
  `spec/rest-service-open-api.yaml` for drift.
- The doc-version guard: keep guide version claims moving with CI pins
  (recorded in CLAUDE.md after upstream's own docs drifted for years, #904).

## Differentiators to protect

Zero-JVM footprint (13.9 MiB binary, millisecond start), the embeddings the
JVM cannot offer (C ABI, Python, WASM, REST sidecar), Paimon support, the raw
Parquet source, and spec-first format implementations verified by foreign
engines. The Azure work above is held to the same bar: an Azure SDK must stay
out of the WebAssembly build the way the AWS SDK now does, behind the same
build tags. Every roadmap decision above defers to these: parity work must not
add a runtime dependency that erodes them.

## Watching, not building

Hudi RFC-93 (write-time Iceberg metadata, which routes around post-hoc sync),
DuckLake (#726), Parquet Variant and geospatial types (#803, #804 — gated on
codec support in `parquet-go`), cross-format indexing (#887), and upstream's
Spark-free runtime thread — the one to watch closest, since its API shape is
the convergence point between the two projects. OneLake and Fabric left this
list on 2026-08-22: they are a stated requirement now, scheduled as T51 and
T52 below.
