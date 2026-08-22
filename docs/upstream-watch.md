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

# Upstream watch

A knowledge base of the upstream
[apache/incubator-xtable](https://github.com/apache/incubator-xtable) project —
what it needs, plans, has working, and has broken — as of **2026-08-22**,
synthesized from a full sweep of its 114 open issues, 62 open pull requests,
the dev@xtable mailing-list archives, and its recent merge history. Issue and
PR numbers below refer to the upstream tracker. This page ages by design:
re-sweep it at each upstream release cutoff (see
[the release train](#the-release-train)).

How this feeds polytable's own planning is worked out in [roadmap.md](roadmap.md);
task-level consequences live in [improvement-plan.md](improvement-plan.md).

## The release train

Upstream moved from ad-hoc releases to a time-boxed train with rotating release
managers. The date holds and scope slips.

- **0.4.0-incubating shipped August 2026** — rc1 was cancelled over
  LICENSE/NOTICE and source-release defects; rc2 passed unanimously (8 +1, 5
  binding, result ~August 20).
- **0.5.0 — cutoff September 30, 2026** (RM: Tim Brown). Six agreed items, all
  still open: Hudi table version 9 in the Hudi target (#834), the
  Hudi-managed pluggable Iceberg format (#722), Delta Kernel as the default
  Delta path (#886), `xtable-spark-runtime` (#836), cross-format indexing
  (#887, rfc), and AI agent tooling (#888).
- **0.6.0 — cutoff November 15, 2026** (Vinoth Chandar offered RM). Headline
  and only labeled item: the Spark 4.0 / Scala 2.13 / Java 17 upgrade (#902),
  with the skipped Spark 3.5 step folded in. Kept out of 0.5.0 so the Delta
  2.4→4.0 jump doesn't land on top of the Kernel flip.
- **0.7.0 — cutoff January 5, 2027.** No scope yet.

There is no roadmap page on the upstream website; the de facto roadmap is the
dev-list scope threads plus release-labeled issues. That is why this page
exists and why the re-sweep triggers are the cutoffs above.

## What upstream is building

- **Delta Kernel as the default Delta path (#886).** The implementation is on
  main (the Kernel conversion target merged June 2026 as #801); the one open
  piece is integration coverage (#903), which already exposes a live defect —
  the Kernel target calls `withSchema()` only at table creation, so
  post-creation schema evolution is silently dropped. Until that lands fixed,
  upstream's Kernel-based Delta target is not a trustworthy conformance oracle
  for polytable's Delta target, particularly for schema-evolution cases.
- **Hudi 1.x (#834, #835, #778).** The Hudi target moves to table version 9,
  keyed by a `xtable.hudi.target.table_version` config defaulting to 9, with
  the column-stats index on by default for 1.1.x. This is the same transition
  polytable hit from the read side (see
  [features-and-limitations.md](features-and-limitations.md)).
- **Write-time Iceberg metadata from Hudi (#894, Hudi RFC-93).** A Hudi 1.1+
  writer maintains an Iceberg metadata tree as it commits
  (`hoodie.table.format=ICEBERG`) — no sync job at all. Not portable (it hooks
  Hudi-internal SPI), but it is an architecture signal: for the Hudi→Iceberg
  pair, upstream's bet is co-generation at write time, which competes with the
  after-the-fact sync model both XTable and polytable implement.
- **Spark 4.0 / Scala 2.13 / Java 17 (#902).** Pure JVM toolchain work — ~18
  files calling Delta's internal Scala APIs to port, 15 files of Scala 2.13
  interop. None of it has a Go analogue; its value to polytable is the engine
  version baseline it fixes (Delta 4.0, Iceberg 1.10+, Hudi 1.2, Paimon 1.3.x).
- **`xtable-spark-runtime` (#836, #838, #839).** A thin provided-scope bundle
  plus a Spark listener that syncs inside the writer's job. JVM-specific, but
  it redefines *when* sync happens; polytable's daemon is the analogous answer.
- **The Spark-free `xtable-java-runtime` DISCUSS thread** (July 2026, active,
  no tracking issue yet). Upstream is trying to escape the Spark dependency
  that polytable never had — the strongest available validation of the port's
  premise, and the thread to track for the runtime API shape upstream
  converges on.

## What is broken upstream, and what it means for the port

Each family below carries a port-relevance verdict from the sweep: does the
same mechanism exist in polytable's design?

**Iceberg current-metadata resolution** (#431, #287, #504, #759, #354). Path
loading assumes the Hadoop `version-hint.text` convention; catalog-writing
engines (Snowflake, Glue/Athena, Polaris) never create it, and Snowflake names
metadata `v<nanosecond>.metadata.json`, which overflows an int32 version parse.
Port-relevant: an Iceberg source must fall back to listing
`metadata/*.metadata.json` with int64/opaque version tokens, and give a
catalog-pointing error for catalog-managed layouts.

**Incremental-sync correctness traps** (#126, #147, #40, #124, #779).
Partition specs must be resolved per-manifest spec-id, not table-current
(#126); expired source history must be detected and fall back to full snapshot
sync — Iceberg does this, Delta's check trusted logical history while VACUUM
had deleted the files (#779, exactly the class polytable closed with Delta
checkpoint support, T36); rollbacks/restores degrade into plain file-removals
so target history diverges (#40); Delta target commits lack
optimistic-concurrency handling (#124). All four mechanisms exist in any
incremental sync implementation.

**Path-canonicalization phantom rewrites** (#586). On a 7M-file table,
alternate snapshots emit spurious add/remove pairs because state diffs key on
data-file paths and two code paths canonicalize them differently.
Port-relevant and sharp: polytable's `DiffFiles` keys on `PhysicalPath` and
recently became scheme-aware — any normalization inconsistency between
snapshot and incremental paths reproduces this.

**Column stats fragility** (#641, #667, #798, #811, #815, #760). NPEs when a
writer omits per-column stats maps (they are optional per field, and Snowflake
omits them); corrupt Iceberg bounds when float/double values are not coerced to
the field's declared width before byte encoding (#798); and a persistent
cost fight — skip-stats options, parallel footer reads, footer fallback when
native stats are missing. Port rule: stats optional, concurrent, footer-backed,
type-exact at bound encoding.

**Foreign-metadata-dir corruption** (#813, #814). Hudi partition discovery
treated `_delta_log/` (holding checkpoint parquet) as a Hudi partition — on a
synced table, each format's reader must exclude every other format's metadata
directories or round-trip tables self-corrupt. Directly port-relevant: a
polytable-synced directory always contains all targets' metadata side by side.

**Schema evolution by name instead of id** (#711, #712, #282, #642). A Delta
column rename came out of the Iceberg target as drop+add instead of a rename
preserving the field id; renames from Iceberg never populate Delta
column-mapping properties; Delta generated columns are silently dropped from
the internal schema. Port rule: field identity is the id, never the name.

**Deletion vectors have a snapshot-path hole** (#640). Upstream's DV
translation covers only the incremental path — the first sync of a table is
always a snapshot, so DVs are lost exactly once per table. Any DV
implementation must convert them in full-snapshot sync too.

**Assorted mechanisms from merged or reviewed PRs worth mirroring:** DATE
partition-value parsing in the Delta value converter (#869); Hudi incremental
must synthesize REMOVEs for files a CLEAN already deleted (#824); table name
must come from the table identifier, never the base path — upstream wrote
`hoodie.table.name=s3a://…` (#494/#630); source-format auto-detection from the
table directory is a cheap CLI win (#830); a `--failOnError`-style exit-code
contract for partial failures (#794); Hudi 0.x loses the timestamp_ntz logical
type in Parquet (#672).

## Where polytable is ahead

- **Paimon** — upstream is still recruiting contributors (#275); polytable
  ships a Paimon source and target.
- **Python bindings** — requested upstream (#253); polytable ships them.
- **Parquet source** — the single-footer schema bug (#901) was fixed in
  polytable first (footer merge plus the Hive partition column); upstream's
  parquet-source effort (#553, #592) is still in flight.
- **Delta checkpoints under log retention** — the failure class behind
  upstream #779 is closed here (T36 in
  [improvement-plan.md](improvement-plan.md)).
- **The premise itself** — no JVM, no Spark, one static binary; upstream's
  jar-size work (#896/#897: 1.1 GB → 423 MB) and the `xtable-java-runtime`
  thread are both movement toward properties polytable starts with.

## Unclaimed ground

- **Deletion vectors (#339 umbrella, sub-tasks #341–#348).** Unstaffed
  upstream despite repeated demand; the one implementation PR (#661, Delta DV
  extraction) has been stalled since March 2025. Upstream's decomposition —
  detect, model, read, write, per format — is a ready-made task breakdown, and
  there is no merged upstream reference to copy: whoever implements it first
  defines the shape.
- **Round-trip conformance testing (#24, #113, #252).** Open upstream since
  the beginning; the only PR is dead onetable-era code. This matches the
  coverage bar recorded in [testing.md](testing.md) — the ground is open on
  both sides.
- **The community-PR stall pattern.** External feature PRs (Unity Catalog
  sync #802, skip-stats #811, ORC #731, footer fallback #760, and the rest of
  the parisni series) sit at COMMENTED for 3–14 months while committers do
  release, Kernel, and spark-runtime work. Treat unmerged upstream mechanisms
  as intent, not spec.

## Standing watch items

- **#715** — approved-but-unmerged changes to the upstream REST service spec;
  diff against the vendored `spec/rest-service-open-api.yaml` for drift.
- **Hudi RFC-93 / #894** — if write-time Iceberg metadata becomes the normal
  Hudi→Iceberg path, the sync-based pair loses relevance; watch adoption.
- **#726 DuckLake** as a possible new format; **#803/#804** Parquet Variant
  and geospatial types (upstream blocked on Spark 4 — a Java-only blocker a Go
  implementation would not share).
- **#810 OneLake catalog** — blocked here on an Azure storage backend either
  way; the read API is Iceberg-REST-compatible.
- **Re-sweep triggers:** the 0.5.0 cutoff (2026-09-30), the 0.6.0 cutoff
  (2026-11-15), and any [RESULT] vote on general@incubator that changes the
  project's incubation status.
