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

# How polytable is tested

polytable is a metadata translator, and a translator that only round-trips its own output hides
every bug that is symmetrical in its reader and writer. The test strategy exists to break that
symmetry: foreign implementations sit on both sides of the converter, writing what polytable reads
and reading what polytable writes.

## The verification gate

`make check` is the gate every change must pass:

```sh
gofmt -l .                                 # formatting
go vet ./...
GOOS=js GOARCH=wasm go vet ./cmd/polytable-wasm
go test -short ./...
golangci-lint run ./...
```

`-short` skips the container suites; run `make test-containers` to include them (requires Docker).
`make test-race` adds the race detector.

## Foreign fixtures: real writers on the read side

`test/testdata/fixtures/` holds tables written by foreign implementations — polytable has never
touched those directories. `test/foreign_fixtures_test.go` and
`test/delta_checkpoint_fixture_test.go` assert polytable's readers against each fixture's
`manifest.json`, which records what the writer reported: files, row counts, schema, partition
values, and column bounds.

| Fixture | Writer | What it proves |
| :--- | :--- | :--- |
| `delta-rs/sales` | delta-rs (`deltalake`) | Partitioned Delta table with a mid-history schema change |
| `delta-rs-checkpoint/orders` | delta-rs | Table whose pre-checkpoint JSON commits were deleted by log cleanup; state is only recoverable through the Parquet checkpoint |
| `delta-rs-deletes/returns` | delta-rs | `DeltaTable.delete()`: a whole-partition delete (pure `remove`, no `add`) and a single-row delete that forces a rewrite (`remove` + `add` in one commit) |
| `delta-rs-compaction/clicks` | delta-rs | Unpartitioned table, `optimize.compact()`: four small files replaced by one, no row changed |
| `pyiceberg/events` | pyiceberg | Iceberg v2 table with Avro manifests and multiple snapshots |

`test/fixtures/generate.py` regenerates them; the manifest records the writer library and version.
Fixtures are deliberately created with different library versions over time — a reader gap tends
to appear at a version boundary, the way Delta checkpoints only matter once a writer has cleaned
up its log.

## Engine verification: real readers on the write side

The reverse direction has real engines read what polytable wrote.
`test/engineverify_duckdb_test.go` runs DuckDB against the converted targets and compares
row-level query results; CI pins the DuckDB version (`integration.yml`), and `docs/how-to.md`
quotes only runs performed with that pinned version. Both directions have caught bugs that no
self-referential test could see — the register (T28, T29, T31) records them.

## Container suites

`test/dockertest_minio_matrix_test.go` runs the sync matrix against a real MinIO S3 endpoint,
`test/dockertest_iceberg_rest_test.go` registers tables in a real Iceberg REST catalog server
(the `tabulario/iceberg-rest` image), and `test/dockertest_azurite_test.go` runs the matrix against
the Azurite Azure Storage emulator. All three start containers via `ory/dockertest` and are skipped
in `-short` mode.

The Azurite suite passes: both sync directions run through the Azure backend against the emulator,
and `abfss://` paths round-trip through `List`. It covers the shared-key credential path only, and
the emulator is not Azure — see `docs/azure-test-environment.md` for what a real subscription adds.

Two Azurite flags are required and set in the suite. `--blobHost 0.0.0.0` makes the listener
reachable through a published port; without it the listener binds the container's loopback and the
connection resets. `--skipApiVersionCheck` is needed because `azblob` sends a newer `x-ms-version`
than Azurite recognizes, which Azurite rejects with `InvalidHeaderValue`.

If every container suite fails at once with `connection reset by peer` on a published port, suspect
the Docker daemon rather than the code. Docker Desktop can reach a state where it accepts the TCP
connection and resets every HTTP request while the container answers correctly on its own network.
Restarting Docker Desktop clears it.

## The coverage bar

Upstream Java XTable's `ITConversionController` is the benchmark this suite must match or exceed:
write through a real engine, sync, read the source and every target back through a real engine,
and compare full datasets — across upserts and deletes, compaction, concurrent writes, time
travel, partition specs, both sync modes, out-of-sync incremental recovery, and corrupted-snapshot
recovery. polytable currently matches the method (foreign engines on both sides) and exceeds it in
engine diversity; deletes and compaction now have a Delta-side fixture each (T46), but time travel,
concurrent writes, and the Iceberg-side delete/compaction fixtures remain gaps. Column rename under
Delta column mapping is untested by design, not by omission: `deltalake` 1.6.3 — the pinned fixture
writer — refuses `delta.columnMapping.mode` outright, at `CREATE TABLE` and at `SET TBLPROPERTIES`
alike, and exposes no rename API anywhere in the package, so no fixture at this writer version can
exercise it. Widening the remaining scenario dimensions is tracked in `docs/improvement-plan.md`
(T30 and the T37 fixture work).

## Unit-test conventions

Tests live in an external `<pkg>_test` package, are table-driven, and everything under `pkg/`
calls `t.Parallel()` in both the parent test and its subtests. The e2e suites in `test/` do not,
and the three `dockertest_*` suites must not. `testify` (`assert` + `require`) is the only
assertion library.

## Benchmarks

`make bench` runs the benchmarks and `make bench-footprint` measures binary size and idle RSS.
They are not part of the gate — timings on a loaded machine are noise. Quote absolute numbers only
with the table's file count attached, and update `SPEC.md` §9 from real output only.
