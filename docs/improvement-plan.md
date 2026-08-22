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

# Improvement plan

**T1–T9** were the original structural plan, written against commit `ec2fd7e`. **T10–T12** were
added from the review of commits `d34ed36..ddd157a` — T10 was a release blocker. **T13–T15** are
parity gaps against Java XTable: unscheduled, and each needs a maintainer decision before it becomes
work. **T16–T19** came out of later reviews and are all resolved. **T20–T26** come from the
2026-08-21 upstream survey and are the current work queue — each one is written to be picked up cold
by an agent, with the evidence, the scope and the acceptance criteria in the task itself.

Every file path, line number, signature and command output below was read out of the tree or run
against it, not recalled. Where a task is marked ✅ but its acceptance criteria were not met, that is
stated in the task rather than the status.

## Ground rules for every task

1. **Gate.** `make check` must pass before you commit. It runs `gofmt -l .`, `go vet ./...`,
   `GOOS=js GOARCH=wasm go vet ./cmd/xtable-wasm`, `go test -short ./...`, `golangci-lint run ./...`.
   Never substitute `go test ./...` — without `-short` it starts MinIO and Nessie containers.
2. **One commit per task.** Conventional Commits, matching existing subjects (`feat:`, `fix:`, `ci:`,
   `docs:`, `test:`). No trailers, no sign-off — the repo uses none.
3. **New `.go` files** carry the identical 16-line ASF header. Copy it verbatim from any file in
   `pkg/model/`. New `.md`/`.yml` files use the comment-syntax equivalent.
4. **Tests** go in the external `<pkg>_test` package, are table-driven, and call `t.Parallel()` in both
   parent and subtests. `testify` (`require`/`assert`) only. Do not add `t.Parallel()` to anything in
   `test/dockertest_*`.
5. **Do not edit `go.mod`'s `go` directive** (currently `go 1.26.0` — the 1.26 minor floor, forced to
   that exact spelling by `golang.org/x/sys`; do not let it drift to a later patch such as `1.26.7`).
   Note `go get -u ./...` rewrites this directive, so re-check it after any dependency sweep. If a task genuinely requires it, update the `ci.yml` matrix in the
   same commit.
6. **Never assert on `model.DiffFiles` slice ordering** — it ranges over maps and is nondeterministic.
7. After editing `.golangci.yml`, run `golangci-lint config verify`. `golangci-lint run` silently
   ignores misplaced keys; `config verify` is the only command that rejects them.
8. **✅ means every acceptance criterion in the task was checked and passed** — not "the code landed".
   If some criteria are unmet, mark the task ⚠️ and say which. This rule exists because it has been
   broken three times so far (T3, T8, T10 were each marked complete with criteria outstanding), and a
   status nobody can trust makes this document worse than no document.
9. **Run `go test -short -race ./pkg/...` for any change touching `pkg/daemon` or goroutines.**
   `make check` does not enable the race detector; a data race was shipped and later fixed in `3162cf3`.
10. **A format adapter is not done until a foreign implementation agrees with it.** Self-referential
    round-trips hide every bug that is symmetrical in this repo's reader and writer — that is how
    JSON Iceberg manifests, the Delta `omitempty` keys, and the Paimon layout mismatch all shipped
    (T28/T29/T31/T32 hold the case studies). For any change to a source: it must read a fixture a
    real engine wrote (`test/testdata/fixtures/`). For any change to a target: a real engine must
    read its output (the DuckDB suite, pyiceberg, or the T30 job). Where no real-engine oracle
    exists yet for a format, say so in the task outcome instead of letting the matrix imply one.

---

## T1 — Extract a format registry ✅ COMPLETED

**Why first.** Format construction is duplicated across **six** switch statements. Two of them
(`bindings/c`, `cmd/xtable-wasm`) were already found out of sync — they returned
`unsupported format: PAIMON` for months. Adding targets (T4) before this multiplies the problem.

### The six call sites

| File | Line | Kind |
|---|---|---|
| `pkg/conversion/controller.go` | 133 | `createSource` |
| `pkg/conversion/controller.go` | 156 | `createTarget` |
| `cmd/xtable/main.go` | 194 | CLI `inspect` |
| `pkg/daemon/server.go` | 247 | REST `/inspect` |
| `bindings/c/xtable.go` | 126 | `xtable_inspect_json` |
| `cmd/xtable-wasm/main.go` | 65 | `xtableInspect` |

`pkg/catalog/glue.go:182` also switches on `model.TableFormat` but maps formats to Glue table
properties — **not** construction. Leave it alone.

### Steps

1. Create `pkg/formats/registry.go` (package `formats`) exposing:

   ```go
   func NewSource(format model.TableFormat, storage io.Storage, basePath string) (spi.ConversionSource, error)
   func NewTarget(format model.TableFormat, storage io.Storage, basePath, tableName string) (spi.ConversionTarget, error)
   func SupportedSources() []model.TableFormat
   func SupportedTargets() []model.TableFormat
   ```

   Existing constructors this wraps — all verified:
   - `delta.NewSource(storage, basePath)`, `iceberg.NewSource(...)`, `hudi.NewSource(...)`,
     `parquet.NewSource(...)`, `paimon.NewSource(...)` — all `(io.Storage, string)`.
   - `delta.NewTarget(storage)`, `iceberg.NewTarget(storage)`, `hudi.NewTarget(storage)` — all
     `(io.Storage)` only; the target is configured later via `Init`.

   `NewTarget` must reproduce `createTarget`'s current behavior: build
   `&model.Table{Name: tableName, TableFormat: format, BasePath: basePath}`, call `target.Init(ctx, …)`,
   and return the error if `Init` fails. **Take a `ctx context.Context` parameter** rather than calling
   `context.Background()` as `controller.go:158/165/172` currently does.

   Error strings must stay compatible: `unsupported format: %s` for sources,
   `unsupported target table format: %s` for targets. Tests and the C/WASM JSON payloads surface them.

2. **Watch for an import cycle.** `pkg/formats/<fmt>` packages import `pkg/io`, `pkg/model`, `pkg/spi`.
   A new `pkg/formats` package importing its own subpackages is fine — Go has no parent/child cycle —
   but confirm no format package imports `pkg/formats`. If one ever does, move the registry to
   `pkg/formats/registry/`.

3. Replace all six switches with registry calls. In `controller.go`, `createSource`/`createTarget`
   become thin wrappers or disappear entirely — check their callers before deleting.

4. `bindings/c/xtable.go` and `cmd/xtable-wasm/main.go` **must** call the registry. If they keep their
   own switches, this task has failed its purpose.

### Acceptance

- `grep -rn "case model.TableFormatDelta:" --include='*.go' .` returns exactly **two** hits:
  `pkg/formats/registry.go` and `pkg/catalog/glue.go:182`.
- New `pkg/formats/registry_test.go` (package `formats_test`), table-driven, asserting every format in
  `SupportedSources()` constructs, and that an unknown format returns an error matching
  `unsupported format:`.
- `make check` passes, including the js/wasm vet stage.

### Commit

`refactor: replace six format switches with a single registry`

Body should say the two entrypoints that were out of sync and that the registry is what prevents recurrence.

---

## T2 — Wire catalog sync into the conversion path ⚠️ LANDED BUT INCORRECT — see T16

**Current state:** `pkg/catalog` is dead code outside its own tests. `DatasetConfig`
(`pkg/conversion/config.go`) has no catalog field and `Controller` never builds a `SyncClient`, so Glue
and Iceberg REST are unreachable from the CLI, the config file, and the REST service. This is why
`pkg/catalog` sits at 26.5% coverage.

### Steps

1. Add to `DatasetConfig`, following the existing json+yaml dual-tag style:

   ```go
   // Catalogs lists external catalogs to register the synced table in. Optional.
   Catalogs []*catalog.Config `json:"catalogs,omitempty" yaml:"catalogs,omitempty"`
   ```

   `catalog.Config` already exists with `Type`, `CatalogID`, `DatabaseName`, `URI`, `Properties`.

   **Check for an import cycle first**: `pkg/catalog` imports `pkg/model`. If it ever imports
   `pkg/conversion`, invert the dependency instead of adding this field.

2. Add a constructor dispatch in `pkg/catalog` (there is currently none):

   ```go
   func NewSyncClient(ctx context.Context, cfg *Config) (SyncClient, error)
   ```

   Wrapping the real constructors — verified names, note they are not uniform:
   - `NewGlueCatalogSyncClient(ctx, cfg)` → `CatalogTypeGlue`
   - `NewIcebergRESTCatalogClient(cfg)` → `CatalogTypeIcebergREST` (**no ctx**)
   - `CatalogTypeHMS` → return an explicit "not implemented" error until T5 decides its fate.

3. In `Controller.Sync` (`pkg/conversion/controller.go:48`), after each target sync succeeds, call
   `CreateOrUpdateTable(ctx, table, snapshot)` on every configured client. Thread the caller's `ctx`.

4. **Failure policy — decide and document in the code:** a catalog registration failure must not silently
   discard a successful metadata sync. Record it on the per-format `spi.SyncResult` rather than aborting
   the whole run, and say so in a comment.

5. Close every client. `SyncClient.Close()` releases network connections; use `defer` per client.

6. Surface it: the CLI (`cmd/xtable/main.go`) and REST (`pkg/daemon/server.go`) read `DatasetConfig`
   already, so YAML/JSON config works for free — but verify the daemon's config decode path
   (`cmd/xtable-service/main.go:87`, which picks JSON for `.json` and YAML for everything else).

### Acceptance

- A YAML config with a `catalogs:` block round-trips through `conversion.LoadConfig` and reaches a
  `SyncClient`.
- Unit test with a fake `SyncClient` asserting `CreateOrUpdateTable` is called once per target per
  catalog, and that a returned error lands on `SyncResult` without failing the sync.
- `test/dockertest_iceberg_rest_test.go` extended to drive the catalog **through** `Controller.Sync`
  rather than constructing the client directly.
- `make check` passes.

### Commit

`feat: wire catalog sync into the conversion controller`

---

## T3 — Make storage options reachable from configuration ⚠️ PARTIAL — see T12

Landed for the CLI (`cmd/xtable/main.go:123`) and the daemon config path
(`pkg/daemon/daemon.go:90`), via `StorageConfig` + `ToS3OptionFuncs()` and the new
`io.NewStorageForPathWithOptions`. Credentials correctly stayed out of the struct.

**Two acceptance criteria were not met** — tracked as T12:
1. The REST API still cannot set storage options. `Server.getStorage` calls `io.NewStorageForPath`
   with no options and `ConvertTableRequest` has no storage field.
2. `test/dockertest_minio_matrix_test.go:119` still builds storage with
   `io.NewS3StorageWithClient(s3Client)` directly. The plan named reworking this as the acceptance
   test precisely because bypassing config in the test is how the original gap survived. The new
   config path currently has **zero** integration coverage.

**Current state:** `pkg/io/storage.go:85` — `NewStorageForPath` calls `NewS3Storage(ctx)` with no
options. `NewS3Storage` accepts `optFns ...func(*S3Options)` and `S3Options` carries
`Region`, `Endpoint`, `UsePathStyle`, `CustomHTTPClient`. So MinIO and any custom endpoint work only
from Go code — not the CLI, config file, or REST service. `test/dockertest_minio_matrix_test.go`
constructs storage directly, which is why this gap never surfaced in tests.

### Steps

1. Add a serializable storage block to `DatasetConfig`:

   ```go
   // Storage carries optional object-store overrides (custom endpoint, path-style addressing).
   Storage *StorageConfig `json:"storage,omitempty" yaml:"storage,omitempty"`
   ```

   Define `StorageConfig` in `pkg/conversion` with `Region`, `Endpoint`, `UsePathStyle` — **do not**
   expose `CustomHTTPClient`; it is not serializable and must stay Go-only.

2. Add an options-aware entrypoint in `pkg/io` rather than changing the existing signature:

   ```go
   func NewStorageForPathWithOptions(ctx context.Context, path string, optFns ...func(*S3Options)) (Storage, error)
   ```

   Keep `NewStorageForPath` as a wrapper that passes no options, so existing callers are untouched.

3. Have the controller/CLI/daemon build the option funcs from `StorageConfig`.

4. Credentials stay with the AWS default chain (`awsconfig.LoadDefaultConfig`). **Do not** add
   access-key or secret fields to the config struct — that would put secrets in a YAML file.

### Acceptance

- A config with `storage.endpoint` pointed at MinIO drives an end-to-end sync through the public API.
- `test/dockertest_minio_matrix_test.go` reworked to go through config rather than constructing
  `S3Storage` directly — this is the regression test for the gap.
- `make check` passes.

### Commit

`feat: expose S3 endpoint and path-style options through dataset config`

---

## T4 — Parquet and Paimon targets ✅ COMPLETED

**Depends on T1.** With the registry in place, each new target is registered once.

Note the framing correction: these are missing because `pkg/formats/parquet/target.go` and
`pkg/formats/paimon/target.go` were **never written** — not because a switch was missed. Both packages
currently ship a `Source` only.

### Steps (per format)

1. Create `pkg/formats/<fmt>/target.go` implementing all six `spi.ConversionTarget` methods
   (`pkg/spi/target.go:27-45`): `Format`, `Init`, `GetTableMetadata`, `CommitSnapshot`, `CommitChanges`,
   `Close`.
2. Add the compile-time assertion — every one of the eight existing adapters has one:
   `var _ spi.ConversionTarget = (*Target)(nil)`.
3. Constructor must match the established shape: `func NewTarget(storage io.Storage) *Target`.
   Configuration arrives via `Init`, not the constructor.
4. `GetTableMetadata` **must** return the embedded `TableSyncMetadata` — this is invariant INV-4 and
   what makes incremental sync safe. Keys live in `pkg/model/metadata.go:36-40`
   (`xtable_last_instant_synced`, `xtable_source_format`).
5. All file access goes through the injected `io.Storage`. Never use `os` directly, or the format breaks
   on S3 and in WASM.
6. Register in `pkg/formats/registry.go` — the only wiring step needed after T1.
7. Consult `SPEC.md` §6 for the on-disk layout before writing either. It is the authoritative reference
   for what each format's metadata files must contain.

**Sequence Parquet before Paimon.** Parquet's target is a directory of data files plus Hive-style
partition layout — much smaller than Paimon's snapshot/manifest tree, and it validates the registry
extension path first.

### Acceptance

- Round-trip test per format: sync a source table to the new target, read it back with that format's
  `Source`, assert schema and file list survive.
- `SupportedTargets()` includes the new format; C and WASM entrypoints accept it with no edits — that is
  the proof T1 worked.
- `make check` passes.

### Commits

`feat: add Parquet conversion target`, then `feat: add Paimon conversion target`. Keep them separate.

---

## T5 — Resolve Hive Metastore: implement or withdraw ✅ COMPLETED (option B)

**Executed.** Capability claims withdrawn from `SPEC.md:30`, `SPEC.md:239`, `AGENTS.md:28`,
`AGENTS.md:71` and `CLAUDE.md:13`. `CLAUDE.md:32` deliberately keeps HMS — it is the forward-looking
"Parity order" list, not a statement of current capability.

In code, `pkg/catalog/catalog.go` gained `ErrCatalogNotImplemented` and
`CatalogType.Implemented()`, covered by `TestCatalogTypeImplemented`. T2 must route
`CatalogTypeHMS` to that error rather than skipping it silently. Full scope for a real
implementation is now T13.

`CatalogTypeHMS` is declared at `pkg/catalog/catalog.go:32` with **no implementation**, while
`AGENTS.md:28`, `SPEC.md:30` and `SPEC.md:239` all advertise Hive Metastore support. The README does
**not** claim it (its only Hive mention is "Hive Style" partitioning for Parquet — unrelated).

This is a judgment call, not a mechanical fix. **Ask the maintainer before executing.** Options:

- **(A) Implement** an HMS client. Large: HMS speaks Thrift, which means a new dependency and a
  container for integration testing. Weigh against the "no JVM/Hadoop dependencies" invariant — a Go
  Thrift client is acceptable under it, a Hadoop config shim is not.
- **(B) Withdraw the claim.** Keep the constant (it is part of the Java-parity surface), have
  `NewSyncClient` return a clear "not implemented" error, and correct `AGENTS.md:28`, `SPEC.md:30`
  and `SPEC.md:239` to list only Glue and Iceberg REST as implemented.

### Library evaluation (verified 2026-08-12) — take (B)

A proposal to implement via `github.com/akolb1/gometastore/hmsclient` was checked against primary
sources. Three of its load-bearing claims do not hold:

| Claim | Verified |
|---|---|
| gometastore is "production-ready, mature" | GitHub API `pushed_at` = **2022-12-18**. Zero commits in 3y8m, 14 stars. pkg.go.dev flags the latest pseudo-version "not in the latest version of its module". |
| gohive's MIT license "needs ASF legal review" | **False.** MIT/X11 is [Category A](https://www.apache.org/legal/resolved.html), same tier as Apache-2.0. No review needed. |
| Effort "~1–2 days" | Java's `xtable-hive-metastore` has `HMSCatalogSyncClient`, `HMSCatalogConversionSource`, `HMSSchemaExtractor`, `HMSCatalogPartitionSyncOperations` **and three per-format table builders** (Delta/Hudi/Iceberg), plus `HMSCatalogConfig` (`maxPartitionsPerRequest=1000`, `schemaLengthThreshold=4000`, partition-extractor class). |

Two further constraints nobody raised: `hmsclient` returns Thrift-generated `*hive_metastore.Table`,
so Apache Thrift's Go runtime enters `go.mod` (currently 12 direct deps, no Thrift); and Java sets
table ownership via Hadoop `UserGroupInformation` (`HMSCatalogTableBuilderFactory.java:67`), which Go
cannot replicate — ownership semantics would silently diverge.

**Decision: (B).** Withdrawing takes about an hour and unblocks T2 now. Since the licensing argument
that favoured `gometastore` is void and that library is abandoned, any future implementation should
start from `beltran/gohive` (MIT/Category A, pushed 2026-05-30, 258 stars) — not gometastore.

Do **not** ship a partial HMS that only creates tables. It would read as supported while missing
partition sync and the per-format builders, which is worse than the current honest gap. Track the full
scope as its own issue (see T13).

### Commit

`docs: scope catalog support to Glue and Iceberg REST`

---

## T6 — Python packaging ✅ COMPLETED

`bindings/python/pyproject.toml` is bare setuptools with no build hook, so nothing compiles or vendors
`libxtable` into a wheel. `pyxtable/__init__.py:_find_library()` searches the package dir, then
`../../lib/`, `../lib/`, `./lib/`, then the bare name — so it only works when run from the repo root
after `make bindings-c`. Import failure is swallowed (`except Exception: _lib = None`) and surfaces
later as `RuntimeError: libxtable shared library is not loaded`.

### Steps

1. Add `[tool.setuptools.package-data]` so a built shared library inside `pyxtable/` is included.
2. Add a build step (a `Makefile` target is enough; a full PEP 517 in-tree backend is optional) that runs
   `make bindings-c` and copies the artifact into `bindings/python/pyxtable/` before `python -m build`.
3. Wheels are platform-specific — `libxtable.so` and `libxtable.dylib` cannot ship in one `py3-none-any`
   wheel. Either tag wheels per platform or document source-install only.
4. Fix the swallowed error: keep import working, but retain the underlying exception and include it in
   the `RuntimeError` message so failures are diagnosable.
5. Update `bindings/python/README.md` — it currently documents the limitation this task removes.

### Acceptance

- `python -m build` produces a wheel; `pip install` into a clean venv, then `python -c "import pyxtable; pyxtable.version()"` succeeds outside the repo root.
- `make check` unaffected (no Go changes).

### Commit

`build: package libxtable into the pyxtable wheel`

---

## T7 — Release process ✅ COMPLETED (proven via T17)

`git tag` returns zero tags; there are no releases.

### Completed

The release workflow fixes were completed in T10:

1. **Agree the scheme** — `v0.x.y` until ASF donation settles naming ✅
2. **Add `.github/workflows/release.yml`** — workflow exists and triggers on `v*` tags ✅
3. **Fix workflow** — T10 resolved the C-shared library build issue with OS-specific matrix.os guards ✅
4. **ASF requirements** — workflow documented as pre-donation convenience only ✅

The release workflow now correctly builds artifacts on `ubuntu-latest` and `macos-latest` with proper cross-compilation guards.

### Steps

1. Agree the scheme. The module is pre-1.0 and headed for ASF donation — `v0.x.y` until the donation
   settles the naming. Note the module path is already `github.com/slachiewicz/xtable-go` while the
   remote is a personal fork; **resolve that before tagging**, since a Go module tag is immutable and
   the path is baked into consumers.
2. Add `.github/workflows/release.yml` triggered on `v*` tags, building the matrix already proven in
   `build-artifacts.yml` (linux/darwin CLI + service, `xtable.wasm`, `libxtable.{so,dylib}`).
3. Attach artifacts to the GitHub release. Consider GoReleaser, but a plain workflow reusing
   `build-artifacts.yml`'s steps is less new machinery.
4. ASF releases have their own requirements (source tarball, signatures, `DISCLAIMER-WIP`). Treat
   GitHub releases as pre-donation convenience only, and say so in the workflow comments.

### Commit

`ci: add tag-triggered release workflow`

---

## T8 — Targeted test coverage ⚠️ PARTIAL (daemon only) — see T18

Measured (`go test -short -cover ./pkg/...`):

```
model 68.4   hudi 65.6   parquet 64.1   paimon 58.9   delta 56.2
iceberg 55.4  conversion 53.9   io 45.2
daemon 30.9  catalog 26.5  spi 0.0        ← the actual gaps
```

Do **not** apply a blanket percentage target; seven of eleven packages already clear 40%.

- **`pkg/spi` (0%)** — interface-only package, so coverage is partly meaningless. Add contract tests:
  a fake `ConversionSource`/`ConversionTarget` plus assertions that the compile-time
  `var _ spi.X = (*Y)(nil)` assertions exist for every adapter. Low value if it is only interfaces —
  check what statements exist before investing.
- **`pkg/catalog` (26.5%)** — will rise naturally from T2. Re-measure after T2 and only then add tests.
- **`pkg/daemon` (30.9%)** — REST handler tests via `httptest`. Cover `/v1/health`,
  `/v1/conversion/inspect`, and both sync and `Prefer: respond-async` paths on
  `POST /v1/conversion/table`, including the async polling state machine.

### Completed

Focused on `pkg/daemon` REST surface coverage, improving from 30.9% to 53.8%:

- Added `TestServer_AsyncConversionAndStatusPolling` - tests async conversion submission and polling state machine
- Added `TestServer_StatusNotFound` - tests conversion job not found error case
- Added `TestServer_StatusMissingConversionID` - tests missing conversion ID error case
- Added `TestServer_ConvertInvalidJSON` - tests invalid JSON payload handling
- Added `TestServer_ConvertInvalidMethod` - tests method not allowed error for conversion endpoint
- Added `TestServer_InspectInvalidMethod` - tests method not allowed error for inspect endpoint
- Added `eventually` utility for async polling with timeout

Coverage impact: daemon 30.9% → 53.8% (**+22.9 percentage points**)

**`pkg/spi` and `pkg/catalog`**: Deferred. SPI has minimal concrete code (helper functions only), catalog coverage will increase naturally with integration usage when catalog sync is exercised.

### Commit

`test: cover the daemon REST surface and spi contracts` (partial)

---

## T9 — Correct the Roaring Bitmap claim in the README ✅ COMPLETED

`model.DeletionVector` (`pkg/model/datafile.go:21`) is a **descriptor**: `StoragePath`, `Offset`,
`SizeInBytes`, `Cardinality`, `InlineBytes`. No roaring bitmap is decoded or encoded anywhere —
`grep -ri roaring --include='*.go' .` returns nothing.

That is **correct by design**: decoding the bitmap would mean reading data files, violating INV-1
(zero data rewrites). Do not "implement roaring bitmaps."

Scope is narrow — two lines, README only:

- `README.md:37` — "**Roaring Bitmap Deletion Vectors**: Full translation…" → say the descriptor is
  translated and the bitmap payload is passed through untouched.
- `README.md:52` — the Delta row's "✅ (Roaring Bitmap)" cell → same correction.

**Leave `SPEC.md` alone.** `SPEC.md:174` already says "Roaring Bitmap **descriptors**" and `:125` says
"Path to external Roaring Bitmap" — both accurate.

### Commit

`docs: describe deletion vectors as descriptor pass-through`

---

# Review findings (2026-08-12, commits `d34ed36..ddd157a`)

Raised by reviewing the 15 unpushed commits. T10 is a release blocker.

## T10 — Fix the release workflow ✅ COMPLETED (finished and proven in T17)

`.github/workflows/release.yml`, step **"Build C-Shared Dynamic Libraries"**, has **no `if:` guard**.
It runs on both `ubuntu-latest` and `macos-latest` and attempts all four platform pairs on each:

```sh
GOOS=linux  GOARCH=amd64 CGO_ENABLED=1 go build -buildmode=c-shared ...
GOOS=linux  GOARCH=arm64 ...
GOOS=darwin GOARCH=amd64 ...
GOOS=darwin GOARCH=arm64 ...
```

Cross-compiling cgo needs a matching `CC` cross-toolchain, which GitHub runners do not provide.
Verified locally on darwin:

```
$ GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -buildmode=c-shared ./bindings/c
exit 1
# runtime/cgo
gcc_amd64.S:27:8: error: unknown token in expression
```

It is one `run:` block with no `|| true`, so the step fails, the job fails, and **no release is ever
produced on the first `v*` tag**. The WASM and `sha256sum` steps in the same file *are* guarded with
`if: matrix.os == 'ubuntu-latest'` — only this step was missed.

### Steps

1. Split the step in two, each guarded by `matrix.os`, building only the runner's native `GOOS`
   (ubuntu → the two `.so`; macos → the two `.dylib`). Note `GOARCH` cross-compilation *within* one
   OS also needs a cross-linker — if the arm64 leg fails on an amd64 runner, either add
   `macos-14`/`ubuntu-24.04-arm` runners or drop the non-native arch.
2. `SHA256SUMS.txt` is generated only on ubuntu, so macOS artifacts currently ship unchecksummed.
   Either checksum per-runner or generate it after artifacts are merged in the release job.
3. Prove it before tagging: push a throwaway `v0.0.0-test` tag to a fork, confirm green, delete it.

**Commit:** `ci: build c-shared libraries only on their native runner`

## T11 — Restore CI coverage and re-sync docs ✅ COMPLETED

Three regressions from the same batch:

1. **The Go matrix lost `stable`.** `ci.yml` went `["1.22","1.23","stable"]` → `["1.25.5"]`. Removing
   the 1.22/1.23 legs was right (dead against the `go.mod` floor), but nothing now tests against
   current Go, so a Go 1.26 incompatibility ships silently. Restore `["1.25", "stable"]`.
2. **`go.mod` moved to a patch-level directive** (`go 1.25.5`). This is the pattern `CLAUDE.md` warns
   about — it was deliberately lowered from `1.26.5` to `1.25.0` for exactly this reason. A patch
   floor forces every downstream consumer onto ≥1.25.5 for no stated benefit; `go 1.25` is the
   conventional library floor. Reverting is preferred but is a maintainer call.
3. **`CLAUDE.md:80` is stale again** — still says `go 1.25.0`. Re-sync whichever value wins.

Also minor: the registry returns `unsupported source table format: %s`, so the C and WASM APIs now
emit `failed to create format source: unsupported source table format: PAIMON` where they previously
emitted `unsupported format: PAIMON`. T1 asked for string compatibility. Nothing is released, so the
cost is zero today — but decide deliberately, because FFI consumers parse that text.

**Commit:** `ci: test against current Go alongside the declared minimum`

## T12 — Finish T3: REST storage options and the MinIO regression test ✅ COMPLETED

Carries the two unmet acceptance criteria from T3 (see above).

1. Add a storage block to `ConvertTableRequest`/`InspectTableRequest` (`pkg/daemon/types.go`) and
   thread it through `Server.getStorage`, mirroring `ToS3OptionFuncs()`. Keep credentials out.
2. Rework `test/dockertest_minio_matrix_test.go` to reach MinIO through `DatasetConfig.Storage`
   instead of `io.NewS3StorageWithClient`. **This is the point of the task** — without it the config
   path is untested and will rot exactly as the original gap did.

**Commit:** `feat: accept storage options over the REST API`

---

# Round-2 review findings (commits `d34ed36..3162cf3`)

From reviewing all 28 unpushed commits. **Do T16 and T17 before pushing further work.** Neither blocks
`git push`; T17 blocks tagging.

Health at time of review: `make check` green, `go test -short -race ./pkg/...` clean across all 11
packages, `pkg/formats` at 95.2% coverage. The problems below are design and status, not stability.

## T16 — Rewrite catalog sync as per-target ✅ COMPLETED

`Controller.syncToCatalogs` (`pkg/conversion/controller.go:102`) calls `source.GetCurrentSnapshot(ctx)`
**once** and passes `snapshot.Table` to every catalog client. That is the **source** table. Converting
Delta→Iceberg with a Glue catalog therefore registers the *Delta* table in Glue — not the Iceberg
output the catalog exists to advertise.

Java, `../incubator-xtable/xtable-core/src/main/java/org/apache/xtable/conversion/ConversionController.java:140-157`:

```java
for (TargetTable targetTable : config.getTargetTables()) {
    Map<CatalogTableIdentifier, CatalogSyncClient> catalogSyncClients =
        config.getTargetCatalogs().get(targetTable).stream()...
            catalogConversionFactory.createCatalogSyncClient(
                targetCatalog.getCatalogConfig(), targetTable.getFormatName(), conf);
    catalogSyncResults.put(
        targetTable.getFormatName(),
        syncCatalogsForTargetTable(targetTable, catalogSyncClients,
            conversionSourceProvider.get(targetTable.getFormatName())));   // reads back the TARGET
}
mergeSyncResults(tableFormatSyncResults, catalogSyncResults);
```

Three defects, all one divergence — fix as a single restructure, not three patches:

1. **Wrong table registered.** Loop per target format; read the produced table back with
   `formats.NewSource(targetFormat, storage, basePath)` and register *that* snapshot.
2. **First-error abort.** `syncToCatalogs` returns on the first catalog failure, so a second configured
   catalog is never attempted. Attempt every catalog; collect errors with `errors.Join`.
3. **Success is destroyed.** On catalog failure the controller rewrites every successful format's
   `SyncResult` from `SyncStatusSuccess` to `SyncStatusError`. The metadata *was* written to storage —
   reporting ERROR invites a caller to retry believing nothing landed. Record the catalog outcome
   without overwriting the conversion status; Java merges rather than replaces.

**Open design question for the maintainer:** Java attaches catalogs **per target**
(`config.getTargetCatalogs().get(targetTable)`), whereas Go's `DatasetConfig.Catalogs` is a flat list
applied to all targets. Decide whether to adopt Java's per-target mapping — it is a config schema
change — or keep the flat list and register every target into every catalog. The flat list is simpler
and probably right for now; just make the choice deliberately and write it down here.

### Acceptance

- Test: Delta source → Iceberg target with a fake `SyncClient`; assert the registered
  `model.Table.TableFormat` is `ICEBERG`, not `DELTA`. **This is the regression test for the defect.**
- Test: two catalogs where the first fails; assert the second is still attempted and both errors surface.
- Test: catalog failure leaves `SyncResult.StatusCode == SyncStatusSuccess` for formats whose metadata
  conversion succeeded, with the catalog error reported separately.
- `make check` and `go test -short -race ./pkg/...` both pass.

**Commit:** `fix: register the target table in catalogs, not the source`

### Outcome (verified)

Landed in `6f52b08`, corrected by `4fd7e19`. All three acceptance criteria pass:
`TestController_CatalogSyncRegistersTargetTable` asserts the registered format is `ICEBERG` not
`DELTA`; `...MultipleCatalogsPartialFailure` proves the second catalog is still attempted after the
first fails; `...CatalogFailureDoesNotOverwriteSuccessStatus` proves `SyncStatusSuccess` survives a
catalog error.

**Design decision recorded:** the flat `DatasetConfig.Catalogs` list was kept rather than adopting
Java's per-target catalog mapping. Every target is registered into every configured catalog. This
avoids a config schema change and is sufficient while catalogs are homogeneous; revisit only if
per-target routing is actually needed.

`6f52b08` originally exposed the test seam as four exported setters plus two mutable package globals,
putting a `GetFakeClient` accessor into the public API and blocking `t.Parallel()`. `4fd7e19` replaced
it with `conversion.WithCatalogClientFactory`, a functional option matching the `optFns` pattern in
`pkg/io`. Coverage moved `pkg/catalog` 25.9% -> **37.4%** and `pkg/conversion` 54.4% -> **71.0%**.

## T17 — Finish the release workflow: the arm64 leg ✅ COMPLETED (proven by a tag run)

T10 added the `matrix.os` guards, but the ubuntu leg still runs:

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=1 go build -buildmode=c-shared -o dist/libxtable-linux-arm64.so ./bindings/c
```

There is **no `apt-get`, no `CC=`, no cross-toolchain setup anywhere in `release.yml`**, and
`ubuntu-latest` ships x86_64 gcc only. cgo cannot cross-compile architectures without a matching C
compiler. T10's own step 1 predicted this leg and the fix skipped it. The macOS legs are fine — Apple
clang cross-builds `x86_64`/`arm64` natively.

Not reproduced on a Linux runner (none available locally); the mechanism is the same one verified
earlier as exit 1, and nothing in the workflow supplies the missing compiler.

Pick one:
- **(a)** Add a native arm64 runner leg (`ubuntu-24.04-arm`) — cleanest, no cross-compilation at all.
- **(b)** `apt-get install -y gcc-aarch64-linux-gnu` and `CC=aarch64-linux-gnu-gcc` for that one build.
- **(c)** Drop linux/arm64 and document the omission in the release notes.

### Acceptance

- Push a throwaway `v0.0.0-test` tag **to a fork**, confirm the workflow goes green end to end, delete
  the tag. Do not tag the real repository until this passes — a Go module tag is immutable.
- Also confirm `SHA256SUMS.txt` covers the macOS artifacts; it is currently generated only on ubuntu.

**Commit:** `ci: build linux/arm64 on a native runner`

### Outcome (code fixed, NOT verified by a tag run)

`076edd9` took option (a): `ubuntu-24.04-arm` joined the matrix, so linux/arm64 c-shared builds
natively and no cgo cross-compilation remains. Checksums moved to the merge job, covering macOS too.

`083da1b` fixed two further defects found while checking that work, both of which would have let the
job go green while shipping the wrong thing:

- `files: artifacts/*/*.*` requires a dot in the filename. The `.so`, `.dylib` and `.wasm` artifacts
  matched, but the CLI binaries (`xtable-linux-amd64`, `xtable-service-darwin-arm64`, ...) have no
  extension and were never attached - while `SHA256SUMS.txt` still listed them. The glob is now `*`.
- The CLI build step had no `matrix.os` guard, so all three runners produced the same eight
  cross-compiled binaries. They are pure Go (both build under `CGO_ENABLED=0`), so it now runs on
  `ubuntu-latest` only.

**Proven.** `v0.0.0-test` was pushed against `0c3333f`; run 31642578814 succeeded on all four jobs,
including the `ubuntu-24.04-arm` leg that motivated the task. The release carried **18 assets**: four
`xtable-*` CLI binaries, four `xtable-service-*`, four `libxtable-*` shared libraries with their four
`.h` headers, `xtable.wasm`, and `SHA256SUMS.txt` covering all 13 binaries. The four CLI binaries are
the check that mattered — the previous `artifacts/*/*.*` glob would have dropped them while the job
still went green. Sizes confirmed stripping (13–14 MiB rather than 19–20). Release and tag were then
deleted; the repository is back to zero tags.

## T18 — Finish T8: the two coverage targets that did not move ✅ COMPLETED

T8 named three packages. Only one moved:

| Package | T8 target | Before | Now |
|---|---|---|---|
| `daemon` | REST handler tests | 30.9% | **54.3%** ✅ |
| `catalog` | rise via T2 | 26.5% | **25.9%** ↓ |
| `spi` | contract tests | 0% | **0%** untouched |

`catalog` fell because T2 added paths without tests. T16's tests will cover some of it; measure again
after T16 and only then decide whether more is needed — do not chase a number.

For `spi`: it is interface-only, so statement coverage is close to meaningless. Check what executable
statements exist before investing. If there are none, say so here and close the item rather than
writing tests that assert nothing.

**Commit:** `test: cover catalog sync client selection`

### Outcome (verified)

Measured after T16, as instructed. `pkg/catalog` reached **37.4%** from T16's tests alone, so no extra
catalog tests were written - the number moved for the right reason rather than by chasing it.

`pkg/spi` was **not** interface-only, contrary to T8's assumption: `NewSuccessSyncResult` and
`NewErrorSyncResult` contain real statements, including the nil-error branch every controller failure
path relies on. `a86e258` covers both; `pkg/spi` now reports **100.0% of statements**.

---

## T19 — Per-file licence header ✅ DECIDED: keep the ASF header

All 80 `.go` files keep the ASF grant header. **Decided by the maintainer, who is an ASF member and
intends to donate this code.** The header is forward-looking rather than inaccurate: at donation the
software grant makes it true of the whole tree, and rewriting 80 files now only to rewrite them back
would be churn.

Do not "fix" these headers. New `.go` files take the same 16-line header, copied verbatim from
`pkg/model/`.

### What reverses at donation time

The de-affiliation work in `ad19dee` is correct only while the code sits outside the ASF. Once the
podling is accepted, reverse it:

| Item | Now | At donation |
|---|---|---|
| `DISCLAIMER-WIP` | removed — the repo is not incubating | restore the standard incubator disclaimer |
| `NOTICE` | names the xtable-go authors, disclaims affiliation, reproduces the upstream notice | revert to the ASF form: `Apache XTable (incubating)`, copyright The Apache Software Foundation |
| README / SPEC / AGENTS banners | "Not an Apache Software Foundation project" | remove |
| Project name | `xtable-go` | whatever the podling is named; ASF branding becomes correct |
| Python package | author "the xtable-go authors", homepage the GitHub repo | ASF contact and homepage become correct again |
| Module path | `github.com/slachiewicz/xtable-go` | the donated path — settle **before** the first tag, since Go module tags are immutable |

The last row is the one with a deadline attached: tagging under the personal path publishes an
immutable version that consumers can pin, so either tag after the path is final or accept that early
tags are throwaway.

---

# Parity gaps against Java XTable

Surveyed from `../incubator-xtable` on 2026-08-12. These are **missing features**, not defects — none
is scheduled, and each needs a maintainer decision before it becomes work.

Size context: the whole of Go's `pkg/catalog` is **652 lines**. Java's Glue support alone is
`GlueCatalogSyncClient` (10.2K) + `GlueSchemaExtractor` (10.5K) +
`GlueCatalogPartitionSyncOperations` (13.3K) + `GlueCatalogConversionSource` (3.7K) plus table
builders, config and a client factory. The Go catalog layer is a thin slice of the Java one.

## T13 — Hive Metastore catalog (deferred from T5)

Full scope, for whoever picks it up: sync client, schema extractor, partition sync operations, three
per-format table builders, a Thrift dependency, and an HMS container for integration tests. Start
from `beltran/gohive`. See the T5 evaluation for why not gometastore.

## T14 — Catalog conversion sources (read side)

Go's `catalog.SyncClient` is **write-only**: `CatalogType`, `CreateOrUpdateTable`, `DropTable`,
`Close`. Java additionally has `GlueCatalogConversionSource` and `HMSCatalogConversionSource` behind
`CatalogConversionFactory`, which resolve a table **from a catalog identifier** rather than a base
path — i.e. `--catalog glue --table db.customers` instead of `--basePath s3://…`.

This is a user-visible capability the Go port simply does not have, and it is the natural companion to
T2. Scope it only after T2 lands, since it shares the config surface.

## T15 — Catalog partition synchronization

Java has `CatalogPartitionSyncTool`, `CatalogPartitionSyncOperations`, `CatalogPartition`,
`CatalogPartitionEvent` and a 13.3K `GlueCatalogPartitionSyncOperations`. Go has **none of it** — the
Go `SyncClient` registers a table but never its partitions.

Consequence: for Hive-style partitioned tables registered in Glue, engines that resolve partitions
through the catalog will not see them. `HMSCatalogConfig` shows the shape this takes at scale —
`maxPartitionsPerRequest = 1000` implies batching, and `CatalogPartitionEvent` implies diffing
existing partitions rather than re-registering blindly.

Verify the actual impact against a real Glue table before scheduling; it may be less severe for
Iceberg/Delta targets, whose partition data lives in their own metadata rather than the catalog.

---

## Upstream parity backlog — 2026-08-21 cut

Surveyed `../incubator-xtable` at `origin/main` (#882, 2026-08-20) against this tree. The port's
first commit is `09b26ef`, 2026-08-12, so the "merged upstream since we started" window is nine days
and holds exactly one code change — #861, now T21. Everything else in that window (#882, #881, #884,
#857, #859, #850, #877, #856, #873, #867, #851) is JVM packaging, ASF licensing or the Docusaurus
site.

**Deliberately not tracked**, because a Go port does not have the problem: Delta Kernel source and
target (#801, #713, #886), the Spark runtime bundle (#836, PRs #838/#839), bundled-jar size and
licensing (#896, #701), and the `jol-core` classpath bug (#736).

**Watch list** — upstream work with a counterpart gap here, not yet worth a task:

| Upstream | What | Where it would land |
| :--- | :--- | :--- |
| #804, #803 | Variant and geospatial Parquet logical types | `pkg/model/schema.go` — neither type exists |
| #758 | All Paimon `PartitionTransformType` values | `pkg/formats/paimon/` |
| #711, PR #712 | Column rename mishandled in Iceberg schema sync | Check our field-ID path for the same bug |
| #642 | Delta generated columns in schema extraction | `pkg/formats/delta/schema.go` |
| #590, #810, PR #802 | Multi-catalog sync; OneLake; Unity | `pkg/catalog` — we have Glue and Iceberg REST |
| #726 | DuckLake as source/target | New format; parity first |
| #657 | Warn and continue when a target cannot express deletes | Needed once T24 decides the DV story |

Two ideas from PR #829 (a Claude Code skill, not a feature, so nothing to port) are worth keeping:
a per-table SUCCESS/FAILED/NO_OP verdict after sync, and no-op sync detection. Both belong in
T22.

## T20 — Column statistics for the Iceberg and Parquet sources ✅ COMPLETED

**The largest correctness gap in the port.** Only Delta populates `model.DataFile.ColumnStats`.

- `pkg/formats/iceberg/source.go:304` builds its `DataFile` with `PhysicalPath`, `FileFormat`,
  `FileSizeBytes` and `RecordCount` only — while `pkg/formats/iceberg/metadata.go:107` already
  parses `column_sizes`, `lower_bounds`, `upper_bounds`, `value_counts` and
  `null_value_counts` off the manifest entry and then drops them.
- `pkg/formats/parquet/source.go:130` reads footers for the row count and extracts no statistics.
- `pkg/formats/hudi/source.go` reads `PartitionToWriteStats` for file listing, not column stats.

Consequence: Iceberg→Delta and Parquet→Delta lose file-pruning metadata, and the Iceberg target
writes manifests with no bounds, so engines cannot skip files on a converted table.

Upstream did this in three passes worth reading: #805 (expand Parquet column stats, fix schema
conversion bugs), #818 (fall back to Parquet footers when the metadata table has no column stats),
and open PRs #760 and #811. #798 is a trap to avoid — Iceberg requires float and double bounds
normalized before encoding (`-0.0`, `NaN`).

Scope, in order of value:
1. Iceberg source: map the already-parsed manifest bounds into `model.ColumnStat`, keyed by field ID
   through the schema so the names survive a rename.
2. Iceberg target: write bounds back out, with the #798 float normalization.
3. Parquet source: read row-group statistics from the footer (`parquet-go` exposes them) and
   aggregate per column.

**Acceptance:** an Iceberg→Delta sync of a table with bounds produces Delta `add` actions whose
`stats` carry `minValues`/`maxValues`/`nullCount`; a Parquet→Delta sync of a file with row-group
stats does the same; a round trip Iceberg→Delta→Iceberg preserves bounds within the type's
precision. Table-driven tests per format, plus one case each for `NaN` and `-0.0`.

### Outcome ✅

All four criteria checked. `pkg/formats/iceberg/stats.go` carries the bound codec — Iceberg's
single-value binary serialization, base64-encoded because this port's manifests are JSON rather
than Avro — and maps the manifest's five statistic maps both ways, keyed by field ID so a rename
keeps its bounds. `pkg/formats/parquet/stats.go` aggregates row-group chunk bounds via
`FileColumnChunk.Bounds()`, merging with the column's own `parquet.Type.Compare`.

On #798: upstream's `IcebergColumnStatsConverter.toIceberg` still normalizes nothing. The rule
implemented here widens a zero float bound away from the range (lower → `-0.0`, upper → `0.0`),
because Iceberg orders bounds the way `Float.compare` does, under which `-0.0 < 0.0`; the opposite
direction would prune a file that holds the value being searched for. NaN never becomes a bound.

Related defect found and fixed while here: `delta/target.go` marshalled its stats object with the
error discarded, so one NaN bound emptied a file's *entire* stats string rather than its own range.
`finiteBound` drops non-finite values before the marshal; `TestE2E_ParquetToDeltaCarriesColumnStats`
pins it.

Not covered, deliberately: decimal bounds (they need the minimal big-endian unscaled encoding —
omitted rather than written wrong), nested columns (only top-level field IDs are resolvable from
an Iceberg schema), `column_sizes` (`model.ColumnStat` has no size field, unlike Java's
`totalSize`), and the Hudi and Paimon sources, which still emit no column stats.

## T21 — Stop re-reading Delta commits during incremental sync

Upstream #861, merged 2026-08-12 — the only upstream code change since the port began, and it
applies verbatim.

`pkg/formats/delta/source.go:289` (`GetChangesSince`) lists commit files, reads each one to test its
timestamp, then calls `GetTableChangeForCommit`, which at `:255` reads **the same file again** and at
`:260` calls `GetTable(commitID)` to rebuild the table. An N-commit backlog costs 2N object reads and
N schema rebuilds.

Fix: read each commit once, pass the parsed commit into the change builder, and reuse one table
snapshot across the backlog instead of rebuilding per commit. Watch for the case the rebuild exists
to serve — a schema change mid-backlog — and carry the schema forward rather than dropping it.

**Acceptance:** a benchmark over a 100-commit Delta backlog shows the object-read count halved (count
reads through a counting `io.Storage` wrapper in the test, do not assert on wall-clock); existing
Delta incremental tests stay green; add a case with a schema change in the middle of the backlog.

### Outcome — done, and the saving is larger than 2× ✅

Measured through a counting `io.Storage` wrapper in
`pkg/formats/delta/incremental_test.go`, a 100-commit backlog cost **5350 object reads** before and
costs **100** after — one read per commit file, nothing else.

The 2N in the task description understated it. `GetTableChangeForCommit` called `GetTable`, which
walks the log prefix from version 0, so the backlog was quadratic: N reads to test instants, N to
convert, and N(N+1)/2 to rebuild the table once per commit, plus a final `GetCurrentTable` prefix
walk (100 + 100 + 5050 + 100).

`GetChangesSince` now walks the log once. `tableFromMetadata` — the schema-parsing half of
`GetTable` — runs only where a commit carries a metaData action; commits in between reuse that table
through `tableAsOf`, a shallow copy that moves the instant and shares the schema. The current table
comes out of the same walk rather than a second one. `changeFromCommit` takes the parsed commit, so
`GetTableChangeForCommit` (still an `spi` method, still a single-commit read plus `GetTable`) and the
backlog walk share one converter.

Two cases guard the schema handling: a metaData action in the middle of the backlog (versions before
it keep one column, versions after it see two), and one in a commit *older* than `fromInstant`, which
is walked but never emitted — the case the per-commit rebuild existed to serve.

`GetCurrentSnapshot` still has the older 2N shape it does not share with this path (a `GetTable`
prefix walk followed by a second walk over every commit). Left alone deliberately; it is a separate
change.

## T22 — Agent-legible CLI: sync mode, dry run, JSON output, timeout ✅ COMPLETED

Upstream issue #889 is open and unresolved, so this is a place the port can lead rather than follow.
Related upstream: #821 and PR #823 (per-table timeout), PR #794 (fail on sync error), #594
(continuous sync).

`cmd/polytable/main.go` exposes three flags — `--datasetConfig`, `--basePath`, `--format` — and prints
emoji-decorated prose to stdout (`:119`, `:124`, `:153`). Exit codes are already correct: `:164`
aggregates `hasErrors` and returns an error, so a failed sync exits non-zero. What is missing:

- `--output json` on `sync` and `inspect`, emitting one machine-readable document — per-table target
  results, durations, `lastInstantSynced`, and errors as strings. Keep the human output as the
  default; send JSON to stdout and progress chatter to stderr.
- `--mode full|incremental` to override `DatasetConfig.SyncMode` from the command line. The field
  exists (`pkg/conversion/config.go:54`) and is only reachable from the config file.
- `--dry-run`, which resolves the source, computes the changes and reports what would be written
  without committing. Needs a no-commit path through `Controller.Sync`, not a flag checked in `main`.
- `--timeout` per table, applied as a `context.WithTimeout` around each dataset.
- A per-table verdict in the JSON: `SUCCESS`, `FAILED`, or `NO_OP` when the incremental path found no
  new commits (`pkg/conversion/controller.go:205` already returns success with an unchanged instant —
  that is the no-op case, and it is currently indistinguishable from real work).

**Acceptance:** `polytable sync --output json` output parses with `encoding/json` and round-trips into a
struct in `cmd/polytable`; `--mode full` forces a snapshot sync on a table that would otherwise go
incremental; `--dry-run` leaves the target's metadata directory byte-identical; a table whose source
has no new commits reports `NO_OP`. Golden-file tests for the JSON shape.

### Outcome ✅ COMPLETED

All four flags landed, plus the `SyncResult.Verdict()` this task named. `pkg/spi/sync.go` gained
`SyncResult.NoOp` and a `Verdict()` method (`SUCCESS`/`FAILED`/`NO_OP`); `pkg/conversion/controller.go`
sets `NoOp` at exactly the site the task named (the empty-changes branch of `syncToTarget`, was
`:205`) and gained `Sync(ctx, cfg, opts ...SyncOption)` with `WithDryRun()` — a controller-level
no-commit path, not a `main`-only flag check, satisfying the task's explicit requirement. Dry run
skips `CommitSnapshot`/`CommitChanges` and skips catalog registration (registering an unwritten
target would read stale or absent state); `Init`/`GetTableMetadata` still run under dry run because
every target adapter's implementation of both is in-memory only (verified by reading all five
`pkg/formats/*/target.go` before relying on it).

`cmd/xtable/output.go` (new file) holds the JSON types (`SyncOutput`, `TableSyncOutput`,
`TargetSyncOutput`, `InspectOutput`) and pure builder functions (`buildTableSyncOutput`,
`buildInspectOutput`) that sort targets by format name, since `Controller.Sync` returns a map.
`cmd/xtable/main.go` gained `--output text|json`, `--mode full|incremental`, `--dry-run`, `--timeout`
on `sync`, and `--output` on `inspect`; JSON goes to stdout via `cmd.OutOrStdout()`, progress chatter
moves to stderr only in JSON mode (`--output text`, the default, keeps both on stdout, matching prior
behavior). `--timeout` wraps each dataset via `withDatasetTimeout`, a helper factored out so the
wiring is unit-testable without depending on storage slow enough to observe a real deadline.

Acceptance, checked directly:
- **JSON round-trip**: `TestSyncOutput_JSONRoundTrip`/`TestInspectOutput_JSONRoundTrip`
  (`cmd/xtable/main_test.go`) marshal a fixed fixture and unmarshal it back into the same struct.
  Also proven against the built binary — `xtable sync --output json` piped through `python3 -c
  "import json; json.load(...)"` parsed cleanly, with progress chatter confirmed absent from stdout.
- **`--mode full` forces a snapshot sync**: `TestController_ModeFullForcesSnapshotSyncEvenWithNoNewCommits`
  and the CLI-level `TestSyncOneDataset_ModeFullForcesSnapshotSync` both show incremental mode going
  `NO_OP` on a table with no new commits, and the same table under forced `SyncMode: FULL` reporting
  `SUCCESS` instead — proving the snapshot path actually ran. Reproduced against the built binary
  with a real Delta→Iceberg table.
- **`--dry-run` leaves target metadata byte-identical**: `TestController_DryRunLeavesTargetByteIdentical`
  (in-memory storage, SHA-256 per file) and `TestSyncOneDataset_DryRunLeavesTargetByteIdentical`
  (local filesystem) both hash every file under the target's metadata directory before and after a
  dry run and assert equality. Reproduced against the built binary: `shasum` of every file under
  `metadata/` was identical before and after `xtable sync --output json --mode full --dry-run`.
- **`NO_OP` verdict**: `TestController_IncrementalSyncWithNoNewCommitsReportsNoOp` and
  `TestSyncOneDataset_NoNewCommitsReportsNoOp` build a real one-commit Delta source, sync it twice,
  and assert the second sync's verdict is `NO_OP` while `StatusCode` stays `SUCCESS`. Reproduced
  against the built binary.
- **Golden-file tests for the JSON shape**: `TestSyncOutput_GoldenFile`/`TestInspectOutput_GoldenFile`
  compare indented JSON against `cmd/xtable/testdata/{sync,inspect}_output.golden.json`, built from a
  fixed-clock fixture so timestamps/durations cannot make the test flaky (`UPDATE_GOLDEN=1` regenerates
  them).

Deviation from the ground rules, forced by the language rather than chosen: `cmd/xtable/main_test.go`
is `package main`, not an external `<pkg>_test` package. Go cannot import a `main` package, so the
black-box convention has no available form here; the tests stay table-driven with `t.Parallel()`
throughout. `make check` passes, including `go test -short -race ./pkg/...` (this task touches
`pkg/conversion` and `pkg/spi`, not goroutine code, but the race run was still executed per the
ground rules and is clean).

## T23 — Catalog table discovery (list and scan) ✅ COMPLETED

Extends T14, and blocked on nothing.

`pkg/catalog` resolves exactly one table at a time: `ConversionSource.GetSourceTable`
(`pkg/catalog/conversion.go:92`). There is no list, scan or pagination anywhere — `grep -rn
"GetTables\|ListTables" pkg/` returns nothing.

The AWS Lambda reference architecture builds its whole design on the operation we lack: page through
the Glue Data Catalog, select tables carrying conversion markers, and convert each. It uses its own
property convention — `xtable_table_type` and `xtable_target_formats` — which appears nowhere in the
Java tree, so we are free to choose ours. Note that
`catalog.TableFormatFromProperties` (`pkg/catalog/conversion.go:100`) already resolves the *source*
format from `table_type` and `spark.sql.sources.provider`, matching Java's `TableFormatUtils`;
nothing resolves a *target* format list.

Scope:
1. Add `ListTables(ctx, database string, filter TableFilter) iter.Seq2[TableIdentifier, error]` to
   `ConversionSource`, implemented with the Glue paginator.
2. Add target-format resolution from table properties, alongside the existing source resolution.
3. Expose it as `polytable sync --catalog glue --database <db>`, converting every marked table.

**Acceptance:** a fake Glue client returning three pages yields every table exactly once and
surfaces a mid-pagination error rather than truncating silently; a table with no target-format
property is skipped, not failed; the CLI path is covered by a test against the fake.

### Outcome ✅ COMPLETED

`ConversionSource` gained `ListTables(ctx, database, TableFilter) iter.Seq2[TableIdentifier, error]`,
implemented in `pkg/catalog/glue_conversion.go` over the SDK's `glue.NewGetTablesPaginator`; the
unexported `glueTableReader` seam widened with the `glue.GetTablesAPIClient` signature so a fake can
be handed straight to the paginator. `TableFilter` carries one field, `RequireConversionMarkers`; its
zero value selects everything. A failing page yields the error and ends the sequence, and abandoning
the sequence stops the paging. `IcebergRESTConversionSource.ListTables` yields a single
`ErrCatalogNotImplemented` — listing is Glue-only for now, and refusing beats reporting an empty
namespace.

**Marker key decision: `polytable_target_formats`**, comma-separated `model.TableFormat` values,
resolved by `catalog.TargetFormatsFromProperties`. It is this project's own convention: Java XTable
has no target-format property at all, and the AWS Lambda reference architecture's
`xtable_target_formats` is that solution's private convention, deliberately not adopted (the doc
comment on `PropTargetFormats` says so). The `polytable_` prefix matches `polytable_synced_time`,
which `GlueCatalogSyncClient` already writes. Absent or blank marker returns `(nil, nil)` so an
unmarked table is skipped; a marker naming an unknown format is an error, since that is a typo the
operator wants to hear about. Source-format resolution is untouched — `TableFormatFromProperties`
still reads `table_type` then `spark.sql.sources.provider`, keeping Java `TableFormatUtils` parity.

`conversion.DiscoverDatasets(ctx, *catalog.Config, CatalogSourceFactory)` (new
`pkg/conversion/discovery.go`) turns a database into `[]*DatasetConfig` — format, paths and name from
the catalog entry, targets from the marker, namespace from the database. `CatalogSourceFactory` is
the read-side counterpart of `CatalogClientFactory` and the fake seam; `ResolveSourceCatalog`'s
inline factory parameter now uses the same named type. Materialization lives in `pkg/conversion`
rather than `cmd/polytable` so the CLI, the daemon and the bindings can all reach it.

`polytable sync` gained `--catalog glue --database <db>` (plus `--catalogId`). `RunE` is now a thin
wrapper over `runSync(cmd, *syncOptions)`; both source paths produce `[]*conversion.DatasetConfig`
and converge on the pre-existing loop, so `--output json`, `--dry-run`, `--timeout` and `--mode`
apply identically to a discovered table and a configured one, and per-table results land in the same
`SyncOutput` document. `--datasetConfig` and `--catalog`/`--database` are mutually exclusive, and
naming neither is an error that names both options. `syncOptions.newCatalogSource` is per-call
injection rather than a package-level variable, so the CLI tests stay parallel-safe.

Acceptance, checked directly:
- **Three pages, every table exactly once**: `TestGlueConversionSourceListTables/three pages yield
  every table exactly once` (`pkg/catalog/conversion_test.go`) drives the real SDK paginator against
  a fake serving 2+2+1 tables with distinct continuation tokens, and asserts a count of 1 per table
  plus exactly three page calls.
- **A mid-pagination error surfaces**: the same test's error subtest fails page 2 of 3 and asserts
  the yielded error names the database and the cause, with only the first page's table yielded —
  never a short listing that looks complete. `TestDiscoverDatasets/a listing error surfaces` shows
  the same at the `pkg/conversion` layer, returning no partial slice.
- **An unmarked table is skipped, not failed**: asserted at all three layers —
  `TestGlueConversionSourceListTables` (filter drops it), `TestTargetFormatsFromProperties` (absent
  and blank markers return `nil, nil`), `TestDiscoverDatasets`, and the CLI test below.
- **The CLI path is covered against the fake**: `TestRunSync_CatalogDiscovery`
  (`cmd/polytable/catalog_sync_test.go`) builds three real one-commit Delta tables, two marked and
  one not, hands `runSync` a fake `catalog.ConversionSource`, and parses the emitted JSON: two tables
  each `SUCCESS` on `ICEBERG`, the unmarked one absent, progress chatter on stderr only. Sibling
  subtests cover `--dry-run` (no `metadata/` directory written), text output, a discovery failure
  surfacing instead of an empty run, and `TestRunSync_SourceSelectionFlags` covers every rejected
  flag combination.

Same `package main` deviation as T22 for the CLI tests, for the same reason: Go cannot import a
`main` package. `make check` passes, and `go test -short -race ./pkg/...` is clean.

## T24 — Deletion vectors beyond Delta: implement or narrow the claim

Deletion-vector code exists only in `pkg/formats/delta/{source,target}.go` and `pkg/model/datafile.go`.
`pkg/formats/iceberg` and `pkg/formats/hudi` contain none.

The README overclaimed this — the format matrix marked Iceberg "✅ (Equality/Positional)" and Hudi
"✅" — and was corrected on 2026-08-21 to `—` for both, in the same spirit as T9. `SPEC.md:178` was
already accurate, scoping the claim to the Delta adapter.

That correction makes the docs honest; it does not close the gap. Upstream tracks the real work as
#345 and #346 (read Delta and Iceberg deletion vectors into the internal representation), #347 and
#348 (write them to the Delta and Iceberg targets), #640 (the snapshot case) and open PR #661.

The decision to make first: `SPEC.md:335` records INV-1 — deletion vectors are translated as
descriptors, never decoded, because decoding would mean reading data files. Iceberg positional
deletes are a *separate Parquet file of row positions*, not a bitmap descriptor, so
Delta↔Iceberg deletion-vector translation may not be expressible without violating INV-1. Resolve
that before writing code. If it is not expressible, the outcome of this task is #657's flag — warn
and continue when the target cannot represent the source's deletes — plus a line in `SPEC.md`
saying so.

## T25 — Audit upstream bug fixes merged before the port started

Out of scope for the 2026-08-21 survey, which only covered the nine days since `09b26ef`. The port
was written against a checkout that already contained these fixes, but whether each survived the
rewrite into Go is **unverified** — a Go reimplementation can reintroduce a bug the Java tree fixed
years earlier, and nothing in this repo has checked.

Highest-value candidates, each with a Java test to port:

| Upstream | Fix | Go file to check |
| :--- | :--- | :--- |
| #826 | Map key path handling in Delta schema extractors | `pkg/formats/delta/schema.go` |
| #795 | NPE for binary type inside map/array schemas | `pkg/formats/delta/schema.go` |
| #828 | Null partition value for composite generated-column partitions | `pkg/formats/delta/source.go` |
| #816 | Batch `INSERT_OVERWRITE` replacecommits dropping adds | `pkg/formats/hudi/timeline.go` |
| #732 | Empty `EarliestCommitToRetain` | `pkg/formats/hudi/source.go` |
| #797 | Iceberg nested comments with a qualified name | `pkg/formats/iceberg/schema.go` |
| #806 | Parquet snapshot sync over multiple partitioned commits | `pkg/formats/parquet/source.go` |

**Acceptance:** one table-driven test per row, written from the Java test case, each either passing
against current Go or accompanied by the fix. Report the tally — a row that already passes is a
result worth recording, not a wasted test.

### Outcome — all seven resolved: 3 already correct, 2 fixed, 2 with no Go counterpart ✅

Tests live in `pkg/formats/<format>/upstream_audit_test.go`, one per row, each naming the Java
commit and test it was written from.

| Upstream | Verdict | What the audit found |
| :--- | :--- | :--- |
| #826 | Passes (path half no-counterpart) | No Go adapter populates `model.Field.ParentPath`, so there is no path to mis-qualify. The portable half — a struct-keyed map keeping its key and value subtrees apart — was already correct. |
| #795 | Passes | Go's parser takes the type node and its nullability directly and never reads field metadata, so the null dereference the binary branch had in Java is structurally impossible. |
| #828 | No counterpart | Generated-column partitions do not exist in Go: every Delta partition column becomes its own `VALUE` field, so nothing joins components with `-` and no `"2013-null-20"` can be produced. |
| #816 | **Fixed** | Mirror image of the Java bug. Go never lost adds, but `HoodieCommitMetadata` had no `partitionToReplaceFileIds` at all, so an `INSERT_OVERWRITE` left the overwritten files in the snapshot beside their replacements. |
| #732 | No counterpart | `IsIncrementalSyncSafeFrom` compares the earliest instant still in the active timeline and reads no clean metadata, so the empty `earliestCommitToRetain` branch cannot arise. |
| #797 | **Fixed** | The Go target writes whole metadata rather than name-addressed column updates, so nothing under-qualifies a name — but nested docs were dropped on the way out and never read back into `Comment` in either direction, so a nested comment could not survive a sync at all. |
| #806 | Passes | The Java change was test-only; the Go source re-crawls on every call and handles successive waves of files, into existing and into new partitions. |

Both no-counterpart rows still got a test, covering the invariant the Java fix protects rather than
the branch that does not exist: for #828, that a missing or null component yields no fabricated
composite; for #732, that unreadable retention metadata resolves to "unsafe" and never to "safe by
default".

Parity gaps the audit surfaced but did not close — each is a feature, not a regression:

- `model.Field.ParentPath` is dead in production code; no format adapter sets it.
- Delta `AddAction.PartitionValues` is `map[string]string`, so a JSON `null` decodes to `""` and a
  null partition value is indistinguishable from an empty one. Fixing the representation ripples
  into every target that formats `Range.MinValue` into a path.
- Generated-column (composite) Delta partitions are unsupported.
- Hudi reads no clean or archival metadata.
- Hudi `GetTableChangeForCommit` ignores its `commitID` and returns the whole snapshot as adds.
- The Delta reader ignores field metadata, so the `__xtable_logical_type` UUID mapping is absent.

## T26 — Investigate the timestamp basis of Delta incremental sync

**Investigate before writing code.** Upstream #779 ("handle log truncation in Delta to Iceberg
incremental sync") is still open, so there is no Java fix to mirror, and the obvious reading of the
title does not match our code.

What the code actually does: `pkg/formats/delta/source.go:322` (`IsIncrementalSyncSafeFrom`) returns
safe when `versions[0].CommitTime <= earliestInstant`. Log cleanup removes a contiguous *prefix* of
commits, so after truncation `versions[0]` is a **later** commit with a **later** timestamp, the
comparison fails, and `pkg/conversion/controller.go:184` already falls back to a snapshot sync. A
fixture that deletes commits below a checkpoint will therefore pass against current code — do not
write one, see it green, and mark this task done.

The exposure worth chasing is that both halves of incremental sync key on the **commit timestamp**
rather than the version number: the safety check compares `CommitTime`, and `GetChangesSince`
(`:289`) selects commits with `CommitTime > fromInstant`. Two consequences to test:

- Two commits written inside the same millisecond share a `CommitTime`. Sync after the first, and
  the second is `> fromInstant` false — silently never synced, with the sync still reporting success.
- A commit whose `commitInfo` timestamp is not greater than its predecessor's (clock skew, or a
  writer that does not enforce monotonicity) is dropped the same way.

Establish whether either reproduces before choosing a fix. If they do, the fix is likely to key
incremental selection on the Delta **version number**, which is monotonic by construction, and keep
the timestamp only for the retention comparison.

**Acceptance:** a written finding either way. If reproduced, a table-driven test per case plus the
fix; if not, a note in this task saying what was tested and why the current basis is sound, so the
next reader does not repeat the investigation.

### Outcome — reproduced, three ways, and fixed ✅

Both suspected cases reproduce, and a third one does. Fixtures are hand-written log files in
`pkg/formats/delta/incremental_test.go` (the Delta target cannot produce these timestamps); each
case is three commits with the last one anomalous, `GetChangesSince(2000)` against them, and the
expectation that version 2 comes back:

| Case | Before | After |
| :--- | :--- | :--- |
| Two commits sharing a millisecond | version 2 dropped, sync reports success | synced |
| Timestamp goes backwards (clock skew) | version 2 dropped | synced |
| Commit with no `commitInfo` action | dropped for **every** `fromInstant`, since its `CommitTime` is 0 | synced |

The third is the widest: `commitInfo` is optional in the Delta protocol, and a commit without it
was invisible to incremental sync regardless of clock behaviour.

The fix keeps the instant an `int64` — target state is a bare timestamp
(`model.KeyLastInstantSynced`), so a version number has nowhere to live — but derives it from
version order rather than trusting the log: `advanceCommitTime(previous, raw)` returns `raw` when it
exceeds the preceding commit's instant and `previous + 1` otherwise. The result increases strictly
with the version, which makes it an injective proxy for the version number, so
`instant > fromInstant` selects exactly the versions after the one `fromInstant` names. A
well-behaved log is unaffected: instants pass through as the raw timestamps. Java resolves the same
ambiguity by mapping the sync instant to a version once and keying the backlog on versions
(`DeltaConversionSource#getCommitsBacklog`, `:204`).

The derived instant is what `GetTable`, `GetTableChangeForCommit` and `GetChangesSince` report,
because the controller persists the last one and feeds it back as the next `fromInstant`; reporting
a raw timestamp while selecting on a derived one would replay the boundary commit forever.
Retention (`IsIncrementalSyncSafeFrom`) still compares raw timestamps — it asks when the earliest
retained commit happened, not which version an instant names — and now returns false when that
commit has no timestamp at all, rather than reading 0 as "old enough".

One migration edge, no code: a target synced before this change whose stored instant sits at an
anomaly can see one commit re-emitted on the next sync. That is at-least-once, not loss.

## T27 — Rename the project to `polytable` ✅ COMPLETED

**Decided by the maintainer, 2026-08-21.** "XTable" is an ASF podling mark, so publishing
independently under `xtable-go` is a trademark problem. The new name is **polytable** — cleared
against the lakehouse space on 2026-08-21 (no project uses it; the only near names are Apache XTable
itself and its predecessor OneTable, which is Onehouse's mark and equally off-limits).

**This task blocks the first tag.** A Go module tag bakes the module path in immutably (see T19's
donation table — this row now resolves the other way: independent publication, not donation naming).

Rename surface:

1. Module path `github.com/slachiewicz/xtable-go` → `github.com/slachiewicz/polytable` — every
   import in the tree, `go.mod`, and the checked-in docs that quote it.
2. Binaries and dirs: `cmd/xtable` → `cmd/polytable`, `cmd/xtable-service` → `cmd/polytable-service`,
   `cmd/xtable-wasm` → `cmd/polytable-wasm` (keep the `//go:build js && wasm` tag and the js/wasm vet
   line in `Makefile`/CI pointed at the new path). Artifact names in `release.yml`,
   `build-artifacts.yml` and `Makefile` follow.
3. C binding: `libxtable` → `libpolytable`; exported symbols `xtable_sync_json`/`xtable_inspect_json`
   → `polytable_*`. Nothing is released, so the FFI break costs nothing today.
4. Python: `pyxtable` → `polytable` (claim the PyPI name before publishing; T6's packaging work
   carries over).
5. REST: `spec/rest-service-open-api.yaml` title/description, then regenerate via `spec/Makefile`.
6. Docs: `README.md`, `SPEC.md`, `AGENTS.md`, `CLAUDE.md`, `NOTICE`. Keep one nominative-fair-use
   line — "a Go implementation of the translation model of Apache XTable (incubating)" — as factual
   attribution; the Apache-2.0 §4(d) attribution in `NOTICE` stays regardless.
7. GitHub repo rename to `polytable` — maintainer action, outside the tree; do it in the same window
   as the module-path commit so the path is never published dangling.

**Deliberately NOT renamed:**

- The on-disk metadata keys `xtable_last_instant_synced` / `xtable_source_format`
  (`pkg/model/metadata.go:36-40`). They are interop with Java-XTable-synced tables, not branding;
  renaming them silently breaks round-tripping against upstream.
- The 16-line ASF grant headers (T19). Donation remains possible after a rename — ASF renames
  incoming podlings anyway — so the T19 decision stands unless the maintainer revisits it.
- References to `../incubator-xtable` and upstream issue numbers in docs — those *name* the upstream
  project, which is exactly what nominative use is for.

**Acceptance:** case-insensitive `grep -ri xtable` over the tree returns only the metadata keys and
their tests, nominative upstream references in docs, and this file's history. `make check` passes.
The C header, wheel and REST spec all carry the new name. No tag exists yet when this lands.

**Commit:** `refactor: rename the project to polytable`

### Outcome ✅

Done in one commit. The module path, the three `cmd/` directories, the C library and its four
exported symbols, the Python package and the REST spec title all carry the new name; `make check`
passes, `oapi-codegen` regenerates from the retitled spec, and the generated `libpolytable.h`
declares `polytable_version`, `polytable_free_string`, `polytable_sync_json` and
`polytable_inspect_json`.

Three on-disk strings the task text did not list were renamed too, because the carve-out's stated
reason is Java interop and these have no Java counterpart: the Delta/Hudi commit operation labels
(`XTABLE_SYNC_SNAPSHOT` → `POLYTABLE_SYNC_SNAPSHOT` and siblings — Java writes
`DeltaOperations.Update("xtable-delta-sync")` and `WriteOperationType.UNKNOWN`), the Parquet
sidecar directory `_xtable_metadata`, and the Glue parameter `xtable_synced_time`. Nothing is
released, so this was the last free moment to change them. `MetadataPropertyPrefix = "xtable_"`
stays with the two keys it prefixes, and its doc comment now says why.

All four C symbols were renamed rather than the two the task names: a `polytable_sync_json` beside
an `xtable_version` would be an incoherent ABI.

What is left on a case-insensitive grep, by category: the two metadata keys and the prefix constant
(plus their `pkg/catalog/rest.go` literals and the `SPEC.md`/`AGENTS.md` invariants that quote
them); nominative references to Apache XTable, `../incubator-xtable`, `xtable.apache.org` and the
upstream Maven module names in docs and `.claude/skills/`; and this file's history of earlier
tasks.

---

# Integration-test plan — 2026-08-21

## T28 — Real-writer fixtures ✅ (Iceberg half unblocked by T31)

Every suite in the tree reads metadata polytable itself wrote, so a reader that agrees with
polytable's writer passes even where the pair disagrees with the format. Check in small tables
written by engines that have never seen this code, and assert the readers against them.

### Steps

1. `test/fixtures/generate.py` writes the fixtures from delta-rs and pyiceberg — JVM-free writers —
   each with three commits, a mid-history column addition, a partition column and numeric columns so
   statistics exist.
2. Beside each fixture, a `manifest.json` records what the writer reported: schema, commit count,
   row totals, per-column bounds, partition values, per-file row counts. The Go tests assert against
   that, so regenerating a fixture regenerates its expectations.
3. `test/foreign_fixtures_test.go` reads each fixture through `pkg/formats.NewSource` and converts
   it to the other formats through `pkg/conversion`.

**Acceptance:** the fixtures are committed and under 1 MB; the tests run under `go test -short` with
no container; every assertion traces to `manifest.json`; a reader gap is fixed or recorded, never
assumed away.

**Commit:** `test: read fixtures written by delta-rs and pyiceberg`

### Outcome ✅ — the fixtures landed and found four reader gaps, one of them fixed here

Fixtures: `test/testdata/fixtures/delta-rs/sales` (delta-rs 1.6.2, 3 commits, 6 data files, 14 rows)
and `test/testdata/fixtures/pyiceberg/events` (pyiceberg 0.11.1, 3 snapshots, 5 metadata versions,
6 data files, 12 rows). 55 KiB in total.

**The Delta source reads delta-rs output correctly** — schema including the merged column, partition
values from `partitionValues` rather than the directory name, per-file row counts, and the bounds
delta-rs recorded. Walked as an incremental backlog it returns all three commits, each carrying the
schema as of that commit rather than the latest. Conversion to Iceberg and Hudi keeps the file list
and the row counts. That is the first evidence in this repo that any polytable reader agrees with a
foreign writer.

**The Iceberg source could not read the pyiceberg table at all.** Two causes, one fixed here:

- *Fixed:* `listMetadataFiles` matched only `v<N>.metadata.json`, the Hadoop-layout name polytable's
  own target writes. Every catalog-backed writer — pyiceberg, the Java library, Spark — writes
  `<%05d version>-<uuid>.metadata.json`, so a real table looked like it had no metadata whatsoever.
  `iceberg.MetadataFileVersion` now accepts both and `listMetadataFiles` carries the path rather than
  rebuilding it from the version, since the UUID cannot be reconstructed. With that, the schema,
  field IDs, partition spec and commit instant of the pyiceberg table all read correctly.
- *Recorded, then fixed by T31:* the file list could not be read at all. See F1.

Since T31 the pyiceberg fixture reads and converts like the delta-rs one:
`TestForeignFixtures_ReadIcebergSnapshot` checks the file list, row counts and partition tuples
against `manifest.json`, and `TestForeignFixtures_ConvertIceberg` runs it through every other
target. F2, F3 and F4 remain recorded rather than fixed, each with a pinned assertion.

### Watch list

- ~~**F1 — Iceberg manifests are Avro, and polytable parses them as JSON.**~~ **Fixed by T31.** The
  pyiceberg fixture now reads and converts; `TestForeignFixtures_ReadIcebergSnapshot` and
  `TestForeignFixtures_ConvertIceberg` assert the file list instead of the failure.
- **F4 — the Hudi target mangles a data file path that carries a URI scheme.** It trims the table's
  base path off `PhysicalPath` with a plain string prefix match. The Iceberg source reports the
  location the manifest recorded, scheme and all (`file:///…`), so the prefix never matches, the
  whole absolute path is stored as if it were relative, and the Hudi source joins it onto the base
  path a second time — `…/events/file:/…/events/data/…`. Found by T31 when a foreign Iceberg table
  first became a conversion source. Pinned by `pathsDoubled` in `TestForeignFixtures_ConvertIceberg`.
- **F2 — the Paimon target and the Paimon source disagree on the table layout.** The target writes
  `metadata/schema-<epoch>.json` and `metadata/manifest.json`; the source reads `schema/schema-*`
  and `snapshot/`, which is the real Paimon layout. Nothing polytable writes as Paimon can be read
  back as Paimon, in either direction of the mismatch. No existing test caught it because the e2e
  matrix round-trips only Delta, Iceberg and Hudi. Pinned in `TestForeignFixtures_ConvertDelta`.
  **Fixed by T32**: the target now writes the source's layout and the pin asserts the round trip.
- **F3 — the raw-Parquet source rebuilds the schema from one data file.** It reads the footer of
  whichever file sorts first, so on a schema-evolved table the result depends on file names, and the
  Hive partition column — which lives in the directory name, not in the file — is missing from the
  schema even though the same source reports it as a partitioning field. The table it returns is
  partitioned by a column its own schema does not contain. **Fixed by T33**: the schema is the merge
  of every footer, the partition column is synthesized into it, and the pin asserts both.

### Out of scope

Spark-written and Hudi fixtures need a JVM; they are T30's, and nothing here presumes what they will
show. Neither fixture proves polytable can *write* a table another engine will read — that is the
other half of the question, and it needs those engines as readers.

Everything above verifies polytable's output with polytable's own reader. A deviation both sides
share is invisible to that: the writer emits it, the reader accepts it, the test goes green, and
the table is unreadable to the engines the project exists to serve. These tasks put independent
readers on the output.

## T29 — Engine verification of polytable output with DuckDB ✅ (Iceberg pairs unblocked by T31)

DuckDB reads Delta through `delta_scan` (delta-kernel-rs) and Iceberg through `iceberg_scan`, is a
single static binary and needs no JVM, which makes it the cheapest independent judge available.

**Scope.** A `test/` suite, gated on `testing.Short()` and on `exec.LookPath("duckdb")`, that syncs
small tables to Delta and Iceberg from three source formats each, then shells out to `duckdb -json`
and asserts on the row count, on a predicate outside the written value range returning nothing, and
on a partition-value predicate returning the right subset. Out of `make check` on purpose; CI runs
it through `integration.yml`, which already invokes `./test/...` without `-short`.

**Acceptance:** the suite passes against a real duckdb; `make check` stays green; CI installs a
pinned duckdb before the test step.

**Commit:** `test: verify Delta and Iceberg outputs with DuckDB`

### Outcome ✅

Verified with **duckdb v1.5.5 (Variegata)** on macOS arm64, core `delta` extension `45c4087` and
core `iceberg` extension `45163a28`. The Iceberg rows below were re-run after T31 landed; before it
they were skipped.

| Pair | Result |
|---|---|
| Parquet → Delta | ✅ read by `delta_scan` |
| Iceberg → Delta | ✅ read by `delta_scan` |
| Hudi → Delta | ✅ read by `delta_scan` |
| Parquet → Iceberg | ✅ read by `iceberg_scan` — after T31 |
| Delta → Iceberg | ✅ read by `iceberg_scan` — after T31 |
| Hudi → Iceberg | ✅ read by `iceberg_scan` — after T31 |

**The Delta writer was broken for every non-polytable reader, and this is the headline finding.**
Two keys were emitted with `omitempty`, so a table with no format options and no field metadata —
that is, every table polytable has ever written — omitted them entirely:

- `metaData.format.options`. The Delta protocol declares it non-nullable. delta-kernel-rs fails the
  whole log with `Encountered unmasked nulls in non-nullable StructArray child`.
- `metadata` on each `schemaString` field. delta-kernel-rs fails with ``missing field `metadata` ``.

Both are now written as empty objects, via `delta.NewParquetFormat()` and a non-omitempty tag, and
`TestDelta_MetadataCarriesKernelRequiredKeys` guards them on the raw JSON. It has to assert on the
JSON: a round trip through polytable's own Delta source passes either way, because the reader
ignores both keys — which is exactly how this survived until an outside reader looked at it. The
same defect would have hit delta-rs, Spark's Delta reader and anything else built on the kernel.

**Iceberg output was not readable by any Iceberg engine.** `pkg/formats/iceberg/target.go` wrote
manifest files and manifest lists as JSON (`*-m0.json`, `snap-*.json`); the Iceberg spec mandates
Avro, and DuckDB rejected them with `Incorrect Avro container file magic number`. That was a
feature gap rather than a bug to fix in a test commit — it needed an Avro writer, and `go.mod` had
no Avro dependency — so the three Iceberg subtests synced, asserted the sync succeeded, then
`t.Skip`ped with the DuckDB error attached. **T31 closed it**: the skip is gone, all six pairs
assert, and the run above is the result. T31's outcome records the two further defects the Iceberg
half exposed once DuckDB could open the metadata at all.

**Not verified:** pruning. `EXPLAIN`/`EXPLAIN ANALYZE` output is not a stable contract across DuckDB
releases, so the suite proves correctness under a predicate rather than that files were skipped.
Reading every column (`SELECT *`) is in the suite so the checks decode data pages rather than
answering from the Parquet footer — this is what confirms the `timestamp(millisecond)` column
survives the Delta round trip.



## T30 — Java XTable interop nightly (unscheduled until T28/T29 land)

The sharpest claim: tables move between polytable and Apache XTable without resync (shared
`xtable_*` keys). A `workflow_dispatch` + nightly job pulls the upstream bundled jar, syncs a
fixture with Java XTable, continues **incrementally** with polytable, and the reverse — asserting
the second tool reports incremental, not a snapshot fallback. Never gates PRs (bench.yml
philosophy: failures prompt investigation, not red builds). This job is also where Spark-written
and Hudi fixtures for T28 get generated. Needs a maintainer decision on jar version pinning.

Follow-ups noted, not scheduled: a bindings smoke lane (the C ABI and wheel are built but never
executed in CI; one `polytable.sync()` via ctypes and a node run of `polytable.wasm`), and
LocalStack-Glue for catalog sync integration.

**Coverage bar (ground rule, 2026-08-22):** upstream's `ITConversionController` (in
`xtable-core`) is the benchmark for cross-engine testing — write through a real engine, sync,
read source and every target back through a real engine, compare full datasets. Its scenario
list (upserts and deletes, compaction, concurrent writes, time travel, partition specs, sync
modes, out-of-sync incremental, corrupted-snapshot recovery, Delta column mapping, metadata
retention) is what this suite must match or exceed, not just the happy-path insert. T28/T29
cover the read/write halves with delta-rs, pyiceberg and DuckDB; the scenario dimension is
still mostly open.

## T31 — Iceberg manifests must be Avro, not JSON ✅

**The largest interop defect in the port**, found 2026-08-21 while planning engine verification.
The Iceberg spec requires Avro manifest lists and manifest files. This adapter uses JSON on both
sides: the source parses manifests with `json.Unmarshal`
(`pkg/formats/iceberg/source.go:217,228`), and the target writes `snap-<id>-<uuid>.json` and
`<uuid>-m0.json` (`pkg/formats/iceberg/target.go:170,191`). Consequences, both directions:

- No real engine (Spark, Trino, DuckDB `iceberg_scan`, pyiceberg) can read a polytable-written
  Iceberg table. T29 is expected to confirm this; do not let a JSON-reading shortcut in that suite
  mask it.
- The polytable source cannot read a real Iceberg table — T28's pyiceberg fixture will hit this on
  the manifest read. Only tables polytable itself wrote round-trip.

Library decision first: `hamba/avro/v2` (maintained, pure Go, schema-first) for a manifest codec
of our own, versus adopting `apache/iceberg-go`'s manifest read/write. Prefer **hamba/avro** unless
inspection shows iceberg-go's manifest layer can be used without dragging in its catalog/table
stack — this repo's value is the thin native implementation, and a second table library blurs it.

Scope:
1. Manifest-list and manifest-entry Avro schemas per the Iceberg v2 spec (field-ids in the Avro
   schema metadata matter — engines read them).
2. Target: write Avro; drop the JSON writer entirely. Nothing is released, so there is no
   migration path to preserve — delete, don't fallback. T20's base64-of-binary bounds encoding was
   explicitly "the JSON accommodation"; in Avro the bounds become native `bytes`, so revisit
   `pkg/formats/iceberg/stats.go` in the same change.
3. Source: read Avro manifests. Keep the JSON read path only as long as existing test fixtures
   need it, then delete those too — foreign fixtures (T28) become the read-side truth.
4. `metadata.json` itself stays JSON — that part is spec-correct today.

**Acceptance:** the T28 pyiceberg fixture is read successfully (schema, files, stats); a
polytable-written Iceberg table is read back by pyiceberg and by DuckDB `iceberg_scan` (extend the
T29 suite — that is the regression test); Iceberg→Delta→Iceberg round trip still preserves bounds;
`grep -rn '\.json' pkg/formats/iceberg/target.go` shows only `metadata.json` writes.

**Commit:** `fix: write and read Iceberg manifests as Avro per the spec`

### Outcome ✅

**Library: `github.com/hamba/avro/v2`, with a manifest codec of our own.** `apache/iceberg-go` was
rejected on its dependency graph rather than on its code: the manifest layer lives in the root
package, and that module's `go.mod` requires arrow-go, gocloud, the Azure and GCP SDKs, substrait,
docker/compose and testcontainers. Go's minimal version selection resolves those for the whole
module whether or not the catalog packages are imported, and several of them — aws-sdk-go-v2 above
all — would move pins this repo sets deliberately. hamba brings six pure-Go dependencies, none
already in the graph beyond `klauspost/compress`, and compiles for `js/wasm`. The codec is
`pkg/formats/iceberg/manifest.go`: the v2 `manifest_entry` and `manifest_file` schemas as literal
JSON with the specification's field ids, and records carried as `map[string]any` in both directions.
That shape is not a shortcut. Writing needs it because the `partition` column's Avro type is derived
per table from the partition spec; reading needs it because the schema is whichever one the writing
engine chose, and hamba decodes against the schema embedded in the file.

The schema is written through `ocf.WithSchemaMarshaler` returning the literal JSON, because a round
trip through the parser is not guaranteed to preserve `field-id` properties — and a manifest without
them is one no engine can resolve columns against. `TestIceberg_ManifestCarriesFieldIDsAndHeader`
asserts on the header bytes rather than on a read-back.

**Two further defects surfaced once DuckDB could open the metadata at all.** Both are exactly the
kind a round trip through polytable's own reader cannot see, which is the T29 lesson repeating:

- *`file_format` was written as the canonical name.* `APACHE_PARQUET` reached the manifest verbatim;
  the specification admits `PARQUET`, `ORC` and `AVRO` only, and DuckDB answers `File format
  'APACHE_PARQUET' not supported`. Now mapped both ways.
- *No name mapping, so every column read as null.* An Iceberg reader binds a Parquet column by the
  field id in the file's own schema. Polytable never writes data files, so the files it describes
  carry no ids, and DuckDB returned the right number of rows with every data column NULL — only the
  partition column, which comes from the manifest's partition tuple, had a value. The fallback
  `schema.name-mapping.default` table property fixes it, and it is what the specification prescribes
  for exactly this case. `TestIceberg_MetadataCarriesNameMapping` guards it on the raw JSON.

**Partitions.** Only the identity transform on `boolean`, `int`, `long`, `float`, `double` and
`string` is written; anything else fails the commit with the transform or type named. Date and the
timestamps are excluded deliberately — their Avro form carries a logical type whose unit this port
does not normalize on either side, and a guessed unit files data under the wrong partition as surely
as a wrong type would. A partition column the source reports but its schema does not contain (the
Hive layouts, F3) is added to the written schema: Iceberg resolves every partition field against a
schema column, and an identity-partitioned column need not be stored in the data files because a
reader materializes it from the partition tuple.

**Bounds** moved from T20's base64-of-binary to native Avro `bytes` in the `k126_v127`/`k129_v130`
entry maps. The single-value binary serialization and the -0.0 widening are unchanged;
`EncodeBound`/`DecodeBound` now take and return `[]byte`.

**Verified.** DuckDB v1.5.5 `iceberg_scan` reads all three →Iceberg pairs of the T29 suite — row
count, a predicate above every written id, a partition predicate, and `SELECT *` decoding every
data page. pyiceberg 0.11.1 scans a polytable-written table through `StaticTable.from_metadata`
(`test/fixtures/verify_pyiceberg.py`, a manual check): every value of every column, from a Delta
source and from a raw-Parquet one, and on a Hive-partitioned directory whose files omit the
partition column it materializes the synthesized column from the partition tuple. The T28 pyiceberg
fixture reads and converts into Delta, Hudi and raw Parquet. Iceberg→Delta→Iceberg still preserves
bounds.

**Fixture relocation.** A fixture's Avro manifests hold absolute paths from the machine that
generated them, and generate.py's placeholder substitution cannot reach inside a compressed
container file. `relocateAvroManifests` in `test/foreign_fixtures_test.go` decodes the records,
replaces the location and encodes them again under the file's own schema and header, so what the
Iceberg source reads is still pyiceberg's shape.

## T32 — Paimon round-trip is broken: target and source disagree on layout ✅

T28's F2. The Paimon target writes `metadata/schema-<epoch>.json`; the Paimon source reads
`schema/schema-*` and `snapshot/`. Nothing polytable writes as Paimon can be read back as Paimon —
the e2e matrix round-trips only Delta, Iceberg and Hudi, so nothing caught it. Decide which side
matches the real Paimon spec (almost certainly the source's `schema/` + `snapshot/` layout — check
`../incubator-xtable`'s Paimon module and the Paimon docs), fix the other side, add Paimon to the
round-trip matrix, and pin it with the T28-style assertion already in place.

**Acceptance:** a Paimon table written by the target is read back by the source; the e2e matrix
includes Paimon↔Delta both ways; the T28 pinning assertion flips from "expected broken" to green.

**Commit:** `fix: align the Paimon target with the source's on-disk layout`

### Outcome ✅ — the source's layout was the spec-correct one

Established from the Paimon sources themselves (`paimon-bundle` 1.3.1, the version
`../incubator-xtable`'s `pom.xml` pins, sources jar in the local Maven repository), because the Java
module reads through Paimon's own `FileStoreTable`/`SnapshotManager` and so records no layout of its
own:

- `SchemaManager.toSchemaPath` → `<table>/schema/schema-<id>`, `SCHEMA_PREFIX = "schema-"`.
- `SnapshotManager.snapshotPath` → `<table>/snapshot/snapshot-<id>`, `SNAPSHOT_PREFIX = "snapshot-"`.
- `HintFileUtils` → `snapshot/LATEST` and `snapshot/EARLIEST`, each holding a bare snapshot id.
- `FileStorePathFactory` → `manifest/manifest-<uuid>-<n>` and `manifest/manifest-list-<uuid>-<n>`.

The ids are counters, not epochs, and none of the files carries a `.json` extension — so the
target's `metadata/schema-<epoch>.json` was wrong on the directory, the name and the suffix.

The target now writes that layout, with `Snapshot`'s own `FIELD_*` names in the snapshot JSON, and
carries the sync metadata in the snapshot's `properties` map (Paimon ≥ 1.2), which keeps INV-4 while
leaving schema files immutable the way Paimon treats them. `CommitChanges` writes one snapshot per
change whose base manifest is the previous state, so the newest snapshot always describes the whole
table. The source resolves the newest snapshot through the `LATEST` hint, falling back to a numeric
scan — the old lexicographic sort put `snapshot-10` before `snapshot-9`.

**One deviation remains, and it is the same defect as T31's:** manifests and manifest lists are
JSON, where Paimon writes Avro (`CoreOptions.MANIFEST_FORMAT` defaults to `avro`). The records mirror
`ManifestEntry`, `ManifestFileMeta` and `DataFileMeta` field for field, but a Paimon engine still
cannot open the result. Closing that needs the Avro writer T31 is adding for Iceberg; nothing else
in the layout blocks it.

Verified: `make check` green; `go test -short -race ./pkg/...` clean;
`TestForeignFixtures_ConvertDelta/paimon` flipped from the "expected broken" pin to the same file
list, row count and schema assertions the other targets get; `TestE2E_PaimonDeltaRoundTrip` covers
Delta → Paimon → Delta.

## T34 — Paimon against the real spec: Avro manifests and engine verification

T32 aligned the on-disk layout with Paimon 1.3.1's own writers (evidence in its outcome), but two
gaps separate "matches the layout" from "follows the spec":

1. **Manifests are JSON; Paimon's `CoreOptions.MANIFEST_FORMAT` defaults to Avro.** The records
   mirror `ManifestEntry`/`ManifestFileMeta`/`DataFileMeta` field for field, but a real Paimon
   engine cannot open them. Same shape of work as T31 — once T31 settles the Avro codec choice,
   reuse it here.
2. **No engine-level verification.** No fixture written by real Paimon exists (T28 covers Delta
   and Iceberg only) and no real reader checks polytable's Paimon output. Check whether `pypaimon`
   (the Python SDK) is mature enough to generate a fixture and/or read our output JVM-free; if
   not, both belong in T30's JVM job alongside the Spark/Hudi fixtures.

**Acceptance:** a real-Paimon-written fixture reads and converts; polytable's Paimon output opens
in a real Paimon reader (pypaimon or the T30 job); manifests are Avro where the spec defaults to
Avro. Until then, the format matrix must not claim Paimon interop beyond polytable↔polytable.

## T33 — Parquet source schema: merge footers, include the partition column

T28's F3, two related defects in `pkg/formats/parquet/source.go`:
1. The schema comes from a single file's footer — whichever sorts first — so a schema-evolved
   directory reports whichever generation that happens to be. Merge footers across files (newest
   superset wins; conflicting types are an error, not a guess).
2. The Hive partition column lives only in directory names, so it is reported as a partition field
   but missing from the schema. Engines converting the output see a partition key that no column
   defines. Synthesize the column (type-inferred from the values, STRING on ambiguity) the way
   Java's parquet source does — check `../incubator-xtable` for its behavior first.

**Acceptance:** a fixture directory with two file generations reports the merged schema
deterministically regardless of file naming; the partition column appears in the schema with a
type; the T28 pinning assertions flip to green.

**Commit:** `fix: merge Parquet footers and surface the Hive partition column`

### Outcome ✅ — both fixes are deliberate divergences from Java

**What Java does**, read from `xtable-core`'s `org.apache.xtable.parquet`:

- *Schema:* `ParquetConversionSource.createInternalTableFromFile` takes one footer — the file with
  the greatest modification time (`getMostRecentParquetFile`). No merge, so a column an older
  generation carries and the newest does not is simply gone. Picking the newest file is better than
  picking the first, and it is where "newest wins" below comes from, but it is still one file.
- *Partition column:* Java never infers partitioning from directory names. The spec comes from
  configuration (`xtable.parquet.source.partition_field_spec_config`, `path:type[:format]`) and
  `ParquetPartitionSpecExtractor.spec` resolves each path against the footer schema with
  `SchemaFieldFinder`, so Java assumes the partition column is physically in the files; a
  Hive-partitioned table where it is not yields a partition field with a null source field. There
  is nothing to port, so the synthesis here is new.

**What this port does instead.** `MergeFooterSchemas` folds every footer into one schema: newest
file first, columns only older files carry appended after, and a column absent from any file
nullable, since its rows have no value. Types must be *identical* — an INT and a LONG column of the
same name conflict rather than widening, because a widened schema is a guess about the writer's
intent — and a conflict is an error naming the column, both types and both files. Files sharing a
modification time fall back to path order; only column order can move that way, never the set or the
types. Each file is still read once: `footerAggregates` collects the row-group statistics before the
merge and `columnStatsForSchema` resolves them against the merged schema afterwards, so a column a
file does not carry contributes no statistics for it rather than zeros.

`partitionSpec` then puts the Hive partition columns in that schema, appended after the physical
ones, typed from the observed directory values: LONG when every value is an integer, DOUBLE when
every value is numeric, DATE when every value is an ISO date, STRING otherwise, and STRING for
anything ambiguous, including Hive's `__HIVE_DEFAULT_PARTITION__` null marker. Synthesized columns
are nullable. A physical column of the same name wins — resolved through `FieldByPath`, so
`Region=eu` over a `region` column is one column and not two — and the directory values then have to
be readable as its type, or the crawl fails rather than describing the table wrongly. Partition
values stay the raw directory strings, which is what every target formats today. Partitioning fields
now come out in directory-nesting order rather than a map's iteration order.

`ExtractHivePartitions` is gone, replaced by `HivePartitionsForFile` and `PartitionColumnSchema`;
nothing outside the package used it.

Verified: `make check` green; `go test -short -race ./pkg/...` clean;
`TestForeignFixtures_ConvertDelta/parquet` flipped from pinning the missing partition column to
asserting every column of the delta-rs manifest, `region` included, with its type — the same fixture
also exercises the merge, since `discount` exists only in the third commit's files.
`TestParquet_SourceMergesFooterSchemas` pins the merge under both file namings.

## T35 — The Hudi target drops a data file path that carries a URI scheme

T31's F4. `pkg/formats/hudi/target.go` computes each file's path relative to the table with
`strings.TrimPrefix(df.PhysicalPath, t.targetTable.BasePath)`. An Iceberg source reports the
location its manifest recorded, scheme included, so the prefix never matches, the absolute path is
stored as if it were relative, and `pkg/formats/hudi/source.go` joins it onto the base path again:
`…/events/file:/…/events/data/…`. Nothing caught it before a foreign Iceberg table could be a
conversion source. **Paimon is confirmed to share the exposure**: after T31 and T32 composed, an
Iceberg→Paimon conversion reads back doubled the same way, pinned by `pathsDoubled` on the Paimon
row of `TestForeignFixtures_ConvertIceberg` (Delta→Paimon is clean — the exposure needs a
scheme-qualified source path). Decide whether the fix belongs in each adapter or in `pkg/io` —
every target that trims a base path by string prefix has the same exposure, so check Delta too
before choosing.

**Acceptance:** Iceberg→Hudi and Iceberg→Paimon conversions report each data file once, under the
table's base path; both `pathsDoubled` pins in `TestForeignFixtures_ConvertIceberg` are deleted
rather than adjusted.

**Commit:** `fix: relativize data file paths against the base path scheme-aware`

### Outcome ✅ — one helper in `pkg/io`, three targets

`io.RelativizePath(physicalPath, basePath) (string, error)` in `pkg/io/storage.go` replaces the
trim-by-prefix in all three targets that carried it. It strips a recognized scheme from *either*
side before comparing — the list is now the package-level `uriSchemes` that `JoinPath` and
`NewStorageForPathWithOptions` already keyed off, so `s3://` and `s3a://` compare equal, which is
right: they name the same store. Both sides go through `path.Clean`, so a base path written with or
without a trailing slash behaves the same, and the match has to end on a separator, so `/data/events`
does not claim `/data/events2/f.parquet`. A path already relative (no scheme, no leading separator)
comes back unchanged — `model.DataFile` documents `PhysicalPath` as "fully qualified URI or relative
path" — unless it climbs out with `..`.

**Which targets carried the pattern.** Hudi (`target.go`, the reported defect), Paimon
(`manifest.go`'s `relativeFilePath`, now gone) and Delta (`target.go`'s `makeRelativePath`). The
Parquet target does not: it only joins, under `_metadata/`. `parquet/partition.go`'s
`HivePartitionsForFile` trims by prefix but is source-side, where the file path and the base path
come from the same crawl and cannot disagree on scheme, so it was left alone.

**A file outside the base path is handled per format, which is the asymmetry worth recording.** Hudi
and Paimon cannot represent one — a write stat's path and a manifest entry's `_FILE_NAME` are both
joined back onto the base path on read — so they fail the commit. Delta can: the protocol allows an
absolute URI in an add action, which is why `delta/source.go`'s `resolveDataPath` accepts one, so the
Delta target keeps the absolute path deliberately. Paimon's `applyDiff` keys files by the same
relative name but falls back to the physical path, since that key is identity only and a file that
cannot be relativized still fails in `entryForDataFile`.

`JoinPath` was fixed in passing: it dropped the leading separator after a scheme, turning
`file:///data/events` into the relative `file://data/events`, so the round trip the targets depend on
did not close for a `file://` base path.

Verified: `make check` green; `go test -short -race ./pkg/...` clean; `go test -short -count=1
./test/` green with both `pathsDoubled` pins in `TestForeignFixtures_ConvertIceberg` deleted — the
field and its handling in `assertFileListMatchesManifest` had no users left and went with them, so
Iceberg→Hudi and Iceberg→Paimon now assert the plain file list every other target asserts.

---

## Ordering

**Current status**

| | Tasks |
|---|---|
| ✅ Done | T1, T3 (via T12), T4, T5, T6, T9, T11, T12, T16, T18, T20, T21, T22, T23, T25, T26, T27, T28, T29, T31, T32, T33, T35 |
| ⚠️ Superseded | T2 → T16 · T8 → T18 |
| ✅ Proven | T7, T10 → T17 — release workflow verified end to end by a throwaway tag |
| 📋 Unscheduled | T13 (HMS), T14 (catalog read side), T15 (partition sync) — parity gaps, need a decision before becoming work |
| 🎯 Open queue | T24 (deletion vectors — decide first), T30 (Java interop nightly), T34 (Paimon real-spec: Avro manifests + engine verification) |

**Picking up the queue.** Suggested value order is T34, then T30.
T24 is a decision before it is code — read `SPEC.md:335` first and do not start writing an Iceberg
deletion-vector translator until the INV-1 question in the task is answered.
before it is code — read `SPEC.md:335` first and do not start writing an Iceberg deletion-vector
translator until the INV-1 question in the task is answered.

Gate at review time: `make check` green, `go test -short -race ./pkg/...` clean, 28 commits unpushed
(`d34ed36..3162cf3`), working tree clean. Pushing is safe; **tagging is not until T17 is proven**.

```
T16 ─────> T18-catalog ──> T14   (fixes catalog sync; its tests also lift pkg/catalog coverage)
T17 ─────> unblocks T7 — nothing can be tagged until the release job runs green
T18 ─────> measure after T16; do not chase a coverage number
T13, T15 ─ unscheduled parity gaps

T16 and T17 are independent of each other — do them in either order, or in parallel.
```

**Do T10 and T11 first.** Both are small, and until T10 lands the release process is decorative.

Do **not** batch T1 with anything. It touches six files across four entrypoints and needs to be
revertable on its own.

## T36 — Delta source: read Parquet checkpoints ✅

Found auditing against upstream #902's engine baseline. The Delta source replayed JSON commits
only: a table whose pre-checkpoint commits were expired by log retention (the default within 30
days of the first checkpoint) had no readable `metaData` and failed outright — and a truncated log
without a checkpoint would silently build a partial snapshot from the surviving tail.

Landed: `pkg/formats/delta/checkpoint.go` reads `_last_checkpoint` and the classic single- or
multi-part checkpoint Parquet (parquet-go), and `loadLogState` seeds every read path (table,
snapshot, incremental) from checkpoint state plus the JSON tail strictly after it. A truncated log
with no checkpoint is now a hard error. v2 checkpoints (sidecars, `v2Checkpoint` reader feature)
are rejected with a clear message — implementing them is a follow-up, and matters more as Delta
4.0 makes them the default.

**Verified:** `test/testdata/fixtures/delta-rs-checkpoint/` is written by delta-rs, whose
`cleanup_metadata()` really deleted the version 0–1 commits (generator asserts it);
`test/delta_checkpoint_fixture_test.go` covers snapshot-from-checkpoint, incremental-safety
refusal across the cleaned history, the truncated-log error, and a full conversion to Iceberg.
`pkg/formats/delta/checkpoint_test.go` pins the v2 rejection and the multi-part merge.

## T37 — Hudi 1.x: confirm and close the timeline-layout gap

Upstream builds against Hudi 1.2 (#902); polytable's target stamps `hoodie.table.version=6`
(0.14-era) and `timeline.go` parses 0.x instants. **Confirmed with a real fixture** —
`test/testdata/fixtures/hudi-1.x/trips`, written by Hudi 1.2.0 through Spark 3.5
(`hoodie.table.version=9`, timeline under `.hoodie/timeline/`): the failure mode was worse than
unreadable. The reader returned an **empty table silently** — zero files, empty schema, exit 0 —
so a sync would have succeeded with empty target metadata. Landed as the first step: a version
guard (`TableProperties.AssertReadableVersion`, floor `hoodie.table.version=6`) makes every read
path refuse loudly, and the Hudi target no longer swallows ReadProperties errors — an unreadable
properties file stops the sync instead of being overwritten as if the target were fresh.
`test/hudi_1x_fixture_test.go` pins the refusal and flips red when someone implements 1.x reading
— replace it with manifest assertions then. Remaining: implement the 1.x timeline
(completion-time instant names, `.hoodie/timeline/` plus `history/`, the metadata-table
directory needs excluding from file listing) or keep the documented 0.x floor. Writing version 6
remains correct — 1.x readers consume it — so the write side is not part of the gap.

Related upstream context noted while here: #810 asks for a OneLake/Fabric catalog client. Its
read API is Iceberg-REST-compatible, so `pkg/catalog/rest.go` is the eventual entry point, but
any OneLake work is blocked on an Azure/`abfss://` storage backend either way (see Non-goals).

## Non-goals

- **Renaming stuttering identifiers** (`delta.DeltaCommit` → `Commit`, `catalog.CatalogType` → `Type`).
  `revive`'s stuttering check is deliberately disabled in `.golangci.yml`. This is a breaking public API
  change and needs a maintainer decision, not a lint-driven sweep.
- **Fixing the three known model defects** (`ParseTableFormat` mixed case, `DiffFiles` map ordering and
  `PhysicalPath`-only keying, `FieldByPath` case-insensitivity). They are documented in `CLAUDE.md`;
  fix them when touching the surrounding code, not as a campaign.
- **GCS/Azure storage backends.** T3 first — the existing S3 configuration path is not reachable from
  config, and adding backends before fixing that repeats the mistake.
- **Changing `go.mod`'s Go directive.**
