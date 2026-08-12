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

Fifteen tasks. **T1–T9** were the original structural plan, written against commit `ec2fd7e`.
**T10–T12** were added from the review of commits `d34ed36..ddd157a` — T10 is a release blocker.
**T13–T15** are parity gaps against Java XTable: unscheduled, and each needs a maintainer decision
before it becomes work.

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
5. **Do not edit `go.mod`'s `go` directive** (currently `go 1.25.5`; see T11 — the patch-level floor is
   itself under review). If a task genuinely requires it, update the `ci.yml` matrix in the same commit.
6. **Never assert on `model.DiffFiles` slice ordering** — it ranges over maps and is nondeterministic.
7. After editing `.golangci.yml`, run `golangci-lint config verify`. `golangci-lint run` silently
   ignores misplaced keys; `config verify` is the only command that rejects them.
8. **✅ means every acceptance criterion in the task was checked and passed** — not "the code landed".
   If some criteria are unmet, mark the task ⚠️ and say which. This rule exists because it has been
   broken three times so far (T3, T8, T10 were each marked complete with criteria outstanding), and a
   status nobody can trust makes this document worse than no document.
9. **Run `go test -short -race ./pkg/...` for any change touching `pkg/daemon` or goroutines.**
   `make check` does not enable the race detector; a data race was shipped and later fixed in `3162cf3`.

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

## T7 — Release process ⚠️ STILL BLOCKED — see T17

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
   settles the naming. Note the module path is already `github.com/apache/incubator-xtable-go` while the
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

## T10 — Fix the release workflow ⚠️ PARTIAL (arm64 leg still fails) — see T17

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

## T16 — Rewrite catalog sync as per-target 🔴 CORRECTNESS

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

## T17 — Finish the release workflow: the arm64 leg 🔴 BLOCKS TAGGING

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

## T18 — Finish T8: the two coverage targets that did not move ⚠️

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

## Ordering

**Current status**

| | Tasks |
|---|---|
| 🔴 Do next | **T16** (catalog registers the wrong table — correctness), **T17** (arm64 leg blocks tagging) |
| ⚠️ Then | **T18** (T8's two unmoved coverage targets) |
| ✅ Done | T1, T3 (via T12), T4, T5, T6, T9, T11, T12 |
| ⚠️ Superseded | T2 → T16 · T7, T10 → T17 · T8 → T18 |
| 📋 Unscheduled | T13 (HMS), T14 (catalog read side), T15 (partition sync) — parity gaps, need a decision before becoming work |

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
