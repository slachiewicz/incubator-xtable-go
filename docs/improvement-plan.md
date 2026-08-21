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

## T23 — Catalog table discovery (list and scan)

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

Every existing e2e and container suite converts tables polytable wrote and reads them back with
polytable. A bug symmetrical in our reader and writer passes all of it. T28–T30 attack that blind
spot, cheapest first. Convention for all three: anything needing Docker, an external binary or the
network gates on `testing.Short()` (never a build tag — matches the dockertest suites and keeps
`make check` self-contained), no `t.Parallel()` in container/binary-gated suites.

## T28 — Real-writer fixtures under `test/testdata/fixtures/`

Check in small tables written by real engines and test that polytable reads foreign metadata, not
just its own. JVM-free generators only: Delta via `delta-rs` (Python `deltalake`), Iceberg via
`pyiceberg`. Each fixture: ~3 commits, a schema change, partitions, column stats; data files a few
KB. Spark-written and Hudi fixtures need a JVM — they belong to T30's job, not here; record the gap.
A committed `test/fixtures/generate.py` documents provenance (run manually, never in CI).

Tests: per-format read tests asserting schema/files/stats extracted from the foreign metadata match
the generator's manifest (a small JSON the generator writes alongside), plus sync tests converting
each fixture through the target matrix. Plain `go test -short` — no Docker.

**Acceptance:** fixtures for Delta (delta-rs) and Iceberg (pyiceberg) committed with a provenance
script and manifest; read + convert tests green; any reader bug they expose fixed in the same
change or filed as its own task in this file.

## T29 — Engine verification of polytable output with DuckDB

After converting a source through the matrix, read the *output* with DuckDB (`delta_scan`,
`iceberg_scan`) — a single static binary, no JVM. New `test/engineverify_duckdb_test.go`: gated on
`testing.Short()` and skipped with a clear message when `duckdb` is not on PATH. Assert row counts
match and that a stats predicate prunes (proves bounds are real, not just present). CI: install the
DuckDB binary in `integration.yml`.

**Acceptance:** Delta and Iceberg outputs of at least three matrix pairs each verified by DuckDB
locally; `integration.yml` runs the suite; the suite skips cleanly where duckdb is absent.

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

---

## Ordering

**Current status**

| | Tasks |
|---|---|
| ✅ Done | T1, T3 (via T12), T4, T5, T6, T9, T11, T12, T16, T18, T20, T21, T22, T25, T26, T27 |
| ⚠️ Superseded | T2 → T16 · T8 → T18 |
| ✅ Proven | T7, T10 → T17 — release workflow verified end to end by a throwaway tag |
| 📋 Unscheduled | T13 (HMS), T14 (catalog read side), T15 (partition sync) — parity gaps, need a decision before becoming work |
| 🎯 Open queue | T23 (catalog discovery), T24 (deletion vectors — decide first), T28 (real-writer fixtures), T29 (DuckDB output verification), T30 (Java interop nightly — after T28/T29) |

**Picking up the queue.** The tasks are independent; take them in any order. Suggested value order
is T23 first. T24 is a decision
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
