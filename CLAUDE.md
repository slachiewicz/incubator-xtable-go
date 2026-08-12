# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go-native port of [Apache XTable (incubating)](https://github.com/apache/incubator-xtable) — omni-directional
metadata translation between Delta Lake, Iceberg, Hudi, Paimon and raw Parquet. Library-only module
(`github.com/apache/incubator-xtable-go`); there is no `main` package yet.

The Java original is checked out at `../incubator-xtable`. `pkg/model` mirrors `xtable-api`'s `Internal*`
types (`InternalTable`, `InternalSchema`, `InternalField`, `InternalDataFile`, `InternalSnapshot`,
`TableChange`, `TableSyncMetadata`), so that tree is where the authoritative semantics live.

## Scope: parity before extensions

Feature parity with Java XTable comes first. The Go-native additions discussed for this project — WASM
builds, `c-shared` FFI libraries, a long-lived streaming sync daemon — are explicitly **out of scope**
until parity lands. Do not start them, and do not add dependencies or abstractions that only exist to
serve them.

Parity order: `pkg/model` → storage layer → Delta + Iceberg full-snapshot sync → incremental sync + CLI →
Hudi + catalog sync (Glue, HMS, Iceberg REST) → deletion vectors, REST service, Paimon.

## Verification gate

Do not report work as done without all four passing. Run them in this order:

```sh
gofmt -l .            # must print nothing
go vet ./...
go test ./...
golangci-lint run ./...
```

`gofmt -l .` is a separate step on purpose: there is no `.golangci.yml`, and golangci-lint v2's default
linter set does **not** include a formatting check. Lint passing does not mean the tree is formatted.
`gofumpt` and `goimports` are not installed — do not put them in a workflow without an install step.

## Licensing

Every `.go` file carries the identical 16-line Apache-2.0 ASF header. New files must too — copy it verbatim
from any existing file in `pkg/model/`. This repo is headed for ASF donation; `LICENSE`, `NOTICE` and
`DISCLAIMER-WIP` at the root come from the parent Java project and should stay in sync with it.

## Go version

`go.mod` pins `go 1.26.5` — a full patch version, which is unusual. With `GOTOOLCHAIN=auto`, editing that
line silently downloads a different toolchain. Leave it alone unless the version bump is the point of the change.

## Known defects in the current model

Do not treat these as intended behavior; fix them when touching the surrounding code.

- `ParseTableFormat` (`pkg/model/types.go`) matches only all-upper or all-lower literals — `"Iceberg"`
  returns an error while `"ICEBERG"` and `"iceberg"` work.
- `DiffFiles` (`pkg/model/diff.go`) builds its result by ranging over maps, so `FilesAdded`/`FilesRemoved`
  ordering is nondeterministic. Tests must not assert slice order. It also keys purely on `PhysicalPath`,
  so a file whose size or record count changed is reported as unchanged.
- `Schema.FieldByPath` (`pkg/model/schema.go`) uses `strings.EqualFold` and so is case-**insensitive**,
  contradicting its own doc comment.

## Testing

Tests live in the external `model_test` package (black-box), are table-driven, and call `t.Parallel()` in
both the parent test and its subtests. `github.com/stretchr/testify` (`assert` + `require`) is the only
non-stdlib dependency. Keep new tests in that style.
