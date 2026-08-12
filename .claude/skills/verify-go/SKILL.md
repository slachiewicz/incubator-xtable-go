---
name: verify-go
description: Run the full xtable-go verification gate - gofmt, go vet, js/wasm vet, go test, golangci-lint - and report exactly which stage failed. Use before reporting any code change as done.
---

Run `make check` from the repo root. It runs all five stages below and stops at the first failure, so when
it fails, run the remaining stages by hand — the user should see every problem in one pass, not just the
first.

```sh
gofmt -l .
go vet ./...
GOOS=js GOARCH=wasm go vet ./cmd/xtable-wasm
go test -short ./...
golangci-lint run ./...
```

Interpreting the results:

- `gofmt -l .` — **any output is a failure**; each line is an unformatted file. Not redundant with
  golangci-lint: `.golangci.yml` configures no formatter, so lint can pass on an unformatted tree. Fix
  with `gofmt -w <file>`.
- `go vet ./...` — silence is success.
- `GOOS=js GOARCH=wasm go vet ./cmd/xtable-wasm` — that file is behind `//go:build js && wasm`, so no
  other stage compiles it. Skipping this is how a WASM break reaches `main` unnoticed.
- `go test -short ./...` — **`-short` is required.** `test/dockertest_minio_matrix_test.go` and
  `test/dockertest_iceberg_rest_test.go` gate on `testing.Short()` rather than a build tag, so the
  unqualified command starts MinIO and Nessie containers and fails outright without a Docker daemon.
  Use `make test-containers` when you actually want that coverage. If a test fails, report the failing
  test name and its assertion output verbatim — never summarize a failure as "a test failed".
- `golangci-lint run ./...` — success prints `0 issues.`

Two failure modes of the lint stage are about the config, not the code:

- `can't load config: unsupported version of the configuration: ""` — `.golangci.yml` is missing its
  `version: "2"` key. Fix with `golangci-lint migrate`.
- Lint passes suspiciously fast and clean — check for a top-level `linters-settings:` key. `run` accepts
  and ignores it, silently disabling everything nested under it. Only `golangci-lint config verify`
  catches this; run it after any config edit. Settings belong under `linters.settings`.

Report the outcome as a short per-stage list with the actual command output for anything that failed.
Never report the gate as passing on the strength of a check you did not run.

If a test failure looks like it depends on slice ordering, check whether it is asserting on the output of
`DiffFiles`, whose ordering is nondeterministic by construction (see CLAUDE.md).
