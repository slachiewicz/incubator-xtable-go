---
name: verify-go
description: Run the full xtable-go verification gate - gofmt, go vet, go test, golangci-lint - and report exactly which stage failed. Use before reporting any code change as done.
---

Run these four checks from the repo root, in order. Do not stop at the first failure — run all four, so
the user sees every problem in one pass.

```sh
gofmt -l .
go vet ./...
go test ./...
golangci-lint run ./...
```

Interpreting the results:

- `gofmt -l .` — **any output is a failure**; each line is an unformatted file. This check is not
  redundant with golangci-lint: there is no `.golangci.yml` in this repo, and the v2 default linter set
  has no formatting linter. Fix with `gofmt -w <file>`.
- `go vet ./...` — silence is success.
- `go test ./...` — if a test fails, report the failing test name and its assertion output verbatim. Do
  not summarize a failure as "a test failed".
- `golangci-lint run ./...` — success prints `0 issues.`

Report the outcome as a short per-stage list with the actual command output for anything that failed.
Never report the gate as passing on the strength of a check you did not run.

If a test failure looks like it depends on slice ordering, check whether it is asserting on the output of
`DiffFiles`, whose ordering is nondeterministic by construction (see CLAUDE.md).
