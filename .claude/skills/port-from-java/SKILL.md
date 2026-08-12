---
name: port-from-java
description: Port a class or subsystem from the Java Apache XTable project to Go. Use when implementing any xtable-go type or converter that has a counterpart in ../incubator-xtable.
disable-model-invocation: true
---

Port the Java type or subsystem named in $ARGUMENTS from Apache XTable to Go.

## 1. Find and read the Java source

The Java project is at `../incubator-xtable`. Locate the counterpart before writing any Go:

```sh
find ../incubator-xtable -name '<ClassName>.java' -not -path '*/target/*'
```

Relevant modules: `xtable-api` (the `Internal*` domain model), `xtable-core` (conversion sources and
targets per format), `xtable-service` (REST layer), `xtable-utilities` (CLI).

Read the whole file, not just the field declarations. Behavior worth carrying over usually lives in the
Lombok annotations (`@Value`, `@Builder`, `@NonNull`), the static factory methods, and the Javadoc. Also
check its direct test in `src/test/java/` — that is where edge-case semantics are pinned down.

## 2. Translate, don't transliterate

Match the semantics; use Go's idioms for the mechanics.

- Java `enum` with a string value → a named `string` type plus typed constants, as `pkg/model/types.go`
  already does.
- Lombok `@Builder` → a plain struct with a `NewX(...)` constructor for the non-trivial cases. Do not
  build a builder type unless the field count genuinely demands it.
- `Optional<T>` / boxed nullable fields → a pointer, or the type's zero value where zero is unambiguous.
- Java collections → slices and maps; keep element types as pointers where the Java side shares
  references (e.g. `[]*Field`).
- Nil-receiver safety: existing methods like `Schema.AllFields` and `FilesDiff.HasChanges` tolerate a nil
  receiver. Follow that.

Note any place the Java behavior cannot be reproduced faithfully, and tell the user rather than silently
diverging.

## 3. Write the file

- Copy the 16-line ASF Apache-2.0 header verbatim from an existing file in `pkg/model/`.
- Add a doc comment on every exported identifier, naming the Java type it corresponds to.
- Add table-driven tests in the external `model_test` package with `t.Parallel()`, matching the style of
  `pkg/model/model_test.go`. Use `testify`'s `require` for preconditions and `assert` for the assertions
  that follow.
- Do not assert on slice ordering where the implementation ranges over a map.

## 4. Verify

Run the `/verify-go` gate. Report the port as done only once all four stages pass.
