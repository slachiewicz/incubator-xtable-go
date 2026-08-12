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

# pyxtable

Python bindings for [Apache XTable (incubating)](https://xtable.apache.org) via the Go
implementation's `c-shared` library — omni-directional lakehouse metadata translation with no JVM.

## Requirements

`pyxtable` is a thin `ctypes` wrapper. It does **not** bundle the native library, so you must build
`libxtable` first, from the repository root:

```sh
make bindings-c      # writes lib/libxtable.{dylib,so}
```

At import time `_find_library()` searches, in order:

1. `<pyxtable package dir>/libxtable.{dylib,so,dll}`
2. `../../lib/` and `../lib/` relative to the package
3. `./lib/` relative to the current working directory
4. the bare library name, left to the system loader

In practice that means **run from the repository root** after `make bindings-c`, or copy the built
library next to `pyxtable/__init__.py`. If it cannot be loaded, import still succeeds and the failure
surfaces on first call as `RuntimeError: libxtable shared library is not loaded`.

## Usage

```python
import pyxtable

pyxtable.version()

pyxtable.inspect("DELTA", "/path/to/delta_table")

pyxtable.sync({
    "sourceFormat": "DELTA",
    "targetFormats": ["ICEBERG"],
    "tableBasePath": "/path/to/delta_table",
    "tableName": "customers",
})
```

`sync` accepts either a dict or a YAML/JSON string. Both `inspect` and `sync` return plain dicts
decoded from the library's JSON output.

## Status

Packaging is incomplete: there is no build hook that compiles or vendors the native library into a
wheel, so `pip install .` produces a distribution that only works alongside a separately built
`libxtable`. Treat this as a source-tree binding rather than a publishable package.
