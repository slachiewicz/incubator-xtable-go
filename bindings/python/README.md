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

`pyxtable` is a `ctypes` wrapper that now bundles the appropriate native library in the wheel.
For development or source installation, build the library from the repository root:

```sh
make bindings-c      # writes lib/libxtable.{dylib,so}
```

At import time `_find_library()` searches, in order:

1. `<pyxtable package dir>/libxtable.{dylib,so,dll}`
2. `../../lib/` and `../lib/` relative to the package
3. `./lib/` relative to the current working directory
4. the bare library name, left to the system loader

**New**: Wheels now include platform-specific native libraries, so standard `pip install` works without manual library building.

If the library cannot be loaded, import still succeeds but the failure surfaces on first call with a detailed error message including the underlying exception for easier diagnosis.

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

## Packaging

The package now includes build automation and proper wheels:

```sh
# Build wheel with bundled native library (platform-specific)
make bindings-python    # builds native lib, copies to pyxtable/, creates wheel in dist/

# Install from built wheel
pip install bindings/python/dist/pyxtable-0.1.0-*.whl

# Or install from source (requires manual library building first)
make bindings-c
pip install bindings/python/
```

**Platform-specific wheels**: macOS wheels bundle `libxtable.dylib`, Linux wheels bundle `libxtable.so`. They cannot be combined in a universal wheel due to platform differences.

## Status

Packaging now includes the native library in wheels via `setuptools.package-data`. Standard pip installation works without manual library building. Error reporting improved to show underlying exceptions for easier troubleshooting.
