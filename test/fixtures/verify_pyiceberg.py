# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Scan a polytable-written Iceberg table with pyiceberg.

A manual check, not part of `make check`: it needs a Python virtualenv, which the Go gate must not.
DuckDB covers the same ground in CI through test/engineverify_duckdb_test.go; this is a second,
independently implemented reader, and it was how T31's acceptance was signed off.

    python3 -m venv .venv
    .venv/bin/pip install pyiceberg pyarrow
    .venv/bin/python test/fixtures/verify_pyiceberg.py <table dir> [expected row count]

The table directory is a Hadoop-layout Iceberg table — what `polytable sync --target-format ICEBERG`
writes. The script reads the newest metadata.json, scans the table and prints the rows, failing on
an empty scan or on a row count that does not match.
"""

import sys
from pathlib import Path

from pyiceberg.table import StaticTable


def newest_metadata(table_dir: Path) -> Path:
    """Return the highest-versioned metadata.json of a Hadoop-layout table."""
    candidates = sorted(table_dir.glob("metadata/*.metadata.json"))
    if not candidates:
        raise SystemExit(f"no iceberg metadata under {table_dir}")

    def version(path: Path) -> int:
        stem = path.name.removesuffix(".metadata.json")
        return int(stem[1:] if stem.startswith("v") else stem.split("-", 1)[0])

    return max(candidates, key=version)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(f"usage: {sys.argv[0]} <table dir> [expected row count]")

    table_dir = Path(sys.argv[1]).resolve()
    expected = int(sys.argv[2]) if len(sys.argv) > 2 else None

    metadata = newest_metadata(table_dir)
    table = StaticTable.from_metadata(f"file://{metadata}")

    print(f"metadata: {metadata.name}")
    print(f"schema:\n{table.schema()}")
    print(f"partition spec: {table.spec()}")

    files = list(table.scan().plan_files())
    print(f"data files: {len(files)}")

    rows = table.scan().to_arrow()
    print(rows.to_pydict())

    if rows.num_rows == 0:
        raise SystemExit("the scan returned no rows")
    # A table whose data files carry no field ids reads as the right number of rows of nulls unless
    # the name mapping is present, so the values are checked, not only the count.
    for name in rows.column_names:
        if rows.column(name).null_count == rows.num_rows:
            raise SystemExit(f"every value of column {name} is null")
    if expected is not None and rows.num_rows != expected:
        raise SystemExit(f"expected {expected} rows, scanned {rows.num_rows}")

    print(f"OK: {rows.num_rows} rows, {len(rows.column_names)} columns")


if __name__ == "__main__":
    main()
