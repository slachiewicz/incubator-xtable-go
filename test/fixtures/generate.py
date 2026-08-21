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

"""Write the table fixtures under test/testdata/fixtures with real engines.

Every other test in this repository reads a table polytable itself wrote, so a reader that agrees
with polytable's own writer passes even when it disagrees with the format. These fixtures come from
delta-rs and pyiceberg instead, and `test/foreign_fixtures_test.go` asserts polytable's readers
against the `manifest.json` this script emits next to each one.

Run it from a virtualenv that is not committed:

    python3 -m venv .venv
    .venv/bin/pip install deltalake pyarrow 'pyiceberg[sql-sqlite]'
    .venv/bin/python test/fixtures/generate.py

Determinism has limits the writers impose. Row values, row counts, column order, partition values
and the commit sequence are fixed here, so the metadata a rerun produces describes the same table.
Commit timestamps, table UUIDs, snapshot IDs and the generated data file names are not: both
writers mint them per run. The Go tests therefore assert against manifest.json, which is regenerated
with the fixture, and never against a literal from a previous run.

The Iceberg fixture records absolute locations, which Iceberg's metadata format requires. Every
occurrence of the generation-time warehouse path in a *.metadata.json is rewritten to the
PATH_PLACEHOLDER token below, and the Go test substitutes the directory it copied the fixture into.
The paths embedded in the Avro manifests are left as generated: they cannot be rewritten without
re-encoding the file. `relocateAvroManifests` in test/foreign_fixtures_test.go does exactly that
re-encoding when it loads the fixture, under the file's own schema and header.
"""

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pyarrow as pa

PATH_PLACEHOLDER = "file:///__POLYTABLE_FIXTURE_ROOT__"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "test" / "testdata" / "fixtures"

# Canonical type names shared by both manifests, so one Go assertion covers both fixtures. They are
# the names of polytable's model.Type constants; the mapping from each writer's own type names is
# spelled out below rather than inferred.
DELTA_TYPE_NAMES = {"long": "LONG", "string": "STRING", "double": "DOUBLE"}
ICEBERG_TYPE_NAMES = {"long": "LONG", "string": "STRING", "double": "DOUBLE"}


def _rmtree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _write_manifest(directory: Path, manifest: dict) -> None:
    with (directory / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


# ---------------------------------------------------------------------------- Delta (delta-rs)


def generate_delta(out_dir: Path) -> dict:
    """Write a three-commit partitioned Delta table with a mid-history column addition."""
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "sales"

    base_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )
    # region values stay URL-safe on purpose: delta-rs percent-encodes the partition directory in
    # the add action's path and polytable's reader does not decode it, so a value needing an escape
    # would fold a second, unrelated question into this fixture.
    commit_one = pa.table(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "region": ["east", "east", "east", "west", "west", "west"],
            "amount": [10.5, 20.25, 30.0, 40.75, 50.5, 60.0],
        },
        schema=base_schema,
    )
    commit_two = pa.table(
        {
            "id": [7, 8, 9, 10],
            "region": ["east", "east", "west", "west"],
            "amount": [70.25, 80.0, 90.5, 100.75],
        },
        schema=base_schema,
    )
    evolved_schema = pa.schema(list(base_schema) + [pa.field("discount", pa.float64(), nullable=True)])
    commit_three = pa.table(
        {
            "id": [11, 12, 13, 14],
            "region": ["east", "east", "west", "west"],
            "amount": [110.0, 120.5, 130.25, 140.0],
            "discount": [1.5, 2.5, 3.5, 4.5],
        },
        schema=evolved_schema,
    )

    write_deltalake(table_dir, commit_one, mode="error", partition_by=["region"], name="sales")
    write_deltalake(table_dir, commit_two, mode="append", partition_by=["region"])
    write_deltalake(
        table_dir, commit_three, mode="append", partition_by=["region"], schema_mode="merge"
    )

    table = DeltaTable(table_dir)
    # deltalake 1.x returns an arro3 table; the Arrow PyCapsule interface carries it into pyarrow.
    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()

    data_files = []
    for add in sorted(adds, key=lambda a: a["path"]):
        data_files.append(
            {
                "path": add["path"],
                "record_count": add["num_records"],
                "size_bytes": add["size_bytes"],
                "partition_values": {"region": add["partition.region"]},
            }
        )

    schema = [
        {
            "name": field.name,
            "type": DELTA_TYPE_NAMES[field.type.type],
            "nullable": field.nullable,
        }
        for field in table.schema().fields
    ]

    def _bounds(column: str) -> dict:
        lows = [a[f"min.{column}"] for a in adds if a.get(f"min.{column}") is not None]
        highs = [a[f"max.{column}"] for a in adds if a.get(f"max.{column}") is not None]
        return {"min": min(lows), "max": max(highs)}

    manifest = {
        "format": "DELTA",
        "table_name": "sales",
        "table_dir": "sales",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "latest_commit_id": str(table.version()),
        "total_rows": sum(f["record_count"] for f in data_files),
        "data_file_count": len(data_files),
        "schema": schema,
        "partition_columns": ["region"],
        "partition_values": sorted({f["partition_values"]["region"] for f in data_files}),
        "column_bounds": {"id": _bounds("id"), "amount": _bounds("amount")},
        "data_files": data_files,
        "schema_evolution": {
            "added_column": "discount",
            "added_at_commit": str(table.version()),
        },
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Commit 2 adds the 'discount' column, so the files of commits 0 and 1 predate it.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


# ---------------------------------------------------------------------------- Iceberg (pyiceberg)


def generate_iceberg(out_dir: Path) -> dict:
    """Write a three-snapshot partitioned Iceberg table with a mid-history column addition."""
    import pyiceberg
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)

    # The catalog database is catalog state, not table state, so it is built in a throwaway
    # directory and never copied into the fixture.
    staging = Path(tempfile.mkdtemp(prefix="polytable-pyiceberg-"))
    try:
        warehouse = staging / "warehouse"
        warehouse.mkdir()
        catalog = SqlCatalog(
            "fixture",
            **{
                "uri": f"sqlite:///{staging / 'catalog.db'}",
                "warehouse": f"file://{warehouse}",
            },
        )
        catalog.create_namespace("lake")

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="category", field_type=StringType(), required=True),
            NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
        )
        spec = PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=IdentityTransform(), name="category"
            )
        )
        table = catalog.create_table("lake.events", schema=schema, partition_spec=spec)

        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("category", pa.string(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ]
        )
        table.append(
            pa.table(
                {
                    "id": [1, 2, 3, 4],
                    "category": ["alpha", "alpha", "beta", "beta"],
                    "value": [1.5, 2.5, 3.5, 4.5],
                },
                schema=arrow_schema,
            )
        )
        table.append(
            pa.table(
                {
                    "id": [5, 6, 7, 8],
                    "category": ["alpha", "alpha", "beta", "beta"],
                    "value": [5.5, 6.5, 7.5, 8.5],
                },
                schema=arrow_schema,
            )
        )

        with table.update_schema() as update:
            update.add_column("label", StringType())

        evolved_schema = pa.schema(list(arrow_schema) + [pa.field("label", pa.string(), nullable=True)])
        table.append(
            pa.table(
                {
                    "id": [9, 10, 11, 12],
                    "category": ["alpha", "alpha", "beta", "beta"],
                    "value": [9.5, 10.5, 11.5, 12.5],
                    "label": ["nine", "ten", "eleven", "twelve"],
                },
                schema=evolved_schema,
            )
        )

        table.refresh()
        files = sorted(table.inspect.files().to_pylist(), key=lambda f: f["file_path"])
        snapshots = table.snapshots()
        current = table.current_snapshot()

        table_location = table.location()
        source_dir = Path(table_location.removeprefix("file://"))
        target_dir = out_dir / "events"
        shutil.copytree(source_dir, target_dir)

        # Iceberg stores absolute locations. Rewrite them in the JSON metadata to a placeholder the
        # Go test substitutes; the Avro manifests keep the generation-time paths.
        for metadata_file in sorted(target_dir.glob("metadata/*.metadata.json")):
            text = metadata_file.read_text(encoding="utf-8")
            metadata_file.write_text(text.replace(table_location, PATH_PLACEHOLDER), encoding="utf-8")

        metadata_versions = sorted(
            int(p.name.split("-", 1)[0]) for p in target_dir.glob("metadata/*.metadata.json")
        )

        manifest = {
            "format": "ICEBERG",
            "table_name": "events",
            "table_dir": "events",
            "writer": {"library": "pyiceberg", "version": pyiceberg.__version__},
            "path_placeholder": PATH_PLACEHOLDER,
            "format_version": table.format_version,
            "snapshot_count": len(snapshots),
            "metadata_versions": metadata_versions,
            "latest_metadata_version": metadata_versions[-1],
            "current_snapshot_id": str(current.snapshot_id),
            "total_rows": sum(f["record_count"] for f in files),
            "data_file_count": len(files),
            "schema": [
                {
                    "name": field.name,
                    "type": ICEBERG_TYPE_NAMES[str(field.field_type)],
                    "nullable": not field.required,
                    "field_id": field.field_id,
                }
                for field in table.schema().fields
            ],
            "partition_columns": ["category"],
            "partition_values": sorted({f["partition"]["category"] for f in files}),
            "data_files": [
                {
                    "path": f["file_path"].removeprefix(table_location).lstrip("/"),
                    "record_count": f["record_count"],
                    "size_bytes": f["file_size_in_bytes"],
                    "partition_values": {"category": f["partition"]["category"]},
                }
                for f in files
            ],
            "schema_evolution": {
                "added_column": "label",
                "added_before_snapshot": str(current.snapshot_id),
            },
            "manifest_encoding": "avro",
            "notes": [
                "Written by pyiceberg; polytable has never touched this directory.",
                "manifest-list and manifest files are Avro OCF, which is what the Iceberg spec"
                " mandates and what every real writer emits.",
                "File paths inside the Avro manifests are the generation-time absolute paths and"
                " are not relocatable.",
            ],
        }
        _write_manifest(out_dir, manifest)
        return manifest
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main() -> int:
    out_root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_OUT
    out_root.mkdir(parents=True, exist_ok=True)

    delta = generate_delta(out_root / "delta-rs")
    print(
        f"delta-rs: {delta['commit_count']} commits, {delta['data_file_count']} files, "
        f"{delta['total_rows']} rows"
    )
    iceberg = generate_iceberg(out_root / "pyiceberg")
    print(
        f"pyiceberg: {iceberg['snapshot_count']} snapshots, {iceberg['data_file_count']} files, "
        f"{iceberg['total_rows']} rows"
    )

    total = sum(
        os.path.getsize(os.path.join(root, name))
        for root, _, names in os.walk(out_root)
        for name in names
    )
    print(f"total fixture size: {total / 1024:.1f} KiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
