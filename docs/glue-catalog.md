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

# Sync to the AWS Glue Data Catalog

The Glue Data Catalog can play three roles in a polytable sync: a registry the
synced target table is written to, a directory polytable discovers tables from,
and a resolver that supplies a source table's location. This page covers all
three. It is the polytable counterpart of the Apache XTable guides
[Syncing to Glue Data Catalog](https://xtable.apache.org/docs/glue-catalog) and
[Registering tables across catalogs](https://xtable.apache.org/docs/how-to-catalog-sync).

Authentication and the AWS Region come from the standard AWS configuration
chain (environment variables, shared config file, or instance role) — the same
chain the AWS CLI uses. There is no per-catalog credentials key.

## Prerequisites

- A source table in Amazon S3. To create one, see
  [Create your first interoperable table](how-to.md).
- AWS credentials with Glue and S3 permissions, for example verified by
  `aws sts get-caller-identity`.

The flows on this page are covered by this repository's unit tests against a
fake catalog client, not by a live AWS run. Reports from real deployments are
welcome.

## Register a synced table in Glue

If the target database does not exist yet, create it:

```shell
aws glue create-database --database-input '{"Name":"polytable_synced_db"}'
```

Add a `catalogs` block to the dataset. After each target format syncs
successfully, polytable creates or updates the Glue table and its partitions:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://my-bucket/tables/people
    tableName: people
    catalogs:
      - type: AWS_GLUE
        databaseName: polytable_synced_db
```

```shell
./bin/polytable sync --datasetConfig my_config.yaml
```

Optional keys on a catalog entry:

- `catalogId`: the AWS account ID that owns the catalog. Defaults to the
  caller's account.
- `maxPartitionsPerRequest`: batch size for partition registration.

The Glue table is registered under `tableName`. Because that one name is used
for every target format, list a single format in `targetFormats` when
registering to a catalog; with several formats, each registration overwrites
the previous one.

Verify the registration:

```shell
aws glue get-table --database-name polytable_synced_db --name people
```

## Discover tables to sync from Glue

Instead of naming tables in a config file, polytable can scan a Glue database
and sync every table that opts in. A table opts in by carrying a
`polytable_target_formats` table property that lists its target formats,
comma-separated — for example `ICEBERG,HUDI`. Set the property with the engine
that owns the table (for example `ALTER TABLE ... SET TBLPROPERTIES`) or in the
Glue console. If you use `aws glue update-table`, pass the complete table
input, because that call replaces the whole table definition.

Then run the sync against the database; each table's format and location come
from its Glue entry:

```shell
./bin/polytable sync --catalog glue --database analytics
```

Tables without the property are skipped. The `--catalogId` flag selects another
account's catalog, and `--dry-run`, `--mode`, `--output`, and `--timeout` work
the same as with a config file.

## Resolve a source table from Glue

A dataset can name its source as a catalog entry instead of a storage path.
polytable reads the format, location, and name from Glue before syncing:

```yaml
targetFormats:
  - ICEBERG
datasets:
  - sourceCatalog:
      catalog:
        type: AWS_GLUE
        databaseName: analytics
      table: people
```

Explicitly set fields win over resolved ones, so you can override, for example,
`tableName` while taking the path from the catalog.

## Not supported

Hive Metastore is declared as a catalog type for config compatibility but has
no client; selecting it fails with a "not implemented" error. BigLake and Unity
Catalog have no native clients either — Unity Catalog is reachable through its
Iceberg REST endpoint instead; see
[Sync to an Iceberg REST catalog](iceberg-rest-catalog.md).
