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

# Sync to an Iceberg REST catalog

polytable registers Iceberg tables it produces in any catalog that speaks the
[Iceberg REST catalog protocol](https://iceberg.apache.org/rest-catalog-spec/),
such as Nessie, Apache Polaris, Unity Catalog, or Tabular. The
integration suite verifies this client against a real REST catalog server, the
`tabulario/iceberg-rest` reference image
(`test/dockertest_iceberg_rest_test.go`, run by `make test-containers`).

The catalog entry takes the catalog's base URI — polytable calls the
`/v1/namespaces/...` routes under it — the namespace as `databaseName`, and an
optional bearer token:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://my-bucket/tables/people
    tableName: people
    catalogs:
      - type: ICEBERG_REST
        uri: http://localhost:8181
        databaseName: analytics
        properties:
          token: MY_BEARER_TOKEN
```

Replace `MY_BEARER_TOKEN` with a token your catalog accepts, or omit the
`properties` block for an unauthenticated catalog. The namespace must already
exist; polytable creates or updates the table inside it after each successful
sync.

### Authentication properties

The `properties` block takes three keys:

- **`auth`**: the authentication mode. Omit it, or leave it empty, for the
  static bearer token in `token`. Set it to `entra`, `entra-id`, or `azure`
  for Microsoft Entra ID. Any other value fails with an error naming the
  accepted ones, so a typo cannot silently leave the catalog
  unauthenticated.
- **`token`**: a static bearer token, used when `auth` is empty. It never
  expires from polytable's point of view, so a long-running sync fails when
  the token does.
- **`scope`**: the Entra ID scope to request, used when `auth` selects Entra.
  It defaults to `https://storage.azure.com/.default`, which is the scope that
  requests the `Storage` audience — the only audience OneLake accepts. Set it
  for a non-OneLake catalog behind Entra ID that expects a different
  audience.

With `auth: entra`, polytable acquires the token through
`DefaultAzureCredential`, which covers workload identity, managed identity, an
environment service principal, and the Azure CLI, and refreshes it before it
expires. This path has not been exercised against a live Fabric workspace.

Entra ID authentication is unavailable in the WebAssembly build, which has no
credential chain; it returns an error saying so.

Register only the `ICEBERG` target in a REST catalog — the protocol carries
Iceberg metadata, so a Delta Lake or Hudi target has nothing to register there.

## Resolve a source table from the catalog

As with Glue, a dataset can name its source through the catalog instead of a
storage path:

```yaml
targetFormats:
  - DELTA
datasets:
  - sourceCatalog:
      catalog:
        type: ICEBERG_REST
        uri: http://localhost:8181
        databaseName: analytics
      table: people
```

polytable reads the table's location and format from the catalog, then syncs as
usual. Explicitly set dataset fields win over resolved ones.

## Compatibility notes

- Unity Catalog: use the workspace's Iceberg REST endpoint
  (`https://<workspace-host>/api/2.1/unity-catalog/iceberg`) with a Databricks
  token in `properties.token`.
- Nessie: the Iceberg REST endpoint is served under `/iceberg` on the Nessie
  server, for example `http://localhost:19120/iceberg`.
- Microsoft OneLake and Fabric: not usable yet. The endpoint is
  `https://onelake.table.fabric.microsoft.com/iceberg`, and it requires prefix
  negotiation that polytable does not implement — see
  [Azure and OneLake](azure.md#catalogs-on-azure) and T53 in
  [the improvement plan](improvement-plan.md). It is also read-only, so it can
  serve a conversion source but never accept a registration.

These endpoint shapes come from the respective vendors' documentation; only the
`tabulario/iceberg-rest` image is exercised by this repository's integration
tests.
