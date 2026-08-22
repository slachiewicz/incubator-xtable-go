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

# Azure Data Lake Storage and OneLake

This is the extended reference for polytable's Azure backend: URI shapes, the credential chain in
full, endpoint derivation, worked dataset configurations, catalog support, and local development
with Azurite. For the short version — the scheme, a minimal config, and the credential order — see
[Sync a table in Azure](cloud-storage.md#sync-a-table-in-azure) in
[Cloud storage](cloud-storage.md); this page goes deeper into the same material rather than
repeating it, and cross-links back where the two overlap.

## Status

Azure Data Lake Storage Gen2 and Microsoft Fabric OneLake are implemented in `pkg/io/azure.go`:
`abfss://`, `abfs://`, `wasbs://`, and `wasb://` paths route to a real client built on the
`azblob` SDK, with credential selection, endpoint derivation, and OneLake's path shape all coded
against Microsoft's published documentation.

Azure Data Lake Storage Gen2 is verified against a real account. On a `StorageV2` account created
with the hierarchical namespace enabled, polytable synced a Delta table to Iceberg and Hudi over
`abfss://`, read every target back, resolved directories correctly, and authenticated through both a
shared key and `DefaultAzureCredential`. The Azurite emulator suite
(`test/dockertest_azurite_test.go`) covers shared key, SAS, anonymous and all four ABFS spellings on
every run.

**OneLake is not verified.** A token has been accepted by the live endpoint, which confirms the
scope and the Entra transport, but no request against a real Fabric workspace has succeeded —
see [Catalogs on Azure](#catalogs-on-azure). `docs/improvement-plan.md` T51 and T52 record exactly
what that leaves open, and [Set up an Azure test environment](azure-test-environment.md) is the
recipe for closing it.

## URI shapes

polytable recognizes four URI schemes for Azure, because Hadoop's ABFS driver spells the same
store four ways and foreign metadata carries whichever one the writing engine was configured with
(`pkg/io/storage.go`'s `azureSchemes`):

- `abfss://<container>@<account>.dfs.core.windows.net/<path>` — ADLS Gen2 over TLS, the
  recommended form.
- `abfs://<container>@<account>.dfs.core.windows.net/<path>` — the same endpoint without TLS.
- `wasbs://<container>@<account>.blob.core.windows.net/<path>` — the older Blob Storage driver
  name, over TLS.
- `wasb://<container>@<account>.blob.core.windows.net/<path>` — the same, without TLS.

All four parse through the same `ParseAzureURI` (`pkg/io/azure.go`), which extracts the container,
the blob path, the account host, and the scheme, and all four are also in `uriSchemes`
independently of whether a backend can serve them — `RelativizePath` treats an unrecognized scheme
as already relative, so leaving one out would have silently mangled a metadata path carrying it.

### Microsoft Fabric OneLake

OneLake is reached through the same `abfss://` (or `abfs://`) scheme, with a different mapping of
URI components. Microsoft documents the shape as:

```
abfs[s]://<workspace>@onelake.dfs.fabric.microsoft.com/<item>.<itemtype>/<path>/<fileName>
```

The account name is always `onelake`, the container name is your workspace name, and the data path
starts at the item — for example
`abfss://myworkspace@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Tables/sales`. No special
case in `ParseAzureURI` or `NewAzureStorage` is needed for this: the host already starts with
`onelake.`, so the existing rule of cutting the account off the first dot-separated label of the
host yields the literal account name `onelake` on its own, with the workspace and item living in
the container and path components instead.

For a workspace name containing spaces, OneLake also accepts the workspace's GUID in place of its
display name in the container position (per Microsoft's OneLake documentation). polytable applies
no validation to the container value — it is passed through to the Blob API request unchanged — so
a GUID container name works the same as a display name.

## Authentication

`NewAzureStorage` (`pkg/io/azure.go`) selects a credential mode with first match wins, in this
order:

1. **A SAS token** — from `AzureOptions.SASToken`; failing that, from the environment variable
   named by `AzureOptions.SASTokenEnv` (the dataset config's `storage.azure.sasTokenEnv` key);
   failing that, from the well-known `AZURE_STORAGE_SAS_TOKEN` environment variable. The client is
   built with `azblob.NewClientWithNoCredential` against the service URL with the token appended
   as a query string.
2. **A shared account key** — from `AzureOptions.AccountKey`; failing that, from the environment
   variable named by `AzureOptions.AccountKeyEnv` (`storage.azure.accountKeyEnv`); failing that,
   from the well-known `AZURE_STORAGE_KEY`. Built with `azblob.NewSharedKeyCredential` and
   `azblob.NewClientWithSharedKeyCredential`.
3. **Anonymous access** — selected explicitly with `AzureOptions.Anonymous` (the dataset config's
   `storage.azure.anonymous` key), for a public container. Built with
   `azblob.NewClientWithNoCredential` against the plain service URL, no token attached.
4. **`DefaultAzureCredential`** — the fallback when none of the above is configured. This one call
   covers several credential sources itself, tried in the order the Azure Identity SDK defines:
   - **Workload identity** — the credential a pod on Azure Kubernetes Service assumes through a
     federated identity, using the token file and client ID AKS injects.
   - **Managed identity** — the identity of the Azure resource polytable runs on (a VM, an App
     Service, a container instance), with no explicit configuration.
   - **An environment service principal** — `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and
     `AZURE_CLIENT_SECRET` (or `AZURE_CLIENT_CERTIFICATE_PATH` for certificate auth).
   - **The Azure CLI** — the identity of an `az login` session on the machine polytable runs on,
     for local development.

An unset or empty named variable — `sasTokenEnv` or `accountKeyEnv` pointing at a variable that
was never set, or set to the empty string — is an error naming the variable, not a silent
fall-through to the next credential mode. A typo in either key would otherwise surface several
steps later as a confusing Entra ID 403, far from the actual mistake.

Credentials are never a configuration-file field. `AzureStorageConfig` (`pkg/conversion/config.go`)
holds `endpoint`, `accountName`, `anonymous`, and now `accountKeyEnv`/`sasTokenEnv` — but the last
two name a variable, never a secret. A SAS token or an account key must reach polytable through the
environment, because a dataset config file gets committed, logged, and POSTed to the REST service,
and a secret in any of those places is a leak waiting to happen. Naming the variable rather than
hardcoding `AZURE_STORAGE_KEY`/`AZURE_STORAGE_SAS_TOKEN` also lets one process serve several
storage accounts, each dataset's config pointing at a different variable holding that account's own
secret. This mirrors `S3Options`, which has no credential fields either.

## Endpoints

polytable derives the blob service URL from the `abfss://`/`abfs://` host by swapping the first
`.dfs.` for `.blob.` (`NewAzureStorage` in `pkg/io/azure.go`); a host already reading `.blob.` (the
`wasbs://`/`wasb://` forms) passes through unaffected, since there is nothing to replace. OneLake
documents the same pair, `onelake.dfs.fabric.microsoft.com` and
`onelake.blob.fabric.microsoft.com`, and states that the blob endpoint carries the same
compatibility as the ADLS one — so the swap is the documented mapping for OneLake too, though no
request from this codebase has yet reached a Fabric workspace to confirm it end to end.

Four cases need an explicit `endpoint` override (`AzureOptions.Endpoint`, the dataset config's
`storage.azure.endpoint` key) because the host cannot be swapped into the right URL:

- **Azurite.** Its blob host is not derivable from an `abfss://`-shaped host at all — the emulator
  puts the account in the path, not the host. See
  [Local development with Azurite](#local-development-with-azurite) below.
- **A OneLake regional endpoint**, `https://<region>-onelake.dfs.fabric.microsoft.com`. Use it
  instead of the global endpoint when data residency matters: resolving the global endpoint for a
  workspace outside its home region can move data across a region boundary during that
  resolution, which the regional endpoint avoids.
- **A workspace private-link FQDN**, when the workspace is configured for private endpoint access.
- **`api.onelake.fabric.microsoft.com`**, a OneLake API form that contains neither `.dfs.` nor
  `.blob.` and so survives the `.dfs.` → `.blob.` swap untouched — it needs to be supplied as-is.

## Worked configurations

Each of the following is a complete dataset config plus the `polytable sync` invocation that uses
it. Save the YAML to a file and pass its path with `--datasetConfig` (`-c`).

### ADLS Gen2 with workload identity

No `storage.azure` block is needed: with no SAS token, account key, or anonymous flag configured,
`NewAzureStorage` falls through to `DefaultAzureCredential`, which resolves workload identity
first when the process is running as a pod with a federated identity configured.

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: abfss://example-container@myaccount.dfs.core.windows.net/tables/people
    tableName: people
```

```shell
./bin/polytable sync --datasetConfig adls-workload-identity.yaml
```

### ADLS Gen2 with a shared key from the environment

Set `AZURE_STORAGE_KEY` in the environment polytable runs in; the config itself carries no secret:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: abfss://example-container@myaccount.dfs.core.windows.net/tables/people
    tableName: people
```

```shell
export AZURE_STORAGE_KEY="<your-account-key>"
./bin/polytable sync --datasetConfig adls-shared-key.yaml
```

### Two datasets, two storage accounts, one process

`AZURE_STORAGE_KEY` is process-wide, so it cannot hold two accounts' keys at once. Name a variable
per dataset with `accountKeyEnv` instead — the config still carries no secret, only the name of
where to find one:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: abfss://sales@acct1.dfs.core.windows.net/tables/sales
    tableName: sales
    storage:
      azure:
        accountKeyEnv: ACCT1_STORAGE_KEY
  - tableBasePath: abfss://events@acct2.dfs.core.windows.net/tables/events
    tableName: events
    storage:
      azure:
        accountKeyEnv: ACCT2_STORAGE_KEY
```

```shell
export ACCT1_STORAGE_KEY="<acct1's-account-key>"
export ACCT2_STORAGE_KEY="<acct2's-account-key>"
./bin/polytable sync --datasetConfig two-accounts.yaml
```

An unset or empty `ACCT1_STORAGE_KEY` fails with an error naming that variable rather than falling
back to `AZURE_STORAGE_KEY` or the Entra ID chain — the same rule applies to `sasTokenEnv`.

### OneLake with Entra ID

The workspace is the container and `onelake` is always the account; with no other credential
configured, `DefaultAzureCredential` is used the same way as for ADLS Gen2 — whichever of
workload identity, managed identity, environment service principal, or the Azure CLI applies to
where polytable runs:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: abfss://myworkspace@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Tables/sales
    tableName: sales
```

```shell
az login   # or rely on managed identity / workload identity in the running environment
./bin/polytable sync --datasetConfig onelake-entra.yaml
```

### Azurite locally

Azurite's blob host is not derivable from the URI, so both `endpoint` and `accountName` are
required, and the account key comes from the environment. See
[Local development with Azurite](#local-development-with-azurite) for starting the container.

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: abfss://lakehouse-e2e@devstoreaccount1.dfs.core.windows.net/tables/people
    tableName: people
    storage:
      azure:
        endpoint: http://127.0.0.1:10000/devstoreaccount1
        accountName: devstoreaccount1
```

```shell
export AZURE_STORAGE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
./bin/polytable sync --datasetConfig azurite-local.yaml
```

## Catalogs on Azure

- **The Iceberg REST catalog with Entra ID** is supported in general: set the catalog entry's
  `type` to `ICEBERG_REST` and add `auth: entra` to its `properties` block. polytable then acquires
  tokens through `DefaultAzureCredential` and refreshes them before they expire
  (`pkg/catalog/entra.go`), rather than presenting a single static token for the whole sync:

  ```yaml
  catalogs:
    - type: ICEBERG_REST
      uri: https://my-catalog.example.com
      databaseName: analytics
      properties:
        auth: entra
  ```

  `scope` defaults to `https://storage.azure.com/.default`, the scope that requests the `Storage`
  audience — the only audience OneLake accepts. Full detail on the `auth`/`token`/`scope`
  properties, and on `entra`/`entra-id`/`azure` as equivalent spellings, is in
  [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md#authentication-properties).
- **OneLake's own Iceberg REST endpoint** is at
  `https://onelake.table.fabric.microsoft.com/iceberg`, addressed by a warehouse of
  `<WorkspaceID>/<DataItemID>` or `<WorkspaceName>/<DataItemName>.<DataItemType>`. Set that
  warehouse in the catalog entry's `properties`:

  ```yaml
  catalogs:
    - type: ICEBERG_REST
      uri: https://onelake.table.fabric.microsoft.com/iceberg
      databaseName: dbo
      properties:
        auth: entra
        warehouse: <WorkspaceID>/<DataItemID>
  ```

  `databaseName` is the Iceberg namespace — `dbo` in Microsoft's examples — not the workspace. The
  workspace and item together are the warehouse, which is why they are separate keys.

  polytable calls `GET /v1/config?warehouse=...` first and puts the returned prefix into every
  later path, which is what this endpoint requires. **No request has yet reached a Fabric
  workspace**, so treat the whole path as unexercised.

  The endpoint is read-only: its advertised operations are `GET` and `HEAD`, so it can serve a
  conversion source but never accept a registration. polytable refuses a write against it with an
  error saying the catalog is read-only rather than a bare status code.

  Writing table data to OneLake through `abfss://` is unaffected — that goes through the storage
  backend, not the catalog.
- **Unity Catalog** is reachable the same way, through its own Iceberg REST endpoint
  (`https://<workspace-host>/api/2.1/unity-catalog/iceberg`) with a Databricks token in
  `properties.token` instead of `auth: entra` — see
  [Compatibility notes](iceberg-rest-catalog.md#compatibility-notes). It has no Azure-specific
  code path of its own; it is one more Iceberg REST catalog.
- **Hive Metastore** (`CatalogTypeHMS`) is declared as a catalog type for configuration
  compatibility but has no client: selecting it fails with `ErrCatalogNotImplemented`
  (`pkg/catalog/catalog.go`).
- **Table discovery from the CLI is Glue-only.** `IcebergRESTConversionSource.ListTables` is
  implemented and paginates, but `polytable sync --catalog ... --database ...` accepts only
  `"glue"` (`parseCatalogTypeFlag` in `cmd/polytable/main.go`), so a REST-catalog source or target
  is named explicitly in a dataset config rather than discovered.
## Local development with Azurite

`test/dockertest_azurite_test.go` starts the emulator with:

```shell
docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --skipApiVersionCheck
```

Both flags are required, and each fails in a way that looks like a polytable bug.

`--blobHost 0.0.0.0`: the image's default command binds the blob listener to the container's own
loopback address, so a published port accepts the TCP connection and then resets it instead of
answering — the listener never sees traffic arriving from outside the container.

`--skipApiVersionCheck`: the `azblob` SDK sends an `x-ms-version` header newer than the emulator
recognizes, and Azurite rejects the first request with
`InvalidHeaderValue: The API version ... is not supported by Azurite`. The emulator trails the
service, so expect this to stay true after each SDK upgrade.

Azurite ships one well-known development account, and polytable's Azurite test uses it as-is —
these are fixed, publicly documented test credentials, not secrets to protect:

- Account name: `devstoreaccount1`
- Account key: `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`

Point a dataset config at it with `storage.azure.endpoint` set to Azurite's blob service URL
(`http://<host>:<port>/devstoreaccount1`) and `storage.azure.accountName` set to
`devstoreaccount1` — Azurite carries the account in the URL path rather than the host, which is
exactly what those two overrides exist for. See
[Azurite locally](#azurite-locally) above for the complete config, and pass the account key
through `AZURE_STORAGE_KEY` rather than a config field, the same as any other shared-key
deployment.

## Check that a setup works

These three commands prove a configuration end to end, and they work the same against Azurite and
against a real account. Run them after any change to credentials, endpoint, or account.

First, sync a table and read the verdict:

```shell
polytable sync --datasetConfig azure.yaml --output json
```

Every target should report `"verdict": "SUCCESS"`. A failure here names which of storage, format, or
credentials broke.

Second, run exactly the same command again:

```shell
polytable sync --datasetConfig azure.yaml --output json
```

Every target should now report `"verdict": "NO_OP"`. This is the more interesting check of the two:
`NO_OP` means polytable wrote its sync metadata into the target on the first run and read it back
from Azure on the second. A second `SUCCESS` instead means the metadata did not round-trip, and
every later sync would redo the whole table.

Third, inspect each format at the same base path:

```shell
polytable inspect --basePath "abfss://<container>@<account>.dfs.core.windows.net/<path>" --format DELTA
polytable inspect --basePath "abfss://<container>@<account>.dfs.core.windows.net/<path>" --format ICEBERG
```

The schema, partition fields, and active data file count should be identical across formats. A
differing file count means a target's write and its own reader disagree.

This sequence has been run against Azurite with a delta-rs-written source table, syncing to Iceberg
and Hudi. It has not been run against a real Azure account — see
[Set up an Azure test environment](azure-test-environment.md).

## WebAssembly

Azure is excluded from the `js/wasm` build behind `//go:build !js`, the same as S3 and Glue.
`pkg/io/azure_js.go` provides the stub that build target links instead: `NewAzureStorage` there
always returns `ErrAzureUnsupported`, so an Azure path in a `polytable-wasm` build fails with a
clear, named error rather than a missing symbol or a silent no-op. `AzureOptions` is duplicated in
that file with the same fields (minus `CustomHTTPClient`, which has no meaning without a real
transport), so code that builds option functions compiles identically on both targets.
`GOOS=js GOARCH=wasm go list -deps ./cmd/polytable-wasm` reports zero Azure SDK packages, keeping
the Azure Identity SDK and its MSAL dependency out of the browser bundle entirely.

## Troubleshooting

- **`invalid storage path: no storage backend for scheme "..."`.** The path's scheme is not one
  polytable recognizes at all, or it recognizes it for path arithmetic but has no client for it
  (`gs://`, `hdfs://`). Check the scheme is spelled `abfss://`, `abfs://`, `wasbs://`, or
  `wasb://`; polytable never falls back to treating an unrecognized scheme as a local path,
  because that would silently create a literal directory named after the scheme.
- **A 403 from OneLake.** OneLake's connection guide states that it accepts tokens in the
  `Storage` audience only. A token acquired for the wrong audience — for example the default
  Microsoft Graph audience some flows request — is rejected even though the credential itself is
  valid. `auth: entra` requests `https://storage.azure.com/.default` by default
  (`DefaultOneLakeScope` in `pkg/catalog/rest_auth.go`) for exactly this reason; if a custom
  `scope` was set, confirm it also requests the `Storage` audience.
- **A long sync fails partway through with an authentication error.** A static SAS token or
  bearer token does not renew itself, and `IcebergRESTCatalogClient`'s static-token path
  (`PropCatalogToken`) presents the same string for the whole run — if the sync outlives the
  token's lifetime, later requests fail while earlier ones succeeded. `auth: entra` on a catalog
  entry replaces the static token with `entraTransport` (`pkg/catalog/entra.go`), which refreshes
  the token whenever it is within five minutes of expiring, so a run of any length keeps a valid
  token on every request. There is no equivalent expiring-credential problem for blob storage
  access itself: `DefaultAzureCredential` already refreshes its own tokens.
- **Every container-backed test fails at once with `connection reset by peer` on a published
  port.** Suspect the Docker daemon before the code. Docker Desktop can reach a state where it
  accepts the TCP connection and resets every HTTP request, while the container answers correctly
  on its own network — reachable by running a client container with
  `--network container:<name>`. The give-away is that unrelated suites fail identically. Restart
  Docker Desktop.
- **A `500 CommunicationError` from OneLake's Iceberg REST endpoint.** This usually means the
  `warehouse` property names a workspace or item that does not exist, not that the service is
  broken: OneLake answers an unknown warehouse with a `500`, not a `404`. Check the warehouse
  against the portal — it is `<WorkspaceID>/<DataItemID>` or
  `<WorkspaceName>/<DataItemName>.<DataItemType>`. A wrong *token audience* fails differently, with
  a `401`.
- **A tool or proxy rejects a `dfs.fabric.microsoft.com` or `blob.fabric.microsoft.com` URL.**
  Microsoft's documentation notes that some tools validate storage URLs against an allowed list
  that includes `dfs.core.windows.net` and rejects anything else, including OneLake's own
  hostnames. This is a property of the other tool's URL validation, not of polytable — if a
  proxy, firewall rule, or SDK-level URL check sits between polytable and OneLake, its allowed
  list needs the `fabric.microsoft.com` hosts added explicitly.

## What's next

- [Cloud storage](cloud-storage.md) for the quick path, S3, and the full list of supported
  schemes.
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) for the complete `auth`/`token`/`scope`
  property reference.
- [Features and limitations](features-and-limitations.md) for the honest, dated summary of what
  is and is not verified.
- [Set up an Azure test environment](azure-test-environment.md) to provision a disposable
  subscription sandbox and close the remaining gaps.
