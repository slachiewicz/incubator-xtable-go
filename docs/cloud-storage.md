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

# Cloud storage

polytable reads a table's source metadata from storage and writes each target
format's metadata back next to it, in the same location. The storage backend is
selected from the URI scheme of the dataset's `tableBasePath` value. This page
lists the supported schemes, explains how to configure Amazon S3,
S3-compatible object stores, and Azure Data Lake Storage, and states plainly
which stores have no backend.
For a complete first sync, see
[Create your first interoperable table](how-to.md).

## Supported storage schemes

polytable supports the following path forms:

- **A plain local path** (`/data/tables/people`): the local filesystem.
- **`file://`**: also the local filesystem, for configurations and metadata
  that record paths as URIs.
- **`s3://` and `s3a://`**: Amazon S3 or an S3-compatible object store. The two
  spellings name the same store and are accepted interchangeably.
- **`abfss://`, `abfs://`, `wasbs://`, and `wasb://`**: Azure Data Lake
  Storage and Microsoft Fabric OneLake. The four spellings match the ones
  Hadoop's ABFS driver accepts; foreign metadata carries whichever one the
  writing engine was configured with.
- **`mem://`**: an in-memory store for tests. Nothing is persisted.

A path with any other scheme is rejected before the sync starts; see
[Google Cloud Storage](#google-cloud-storage).

## Sync a table in Amazon S3

To sync a table that lives in S3, point the dataset's `tableBasePath` key at an
`s3://` URI:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://example-bucket/tables/people
    tableName: people
```

### Credentials

polytable discovers credentials through the standard AWS credential chain of
the AWS SDK for Go — the same chain that the AWS CLI uses. There is no
polytable-specific credential configuration. The chain includes the following
sources:

- Environment variables such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  and `AWS_SESSION_TOKEN`.
- The shared config and credentials files in the `~/.aws/` directory, including
  named profiles selected with the `AWS_PROFILE` environment variable.
- IAM Identity Center (SSO) sessions created with `aws sso login`.
- The IAM role of the environment polytable runs in: an EC2 instance profile,
  an ECS task role, or an EKS pod identity.

If the AWS CLI can list the bucket from the same environment, polytable can
too.

### Region

The AWS SDK resolves the region from the environment (`AWS_REGION`) or from the
active profile in the shared config file. To pin the region for one dataset
regardless of the environment, set the `storage.region` key; it takes
precedence over the environment and the config file.

### IAM permissions

A sync touches only the table's own prefix. Grant the identity that runs
polytable permission to do the following:

- List and read objects under the table's base path, to read the source
  metadata and, for some sources, Parquet file footers.
- Write objects under the same base path. Each target's metadata directory —
  for example `metadata/` for Iceberg, `_delta_log/` for Delta Lake, or
  `.hoodie/` for Hudi — is written next to the data. There is no separate
  output bucket or output path to grant.

The sync path lists, reads, and writes objects; it doesn't delete them, and it
never rewrites or moves data files.

## S3-compatible object stores

For an object store that speaks the S3 API on a custom endpoint, add a
`storage` block to the dataset. The following configuration syncs a table in
MinIO:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://warehouse/people
    tableName: people
    storage:
      region: us-east-1
      endpoint: http://localhost:9000
      usePathStyle: true
```

This MinIO configuration is exercised against a real MinIO container by the
repository's integration suite (`make test-containers`). The same keys apply
to other S3-compatible stores, such as Ceph RGW or Cloudflare R2, but no tests
exercise those stores — treat them as unverified.

## Sync a table in Azure

This section covers the quick path — the scheme, a minimal config, and the
credential order. For URI shapes in full (including the OneLake mapping and
its GUID workspace form), every credential source in the Entra ID chain, all
four endpoint-override cases, worked configs for workload identity, a shared
key, OneLake, and Azurite, catalog support, and Azure-specific
troubleshooting, see [Azure Data Lake Storage and OneLake](azure.md).

To sync a table in Azure Data Lake Storage, point the dataset's
`tableBasePath` key at an `abfss://` URI. The container comes first, then the
storage account host after an `@`, then the path within the container:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: abfss://example-container@myaccount.dfs.core.windows.net/tables/people
    tableName: people
```

For a table in Microsoft Fabric OneLake, the container is the workspace and
the item path follows it, for example
`abfss://myworkspace@onelake.dfs.fabric.microsoft.com/mylake.Lakehouse/Tables/sales`.
`abfs://`, `wasbs://`, and `wasb://` address the same backend and are accepted
too, since foreign metadata may record any of the four.

### Credentials

polytable selects Azure credentials in the following order, using the first
one that is configured:

1. A SAS token, from the `AZURE_STORAGE_SAS_TOKEN` environment variable.
2. A shared account key, from the `AZURE_STORAGE_KEY` environment variable.
3. Anonymous access, if the dataset's `storage.azure.anonymous` key is set to
   `true`, for a public container.
4. The Entra ID default credential chain (`DefaultAzureCredential`), which
   covers workload identity, managed identity, an environment service
   principal, and the Azure CLI, in that order.

Credentials are never a configuration-file field. `storage.azure` carries
only `endpoint`, `accountName`, and `anonymous`; a SAS token or an account key
must reach polytable through the environment instead, because a config file
gets committed, logged, and POSTed to the REST service.

### The blob service endpoint

polytable derives the blob service URL from the `abfss://` host by swapping
the first `.dfs.` for `.blob.`. OneLake documents both endpoints and states
that its blob endpoint carries the same compatibility as its ADLS endpoint, so
the derivation matches the documented pair. No request from polytable has
reached a Fabric workspace yet.

Set `storage.azure.endpoint` to override the derived URL. Four cases need it:

- Azurite, whose blob host isn't derivable this way.
- A OneLake regional endpoint,
  `https://<region>-onelake.dfs.fabric.microsoft.com`. Use it rather than the
  global endpoint when data residency matters: resolving the global endpoint
  for a workspace in another region can take data across a region boundary.
- A workspace private-link FQDN.
- The `api.onelake.fabric.microsoft.com` form, which contains neither `.dfs.`
  nor `.blob.` and so passes through the swap unchanged.

### WebAssembly

The Azure backend is excluded from the WebAssembly build (`//go:build !js`),
the same as the S3 and Glue backends. An Azure path in that build returns
`ErrAzureUnsupported`.

End-to-end coverage against Azurite exists as
`test/dockertest_azurite_test.go`, run by `make test-containers`, but it has
not been run in this environment because no Docker daemon was available.

## Google Cloud Storage

Google Cloud Storage (`gs://`) has no storage backend in polytable. A
`gs://` path fails at storage selection with an error that names the
unsupported scheme and lists the supported ones. The failure is deliberate:
falling through to local storage would misread `gs://bucket/table` as a
relative directory and create a literal `gs:` directory on the first write.

For a table that lives in GCS, use the upstream Java implementation, Apache
XTable — see its
[Creating your first interoperable table](https://xtable.apache.org/docs/how-to)
guide, which covers that store.

## Storage configuration reference

The `storage` block is set per dataset. Its top-level keys apply only to
`s3://` and `s3a://` paths:

- **`region`**: the AWS region for S3 requests, for example `us-west-2`. Takes
  precedence over the region from the environment and the shared config file.
- **`endpoint`**: a custom S3 endpoint URL, for example `http://localhost:9000`
  for MinIO. When unset, requests go to the standard AWS endpoints.
- **`usePathStyle`**: set to `true` to address buckets path-style
  (`endpoint/bucket/key`) instead of the default virtual-hosted style
  (`bucket.endpoint/key`). MinIO requires it in its default configuration. This key
  takes effect only when the `endpoint` key is also set.

A nested `storage.azure` block applies to `abfss://`, `abfs://`, `wasbs://`,
and `wasb://` paths. It has the following keys:

- **`endpoint`**: overrides the blob service URL that polytable derives from
  the path's host. Azurite needs it, and so does any deployment whose blob
  host isn't derivable from its `abfss://` host.
- **`accountName`**: overrides the storage account parsed from the path's
  host. Azurite needs it: its service URL carries the account in the path
  rather than the host.
- **`anonymous`**: set to `true` to read without credentials, for a public
  container.

The block holds no credential fields — a SAS token or a shared account key
reaches polytable only through the `AZURE_STORAGE_SAS_TOKEN` and
`AZURE_STORAGE_KEY` environment variables; see Credentials under
[Sync a table in Azure](#sync-a-table-in-azure) above.

## What's next

- [Create your first interoperable table](how-to.md), including a local
  end-to-end walkthrough.
- [Sync to the AWS Glue Data Catalog](glue-catalog.md) to register the synced
  table in Glue or discover tables to sync from it.
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) such as Nessie,
  Polaris, or Unity Catalog.
- [Azure Data Lake Storage and OneLake](azure.md) for the extended Azure
  reference: full credential chain, endpoint derivation, worked configs, and
  Azurite.
