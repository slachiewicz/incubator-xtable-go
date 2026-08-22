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
lists the supported schemes, explains how to configure Amazon S3 and
S3-compatible object stores, and states plainly which stores have no backend.
For a complete first sync, see
[Create your first interoperable table](how-to.md).

## Supported storage schemes

polytable supports the following path forms:

- **A plain local path** (`/data/tables/people`): the local filesystem.
- **`file://`**: also the local filesystem, for configurations and metadata
  that record paths as URIs.
- **`s3://` and `s3a://`**: Amazon S3 or an S3-compatible object store. The two
  spellings name the same store and are accepted interchangeably.
- **`mem://`**: an in-memory store for tests. Nothing is persisted.

A path with any other scheme is rejected before the sync starts; see
[Google Cloud Storage and Azure](#google-cloud-storage-and-azure).

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

## Google Cloud Storage and Azure

Google Cloud Storage (`gs://`) and Azure Data Lake Storage, including OneLake
(`abfss://`), have no storage backend in polytable. A `gs://` or `abfss://`
path fails at storage selection with an error that names the unsupported
scheme and lists the supported ones. The failure is deliberate: falling
through to local storage would misread `gs://bucket/table` as a relative
directory and create a literal `gs:` directory on the first write.

For a table that lives in GCS or ADLS, use the upstream Java implementation,
Apache XTable — see its
[Creating your first interoperable table](https://xtable.apache.org/docs/how-to)
guide, which covers both stores.

Interest in Microsoft OneLake and Fabric is tracked upstream in
[apache/incubator-xtable#810](https://github.com/apache/incubator-xtable/issues/810).
Any future OneLake read path in polytable would go through the
Iceberg REST-compatible endpoint — see
[Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) — but nothing is
implemented.

## Storage configuration reference

The `storage` block is set per dataset and applies only to `s3://` and
`s3a://` paths. It has the following keys:

- **`region`**: the AWS region for S3 requests, for example `us-west-2`. Takes
  precedence over the region from the environment and the shared config file.
- **`endpoint`**: a custom S3 endpoint URL, for example `http://localhost:9000`
  for MinIO. When unset, requests go to the standard AWS endpoints.
- **`usePathStyle`**: set to `true` to address buckets path-style
  (`endpoint/bucket/key`) instead of the default virtual-hosted style
  (`bucket.endpoint/key`). MinIO requires it in its default configuration. This key
  takes effect only when the `endpoint` key is also set.

## What's next

- [Create your first interoperable table](how-to.md), including a local
  end-to-end walkthrough.
- [Sync to the AWS Glue Data Catalog](glue-catalog.md) to register the synced
  table in Glue or discover tables to sync from it.
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) such as Nessie,
  Polaris, or Unity Catalog.
