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

# Amazon S3 and AWS Glue

This is the extended reference for polytable's AWS backend: URI shapes, the credential chain in
full, region and endpoint resolution, worked dataset configurations, the Glue Data Catalog, AWS's
Iceberg REST endpoints, and IAM. For the short version — the scheme, a minimal config, and the
credential order — see [Sync a table in Amazon S3](cloud-storage.md#sync-a-table-in-amazon-s3) in
[Cloud storage](cloud-storage.md); this page goes deeper into the same material rather than
repeating it. The Glue registration walkthrough — table discovery, source resolution, and the
exact `catalogs` block — lives in [Sync to the AWS Glue Data Catalog](glue-catalog.md); this page
cross-links to it rather than duplicating it.

## Status

Amazon S3 is implemented in `pkg/io/s3.go`: `S3Storage`, built on the AWS SDK for Go v2, with
credential discovery, region resolution, and an optional custom endpoint for S3-compatible stores.
It is exercised on every CI run: `test/dockertest_minio_matrix_test.go` starts a real MinIO
container and drives Delta, Iceberg, and Hudi conversions through it, and `.github/workflows/integration.yml`
runs `go test -race -v -count=1 ./test/...` — without `-short`, so the MinIO matrix runs rather
than being skipped — on every push and pull request against `main`.

**The AWS Glue Data Catalog client and partition sync have only ever been tested against fakes.**
`pkg/catalog/glue.go`, `glue_conversion.go`, and `glue_partition.go` are covered by unit tests that
drive an in-memory `glue.Client` substitute; no run in this repository has reached a real Glue
catalog. `docs/improvement-plan.md` T15 is titled
"⚠️ LANDED, NOT VERIFIED AGAINST A REAL GLUE CATALOG" and states plainly that "every test drives a
fake" and that, until a real Glue catalog (or LocalStack) has registered a partitioned table and an
engine has resolved partitions through it, "the claim is untested end to end."

This is a real asymmetry with the Azure backend, which now has a live nightly lane against a real
ADLS Gen2 account (see [Azure Data Lake Storage and OneLake](azure.md#status)) — AWS has no
equivalent workflow for Glue; `.github/workflows/` has no `aws-live.yml` alongside its
`azure-live.yml`.

## URI shapes

polytable recognizes two URI schemes for S3, both parsed by the same `ParseS3URI`
(`pkg/io/s3.go`):

- `s3://<bucket>/<key>` — the standard scheme.
- `s3a://<bucket>/<key>` — the scheme Hadoop's S3A connector uses.

Both name the same object store and are accepted interchangeably: `RelativizePath`
(`pkg/io/storage.go`) strips the scheme from both a table's base path and a physical path before
comparing them, specifically so that a base path recorded as `s3://` matches a data file whose
metadata carries `s3a://`, or the reverse — a mismatch that upstream engines produce depending on
which Hadoop connector wrote the table. Both schemes are also members of `uriSchemes`
(`pkg/io/storage.go`), the list `JoinPath` and `TrimScheme` iterate, independently of whether a
storage backend exists for a scheme.

## Credentials

`NewS3Storage` (`pkg/io/s3.go`) does not select a credential itself. It calls
`awsconfig.LoadDefaultConfig(ctx, ...)` from the AWS SDK for Go v2, the same resolution the AWS CLI
and every other AWS SDK use, and lets it decide. The chain tries, in order:

1. **Environment variables** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
   `AWS_SESSION_TOKEN` for temporary credentials.
2. **The shared configuration and credentials files** — `~/.aws/config` and `~/.aws/credentials`,
   including a named profile selected with the `AWS_PROFILE` environment variable.
3. **IAM Identity Center (SSO)** — a session created with `aws sso login`, resolved through the
   profile's `sso_session` configuration.
4. **Container credentials** — the task role injected into an ECS task or an EKS pod running with
   IAM roles for service accounts (IRSA), read from the URI in
   `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or `AWS_CONTAINER_CREDENTIALS_FULL_URI`.
5. **EC2 instance metadata (IMDS)** — the instance profile role of the EC2 instance polytable runs
   on, fetched from the metadata service.

**`S3Options` (`pkg/io/s3.go`) has no credential fields at all** — only `Region`, `Endpoint`,
`UsePathStyle`, and `CustomHTTPClient`. This is deliberate and mirrors the Azure backend
(`AzureOptions` holds no secret either; see
[Credentials](azure.md#authentication) in the Azure reference): a dataset config file gets
committed, logged, and POSTed to the REST service, and a secret sitting in any of those places is a
leak waiting to happen. There is no `storage.accessKeyId`/`storage.secretAccessKey` key to set and
no code path that reads one — the only way to hand polytable an AWS credential is through the
chain above, in the environment or the runtime identity polytable executes under.

If the AWS CLI can reach the bucket from the same environment, polytable can too, since both
resolve credentials the same way.

## Region and endpoint

The AWS SDK resolves the region from `configFns` built from `S3Options.Region` if set, then falls
through to its own resolution — the `AWS_REGION` (or `AWS_DEFAULT_REGION`) environment variable, or
the active profile's `region` key in the shared config file. Setting the dataset's `storage.region`
key (`StorageConfig.Region` in `pkg/conversion/config.go`) takes precedence over both, since
`ToOptionFuncs` only appends the region option when the key is non-empty, and `NewS3Storage` passes
it to `awsconfig.WithRegion` ahead of the SDK's own resolution.

`storage.endpoint` (`StorageConfig.Endpoint`) is needed for any S3-compatible store that isn't AWS
itself — MinIO, Ceph RGW, Cloudflare R2 — since there is no other way to point the client at a
non-AWS host.

**`usePathStyle` takes effect only when `endpoint` is also set.** In `NewS3Storage`, the block that
applies `UsePathStyle` is nested inside `if opts.Endpoint != ""`:

```go
if opts.Endpoint != "" {
    s3OptFns = append(s3OptFns, func(o *s3.Options) {
        o.BaseEndpoint = aws.String(opts.Endpoint)
        o.UsePathStyle = opts.UsePathStyle
    })
}
```

Setting `storage.usePathStyle: true` with no `storage.endpoint` is silently inert: the option
function that would apply it is never appended, and the request goes out virtual-hosted style
against the standard AWS endpoint regardless. `docs/cloud-storage.md` already documents this trap
for the short path; it holds here too.

## Worked configurations

Each of the following is a complete dataset config plus the `polytable sync` invocation that uses
it. Save the YAML to a file and pass its path with `--datasetConfig` (`-c`).

### S3 with an instance profile or SSO

No `storage` block is needed: with no region override, `LoadDefaultConfig` resolves the region from
the environment or the shared config file, and the identity comes from whichever credential source
in the chain applies — an EC2 instance profile, an ECS task role, an EKS pod identity, or an active
`aws sso login` session.

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://example-bucket/tables/people
    tableName: people
```

```shell
./bin/polytable sync --datasetConfig s3-default-chain.yaml
```

### S3 with a named profile

Point `AWS_PROFILE` at a profile in `~/.aws/config` before running polytable; the config carries no
credential of its own:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://example-bucket/tables/people
    tableName: people
    storage:
      region: us-west-2
```

```shell
export AWS_PROFILE=my-profile
./bin/polytable sync --datasetConfig s3-named-profile.yaml
```

### MinIO or another S3-compatible store

`storage.endpoint` and `storage.usePathStyle` are both required together — path-style addressing is
what lets a bucket-per-path host like MinIO resolve the bucket from the URL path instead of a
virtual-hosted subdomain. Credentials still come from the environment; MinIO's static access
key/secret pair is presented to the AWS SDK exactly as an AWS access key/secret pair would be:

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

```shell
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=<your-minio-password>
./bin/polytable sync --datasetConfig minio-local.yaml
```

This shape is exercised against a real MinIO container by `test/dockertest_minio_matrix_test.go`.
The same keys apply to Ceph RGW or Cloudflare R2, but those stores are not covered by this
repository's tests — treat them as unverified.

### S3 plus Glue registration

Add a `catalogs` block to register the synced table. `polytable_synced_time` and `table_type` (or
`spark.sql.sources.provider` for Delta) are written automatically by `GlueCatalogSyncClient`; there
is nothing catalog-specific to add to `storage` — Glue authenticates through the same
`LoadDefaultConfig` chain as S3 itself (`pkg/catalog/glue.go`):

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://example-bucket/tables/people
    tableName: people
    catalogs:
      - type: AWS_GLUE
        databaseName: polytable_synced_db
```

```shell
./bin/polytable sync --datasetConfig s3-glue.yaml
```

See [Sync to the AWS Glue Data Catalog](glue-catalog.md) for the full registration, discovery, and
source-resolution walkthrough, including the `aws glue create-database` prerequisite.

## Glue

`GlueCatalogSyncClient` (`pkg/catalog/glue.go`) implements `SyncClient`:

- **`CreateOrUpdateTable`** calls `GetTable` first; an `EntityNotFoundException` means the table
  doesn't exist yet, so it calls `CreateTable`, otherwise it calls `UpdateTable`. The table
  definition it writes (`buildTableInput`) carries the schema as Glue columns (partition fields
  excluded from the regular column list and listed separately as `PartitionKeys`), a Hive
  input/output format and SerDe appropriate to the target format (Parquet-backed for Delta and
  Hudi, the Iceberg Hive SerDe for Iceberg), `StorageDescriptor.Location` set to the table's base
  path, and two parameters common to every format: `EXTERNAL=TRUE` and `polytable_synced_time` (a
  Unix millisecond timestamp). Beyond those two, the format-specific parameters differ: Delta
  writes both `table_type=DELTA` and `spark.sql.sources.provider=delta`; Hudi writes only
  `spark.sql.sources.provider=hudi` (no `table_type`); Iceberg writes only `table_type=ICEBERG`.
  This matters for discovery, covered next: `TableFormatFromProperties` reads `table_type` first
  and falls back to `spark.sql.sources.provider`, so a Hudi table is resolved by the fallback key
  alone.
- **`DropTable`** calls `DeleteTable` against the configured database, or an explicit database
  passed to the call.
- **Partition sync** happens automatically after `CreateOrUpdateTable` succeeds, driven from
  `pkg/conversion/controller.go`'s `syncPartitions`: it type-asserts the catalog client to
  `PartitionSyncOperations` (Glue satisfies it via the embedded `GluePartitionSyncOperations` in
  `pkg/catalog/glue_partition.go`) and reconciles partitions only when the synced table actually has
  partitioning fields — an unpartitioned table is left alone rather than having its (nonexistent)
  partitions "reconciled" to empty. Partition batches are capped by AWS Glue's own service limits
  (100 per `BatchCreatePartition` call, 25 per `BatchDeletePartition` call) and by
  `catalogs[].maxPartitionsPerRequest`, which defaults to `DefaultMaxPartitionsPerRequest` (1000).
  Iceberg REST catalogs don't implement `PartitionSyncOperations` at all — Iceberg tracks
  partitioning in its own metadata — so this step is a no-op for them.

**Discovery** (`--catalog glue --database <name>`, `cmd/polytable/main.go`) scans a Glue database
instead of reading a config file. `parseCatalogTypeFlag` accepts only `"glue"` (or `AWS_GLUE`)
today, so this is Glue-only. A table opts in by carrying the `polytable_target_formats` table
property (`catalog.PropTargetFormats` in `pkg/catalog/conversion.go`), a comma-separated list of
target formats such as `ICEBERG,HUDI`; `TableFilter.RequireConversionMarkers` is what discovery uses
to skip every table that doesn't carry it. `TableFormatFromProperties` resolves a table's own format
by reading `table_type` first, falling back to `spark.sql.sources.provider` — the same two keys
Java XTable reads, so a table registered by either implementation resolves identically. `--catalogId`
selects another account's catalog; `--dry-run`, `--mode`, `--output`, and `--timeout` behave the
same as with a config file.

**Known limitation:** a catalog entry registers every target format under the same `tableName`.
Syncing several target formats to one Glue-registered dataset means each successful registration
overwrites the previous one — see
[Features and limitations](features-and-limitations.md#catalogs) and
[Sync to the AWS Glue Data Catalog](glue-catalog.md#register-a-synced-table-in-glue). Use one target
format per catalog-registered dataset until per-target catalog identifiers exist (`docs/improvement-plan.md`
T49).

## Iceberg REST on AWS

Both the AWS Glue Data Catalog and Amazon S3 Tables expose an Iceberg REST endpoint of their own,
separate from the native Glue API `GlueCatalogSyncClient` speaks:

- **AWS Glue's Iceberg REST endpoint** is `https://glue.<region>.amazonaws.com/iceberg` (for
  example `https://glue.us-east-1.amazonaws.com/iceberg`), documented at
  [Connecting to the Data Catalog using AWS Glue Iceberg REST endpoint](https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html).
  Its `warehouse` is the AWS account ID that owns the catalog (or a `/`-separated nested-catalog
  path for a non-default catalog), and it authenticates requests with AWS Signature Version 4
  (SigV4) — AWS's own example Spark configuration sets
  `spark.sql.catalog.<name>.rest.sigv4-enabled=true` and
  `spark.sql.catalog.<name>.rest.signing-name=glue`.
- **Amazon S3 Tables' Iceberg REST endpoint** is `https://s3tables.<region>.amazonaws.com/iceberg`,
  documented at
  [Accessing tables using the Amazon S3 Tables Iceberg REST endpoint](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html).
  Its `warehouse` is the table bucket's ARN, and it also requires SigV4-signed requests.

polytable would reach either endpoint the same way it reaches Nessie, Polaris, or Unity Catalog: a
`catalogs` entry with `type: ICEBERG_REST`, `uri` set to the endpoint, and `properties.warehouse`
set to the account ID or bucket ARN. `IcebergRESTCatalogClient` (`pkg/catalog/rest.go`) negotiates a
path prefix from every catalog it talks to by calling `GET /v1/config?warehouse=...`
(`pkg/catalog/rest_config.go`) and using the prefix the catalog returns for every later request —
mechanism that is generic, not AWS- or OneLake-specific.

**Authentication.** `restHTTPClient` (`pkg/catalog/rest_auth.go`) recognizes four `auth` modes: the
default, a static bearer token carried in `properties.token`; `entra`/`entra-id`/`azure` for
Microsoft Entra ID; `oauth2`/`oauth`/`client-credentials` for the Iceberg REST specification's own
OAuth2 client-credentials grant (Apache Polaris and Snowflake Open Catalog, which is Polaris); and
`sigv4`/`aws`, added in `a30d5b2`, which signs every request with AWS Signature Version 4. A
`sigv4`-authenticated request to Glue's Iceberg REST endpoint has been run against a real AWS
account: `ListTables` completed with no authentication error, and returned zero tables because the
account's Glue catalog had none registered.

**S3 Tables has since been run too, against a live table bucket in `eu-north-1`.** `ListTables`
negotiated the prefix, listed namespaces and discovered a table on the first attempt, with the
region and signing name (`s3tables`) derived from the endpoint host and no properties set beyond
`warehouse`.

That run is worth reading for what its `GET /v1/config` returns, because it is unlike Glue's:

```json
{"defaults":{"prefix":"arn%3Aaws%3As3tables%3A<region>%3A<account>%3Abucket%2F<name>", ...},
 "overrides":{}}
```

The prefix arrives under **`defaults`**, with `overrides` present but empty, and it is **already
percent-encoded** — the ARN's `:` and `/` are `%3A` and `%2F`. Those are precisely the two shapes
T58 fixed after finding them on Nessie and Lakekeeper. A client reading only `overrides` computes no
prefix here, and one escaping the prefix per segment turns every `%` into `%25`. Either alone breaks
S3 Tables, so AWS's own service was affected by bugs found on two unrelated catalogs.

Reading a table's **data** is a separate matter from authenticating to the catalog. S3 Tables stores
data in a managed bucket normally reached through short-lived, table-scoped credentials vended from
`GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`. polytable does not call that
endpoint — `pkg/io` resolves S3 credentials from its own chain — so as recorded under T64 you must
configure credentials for that bucket yourself. This was confirmed by reading the client rather than
by observing a denial, since the probe ran with account-root credentials.

Writing table data through `s3://` is unaffected by any of this — that goes through `S3Storage`,
never through the Iceberg REST catalog client.

## IAM

**For S3**, grant the identity polytable runs as the following actions, scoped to the table's bucket
and the table's own prefix:

- `s3:GetObject` and `s3:ListBucket` — to read the source table's metadata and, for some sources,
  Parquet file footers.
- `s3:PutObject` — to write each target's metadata directory next to the data (`metadata/` for
  Iceberg, `_delta_log/` for Delta Lake, `.hoodie/` for Hudi).
- `s3:DeleteObject` — polytable's own sync path does not delete objects, but a bucket's lifecycle
  tooling or a later polytable feature may need it; grant it only if your operational practice
  requires it.
- The multipart actions — `s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts` — are the
  standard companions to `s3:PutObject` in AWS's own example S3 bucket policies, covering a client
  that uploads large objects in parts. **`S3Storage.Write` (`pkg/io/s3.go`) calls a single
  `PutObject`, not the SDK's multipart upload manager**, so today's polytable code does not itself
  exercise these actions; include them if the policy also has to cover other tools writing into the
  same prefix, or as a forward-looking grant.

**For Glue**, a role also needs `glue:GetTable`, `glue:GetTables`, `glue:CreateTable`,
`glue:UpdateTable`, and `glue:DeleteTable` for table registration, plus `glue:GetPartitions`,
`glue:BatchCreatePartition`, `glue:BatchDeletePartition`, and `glue:UpdatePartition` if the synced
table is partitioned. **These are a separate service from S3: a role granting `s3:*` on the bucket
has no Glue permissions at all**, and a sync that only writes to S3 but also names a `catalogs`
block fails at the Glue call with an access-denied error that has nothing to do with the bucket
policy.

## WebAssembly

Both S3 and Glue are excluded from the `js/wasm` build behind `//go:build !js`. `pkg/io/s3_js.go`
provides the stub `NewS3Storage` that always returns `ErrS3Unsupported`, and
`pkg/catalog/glue_js.go` provides `NewGlueCatalogSyncClient` and `NewGlueConversionSource` stubs that
always return `ErrGlueUnsupported`. Both files duplicate the option/config struct shapes their
non-wasm counterparts expose (`S3Options` minus `CustomHTTPClient`, which has no meaning without a
real HTTP transport) so that code building option functions compiles identically on both targets. As
with Azure, this keeps the AWS SDK for Go — and its network and credential-file dependencies — out
of the browser bundle entirely: a browser sandbox has no AWS credential chain and no unrestricted
network egress to reach S3 or Glue.

## Troubleshooting

- **`invalid storage path: no storage backend for scheme "..."`.** The scheme isn't one polytable
  recognizes, or it recognizes it for path arithmetic but has no client for it (`gs://`, `hdfs://`).
  Check the scheme is spelled `s3://` or `s3a://`; polytable never falls back to treating an
  unrecognized scheme as a local path.
- **A 403 that is really a missing region.** `LoadDefaultConfig` can succeed with no region resolved
  at all if neither the environment, the profile, nor `storage.region` sets one, and some S3
  operations then fail with an authorization-shaped error rather than a clear "no region" message.
  Set `AWS_REGION` or the dataset's `storage.region` explicitly rather than assuming the SDK found
  one.
- **`usePathStyle: true` with no effect.** If requests still go out virtual-hosted style against a
  non-AWS endpoint, check that `storage.endpoint` is also set — see
  [Region and endpoint](#region-and-endpoint) above; the option is silently dropped without it.
- **Expired SSO credentials.** `LoadDefaultConfig` reads a cached SSO token but does not refresh an
  expired one; a sync that worked earlier in the day failing with a credential error usually means
  the session from `aws sso login` has expired. Run `aws sso login` again for the profile named by
  `AWS_PROFILE`.
- **A Glue `EntityNotFoundException` on the database, not the table.** `CreateOrUpdateTable` only
  handles a missing *table* — it treats that as "create it" — but a missing *database* surfaces the
  same exception type from a different call and is not handled specially: it means the database
  named in `catalogs[].databaseName` does not exist yet. Create it first with
  `aws glue create-database --database-input '{"Name":"<name>"}'`, as
  [Sync to the AWS Glue Data Catalog](glue-catalog.md#register-a-synced-table-in-glue) shows.

## What's next

- [Cloud storage](cloud-storage.md) for the quick path, the full scheme list, and Azure's short
  path.
- [Sync to the AWS Glue Data Catalog](glue-catalog.md) for the complete registration, discovery, and
  source-resolution walkthrough.
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) for Nessie, Polaris, Unity Catalog, and
  the full `auth`/`token`/`scope`/`warehouse` property reference.
- [Azure Data Lake Storage and OneLake](azure.md) for the equivalent extended reference on Azure,
  including the live nightly verification lane AWS does not yet have.
- [Features and limitations](features-and-limitations.md) for the honest, dated summary of what is
  and is not verified.
