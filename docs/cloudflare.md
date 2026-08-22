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

# Cloudflare R2 and R2 Data Catalog

Cloudflare R2 reaches polytable through two existing code paths rather than a new one: R2's data
plane is S3-compatible, so it is served by `pkg/io/s3.go`, the same backend documented in
[Cloud storage](cloud-storage.md#s3-compatible-object-stores); and R2 Data Catalog speaks the
Iceberg REST catalog protocol, so it is served by the client documented in
[Sync to an Iceberg REST catalog](iceberg-rest-catalog.md), the same one used for Nessie, Polaris,
and Unity Catalog. This page is the reference and the setup recipe together, because Cloudflare's
surface here is small enough that splitting it the way
[Azure](azure.md) and [AWS](aws.md) split theirs, into an extended reference plus a separate
test-environment recipe, would mean two thin pages instead of one useful one.

## Status

**R2 Data Catalog is a managed Iceberg REST catalog built into an R2 bucket, and it is in open
beta** — `wrangler`'s own command output says so. Against a live account, polytable's
`IcebergRESTConversionSource` negotiated the catalog's path prefix and listed namespaces
successfully. That account's catalog was empty, so the listing returned zero tables — that is the
honest limit of what has been exercised. **No table has been read from, or written to, R2 Data
Catalog through polytable.** R2's storage side needs no separate verification beyond what
[Cloud storage](cloud-storage.md#s3-compatible-object-stores) already states: it is one more
S3-compatible endpoint, in the same unverified category as Ceph RGW.

## Enable R2 Data Catalog on a bucket

Enable, inspect, and disable the catalog with `wrangler`:

```shell
npx wrangler r2 bucket catalog enable <bucket>
npx wrangler r2 bucket catalog get <bucket>
npx wrangler r2 bucket catalog disable <bucket>
```

`wrangler` also has `compaction` and `snapshot-expiration` subcommands under `r2 bucket catalog`
for the catalog's maintenance jobs; polytable does not call either.

Enabling the catalog prints its URI and its warehouse identifier, in the following shapes:

```
Catalog URI: https://catalog.cloudflarestorage.com/<account_id>/<bucket>
Warehouse:   <account_id>_<bucket>
```

The warehouse joins the account ID and the bucket name with an underscore, not a slash.

## Authenticate with an R2 API token

R2 Data Catalog authenticates with a Cloudflare R2 API token, presented as a plain bearer token.
That is exactly polytable's existing static `properties.token` field on a catalog entry — unlike
Polaris, which needs OAuth2, or AWS's own Iceberg REST endpoints, which need SigV4, R2 Data
Catalog needs no new authentication code at all.

Create the token from the dashboard: **R2 Object Storage → Account details → API Tokens → Manage →
Create API token**.

**A Data Catalog token cannot be scoped to a single bucket.** Catalog access requires
account-level `Admin Read & Write` or `Admin Read only` permission; object-level R2 permissions do
not grant it. State this to yourself before creating one: a token that can read the catalog can
read the catalog for every bucket in the account, including production buckets that have nothing
to do with this sync. For read-path testing, create an `Admin Read only` token. If write access is
wanted, prefer creating it in a separate Cloudflare account rather than granting
`Admin Read & Write` next to production data.

## Worked configuration

Point the catalog entry at the printed URI and warehouse, and the dataset's `tableBasePath` at the
same bucket over R2's S3-compatible endpoint:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://<bucket>/tables/people
    tableName: people
    storage:
      region: auto
      endpoint: https://<account_id>.r2.cloudflarestorage.com
    catalogs:
      - type: ICEBERG_REST
        uri: https://catalog.cloudflarestorage.com/<account_id>/<bucket>
        databaseName: analytics
        properties:
          warehouse: <account_id>_<bucket>
          token: <r2_api_token>
```

```shell
./bin/polytable sync --datasetConfig cloudflare.yaml
```

`databaseName` is the Iceberg namespace inside the catalog, not the bucket — it must already exist
under this warehouse before polytable syncs into it, the same rule as any other Iceberg REST
catalog (see [Resolve a source table from the catalog](iceberg-rest-catalog.md#resolve-a-source-table-from-the-catalog)
for the source-side equivalent). The R2 API token used for the S3-compatible endpoint's own
credentials (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, per the standard AWS credential chain
described in [Cloud storage](cloud-storage.md#credentials)) is a separate token from the catalog's
bearer token above — R2 lets an S3 API token be scoped to one bucket, which the catalog token
cannot be. Because the catalog token carries account-wide admin scope, treat a dataset config file
that embeds it as sensitive, the same way a committed AWS or Azure secret would be.

`usePathStyle` follows the same rule documented for MinIO: it only takes effect when `endpoint` is
also set. Whether R2 needs it is **unverified** — nothing in this repository has read or written R2
object storage, only its catalog. If the configuration above fails to address the bucket, set
`usePathStyle: true` and try again; that is the likelier of the two shapes for an endpoint that
carries the account in the host and the bucket in the path.

## What polytable does not use here

`GET /v1/config?warehouse=<warehouse>` against a live R2 Data Catalog returns an `overrides` block
carrying `s3.signer.uri`, and its `endpoints` list includes
`GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` — R2 vends per-table storage
credentials through the catalog itself, the way a fully catalog-integrated client would consume
them. **polytable does not call either.** It resolves storage configuration entirely separately
from the catalog, through the `storage` block shown above, regardless of which catalog backs a
sync. The consequence: configure R2's S3 credentials yourself, as static AWS-shaped credentials in
the environment; do not expect the catalog to hand polytable a scoped, short-lived credential for
you.

The same response's `overrides.prefix` is a UUID, not the bucket name — polytable's prefix
negotiation (`pkg/catalog/rest_config.go`) treats this the same as any other catalog's prefix,
opaque and carried on every later request. `defaults` was empty on the account checked; `overrides`
is what carried the real prefix.

## Teardown

Undo setup in the reverse order, so nothing is left reachable after the last step:

```shell
npx wrangler r2 bucket catalog disable <bucket>
```

Then delete the R2 API token from the dashboard (**R2 Object Storage → Account details →
API Tokens**), and delete the bucket itself if it existed only for this test.

## Troubleshooting

- **`401 Unauthorized: Missing Authorization header`.** The catalog endpoint returns this to any
  request with no `Authorization` header at all — confirmed against a live account. It does not by
  itself distinguish a missing token from a wrong one; check that `properties.token` is set and
  that the value reached the process (a templated dataset config with an unset variable silently
  producing an empty string is the usual cause).
- **A 403 where a token-scope problem is likely.** Given the account-scope rule above, a token
  created with only object-level or single-bucket permissions is the first thing to check — it
  will authenticate as a token but be refused by the catalog regardless of which bucket the
  request names.
- **An unknown warehouse's behavior is untested here.** Polaris answers a typo'd warehouse with a
  `404` and OneLake with a `500` (see
  [Azure and OneLake troubleshooting](azure.md#troubleshooting)); no request against R2 Data
  Catalog with a wrong warehouse has been made, so which status it returns is not established by
  this page.

## What's next

- [Cloud storage](cloud-storage.md) for the full list of supported schemes and the
  S3-compatible-store credential and endpoint keys used above.
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) for the complete `auth` / `token` /
  `warehouse` property reference shared by every REST catalog polytable talks to.
- [Features and limitations](features-and-limitations.md) for the honest, dated summary of what is
  and is not verified.
