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

# Snowflake Open Catalog

polytable can register an Iceberg target in Snowflake Open Catalog, because Open Catalog speaks
the same [Iceberg REST catalog protocol](iceberg-rest-catalog.md) as Nessie, Apache Polaris, Unity
Catalog, and R2 Data Catalog. This page is the reference and the setup recipe together, the same
shape as [Cloudflare R2 and R2 Data Catalog](cloudflare.md), because Open Catalog's surface here is
one account, one connection, and one catalog entry — not enough to justify splitting a reference
page from a separate test-environment page the way [Azure](azure.md) and
[Azure test environment](azure-test-environment.md) do.

## Status

**Nothing in this repository has ever connected to Snowflake.** Everything below comes from
Snowflake's own documentation, not from a request that reached a Snowflake account.

polytable does have code for the authentication Open Catalog requires: `restHTTPClient`
(`pkg/catalog/rest_auth.go`) now recognizes `auth: oauth2` (also spelled `oauth` or
`client-credentials`), which exchanges a client id and secret for a bearer token at the catalog's
`/v1/oauth/tokens` endpoint and refreshes it before it expires — this is the OAuth2
client-credentials mechanism the Iceberg REST specification itself defines, and the one both Apache
Polaris and Snowflake Open Catalog speak. That lands the OAuth2 half of **T59** in
`docs/improvement-plan.md` (its SigV4 half landed separately, for AWS's native endpoints).

**That code is verified only against `httptest` fakes standing in for a token endpoint**
(`pkg/catalog/oauth2_test.go` says so in its own file comment: "None of this reaches, or claims to
reach, a live Apache Polaris deployment or Snowflake Open Catalog: that leg is unverified.") No
request built by this code has reached a real Polaris container, let alone Snowflake. Treat
everything past this paragraph as the documented shape of an untested path, not a confirmed one.

## Open Catalog is not the Snowflake warehouse

The first thing to get right, because it is the thing a reader familiar with Snowflake gets wrong
first: **polytable talks to Snowflake Open Catalog, not to the Snowflake SQL data warehouse.** Open
Catalog is a separate account within Snowflake, with its own users, permissions, and connections —
even for an organization that already runs a Snowflake warehouse. Creating an Open Catalog account
does not reuse warehouse credentials, and a warehouse role has no standing in Open Catalog until
someone grants it one there explicitly.

**Snowflake Open Catalog is Apache Polaris.** Its endpoint path is literally `/polaris/api/catalog`.
That is not a coincidence of naming — it is the same server, so the same Iceberg REST protocol
polytable already implements against Polaris, Nessie, and the others applies unchanged. Practically,
this means a local Polaris container is a faithful, free rehearsal of what polytable would do
against a real Open Catalog account.

If instead you want to *query* a table polytable already wrote, through Snowflake SQL rather than
register it in Open Catalog, see the [Snowflake section of Query a synced table](query-engines.md#snowflake)
— that path goes through an external volume and a Snowflake-native catalog integration, and does not
involve Open Catalog at all.

## Rehearse against a local Polaris container first

Because Open Catalog is Polaris, standing up a local Polaris container exercises the same client
code path polytable would use against Snowflake, at no cost and with no account to create or tear
down — and, per [Status](#status) above, it is a rehearsal this repository has not actually run for
the OAuth2 path, so it is also the first place to find out whether that code works against real
Polaris at all. Apache Polaris publishes an image for exactly this:

```shell
docker run -p 8181:8181 -p 8182:8182 apache/polaris:latest
```

Port 8181 serves the REST API; 8182 serves metrics and health. Polaris's own
[quickstart](https://polaris.apache.org/releases/1.0.0/getting-started/quickstart/) and
[using Polaris](https://polaris.apache.org/releases/1.0.0/getting-started/using-polaris/) guides are
the authoritative source for the current bootstrap credentials and CLI invocation, since both have
changed between Polaris releases; the shape below is theirs, reproduced for orientation. With a root
`CLIENT_ID`/`CLIENT_SECRET` bootstrapped, create a catalog, a principal, a principal role, and a
catalog role carrying `CATALOG_MANAGE_CONTENT` — the same privilege Open Catalog asks for in
production:

```shell
./polaris --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  catalogs create --storage-type file quickstart_catalog

./polaris --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  principals create quickstart_user
./polaris --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  principal-roles create quickstart_user_role

./polaris --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  catalog-roles create --catalog quickstart_catalog quickstart_catalog_role
./polaris --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  privileges catalog grant --catalog quickstart_catalog \
  --catalog-role quickstart_catalog_role CATALOG_MANAGE_CONTENT

./polaris --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  principal-roles grant --principal quickstart_user quickstart_user_role
./polaris --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  catalog-roles grant --catalog quickstart_catalog \
  --principal-role quickstart_user_role quickstart_catalog_role
```

The resulting principal's client id and secret are what go into the `clientId` and
`clientSecretEnv`-named properties in [Worked configuration](#worked-configuration) below — a
Polaris rehearsal and a Snowflake account differ only in which credential and URI you plug in.

Note the path difference between the two: a self-hosted Polaris server serves `/api/catalog`, while
Snowflake Open Catalog serves the same routes under `/polaris/api/catalog`. Everything past that
path segment — `/v1/config`, `/v1/oauth/tokens`, `/v1/{prefix}/namespaces/...` — is identical.
`pkg/catalog/oauth2.go`'s own comments note that reaching a self-hosted Polaris container required
an extra `Polaris-Realm: POLARIS` header, confirmed against a local container and attachable through
the `header.<Name>` config property (`PropCatalogHeaderPrefix`, documented in
[Sync to an Iceberg REST catalog](iceberg-rest-catalog.md)). Whether Snowflake's hosted deployment
needs an equivalent header is not established here — it is exactly the kind of detail a Polaris
rehearsal cannot answer, since Snowflake's realm configuration is its own.

`pkg/catalog/rest_prefix_test.go` and `test/dockertest_iceberg_rest_test.go` cover the wider Iceberg
REST protocol against `httptest` fakes and the `tabulario/iceberg-rest` reference image; T58 in
`docs/improvement-plan.md` records a run against a live Apache Polaris container that found and
fixed four real conformance defects, though that run predates the OAuth2 client-credentials code and
did not exercise it. None of that exercise reached Snowflake's own deployment of Polaris, which may
not behave identically in every respect.

## Create the Open Catalog account

From Snowsight, in an existing Snowflake organization: **Admin → Accounts**, then the **+ Account**
drop-down, then **Create Snowflake Open Catalog Account**. The dialog asks for Cloud, Region, and
Edition, then Account Name, User Name, Password, and Email. Submitting it provisions a new,
billable Snowflake account dedicated to Open Catalog — see [Teardown](#teardown) for what "billable"
means in practice and how to stop it.

## Create the service connection and copy the credential

polytable authenticates as a service, not as the user created above, so the next step is a
connection. In the Open Catalog UI: **Connections** tab → **+ Connection**. The dialog offers
either a new principal role or an existing one; create a new one for a polytable sync so its access
can be scoped and revoked independently of anything else using the account.

Completing the dialog returns a credential shaped `<CLIENT_ID>:<CLIENT_SECRET>`. Snowflake's own
documentation is explicit that **you won't be able to retrieve these text strings from the Open
Catalog service later, so you must copy them now** — there is no "regenerate and see the old value"
option, only "regenerate and invalidate the old value." A reader who closes the dialog before
copying both halves has to create a new connection from scratch.

The principal role behind the connection needs a catalog role carrying **`CATALOG_MANAGE_CONTENT`**
on the catalog polytable will sync into — the same privilege granted in the Polaris rehearsal above.
That single privilege covers create, read, and write on tables, which is what a conversion sync
needs to register a table and later update it.

## Authenticate with OAuth2 client-credentials

Set `properties.auth` to `oauth2` and give it the two halves of the connection credential:

- **`clientId`**: the `<CLIENT_ID>` half. This is not sensitive on its own — an id without its
  matching secret cannot mint a token — so it is an ordinary config property
  (`PropCatalogOAuth2ClientID`).
- **`clientSecretEnv`**: the *name* of an environment variable holding the `<CLIENT_SECRET>` half,
  never the secret itself (`PropCatalogOAuth2ClientSecretEnv`). See
  [Where the client secret goes](#where-the-client-secret-goes) for why the config property names a
  variable instead of carrying a value.
- **`scope`** (optional): requested as `PRINCIPAL_ROLE:<principal_role_name>`, per Snowflake's own
  documentation of the connection's scope shape. Omit it and polytable sends no `scope` form field
  at all, since the OAuth2 client-credentials grant treats it as optional — a real Open Catalog
  connection likely rejects an unscoped request, so set it explicitly rather than relying on that
  default.
- **`oauth2TokenEndpoint`** (optional): overrides where the token request goes. Left unset, polytable
  derives `<catalog-uri>/v1/oauth/tokens`, which for Open Catalog's URI shape resolves to
  `https://<open_catalog_account_identifier>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens`
  — the standard Iceberg REST location, so this should not need overriding for Snowflake.

With these set, polytable fetches a token on the first request to the catalog and refetches it
roughly 30 seconds before it expires (`oauth2RefreshMargin` in `pkg/catalog/oauth2.go`), rather than
presenting one token until the catalog rejects it — the failure mode a hand-fetched static
`properties.token` has on any sync long enough to outlast the token's lifetime. As
[Status](#status) says, this refresh behavior is checked against `httptest` fakes only; whether it
holds up against Open Catalog's actual token lifetime and response shape is unconfirmed.

## Worked configuration

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://my-bucket/tables/people
    tableName: people
    catalogs:
      - type: ICEBERG_REST
        uri: https://<open_catalog_account_identifier>.snowflakecomputing.com/polaris/api/catalog
        databaseName: analytics
        properties:
          warehouse: <catalog_name>
          auth: oauth2
          clientId: <open_catalog_client_id>
          clientSecretEnv: SNOWFLAKE_OPEN_CATALOG_CLIENT_SECRET
          scope: PRINCIPAL_ROLE:<principal_role_name>
```

```shell
export SNOWFLAKE_OPEN_CATALOG_CLIENT_SECRET=<open_catalog_client_secret>
./bin/polytable sync --datasetConfig snowflake.yaml
```

`<open_catalog_account_identifier>` "might be the account locator by itself (for example,
`xy12345`) or include additional segments," in Snowflake's own words, depending on the account's
region and cloud — read it off the account's own connection details rather than guessing its shape.
`databaseName` is the Iceberg namespace inside the catalog, the same rule as every other REST
catalog documented in this repository (see
[Resolve a source table from the catalog](iceberg-rest-catalog.md#resolve-a-source-table-from-the-catalog)):
it must already exist under the named catalog before polytable syncs into it. `<open_catalog_client_id>`
is a placeholder for the `clientId` half only — the matching secret never appears in this file; it
lives in `SNOWFLAKE_OPEN_CATALOG_CLIENT_SECRET`, set in the shell that runs the sync, not in the
config.

## The warehouse is the catalog name, and it is case sensitive

This is the one property that differs from every other catalog documented in this repository, so
state it plainly: for Snowflake Open Catalog, **`properties.warehouse` is the catalog's name, and
matching it is case sensitive.** It is not the account, and not an ARN.

- AWS Glue's Iceberg REST endpoint takes the account id as its warehouse (see
  [Amazon S3 and AWS Glue](aws.md)).
- AWS S3 Tables takes the table bucket's ARN.
- Cloudflare R2 Data Catalog takes `<account_id>_<bucket>` (see
  [Cloudflare R2 and R2 Data Catalog](cloudflare.md)).
- Snowflake Open Catalog takes the catalog name you gave it when it was created — the same string
  Polaris calls a catalog, not an account or resource identifier at all — and a case mismatch fails
  where an AWS-style identifier would not have cared.

## Where the client secret goes

The `CLIENT_ID:CLIENT_SECRET` pair from [the connection step](#create-the-service-connection-and-copy-the-credential)
is a long-lived credential capable of minting bearer tokens for the principal role it names. That is
exactly why `clientSecretEnv` in the config above **names an environment variable rather than
carrying the secret**: `pkg/catalog/oauth2.go`'s own comment states the rule directly — "a dataset
config gets committed to git, logged, and POSTed to the REST service (the rule T51 and T55 settled
for Azure credentials), and an OAuth2 client secret is exactly the kind of long-lived credential
that rule exists to keep out of those places." The pattern mirrors `AzureOptions.AccountKeyEnv`
(see [Credentials](azure.md#authentication)): the config points at a variable, never at a value, so
the same committed file works across environments and machines, each supplying its own secret
through its own environment.

An unset or empty `clientSecretEnv`-named variable is an error naming both the property and the
variable, the same discipline `resolveAzureCredential` follows — not a silent fall-through to an
unauthenticated request.

## What's untested

To be direct about the boundary of what this page states versus what it verifies:

- No request from this repository has reached `*.snowflakecomputing.com` in any form — not account
  creation, not a connection, not a token exchange, not a catalog operation.
- The OAuth2 client-credentials code exists and is unit-tested, but only against `httptest` fakes
  standing in for a token endpoint. No test in this repository has run it against a live Apache
  Polaris container, let alone Snowflake Open Catalog.
- Whether Snowflake's deployment of Polaris returns the same `GET /v1/config` shape — the same
  `overrides.prefix`, the same `404` on a bad warehouse, the same `namespace-separator`, the same
  need (or lack of one) for a `Polaris-Realm`-style header — that a self-hosted Polaris container
  returns (per T58's findings) is not established. Assume parity because it is the same server
  software; do not assume it is verified.
- T61 in `docs/improvement-plan.md` records that Polaris advertises a `namespace-separator` (`%1F`
  by default) for multi-level namespaces, which polytable currently ignores. A single-level
  `databaseName`, the only kind shown above, is unaffected; a namespace containing a separator
  character is not yet handled correctly against any Polaris-based catalog, Snowflake included.

## Teardown

An Open Catalog account is a real, billable Snowflake account, not a free sandbox — undo setup in
the reverse order so nothing keeps costing money after you are done:

1. Delete the service connection created above, from the Open Catalog UI's **Connections** tab,
   which invalidates its `CLIENT_ID:CLIENT_SECRET` immediately.
2. Delete the catalog and any principal roles or catalog roles created for this test, from the same
   UI.
3. Drop the Open Catalog account itself from **Admin → Accounts** in Snowsight, the same place it
   was created. Confirm the drop in Snowflake's account view rather than assuming the UI action
   completed — an account left in a "pending drop" state can still accrue charges until the drop
   finishes.

## Troubleshooting

- **A dataset config's `clientSecretEnv`-named variable is unset at sync time.** polytable refuses
  to build the client and names both the property and the missing variable in the error, rather than
  falling through to an unauthenticated request — confirm the variable is set in the exact process
  that runs the sync, not just in an interactive shell.
- **A case-mismatched `warehouse` fails in a way that looks like a missing catalog.** Given
  [the warehouse rule above](#the-warehouse-is-the-catalog-name-and-it-is-case-sensitive), a
  catalog named `Analytics` addressed as `analytics` is the first thing to check when Open Catalog
  behaves as though the catalog does not exist.
- **A token request fails with no more detail than a status code.** `pkg/catalog/oauth2.go`
  surfaces the token endpoint's status code and body verbatim in that case; check that against
  Snowflake's own OAuth2 error responses, since none have been catalogued here.
- **Whether an unknown warehouse or namespace returns a `404` or something else is not established
  here.** Polaris itself answers a typo'd warehouse with `404 NotFoundException` (T58); whether
  Snowflake's deployment matches has not been checked.

## What's next

- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) for the complete `auth` / `token` /
  `warehouse` / `header.<Name>` property reference shared by every REST catalog polytable talks to.
- [Cloudflare R2 and R2 Data Catalog](cloudflare.md) for the closest existing page in structure and
  in how little of the target service this repository has actually touched.
- [Query a synced table](query-engines.md#snowflake) for reading a polytable-written Iceberg table
  from Snowflake SQL — a different, unrelated path that does not go through Open Catalog.
- [Features and limitations](features-and-limitations.md) for the honest, dated summary of what is
  and is not verified across the whole project.
- `docs/improvement-plan.md`, task **T59**, for the acceptance criteria this page's untested claims
  still need closed: a real Polaris container and a real Open Catalog account, not just `httptest`.
