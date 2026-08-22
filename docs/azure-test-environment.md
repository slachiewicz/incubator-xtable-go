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

# Build a disposable Azure test environment

polytable's Azure code (`pkg/io/azure.go`, `pkg/catalog/rest_auth.go`,
`pkg/catalog/entra.go`) has never run against a real Azure subscription. It
compiles, it is unit-tested against fakes and `httptest`, and the Azurite
emulator suite (`test/dockertest_azurite_test.go`) covers the Blob API—but
nothing in the tree has opened a socket to `*.core.windows.net` or
`*.fabric.microsoft.com`. This page is a recipe for standing up a small,
disposable Azure environment in your own subscription to close that gap, and
for tearing it down again without leaving cost behind. It does not replace the
Azurite suite; it exercises what Azurite cannot: real credentials, real DNS,
and Microsoft Fabric OneLake.

## What this buys you

Running the commands below settles these specific, currently unverified
claims:

- **The ADLS Gen2 blob-endpoint path.** `NewAzureStorage` (`pkg/io/azure.go`)
  derives the blob service URL from an `abfss://` host by swapping the first
  `.dfs.` for `.blob.`. This has only been checked against Azurite's
  synthetic host; it has never been checked against a real
  `<account>.dfs.core.windows.net` account.
- **Each of the four credential modes**—shared key, SAS token, Entra ID as a
  logged-in user, and Entra ID as a service principal—`NewAzureStorage`
  selects between, in priority order. Azurite only exercises shared key.
- **The OneLake `abfss://` shape**, `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>.<itemtype>/<path>`,
  where `ParseAzureURI` treats the workspace as the container and `onelake`
  as the literal account name. This is sourced from Microsoft's documentation
  but has never been requested.
- **The OneLake Storage-audience token rule.** `DefaultOneLakeScope`
  (`pkg/catalog/rest_auth.go`) requests `https://storage.azure.com/.default`
  because Microsoft's documentation says OneLake accepts tokens in the
  `Storage` audience only. No token built by this code has ever been
  presented to a Fabric endpoint.
- **Whether OneLake serves the Blob API for listing.** `NewAzureStorage`'s
  `.dfs.` → `.blob.` swap assumes `onelake.blob.fabric.microsoft.com` behaves
  like the ADLS Gen2 blob endpoint for `List`. The code comment next to the
  swap says this is "the documented mapping rather than a guess, though no
  request from this package has yet reached a Fabric workspace"—this
  environment is what removes that qualifier.

These map directly onto the unmet acceptance criteria in T51 and T52 of
`docs/improvement-plan.md`; see [What to run once it exists](#what-to-run-once-it-exists).

## Cost and safety first

Read this section before running anything.

Everything below lives in **one resource group**, so tearing down the storage
half of the sandbox is a single command:
`az group delete --name $RG --yes`. Do that as soon as you are done for the
day; do not leave the resource group running overnight "just in case".

What things cost, roughly, and point at the
[Azure pricing calculator](https://azure.microsoft.com/pricing/calculator/)
for a number specific to your region—prices are not given as fact here and
they change:

- **The resource group itself: free.** A resource group is a management
  container with no charge on its own.
- **The storage account, container, reads and writes: on the order of cents
  for a session like this one.** A few megabytes of data, a handful of list
  and blob operations, and a short lifetime add up to a fraction of a cent to
  a few cents. Leaving the account running for weeks with no traffic still
  costs a small, non-zero amount for the data at rest.
- **Role assignments, service principals, and Entra ID tokens: free.** Azure
  RBAC and Entra ID app registrations carry no charge on the free tier used
  here.
- **Microsoft Fabric capacity is the expensive one, and it is billed by the
  hour while it is running, not by usage.** An F2 capacity (Fabric's smallest
  paid SKU) bills continuously from the moment it is created until it is
  either **paused** or **deleted**—an idle F2 with no workspace activity
  still accrues the same hourly charge as a busy one. Left running
  unattended for a month, an F2 capacity is a real bill, not a rounding
  error; check the pricing calculator for the current per-hour rate in your
  region before creating one. The free 60-day Fabric trial capacity avoids
  this entirely and is enough for everything this page needs—prefer it,
  and only reach for a paid F2 capacity if the trial has expired or is
  unavailable in your tenant. Whichever you use, **pause or delete it the
  moment you are done**; see [Teardown](#teardown) for exactly which command
  applies to which resource.

## Prerequisites

- **Azure CLI.** Every command below is checked against the CLI's documented
  command surface, not run, since this environment has no `az` installed.
  Confirm your version with `az --version` and run `az <command> --help`
  before pasting anything that looks unfamiliar—flag names occasionally
  change between CLI releases, and any command below flagged "unverified" is
  exactly where to do that first.
- **Subscription roles**, and this is the part people get wrong: creating a
  storage account, assigning a role, and registering an app each need a
  **different** permission, and having one does not imply the others.
  - Creating the resource group and storage account needs **Contributor**
    (or the narrower **Storage Account Contributor**) on the subscription or
    a resource group within it.
  - **Assigning a role**—the `az role assignment create` calls below—
    needs `Microsoft.Authorization/roleAssignments/write`, which
    **Contributor does not grant**. You need **Owner** or **User Access
    Administrator** at the scope you are assigning against. This is the
    single most common failure in this whole recipe: a Contributor can
    create the storage account and then cannot grant themselves or anyone
    else data-plane access to it.
  - **Registering an app** (`az ad sp create-for-rbac`) needs permission in
    Microsoft Entra ID, not the Azure subscription. Most tenants allow any
    signed-in user to register an application by default (the "Users can
    register applications" tenant setting); a locked-down tenant requires
    the **Application Developer** role or better.
  - Check what you actually have before starting:
    ```shell
    az account show
    az role assignment list --assignee "$(az account show --query user.name -o tsv)" --all -o table
    ```
    If the second command shows no `Owner` or `User Access Administrator`
    row at a scope that covers your subscription or resource group, the role
    assignment steps below will fail with an authorization error, and you
    need someone who holds one of those roles to run them for you.

## The storage sandbox

Set the names and region once at the top; storage account names must be
globally unique, lowercase, and 3–24 alphanumeric characters, so the `$RANDOM`
suffix avoids a collision.

```shell
LOCATION=eastus2
RG=polytable-azure-test
STORAGE_ACCOUNT=polytabletest$RANDOM
CONTAINER=polytable-sandbox
```

Create the resource group everything else will live in.

```shell
az group create --name "$RG" --location "$LOCATION"
```

Create a StorageV2 account with the hierarchical namespace enabled. The
hierarchical namespace is what makes this an ADLS Gen2 account rather than
plain Blob Storage—it is the feature `abfss://` addresses and the one
`pkg/io/azure.go`'s comments assume throughout. `--hns` is the documented
short alias for `--enable-hierarchical-namespace`; either spelling works, and
the property is accepted only when `--kind` is `StorageV2`:

```shell
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hns true
```

Fetch the account key now—the shared-key path needs it, and it is also the
simplest way to create the container and seed data before any RBAC role has
had time to propagate.

```shell
ACCOUNT_KEY=$(az storage account keys list \
  --account-name "$STORAGE_ACCOUNT" \
  --resource-group "$RG" \
  --query "[0].value" -o tsv)

az storage container create \
  --name "$CONTAINER" \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$ACCOUNT_KEY"
```

Seed a real table rather than empty bytes. `test/testdata/fixtures/delta-rs-checkpoint/orders`
in this repository is a Delta table written by delta-rs, already used by
`test/delta_checkpoint_fixture_test.go`—upload it as-is:

```shell
az storage blob upload-batch \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$ACCOUNT_KEY" \
  --destination "$CONTAINER" \
  --destination-path tables/orders \
  --source test/testdata/fixtures/delta-rs-checkpoint/orders
```

The table now lives at
`abfss://$CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/tables/orders`.

### Credential mode 1: shared key

`NewAzureStorage` reads the account key from the `AZURE_STORAGE_KEY`
environment variable. This is the first credential polytable checks, ahead
of everything else, so **unset the other three Azure credential environment
variables before testing a different mode**—a leftover `AZURE_STORAGE_KEY`
or `AZURE_STORAGE_SAS_TOKEN` silently wins over the mode you meant to test.

```shell
unset AZURE_STORAGE_SAS_TOKEN
export AZURE_STORAGE_KEY="$ACCOUNT_KEY"

./bin/polytable inspect \
  --basePath "abfss://$CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/tables/orders" \
  --format DELTA
```

### Credential mode 2: SAS token

Generate a container SAS with the permissions the `Storage` interface
actually needs: `r` (read, for `Read`/`Exists`), `a`+`c`+`w` (add, create,
write—`UploadBuffer` needs create and write together, for `Write`), `d`
(delete, for `Delete`), and `l` (list, for `List`)—`racwdl` covers every
method the interface exposes. Give it a short expiry; two hours is plenty for
a test session.

```shell
EXPIRY=$(date -u -d '2 hours' '+%Y-%m-%dT%H:%MZ')   # GNU date; on macOS use: date -u -v+2H '+%Y-%m-%dT%H:%MZ'

SAS=$(az storage container generate-sas \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$ACCOUNT_KEY" \
  --name "$CONTAINER" \
  --permissions racwdl \
  --expiry "$EXPIRY" \
  -o tsv)

unset AZURE_STORAGE_KEY
export AZURE_STORAGE_SAS_TOKEN="$SAS"

./bin/polytable inspect \
  --basePath "abfss://$CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/tables/orders" \
  --format DELTA
```

### Credential mode 3: Entra ID as a user

Neither shared key nor SAS applies here—`NewAzureStorage` falls through to
`azidentity.NewDefaultAzureCredential`, which includes the Azure CLI's cached
login among its sources. Log in, then grant your account data-plane access.

```shell
unset AZURE_STORAGE_KEY AZURE_STORAGE_SAS_TOKEN
az login

STORAGE_ID=$(az storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RG" --query id -o tsv)
USER_UPN=$(az account show --query user.name -o tsv)

az role assignment create \
  --assignee "$USER_UPN" \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_ID"
```

**This is the single most common failure mode in this whole environment:**
being **Owner** on the subscription does **not** grant blob data access.
Owner is a control-plane role; reading and writing blob contents is a
data-plane operation gated by its own RBAC role, and `Storage Blob Data
Contributor` (or `Storage Blob Data Reader` for read-only) is the one that
grants it. An Owner who skips this step gets `AuthorizationPermissionMismatch`
on every blob call despite being able to see and manage the account itself in
the portal.

Role assignments also take a few minutes to propagate. If the very first
request after `az role assignment create` still returns
`AuthorizationPermissionMismatch`, that is very likely propagation delay, not
a wrong role—wait a couple of minutes and retry before assuming the
assignment is wrong.

```shell
./bin/polytable inspect \
  --basePath "abfss://$CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/tables/orders" \
  --format DELTA
```

### Credential mode 4: service principal

Same fallthrough to `DefaultAzureCredential`, this time via its environment
service-principal source, which reads `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,
and `AZURE_TENANT_ID`. Create the principal with the same data-plane role
scoped to the same storage account:

```shell
az ad sp create-for-rbac \
  --name "polytable-azure-test-sp" \
  --role "Storage Blob Data Contributor" \
  --scopes "$STORAGE_ID"
```

This prints JSON with `appId`, `password`, and `tenant`. Map those into the
three environment variables `DefaultAzureCredential` reads—never into a
config file:

```shell
unset AZURE_STORAGE_KEY AZURE_STORAGE_SAS_TOKEN
export AZURE_CLIENT_ID="<appId>"
export AZURE_CLIENT_SECRET="<password>"
export AZURE_TENANT_ID="<tenant>"

./bin/polytable inspect \
  --basePath "abfss://$CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/tables/orders" \
  --format DELTA
```

Secrets never belong in `storage.azure` in a dataset config—that block only
carries `endpoint`, `accountName`, and `anonymous` by design
(`pkg/conversion/config.go`). Every credential above reaches polytable
through an environment variable instead, precisely so a config file can be
committed, logged, or POSTed to the REST service without leaking one.

### Sync and inspect through a dataset config

With any one of the four credential modes active, run a full sync into
Iceberg next to the seeded Delta table:

```yaml
# polytable-azure-test.yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: abfss://<container>@<storage-account>.dfs.core.windows.net/tables/orders
    tableName: orders
```

```shell
./bin/polytable sync --datasetConfig polytable-azure-test.yaml

./bin/polytable inspect \
  --basePath "abfss://$CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/tables/orders" \
  --format ICEBERG
```

## The managed-identity variant

A laptop can exercise shared key, SAS, a user login, and a service principal,
but not managed identity or workload identity—both depend on the identity
platform of a specific Azure compute host, so this part can only be verified
by deploying there.

- **On an Azure VM:** enable a system-assigned identity, then grant that
  identity's principal the same data-plane role used above:
  ```shell
  az vm identity assign --name <vm-name> --resource-group "$RG"
  PRINCIPAL_ID=$(az vm identity show --name <vm-name> --resource-group "$RG" --query principalId -o tsv)
  az role assignment create --assignee-object-id "$PRINCIPAL_ID" --assignee-principal-type ServicePrincipal \
    --role "Storage Blob Data Contributor" --scope "$STORAGE_ID"
  ```
  No environment variables are needed on the VM at all: `DefaultAzureCredential`
  reaches the instance metadata service directly. Running `polytable inspect`
  or `polytable sync` from inside the VM with no Azure environment variables
  set is the test.
- **On AKS with workload identity:** enable the OIDC issuer and workload
  identity on the cluster, create a user-assigned managed identity, federate
  it to a Kubernetes service account, and grant that managed identity's
  principal the storage role:
  ```shell
  az aks update --name <cluster> --resource-group "$RG" --enable-oidc-issuer --enable-workload-identity
  az identity federated-credential create --name polytable-fed --identity-name <managed-identity-name> \
    --resource-group "$RG" --issuer <cluster-oidc-issuer-url> \
    --subject system:serviceaccount:<namespace>:<service-account-name>
  ```
  AKS's workload-identity webhook injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
  `AZURE_FEDERATED_TOKEN_FILE`, and `AZURE_AUTHORITY_HOST` into the pod
  automatically; nothing in polytable's own configuration changes. This is the
  deployment shape that matters in production for a daemon or a scheduled
  job, and it is the one shape in this whole page that categorically cannot
  be exercised from a workstation.

## The OneLake and Fabric sandbox

Several steps here have no CLI equivalent and need the Fabric portal. Each one
says so where it applies.

1. **Get capacity.** Prefer the free 60-day Fabric trial:
   [app.fabric.microsoft.com](https://app.fabric.microsoft.com), account
   menu → "Start trial". This is portal-only. If the trial is unavailable in
   your tenant, an F2 capacity can be purchased as an Azure resource
   (`Microsoft.Fabric/capacities`) through the Azure portal's "Create a
   resource" flow; creating and pausing it through `az` is not verified
   here—the `microsoft-fabric` CLI extension may or may not expose
   `capacity`/`pause`/`resume` subcommands, and this was not checked. Use the
   portal for this step unless you confirm otherwise with
   `az extension list-available` and its `--help` output.
2. **Create a workspace and assign it the capacity.** Fabric portal only:
   Workspaces → New workspace, then in the workspace's settings assign it to
   the trial or F2 capacity from step 1. Give the workspace a name with no
   spaces—a space in the workspace name breaks the `abfss://<workspace>@onelake...`
   URI, which has no escaping rule for it. If you cannot avoid spaces, use the
   workspace's GUID (visible in the browser URL,
   `app.fabric.microsoft.com/groups/<workspaceId>/...`) in the URI instead of
   its display name.
3. **Create a lakehouse.** Inside the workspace: New item → Lakehouse.
   Fabric portal only. Note the lakehouse's item name (or its GUID, from the
   URL when the lakehouse is open)—both forms work in the `abfss://` path.
4. **Put a table in it.** A new lakehouse has no tables, and nothing earlier
   on this page puts one there—the ADLS Gen2 upload in
   [The storage sandbox](#the-storage-sandbox) reaches a different account
   entirely. This step is portal-only and not verified against a live
   workspace: open the lakehouse and use its "Start with sample data" or
   "Get data" option to load a managed table, then use whatever table name
   that produces (Fabric lakehouse managed tables are Delta tables, so
   `--format DELTA` below still applies). Loading the repository's own
   delta-rs fixture into OneLake instead would need a OneLake-aware upload
   tool, which is unverified here—prefer the sample-data path unless you
   have already confirmed one.
5. **Construct the URI**, following the shape Microsoft documents and
   `ParseAzureURI` already parses:
   ```
   abfss://<workspace-name-or-guid>@onelake.dfs.fabric.microsoft.com/<lakehouse-name-or-guid>.Lakehouse/Tables/<table-name>
   ```
6. **The Storage-audience token rule.** OneLake accepts Entra ID tokens for
   the `Storage` audience only—this is exactly what
   `DefaultOneLakeScope = "https://storage.azure.com/.default"` requests. To
   pull a token by hand for ad-hoc testing, outside of polytable:
   ```shell
   az account get-access-token --resource https://storage.azure.com/ --query accessToken -o tsv
   ```
   A token for any other resource (for example the default ARM audience) is
   rejected by OneLake; if a manual request against OneLake returns 401 or
   403, check the audience first.
7. **Before running polytable**, clear every leftover Azure credential
   environment variable, not only the storage-account ones—OneLake has no
   account key and no SAS token for the `onelake` pseudo-account, and it has
   no use for a service principal scoped to a different storage account
   either. `NewAzureStorage` checks `AZURE_STORAGE_SAS_TOKEN`, then
   `AZURE_STORAGE_KEY`, before it ever reaches `DefaultAzureCredential`, and
   `DefaultAzureCredential` itself checks the `AZURE_CLIENT_ID` /
   `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` environment service principal
   before falling back to the Azure CLI login. Any of the five left over
   from an earlier credential-mode test is sent to OneLake, is meaningless
   or wrong there, and fails confusingly rather than falling back to your
   `az login` session:
   ```shell
   unset AZURE_STORAGE_KEY AZURE_STORAGE_SAS_TOKEN AZURE_CLIENT_ID AZURE_CLIENT_SECRET AZURE_TENANT_ID
   az login
   ./bin/polytable inspect \
     --basePath "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<table-name>" \
     --format DELTA
   ```
   A successful read here is the check for both the blob-endpoint swap and
   the "does OneLake serve the Blob API for listing" question—`inspect`
   calls `List` before it calls `Read`.

**The catalog side (T52) cannot be checked yet, and the reason is not your
environment.** Microsoft documents OneLake's Iceberg REST endpoint at
`https://onelake.table.fabric.microsoft.com/iceberg`, addressed by a warehouse
of `<WorkspaceID>/<DataItemID>` or
`<WorkspaceName>/<DataItemName>.<DataItemType>`. Two things block polytable
from using it:

- The endpoint requires prefix negotiation. A client calls
  `GET /v1/config?warehouse=<Warehouse>` first and puts the returned
  `overrides.prefix` into every later path. polytable builds
  `/v1/namespaces/...` with no prefix segment, so every request misses. This
  is T53 in [the improvement plan](improvement-plan.md).
- polytable has no field to carry a warehouse. `catalog.Config` has `URI`,
  `DatabaseName` and `Properties`, and `databaseName` maps to the Iceberg
  namespace—`dbo` in Microsoft's examples—not to the workspace. T53 covers
  adding one.

The endpoint is also read-only: its advertised operations are `GET` and
`HEAD`, so it can serve a conversion source but can never accept a
registration.

What you can check today without polytable is that the environment itself
works, which is worth doing before T53 lands so the two are not debugged
together:

```shell
TOKEN=$(az account get-access-token --resource https://storage.azure.com/ --query accessToken -o tsv)
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://onelake.table.fabric.microsoft.com/iceberg/v1/config?warehouse=<Workspace>/<DataItem>"
```

A `200` with an `overrides.prefix` field confirms the workspace, the token
audience and the endpoint all work, and gives you the prefix value T53 will
negotiate automatically.

## Teardown

**Storage side—one command, because everything above lives in `$RG`:**

```shell
az group delete --name "$RG" --yes
```

Two things do **not** live in that resource group and are the two people
forget, and get billed for:

- **A Fabric trial capacity or a workspace** exist only in the Fabric admin
  surfaces, never in an Azure resource group—remove them from the Fabric
  portal (workspace settings → Remove this workspace; capacity settings →
  end trial). If you instead purchased an F2 capacity as an ARM resource
  (`Microsoft.Fabric/capacities`) and created it inside `$RG`, `az group
  delete` above does remove it—but confirm this in the portal's cost view
  afterward rather than assuming. Pausing a capacity (instead of deleting it)
  stops its hourly compute charge, which is the bulk of an F2's cost, but any
  OneLake data left under it is stored data, not capacity compute, and is
  billed separately regardless of the capacity's paused or running state—so
  "paused" is not the same as "zero cost while data remains."
- **The app registration behind the service-principal step** was created
  with `az ad sp create-for-rbac` and is not a subscription resource either:
  ```shell
  az ad app delete --id <appId>
  ```
  Deleting the app registration also removes its service principal.

Confirm the resource group is actually gone before considering the sandbox
closed:

```shell
az group show --name "$RG"   # should fail with ResourceGroupNotFound
```

## What to run once it exists

Each item below is one of T51 or T52's stated unmet acceptance criteria
(`docs/improvement-plan.md`), turned into the specific command that would
close it.

- **T51, criterion 1—"the Azurite end-to-end suite has never executed."**
  This one needs a Docker daemon, not Azure at all:
  ```shell
  make test-containers
  # or, narrower:
  go test ./test -run TestDockertest_Azurite_FullLakehouseMatrix -v
  ```
- **T51, criterion 2—"no credential mode is exercised."** The four
  `inspect` invocations under [The storage sandbox](#the-storage-sandbox),
  one per credential mode with the other three Azure environment variables
  unset, are the check. Record which modes you ran and their result.
- **T51, criterion 3—"no OneLake request has been made."** The single
  `inspect` call against a real `onelake.dfs.fabric.microsoft.com` lakehouse
  in [The OneLake and Fabric sandbox](#the-onelake-and-fabric-sandbox)
  closes this: a successful read proves the `.dfs.` → `.blob.` derivation is
  correct against a real Fabric host, and since `inspect` lists before it
  reads, it also answers whether OneLake serves the Blob API for listing.
- **T51, criterion 4—"`wasbs://` and `wasb://` are routed and
  parse-tested but never run."** Repeat one `inspect` call against the ADLS
  Gen2 account from the storage sandbox, spelled with `wasbs://` instead of
  `abfss://`:
  ```shell
  ./bin/polytable inspect \
    --basePath "wasbs://$CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/tables/orders" \
    --format DELTA
  ```
- **T52, criterion 1—"no Fabric lakehouse table has been resolved or
  converted."** Blocked on T53, not on this environment: polytable cannot
  address the endpoint until it negotiates the prefix. The `curl` against
  `/v1/config` in
  [The OneLake and Fabric sandbox](#the-onelake-and-fabric-sandbox) is the
  part you can run today, and its `overrides.prefix` value is what T53 will
  fetch automatically.
- **T52, criterion 2—"a single token being presented to a Fabric
  endpoint" has not happened.** The same `curl` settles the audience rule
  independently of polytable: it uses
  `az account get-access-token --resource https://storage.azure.com/`, the
  audience `DefaultOneLakeScope` requests. A `200` proves the scope is right
  before any Go code is involved.
- **T52, criterion 3—the prefix gap.** Not runnable here; it is T53's
  acceptance.
- **T52, criterion 4—"listing is still unavailable."** The endpoint
  advertises `GET /v1/{prefix}/namespaces` and
  `GET /v1/{prefix}/namespaces/{namespace}/tables`, so this is an
  implementation gap rather than a protocol limit, and T53 covers closing it.
