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

# Build a disposable AWS test environment

polytable's S3 code (`pkg/io/s3.go`) is exercised end to end by
`test/dockertest_minio_matrix_test.go`, which runs the full Delta/Iceberg/Hudi
matrix against a real MinIO container. Its Glue code
(`pkg/catalog/glue.go`, `glue_partition.go`, `glue_conversion.go`) has no such
coverage. `docs/improvement-plan.md` T15 records this plainly: partition
sync "**landed, not verified against a real Glue catalog**"—"every test
drives a fake. Until a real Glue catalog … has registered a partitioned
table this way and an engine has resolved partitions through it, the claim
is untested end to end." This page is a recipe for standing up a small,
disposable AWS environment in your own account to close that gap for both
halves—S3 against real object-store semantics, and Glue against the real
service—and for tearing it down again without leaving cost or credentials
behind.

## What this buys you

Running the commands below settles these specific, currently unverified
claims:

- **Real Glue table registration.** `GlueCatalogSyncClient.CreateOrUpdateTable`
  (`pkg/catalog/glue.go`) has only ever called a fake `glue.Client`
  substitute in tests. Nothing has confirmed that the `TableInput` it
  builds—SerDe library, input/output format, `EXTERNAL_TABLE` type, the
  `table_type`/`spark.sql.sources.provider` parameters per format—is one
  Glue actually accepts.
- **Real partition synchronization**, which is exactly what T15 is waiting
  on. `catalog.SyncPartitions`, called from
  `pkg/conversion/controller.go`'s `syncPartitions`, diffs desired against
  existing partitions and batches `BatchCreatePartition` /
  `BatchDeletePartition` / `UpdatePartition` calls
  (`pkg/catalog/glue_partition.go`) against Glue's per-call limits (100
  create, 25 delete)—limits that are asserted in code but have never been
  exercised against the service that enforces them.
- **Glue table discovery**, `GlueConversionSource.GetSourceTable` and
  `ListTables` (`pkg/catalog/glue_conversion.go`), and the CLI path that
  calls it: `polytable sync --catalog glue --database <db>`. This is T23's
  scan path; T15's outcome note records that it too has only run against
  fakes.
- **Real S3 semantics that MinIO approximates rather than reproduces**:
  eventual-consistency edge cases, real IAM-scoped `403`s versus MinIO's
  simpler auth model, and region-qualified bucket creation, which MinIO
  ignores.
- **Credential resolution outside a static access key**: an IAM role
  assumed through IMDS on an EC2 instance, an SSO-sourced profile, and the
  default credential chain's provider order—none of which the MinIO
  dockertest touches, since that test sets `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` directly.
- **A region-scoping gap worth knowing before you start**: both
  `NewGlueCatalogSyncClient` and `NewGlueConversionSource` call
  `awsconfig.LoadDefaultConfig(ctx)` with no region option at all. Unlike S3,
  where `--storage-region` / `StorageConfig.Region` reach the S3 client
  directly, nothing in `DatasetConfig` or `catalog.Config` carries a region
  for Glue—it comes solely from `AWS_REGION`/`AWS_DEFAULT_REGION` or the
  active profile's `region`. Forgetting to set one of those is the single
  most likely reason the Glue calls below fail with an endpoint-resolution
  error rather than an authorization one.

## Cost and safety first

Read this section before running anything.

Everything below lives in **one S3 bucket and one Glue database**, so
teardown is short; see [Teardown](#teardown).

What things cost, roughly—point at the
[AWS Pricing Calculator](https://calculator.aws/) for a number specific to
your region, since prices are not given as fact here and they change:

- **S3 storage, requests, and data transfer for a session like this one:
  on the order of cents.** A few megabytes of fixture data and a handful of
  `PutObject`/`GetObject`/`ListObjectsV2` calls are a fraction of a cent.
  Leaving the bucket populated for weeks still costs a small, non-zero
  amount for storage at rest.
- **The Glue Data Catalog: free at this scale, with a caveat.** AWS Glue's
  first million objects stored and first million access requests per month
  are within the Data Catalog's free tier as of this writing—a handful of
  tables and partitions from this page does not approach either limit. This
  is stated at the order-of-magnitude level only; confirm the current free
  tier and per-request pricing on the calculator before assuming it applies
  to you.
- **IAM roles, policies, users, access keys, and the OIDC provider: free.**
  IAM itself carries no charge.
- **Nothing here provisions EC2, Athena, or any compute service by
  default.** The optional Athena check under
  [What to run once it exists](#what-to-run-once-it-exists) and the
  optional EC2 instance-profile variant are the two places a real, metered
  resource could appear—both are called out where they occur.

**Hard teardown step, do this as soon as you are done for the day:** empty
and delete the bucket, delete the Glue database (which cascades to its
tables), and remove the IAM role/policy/OIDC provider. The full sequence is
in [Teardown](#teardown). Do not leave the sandbox running "just in
case"—nothing here needs to persist between sessions.

## Rehearse locally first

Not everything needs a real account.

- **The S3 half is fully rehearsable at zero cost.** MinIO
  (`test/dockertest_minio_matrix_test.go`) already runs the complete
  Delta→Iceberg/Hudi matrix against a real S3-protocol server, and
  `make test-containers` runs it today. If you have not run it, do that
  first—anything the sandbox below finds broken in `pkg/io/s3.go` itself
  should already have shown up there.
- **Nothing in this tree rehearses Glue.** There is no LocalStack or moto
  fixture wired into `test/`—`docs/improvement-plan.md` names LocalStack
  only as a *possible future* follow-up under T30, not as existing
  coverage. `moto` (a Python AWS-service mock library) and
  [LocalStack](https://www.localstack.cloud/) both advertise Glue Data
  Catalog emulation—the exact URL and current feature coverage are not
  re-verified in this session—and could stand in for a real account while
  iterating on IAM policy JSON or
  Glue API call shapes, but polytable has never been run against either, so
  treat a clean run there as encouraging, not as confirmation. It does
  **not** settle T15—only a real Glue catalog does,
  because the acceptance criterion is specifically "verify the actual
  impact against a real Glue table."
- Net effect: rehearse the S3 half with MinIO, which this repository
  already does continuously; there is no equivalent local rehearsal for the
  Glue half, so the sandbox below is where that first happens.

## Prerequisites

- **AWS CLI v2.** Verify with `aws --version`; verified here against
  `aws-cli/2.33.2`. Confirm any command below against your installed
  version with `aws <command> help` before pasting it—flag names
  occasionally change between CLI releases.
- **An AWS account** with billing enabled (a new account's free tier
  comfortably covers this page).
- **Permissions, split by who does what—and this split is the part
  people get wrong**, same as the Azure page's roles-vs-data-plane
  distinction:
  - **Creating the sandbox**—the bucket, the Glue database, and (for the
    credential and OIDC sections) the IAM policy, role, user, and OIDC
    provider—needs `s3:CreateBucket`, `glue:CreateDatabase`, and a set of
    `iam:Create*`/`iam:Put*`/`iam:Attach*` actions. **A plain S3 or Glue
    user does not have these.** Creating IAM roles and policies is itself a
    privileged operation, distinct from anything the sandbox's own role
    will be granted—an account that can read and write S3 objects all day
    may still be unable to create the role that lets `polytable` do the
    same, and vice versa.
  - **The sandbox's own IAM role or user** needs only the least-privilege
    S3 and Glue actions polytable's code actually calls—enumerated below,
    not `s3:*` or `glue:*`.
  - Check what you are signed in as before starting:
    ```shell
    aws sts get-caller-identity
    ```
    This call needs no permissions of its own (AWS explicitly documents
    that a deny on `sts:GetCallerIdentity` still returns this information),
    so it is always safe to run first to confirm which account and
    identity every later command will act as.

## The sandbox

Set the names and region once at the top. S3 bucket names must be globally
unique and lowercase, so the `$RANDOM` suffix avoids a collision; a Glue
database name is only unique within your account and region, so it needs
no such suffix.

```shell
REGION=us-east-1
BUCKET=polytable-test-$RANDOM
GLUE_DB=polytable_test
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Every aws-cli and Glue-client call below reads this rather than a --region
# flag threaded through each command; see the region-scoping gap noted above
# under "What this buys you"—Glue's own client takes no region option at all.
export AWS_REGION="$REGION"
```

Create the bucket. **Outside `us-east-1`, S3 requires an explicit location
constraint**; inside it, the flag must be omitted entirely, or `CreateBucket`
rejects the request—this asymmetry is documented in the CLI's own
`create-bucket` help text and is the single most common `create-bucket`
mistake:

```shell
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
fi
```

Create the Glue database:

```shell
aws glue create-database --database-input '{"Name": "'"$GLUE_DB"'"}'
```

Seed a real, partitioned table rather than empty bytes.
`test/testdata/fixtures/delta-rs-checkpoint/orders` in this repository is a
Delta table written by delta-rs, partitioned by `region`
(`region=north`/`west`/`south`/`east`), and it is exactly the shape T15's
partition-sync check needs:

```shell
aws s3 cp test/testdata/fixtures/delta-rs-checkpoint/orders \
  "s3://$BUCKET/tables/orders" --recursive
```

The table now lives at `s3://$BUCKET/tables/orders`.

### Least-privilege IAM policy

Do **not** grant `s3:*` or `glue:*`. The policy below enumerates exactly
what `pkg/io/s3.go` and the three Glue catalog files call.

The S3 actions come directly from `S3Storage`'s methods: `Read`/`Exists`
use `GetObject`/`HeadObject`, `Write` uses `PutObject`, `Delete` uses
`DeleteObject`, and `List` uses `ListObjectsV2`, which is authorized by the
`s3:ListBucket` IAM action—and, the classic mistake, against the
**bucket** ARN, not the `/*` object ARN.

The Glue actions come from `GlueCatalogSyncClient` (`GetTable`,
`CreateTable`, `UpdateTable`, `DeleteTable`), `GluePartitionSyncOperations`
(`GetPartitions`, `BatchCreatePartition`, `BatchDeletePartition`,
`UpdatePartition`, plus `GetTable` again to read the table's storage
descriptor before writing a partition), and `GlueConversionSource`
(`GetTable`, `GetTables` for the scan path). Glue authorizes against three
resource levels—catalog, database, and table—mirroring Hive's
namespace hierarchy; **this three-ARN shape is sourced from AWS's Glue IAM
documentation, not confirmed by a live policy simulation in this session**,
so treat it as the starting point rather than gospel if a call is denied
unexpectedly.

```shell
cat > /tmp/polytable-sandbox-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PolytableS3Object",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::$BUCKET/*"
    },
    {
      "Sid": "PolytableS3Bucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::$BUCKET"
    },
    {
      "Sid": "PolytableGlue",
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartitions",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:UpdatePartition"
      ],
      "Resource": [
        "arn:aws:glue:$REGION:$ACCOUNT_ID:catalog",
        "arn:aws:glue:$REGION:$ACCOUNT_ID:database/$GLUE_DB",
        "arn:aws:glue:$REGION:$ACCOUNT_ID:table/$GLUE_DB/*"
      ]
    }
  ]
}
EOF
```

Note what is deliberately absent: `glue:CreateDatabase` and
`glue:DeleteDatabase` are not in this policy, because polytable never calls
either—the database is created once, by hand, as part of standing up the
sandbox, and `GlueCatalogSyncClient` only ever creates, updates, and drops
*tables* inside it.

Create the managed policy from the file above:

```shell
POLICY_ARN=$(aws iam create-policy \
  --policy-name polytable-sandbox-policy \
  --policy-document file:///tmp/polytable-sandbox-policy.json \
  --query Policy.Arn --output text)
```

### Dataset config and CLI invocations

`StorageConfig` (`pkg/conversion/config.go`) carries `region`, `endpoint`
and `usePathStyle` for S3—no credential fields, by the same design as its
Azure counterpart: secrets reach polytable through the environment or the
AWS credential chain, never through a config file that gets committed,
logged, or POSTed to the REST service.

```yaml
# polytable-aws-test.yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://<bucket>/tables/orders
    tableName: orders
    storage:
      region: <region>
    catalogs:
      - type: AWS_GLUE
        databaseName: <glue-database>
```

Only one target format is listed on purpose. `pkg/conversion/controller.go`'s
`Sync` registers with every configured catalog once per successful target
format (`syncTargetToCatalogs`, called inside the per-target loop), so two
targets sharing one Glue table name would register the same table twice
with a different `TableInput` each time—Iceberg's SerDe and `table_type`
parameter overwritten by Hudi's, or vice versa, depending on which target
happens to sync last. `catalogs` is what drives both table registration
and—because the `orders` fixture is partitioned—partition
synchronization automatically: `syncPartitions` calls
`catalog.SyncPartitions` whenever `snapshot.Table.PartitioningFields` is
non-empty, with no separate flag to opt in.

```shell
./bin/polytable sync --datasetConfig polytable-aws-test.yaml

./bin/polytable inspect \
  --basePath "s3://$BUCKET/tables/orders" \
  --format ICEBERG
```

## Credentials

`NewS3Storage` and both Glue constructors call
`awsconfig.LoadDefaultConfig(ctx)` with no credential option—every mode
below is the standard AWS SDK v2 default credential chain, not something
polytable-specific.

### An IAM user with access keys

The simplest mode, and, deliberately, the least good option: a long-lived
access key has no expiry, is easy to leave in a shell history or a CI
secret store, and grants exactly what its attached policy says for as long
as it exists.

```shell
aws iam create-user --user-name polytable-sandbox-user
aws iam attach-user-policy --user-name polytable-sandbox-user --policy-arn "$POLICY_ARN"
aws iam create-access-key --user-name polytable-sandbox-user
```

`create-access-key` prints `AccessKeyId` and `SecretAccessKey` once—map
them into the two environment variables the default credential chain
checks first, ahead of a profile or SSO session:

```shell
export AWS_ACCESS_KEY_ID="<access-key-id>"
export AWS_SECRET_ACCESS_KEY="<secret-access-key>"
unset AWS_SESSION_TOKEN

./bin/polytable inspect --basePath "s3://$BUCKET/tables/orders" --format DELTA
```

Because these two variables are checked first, **a leftover pair from an
earlier test silently wins over the profile or SSO mode you meant to test
next**—unset both before moving to the next subsection, exactly as the
Azure page warns about `AZURE_STORAGE_KEY`.

### A named profile

```shell
aws configure --profile polytable-sandbox
# prompts for the same access key, secret key, region and output format,
# written to ~/.aws/credentials and ~/.aws/config under [polytable-sandbox]
```

```shell
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
export AWS_PROFILE=polytable-sandbox

./bin/polytable inspect --basePath "s3://$BUCKET/tables/orders" --format DELTA
```

`LoadDefaultConfig` reads `AWS_PROFILE` and resolves the named profile's
credentials, region, and any role-assumption chain configured under
it—there is no `--profile` flag or credential field anywhere in polytable's
own configuration, matching `S3Options`' and `AzureStorageConfig`'s
documented rationale that secrets never belong in a config file.

### AWS IAM Identity Center (SSO)

```shell
aws configure sso --profile polytable-sandbox-sso
aws sso login --profile polytable-sandbox-sso
```

```shell
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
export AWS_PROFILE=polytable-sandbox-sso

./bin/polytable inspect --basePath "s3://$BUCKET/tables/orders" --format DELTA
```

SSO tokens expire (typically in hours); `LoadDefaultConfig` refreshes the
underlying AWS credentials automatically while the cached SSO token is
still valid, but once that token itself expires every polytable call fails
with an expired-token error until you run `aws sso login` again—there is
no automatic re-authentication.

### An EC2 instance profile

This is the one credential mode that, like the Azure page's managed
identity, cannot be exercised from a workstation—it depends on the
Instance Metadata Service (IMDS) of a real EC2 host.

Write the standard EC2 trust policy first, then create the role against it:

```shell
cat > /tmp/polytable-ec2-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role --role-name polytable-sandbox-role \
  --assume-role-policy-document file:///tmp/polytable-ec2-trust-policy.json
aws iam attach-role-policy --role-name polytable-sandbox-role --policy-arn "$POLICY_ARN"
aws iam create-instance-profile --instance-profile-name polytable-sandbox-profile
aws iam add-role-to-instance-profile \
  --instance-profile-name polytable-sandbox-profile --role-name polytable-sandbox-role
```

Attach `polytable-sandbox-profile` to an EC2 instance at launch (or with
`aws ec2 associate-iam-instance-profile` on a running one—not run here).
From inside that instance, set no AWS environment variables at all:
`LoadDefaultConfig` reaches IMDS directly, and running `polytable inspect`
or `polytable sync` there with a clean environment is the test. An EC2
instance is a metered resource for as long as it runs—this variant is
the one place in this section where standing up the test itself has an
hourly cost, separate from anything S3 or Glue charge.

## GitHub Actions with OIDC, no static keys

This repository has no AWS counterpart to `.github/workflows/azure-live.yml`
today; the shape below is what that lane would look like, mirroring it
structurally: workflow_dispatch and a nightly schedule only, `id-token:
write` and `contents: read` and nothing broader, and no client secret
anywhere.

Create the OIDC identity provider for GitHub's token issuer. AWS now
verifies the provider's TLS certificate against its own trusted root CAs
rather than a caller-supplied thumbprint for well-known issuers—the
`--thumbprint-list` flag still exists on `create-open-id-connect-provider`
but whether it is still mandatory in every account/region is not verified
in this session; a thumbprint is included below for compatibility:

```shell
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

Create the role with a trust policy pinned to this specific repository
**and branch**—never a wildcard on `sub`:

```shell
cat > /tmp/polytable-gha-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Federated": "arn:aws:iam::$ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"},
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com"},
      "StringLike": {"token.actions.githubusercontent.com:sub": "repo:<owner>/<repo>:ref:refs/heads/main"}
    }
  }]
}
EOF

aws iam create-role --role-name polytable-gha-aws-live \
  --assume-role-policy-document file:///tmp/polytable-gha-trust-policy.json
aws iam attach-role-policy --role-name polytable-gha-aws-live --policy-arn "$POLICY_ARN"
```

The workflow itself would use `aws-actions/configure-aws-credentials` with
`role-to-assume` (major version at time of writing not verified
here—check the marketplace listing for the current major tag):

```yaml
permissions:
  id-token: write
  contents: read

on:
  workflow_dispatch: {}
  schedule:
    - cron: "23 5 * * *"

jobs:
  aws-live:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::<account-id>:role/polytable-gha-aws-live
          aws-region: <region>
      - run: go test -count=1 -v -run TestAWSLive ./test/
```

`TestAWSLive` does not exist yet either—`test/azure_live_test.go`'s
`TestAzureLive_ADLSGen2Account` is the pattern a Go counterpart would
follow: gated on an environment variable such as `POLYTABLE_AWS_BUCKET` so
that `make check`, a plain `go test ./...`, and the MinIO dockertest lane
are all unaffected by its absence, and cleaning up everything it wrote
under a run-unique prefix in `t.Cleanup`.

**On a public repository, this workflow must never trigger on
`pull_request` or `pull_request_target`.** Either trigger would hand a
fork-authored run—for `pull_request_target`, one that checks out
fork-authored code while still keeping base-repo secrets and
permissions—the OIDC token exchange and, through it, write access to the
sandbox. And the trust policy's `sub` condition must pin the ref (`ref:refs/heads/main`)
rather than use a wildcard like `repo:<owner>/<repo>:*`, which would let
any branch or pull-request-derived subject assume the role.

## Teardown

Empty and delete the bucket—a non-empty bucket refuses `rb` outright:

```shell
aws s3 rm "s3://$BUCKET" --recursive
aws s3 rb "s3://$BUCKET"
```

Delete every table in the Glue database, then the database itself—Glue
does not cascade-delete tables automatically in a way this recipe should
rely on, so remove them explicitly first:

```shell
for t in $(aws glue get-tables --database-name "$GLUE_DB" --query 'TableList[].Name' --output text); do
  aws glue delete-table --database-name "$GLUE_DB" --name "$t"
done
aws glue delete-database --name "$GLUE_DB"
```

Remove the IAM surface, in dependency order—a role or user with an
attached policy refuses to delete until the policy is detached first:

```shell
aws iam delete-access-key --user-name polytable-sandbox-user --access-key-id <access-key-id>
aws iam detach-user-policy --user-name polytable-sandbox-user --policy-arn "$POLICY_ARN"
aws iam delete-user --user-name polytable-sandbox-user

aws iam remove-role-from-instance-profile \
  --instance-profile-name polytable-sandbox-profile --role-name polytable-sandbox-role
aws iam delete-instance-profile --instance-profile-name polytable-sandbox-profile
aws iam detach-role-policy --role-name polytable-sandbox-role --policy-arn "$POLICY_ARN"
aws iam delete-role --role-name polytable-sandbox-role

aws iam detach-role-policy --role-name polytable-gha-aws-live --policy-arn "$POLICY_ARN"
aws iam delete-role --role-name polytable-gha-aws-live

aws iam delete-policy --policy-arn "$POLICY_ARN"
aws iam delete-open-id-connect-provider \
  --open-id-connect-provider-arn "arn:aws:iam::$ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
```

**Two things do not live in the bucket and are the two people forget:**

- **The Glue database and every table registered in it.** They are a
  separate service from S3 with their own lifecycle; deleting the bucket
  does nothing to them, and a forgotten table registration is exactly the
  kind of stray state this page exists to avoid leaving behind.
- **The IAM user's access keys, and the user, role, policy, and OIDC
  provider themselves.** None of these are scoped to the bucket or the
  region—an access key in particular is a standing credential that keeps
  working even after the bucket and database it was meant for are gone,
  which is reason enough to delete it promptly rather than "when I get to
  it."

Confirm the bucket is actually gone before considering the sandbox closed:

```shell
aws s3api head-bucket --bucket "$BUCKET"   # should fail with a 404 / Not Found
```

## What to run once it exists

Each item below turns one of T15's unmet criteria, or a directly related
gap noted alongside it, into a specific command.

- **T15's core claim—"verify the actual impact against a real Glue
  table."** The `sync` invocation under
  [Dataset config and CLI invocations](#dataset-config-and-cli-invocations),
  using the partitioned `orders` fixture and a `catalogs:` block, is the
  check: it calls `CreateOrUpdateTable` and then, because the table is
  partitioned, `SyncPartitions`. Confirm both landed:
  ```shell
  aws glue get-table --database-name "$GLUE_DB" --name orders
  aws glue get-partitions --database-name "$GLUE_DB" --table-name orders
  ```
  Four partitions (`region=north`/`west`/`south`/`east`) confirms the
  partition-diff-and-batch path in `pkg/catalog/glue_partition.go` ran
  against the real service, not a fake.
- **T15's stated end state—"an engine has resolved partitions through
  it."** Nothing in this repository runs a query engine, so this is the
  one criterion this page cannot close on its own. Athena is the natural
  check, since it reads the Glue Data Catalog directly with no separate
  metastore to configure—this incurs its own (typically per-query,
  data-scanned) charge, separate from everything above, so treat it as
  optional and check current pricing before running it:
  ```shell
  aws athena start-query-execution \
    --query-string "SELECT region, count(*) FROM \"$GLUE_DB\".orders GROUP BY region" \
    --result-configuration OutputLocation="s3://$BUCKET/athena-results/" \
    --query-execution-context Database="$GLUE_DB"
  ```
  A result with all four regions confirms Athena resolved the partitions
  polytable registered, closing T15 end to end. A failure here is itself
  signal, not necessarily a setup mistake: whether Athena accepts the
  Iceberg `TableInput` `pkg/catalog/glue.go`'s `buildTableInput`
  writes—which carries no `metadata_location` parameter—as a queryable table is
  exactly the open question T15 asks about, not something this page can
  guarantee in advance.
- **T23's discovery-scan path, landed under the same task but with the
  same "only checked against fakes" caveat.** Run the CLI's catalog-scan
  mode instead of a dataset config:
  ```shell
  ./bin/polytable sync --catalog glue --database "$GLUE_DB"
  ```
- **`GetSourceTable`—the single-table lookup the CLI scan does not
  exercise**, per T15's outcome note ("the CLI exposes the scan, not the
  single-table lookup"). A `sourceCatalog:` block reaches it directly:
  ```yaml
  sourceCatalog:
    catalog:
      type: AWS_GLUE
      databaseName: <glue-database>
    table: orders
  ```
- **The region-scoping gap** noted under
  [What this buys you](#what-this-buys-you): run the sync once with
  `AWS_REGION` unset and a profile whose `region` is also unset, and
  confirm the Glue call fails with an endpoint-resolution error rather than
  succeeding against the wrong region silently.
