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

# Query a synced table

polytable writes standard Delta Lake, Apache Iceberg, Apache Hudi, and Apache
Paimon metadata, so any engine that reads one of those formats reads a
polytable-synced table exactly as it reads a natively written one. There is no
polytable driver, connector, or plugin: you configure the engine for the target
format the same way its own documentation describes, and point it at the synced
table.

A synced table directory carries each target's metadata side by side —
`_delta_log/` for Delta Lake, `metadata/` for Iceberg, `.hoodie/` for Hudi —
all describing the same untouched Parquet data files. Different engines can
therefore read the same directory through different formats at the same time.

Because the output is standard metadata, the engine guides that the upstream
[Apache XTable (incubating)](https://xtable.apache.org) project publishes
transfer to polytable output; this page adapts them. The upstream guides cover
the Delta Lake, Iceberg, and Hudi formats. For an engine's Paimon support, see
that engine's own documentation.

## Register the table in a catalog

Several of the engines on this page locate tables through a catalog rather
than a file path. polytable can register synced tables in the AWS Glue Data
Catalog and in any Iceberg REST catalog as part of the sync run:

- [Sync to the AWS Glue Data Catalog](glue-catalog.md)
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md)

polytable has no Hive Metastore client. Where an engine needs a Hive Metastore
registration, create the table entry manually or use the upstream Apache
XTable [Hive Metastore sync](https://xtable.apache.org/docs/hms).

## DuckDB

DuckDB reads a synced table straight from its directory, with no catalog and
no server. This is the one engine flow that this repository verifies: the
engine-verification suite in `test/engineverify_duckdb_test.go` syncs Delta
Lake and Iceberg targets from Parquet, Delta Lake, Iceberg, and Hudi sources,
then checks row
counts, partition predicates, and full-column reads through the DuckDB CLI.

For an Iceberg target, use the `iceberg` extension's `iceberg_scan` function
on the table directory. DuckDB finds the current metadata version through the
`metadata/version-hint.text` file that polytable writes:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('/path/to/table');
```

For a Delta Lake target, use the `delta` extension's `delta_scan` function:

```sql
INSTALL delta; LOAD delta;
SELECT * FROM delta_scan('/path/to/table');
```

Each extension needs network access the first time you install it; afterwards
DuckDB caches it per user.

## Apache Spark

Spark reads a synced table with the same packages and session configuration
that the target format itself requires — locally or on services like Amazon
EMR, Google Cloud Dataproc, Azure HDInsight, or Databricks. The following
shell commands, from the upstream guide, start `pyspark` with each format's
reader; adjust the artifact coordinates to your Spark, Scala, and format
versions.

For a Hudi target:

```shell
pyspark \
  --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:1.2.0 \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
```

For a Delta Lake target:

```shell
pyspark \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

For an Iceberg target:

```shell
pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.2 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog"
```

In the session, load the table by path:

```python
df = spark.read.format("delta").load("/path/to/table")   # or "iceberg"

hudi_options = {
    "hoodie.metadata.enable": "true",
    "hoodie.datasource.write.hive_style_partitioning": "true",
}
df = spark.read.format("hudi").options(**hudi_options).load("/path/to/table")
```

The upstream project notes that synced Hudi target tables need a Hudi 0.14.0
or later reader.

## Trino

Trino queries Hudi, Delta Lake, and Iceberg tables through its
[Hudi](https://trino.io/docs/current/connector/hudi.html),
[Delta Lake](https://trino.io/docs/current/connector/delta-lake.html), and
[Iceberg](https://trino.io/docs/current/connector/iceberg.html) connectors,
with no extra configuration for synced tables. The Hudi and Delta Lake
connectors locate tables through a Hive Metastore or AWS Glue; the Iceberg
connector additionally supports Iceberg REST catalogs, which polytable can
register tables in directly. After the table is registered, query it like any
other:

```sql
SELECT * FROM iceberg.my_schema.my_table;
```

Substitute the catalog and schema names your Trino deployment defines for each
format's connector.

## Presto

Presto follows the same pattern as Trino: the
[Hudi](https://prestodb.io/docs/current/connector/hudi.html),
[Delta Lake](https://prestodb.io/docs/current/connector/deltalake.html), and
[Iceberg](https://prestodb.io/docs/current/connector/iceberg.html) connectors
read synced tables without extra configuration, once the table is registered
in the metastore the connector uses:

```sql
SELECT * FROM hudi.my_schema.my_table;
```

The upstream project documents one limitation: Delta Lake generated columns —
which the sync uses for some partition columns — read as `NULL` from the
Presto CLI.

## Amazon Athena

Athena reads synced tables of any target format through the AWS Glue Data
Catalog. Either let polytable register the table during the sync — see
[Sync to the AWS Glue Data Catalog](glue-catalog.md) — or create the table
with a DDL statement as described in the Athena documentation for
[Hudi](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html),
[Delta Lake](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html),
and
[Iceberg](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
tables. Once registered, the table queries like any other:

```sql
SELECT * FROM my_database.my_table;
```

The upstream project documents that Athena engine version 3 ships a Hudi
0.12.2 reader, while synced Hudi target tables need Hudi 0.14.0 — so Hudi
target tables do not work from Athena. Query the Iceberg or Delta Lake target
instead.

## Amazon Redshift Spectrum

Redshift Spectrum reads synced Hudi and Iceberg tables through an external
schema that points at the Glue database holding the table registration.
Redshift infers each table's schema and format from the catalog:

```sql
CREATE EXTERNAL SCHEMA synced_schema
FROM DATA CATALOG
DATABASE 'GLUE_DATABASE_NAME'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

SELECT * FROM synced_schema.my_table;
```

Replace `GLUE_DATABASE_NAME`, `ACCOUNT_ID`, and `ROLE_NAME` with your Glue
database and an IAM role that can read Amazon S3 and the Glue Data Catalog.

For a Delta Lake target, Redshift Spectrum relies on a Delta manifest file
rather than reading `_delta_log/` directly. Generate the manifest by following
the [Delta Lake integration steps](https://docs.delta.io/latest/redshift-spectrum-integration.html)
or by selecting the symlink-table option when a Glue crawler registers the
table; then create the external schema the same way.

## Google BigQuery

BigQuery reads a synced Iceberg table as a BigLake external table created from
the Iceberg metadata file:

```sql
CREATE EXTERNAL TABLE my_dataset.my_table
WITH CONNECTION `PROJECT_ID.LOCATION.CONNECTION_ID`
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://BUCKET/path/to/table/metadata/vN.metadata.json']
);
```

Replace `vN` with the newest metadata version; the
`metadata/version-hint.text` file records it. This method pins one metadata
version, so re-create the table or use
[BigLake Metastore](https://cloud.google.com/bigquery/docs/iceberg-tables)
after each sync. BigQuery can also query Hudi and Delta Lake tables through
[manifest files](https://cloud.google.com/bigquery/docs/query-open-table-format-using-manifest-files).

BigQuery reads from Cloud Storage, and polytable does not support `gs://`
paths — see the storage support note in
[Create your first interoperable table](how-to.md). To produce synced metadata
in place on Cloud Storage, use the upstream Java project.

## Snowflake

Snowflake reads a synced Iceberg target as an Iceberg table over user-supplied
storage. Create an external volume for the bucket, a catalog integration for
Iceberg files in object storage (or use AWS Glue as the catalog source), and
then an Iceberg table pointing at the metadata file that polytable wrote:

```sql
CREATE ICEBERG TABLE my_table
EXTERNAL_VOLUME = 'VOLUME_NAME'
CATALOG = 'CATALOG_NAME'
METADATA_FILE_PATH = 'path/to/table/metadata/vN.metadata.json';
```

Replace `vN` with the newest metadata version from
`metadata/version-hint.text`. For the external volume and catalog integration
setup, see the
[Snowflake Iceberg table documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg).
Like the BigQuery metadata-file method, this pins one metadata version until
you refresh the table.

## StarRocks

StarRocks queries Hudi, Delta Lake, and Iceberg tables through its
[external catalog](https://docs.starrocks.io/docs/data_source/catalog/catalog_overview/)
feature, with no extra configuration for synced tables. The upstream example
uses a unified catalog backed by a Hive Metastore:

```sql
CREATE EXTERNAL CATALOG unified_catalog_hms PROPERTIES (
  "type" = "unified",
  "unified.metastore.type" = "hive",
  "hive.metastore.uris" = "thrift://HIVE_METASTORE_HOST:9083"
);

SELECT * FROM unified_catalog_hms.my_schema.my_table;
```

Because polytable has no Hive Metastore client, register the table in the
metastore manually or with upstream tooling, as described in
[Register the table in a catalog](#register-the-table-in-a-catalog).

## Microsoft Fabric

Fabric consumes Delta Lake tables through OneLake shortcuts: a shortcut in a
Lakehouse links to the synced table's storage location, after which the table
is queryable from T-SQL, Spark, and Power BI. Shortcuts can reference Azure
Data Lake Storage Gen2 and Amazon S3, among other stores.

polytable does not support `abfss://` paths, so it cannot produce the Delta
metadata in place on ADLS — for that flow, run the upstream Java project as
its [Fabric guide](https://xtable.apache.org/docs/fabric) describes. For a
table that lives in Amazon S3, sync it to a Delta Lake target with polytable
and create an S3 shortcut to the table directory in your Lakehouse.

## Verification status

Except for DuckDB, the engine flows on this page are derived from the upstream
Apache XTable documentation and from the engines' own documentation; they have
not been run against this repository's output. The DuckDB flow is exercised on
every full test run by `test/engineverify_duckdb_test.go`.
