"""Regenerate the test/testdata/fixtures/hudi-1.x fixture.

Requirements: JDK 17 on JAVA_HOME (Spark 3.5 does not run on newer JDKs),
`pip install 'pyspark==3.5.*'`, and network access so spark.jars.packages can
fetch org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0.

Run from any directory; the table lands in ./hudi-1.2/trips next to this
script. Copy that over test/testdata/fixtures/hudi-1.x/trips and refresh
manifest.json if row counts or versions change.
"""
import os
from pyspark.sql import SparkSession

WORK = os.path.dirname(os.path.abspath(__file__))
TABLE_PATH = os.path.join(WORK, "hudi-1.2", "trips")

spark = (
    SparkSession.builder.appName("hudi-fixture")
    .master("local[2]")
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0")
    .config("spark.jars.ivy", os.path.join(WORK, "ivy2"))
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

hudi_opts = {
    "hoodie.table.name": "trips",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.partitionpath.field": "city",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
}

rows1 = [
    (1, "alice", 10.5, 1000, "warsaw"),
    (2, "bob", 20.0, 1001, "warsaw"),
    (3, "carol", 30.25, 1002, "krakow"),
    (4, "dave", 40.0, 1003, "krakow"),
    (5, "erin", 50.75, 1004, "gdansk"),
]
cols = ["id", "name", "fare", "ts", "city"]

df1 = spark.createDataFrame(rows1, cols)
(df1.write.format("hudi").options(**hudi_opts)
    .option("hoodie.datasource.write.operation", "insert")
    .mode("overwrite").save(TABLE_PATH))
print("=== commit 1 (insert) done ===")

rows2 = [
    (2, "bob-updated", 99.0, 2000, "warsaw"),   # update
    (6, "frank", 60.0, 2001, "gdansk"),          # new row
]
df2 = spark.createDataFrame(rows2, cols)
(df2.write.format("hudi").options(**hudi_opts)
    .option("hoodie.datasource.write.operation", "upsert")
    .mode("append").save(TABLE_PATH))
print("=== commit 2 (upsert) done ===")

out = spark.read.format("hudi").load(TABLE_PATH)
out.orderBy("id").show(truncate=False)
print("row count:", out.count())
spark.stop()
