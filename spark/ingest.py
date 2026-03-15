import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)  # <-- Add this

# Configuration from Docker Compose environment
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1")

print("🚀 Initializing Spark Session for Enwyse Lakehouse...")

spark = (
    SparkSession.builder.appName("Enwyse-Batch-Ingestion")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.authentication.type", "NONE")
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse")
    .config("spark.sql.catalog.nessie.s3.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

LANDING_ZONE = "s3a://landing-zone/telemetry/"

print("✅ Spark Session ready. Starting Airflow-triggered batch run...")

try:
    # the exact expected schema
    telemetry_schema = StructType(
        [
            StructField("time", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("solar_yield_kw", DoubleType(), True),
            StructField("battery_soc_pct", DoubleType(), True),
        ]
    )

    # tell spark to recursively search all subfolders for json
    df = (
        spark.read.schema(telemetry_schema)
        .option("recursiveFileLookup", "true")
        .json(LANDING_ZONE)
    )

    if df.isEmpty():
        print("No new data found in landing zone. Exiting successfully.")
    else:
        print(f"Found {df.count()} records. Appending to Iceberg...")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

        # Write to Iceberg
        df.write.format("iceberg").mode("append").saveAsTable(
            "nessie.gold.sensor_telemetry_historical"
        )

        # Clean up the processed files
        URI = spark._jvm.java.net.URI
        Path = spark._jvm.org.apache.hadoop.fs.Path
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
        fs = FileSystem.get(URI(LANDING_ZONE), spark._jsc.hadoopConfiguration())
        fs.delete(Path(LANDING_ZONE), True)

        print("✅ Batch complete! Appended to Lakehouse and cleared landing zone.")

except Exception as e:
    if "Path does not exist" in str(e):
        print("Landing zone bucket empty or missing. Exiting successfully.")
    else:
        print(f"⚠️ Pipeline error: {e}")
        raise e
