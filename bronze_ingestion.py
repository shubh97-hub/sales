# =============================================================================
# Bronze Layer - Raw Data Ingestion
# Sales BI Pipeline | Medallion Architecture
# =============================================================================
# Reads raw SaleData.xlsx from Azure Data Lake (ADLS Gen2) and writes it as
# Delta format into the Bronze layer with minimal transformation.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SalesPipeline_Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Config — update storage account name and container names as needed
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT  = "your_storage_account"
RAW_CONTAINER    = "raw"
BRONZE_CONTAINER = "bronze"

RAW_PATH    = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/SaleData.xlsx"
BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/"

# ---------------------------------------------------------------------------
# Schema Definition
# ---------------------------------------------------------------------------
sales_schema = StructType([
    StructField("OrderDate",   TimestampType(), True),
    StructField("Region",      StringType(),    True),
    StructField("Manager",     StringType(),    True),
    StructField("SalesMan",    StringType(),    True),
    StructField("Item",        StringType(),    True),
    StructField("Units",       DoubleType(),    True),
    StructField("Unit_price",  DoubleType(),    True),
    StructField("Sale_amt",    DoubleType(),    True),
])

# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
def ingest_bronze():
    logger.info("Starting Bronze ingestion from: %s", RAW_PATH)

    df_raw = (
        spark.read
             .format("com.crealytics.spark.excel")   # spark-excel library
             .option("header", "true")
             .option("inferSchema", "false")
             .option("dataAddress", "'Sales Data'!A1")  # sheet name
             .schema(sales_schema)
             .load(RAW_PATH)
    )

    # Audit columns
    df_bronze = (
        df_raw
        .withColumn("_ingested_at",  current_timestamp())
        .withColumn("_source_file",  input_file_name())
        .withColumn("_pipeline_run", lit("ADF_SalesPipeline_v1"))
    )

    row_count = df_bronze.count()
    logger.info("Rows read from source: %d", row_count)

    # Write to Bronze Delta table (append; ADF triggers this on schedule)
    (
        df_bronze.write
                 .format("delta")
                 .mode("append")
                 .option("mergeSchema", "true")
                 .save(BRONZE_PATH)
    )

    logger.info("Bronze layer written to: %s", BRONZE_PATH)
    return row_count

if __name__ == "__main__":
    rows = ingest_bronze()
    print(f"[Bronze] Ingestion complete — {rows} rows loaded.")
    spark.stop()
