# =============================================================================
# Silver Layer - Data Cleaning & Transformation
# Sales BI Pipeline | Medallion Architecture
# =============================================================================
# Reads from Bronze Delta, applies cleaning, validation, and derived columns,
# then writes analytics-ready data to the Silver Delta layer.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, to_date, year, month, quarter,
    when, round as spark_round, current_timestamp, lit,
    dayofweek, date_format
)
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SalesPipeline_Silver_Transform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT  = "your_storage_account"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/"

# Valid reference sets (from known data)
VALID_REGIONS  = ["East", "Central", "West"]
VALID_ITEMS    = ["Television", "Home Theater", "Cell Phone", "Desk", "Video Games"]
VALID_MANAGERS = ["Martha", "Hermann", "Timothy", "Douglas"]

# ---------------------------------------------------------------------------
# Transformation Logic
# ---------------------------------------------------------------------------
def transform_silver():
    logger.info("Reading Bronze Delta from: %s", BRONZE_PATH)

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)

    # ------------------------------------------------------------------
    # 1. Drop rows with nulls in critical columns
    # ------------------------------------------------------------------
    critical_cols = ["OrderDate", "Region", "Item", "Units", "Unit_price"]
    df_cleaned = df_bronze.dropna(subset=critical_cols)

    dropped = df_bronze.count() - df_cleaned.count()
    logger.info("Rows dropped due to nulls in critical columns: %d", dropped)

    # ------------------------------------------------------------------
    # 2. Standardise string columns
    # ------------------------------------------------------------------
    df_std = (
        df_cleaned
        .withColumn("Region",   upper(trim(col("Region"))))
        .withColumn("Manager",  trim(col("Manager")))
        .withColumn("SalesMan", trim(col("SalesMan")))
        .withColumn("Item",     trim(col("Item")))
    )

    # ------------------------------------------------------------------
    # 3. Filter invalid domain values
    # ------------------------------------------------------------------
    df_valid = (
        df_std
        .filter(col("Region").isin([r.upper() for r in VALID_REGIONS]))
        .filter(col("Item").isin(VALID_ITEMS))
        .filter(col("Units")      > 0)
        .filter(col("Unit_price") > 0)
    )

    invalid_rows = df_std.count() - df_valid.count()
    logger.info("Rows removed during domain validation: %d", invalid_rows)

    # ------------------------------------------------------------------
    # 4. Derived / enriched columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_valid
        # Recalculate Sale_amt from source columns (formula columns are unreliable)
        .withColumn("Sale_amt",     spark_round(col("Units") * col("Unit_price"), 2))

        # Date dimensions
        .withColumn("OrderDate",    to_date(col("OrderDate")))
        .withColumn("Year",         year(col("OrderDate")))
        .withColumn("Month",        month(col("OrderDate")))
        .withColumn("Quarter",      quarter(col("OrderDate")))
        .withColumn("MonthName",    date_format(col("OrderDate"), "MMMM"))
        .withColumn("DayOfWeek",    date_format(col("OrderDate"), "EEEE"))

        # Revenue band segmentation
        .withColumn("RevenueBand",
            when(col("Sale_amt") >= 100_000, "High")
            .when(col("Sale_amt") >= 30_000,  "Medium")
            .otherwise("Low")
        )

        # Item category mapping
        .withColumn("Category",
            when(col("Item").isin("Television", "Home Theater", "Cell Phone"), "Electronics")
            .when(col("Item").isin("Video Games"), "Gaming")
            .when(col("Item").isin("Desk"), "Furniture")
            .otherwise("Other")
        )

        # Audit
        .withColumn("_transformed_at",  current_timestamp())
        .withColumn("_layer",           lit("silver"))

        # Drop Bronze audit columns
        .drop("_ingested_at", "_source_file", "_pipeline_run")
    )

    row_count = df_enriched.count()
    logger.info("Silver rows ready to write: %d", row_count)

    # ------------------------------------------------------------------
    # 5. Write to Silver Delta (overwrite for full refresh)
    # ------------------------------------------------------------------
    (
        df_enriched.write
                   .format("delta")
                   .mode("overwrite")
                   .option("overwriteSchema", "true")
                   .save(SILVER_PATH)
    )

    logger.info("Silver layer written to: %s", SILVER_PATH)
    return row_count

if __name__ == "__main__":
    rows = transform_silver()
    print(f"[Silver] Transformation complete — {rows} clean rows written.")
    spark.stop()
