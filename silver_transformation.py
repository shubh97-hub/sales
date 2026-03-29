# Databricks Notebook - Silver Layer Transformation
# Dataset : Buy Anything Sales (1,000 orders, Jan–Dec 2024)
# Layer   : Silver (Cleaned & Validated)
# Description: Reads Bronze, applies cleaning, type casting, and derives new columns.

# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, upper, to_date,
    year, month, quarter, date_format,
    when, isnull, round, current_timestamp, lit
)
from pyspark.sql.types import IntegerType, DoubleType
from scripts.config import STORAGE_ACCOUNT, SILVER_PATH
from scripts.utils import log_message, get_row_count, check_nulls

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("BuyAnything_Silver_Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

log_message("Silver transformation started.")

# ============================================================
# READ FROM BRONZE
# ============================================================
df_bronze = spark.read.format("delta").table("bronze_sales")
log_message(f"Bronze records read: {get_row_count(df_bronze)}")

# ============================================================
# STEP 1: DROP EXACT DUPLICATES
# ============================================================
df_clean = df_bronze.dropDuplicates()
log_message(f"After deduplication: {get_row_count(df_clean)}")

# ============================================================
# STEP 2: DROP NULLS ON CRITICAL FIELDS
# ============================================================
critical_cols = ["OrderID", "OrderDate", "CustomerID", "ProductID", "Quantity", "UnitPrice", "TotalPrice"]
df_clean = df_clean.dropna(subset=critical_cols)
log_message(f"After null drop on critical cols: {get_row_count(df_clean)}")

# ============================================================
# STEP 3: CAST & PARSE DATA TYPES
# Date format in source: MM/dd/yyyy (e.g. 12/09/2024)
# ============================================================
df_clean = df_clean \
    .withColumn("OrderID",     col("OrderID").cast(IntegerType())) \
    .withColumn("CustomerID",  col("CustomerID").cast(IntegerType())) \
    .withColumn("ProductID",   col("ProductID").cast(IntegerType())) \
    .withColumn("Quantity",    col("Quantity").cast(IntegerType())) \
    .withColumn("UnitPrice",   col("UnitPrice").cast(DoubleType())) \
    .withColumn("TotalPrice",  col("TotalPrice").cast(DoubleType())) \
    .withColumn("OrderDate",   to_date(col("OrderDate"), "MM/dd/yyyy"))

# ============================================================
# STEP 4: STANDARDIZE STRING COLUMNS
# ============================================================
df_clean = df_clean \
    .withColumn("CustomerName",    initcap(trim(col("CustomerName")))) \
    .withColumn("CustomerEmail",   trim(col("CustomerEmail"))) \
    .withColumn("Country",         initcap(trim(col("Country")))) \
    .withColumn("ProductCategory", initcap(trim(col("ProductCategory")))) \
    .withColumn("ProductName",     initcap(trim(col("ProductName")))) \
    .withColumn("SalesRegion",     initcap(trim(col("SalesRegion"))))

# ============================================================
# STEP 5: FILTER OUT INVALID QUANTITIES / PRICES
# ============================================================
df_clean = df_clean.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0) &
    (col("TotalPrice") > 0)
)
log_message(f"After value range filter: {get_row_count(df_clean)}")

# ============================================================
# STEP 6: ADD TIME DIMENSION COLUMNS
# ============================================================
df_clean = df_clean \
    .withColumn("OrderYear",      year(col("OrderDate"))) \
    .withColumn("OrderMonth",     month(col("OrderDate"))) \
    .withColumn("OrderQuarter",   quarter(col("OrderDate"))) \
    .withColumn("OrderMonthName", date_format(col("OrderDate"), "MMMM"))

# ============================================================
# STEP 7: VALIDATE TotalPrice CONSISTENCY
# TotalPrice should equal Quantity * UnitPrice (within rounding tolerance)
# Flag records where discrepancy > 1%
# ============================================================
df_clean = df_clean.withColumn(
    "price_validation_flag",
    when(
        abs(col("TotalPrice") - (col("Quantity") * col("UnitPrice"))) /
        (col("Quantity") * col("UnitPrice")) > 0.01,
        lit("MISMATCH")
    ).otherwise(lit("OK"))
)

mismatch_count = df_clean.filter(col("price_validation_flag") == "MISMATCH").count()
log_message(f"TotalPrice mismatches flagged: {mismatch_count}")

# ============================================================
# STEP 8: ADD METADATA
# ============================================================
df_silver = df_clean \
    .withColumn("processed_timestamp", current_timestamp()) \
    .withColumn("layer", lit("silver"))

# ============================================================
# NULL REPORT
# ============================================================
check_nulls(df_silver)

# ============================================================
# WRITE TO DELTA LAKE — SILVER (Overwrite, idempotent)
# ============================================================
silver_output_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SILVER_PATH}/sales/"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_output_path)

# ============================================================
# REGISTER TABLE
# ============================================================
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_sales
    USING DELTA
    LOCATION '{silver_output_path}'
""")

log_message(f"Silver complete. Records written: {get_row_count(df_silver)}")
