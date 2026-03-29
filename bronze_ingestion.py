# Databricks Notebook - Bronze Layer Ingestion
# Dataset : Buy Anything Sales (1,000 orders, Jan–Dec 2024)
# Layer   : Bronze (Raw Ingestion)
# Description: Reads raw CSV from ADLS and writes to Delta Lake Bronze table.

# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from scripts.config import STORAGE_ACCOUNT, BRONZE_PATH, RAW_PATH
from scripts.utils import log_message, get_row_count

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("BuyAnything_Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

log_message("Bronze ingestion started — Buy Anything Sales 2024.")

# ============================================================
# READ RAW CSV FROM ADLS
# Columns: OrderID, OrderDate, CustomerID, CustomerName,
#          CustomerEmail, Country, ProductID, ProductCategory,
#          ProductName, Quantity, UnitPrice, TotalPrice, SalesRegion
# ============================================================
raw_path = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/{RAW_PATH}/buy_anything_sales.csv"

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(raw_path)

log_message(f"Raw records loaded: {get_row_count(df_raw)}")
# Expected: 1,000 rows | 13 columns

# ============================================================
# ADD METADATA COLUMNS
# ============================================================
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .withColumn("layer", lit("bronze"))

# ============================================================
# WRITE TO DELTA LAKE — BRONZE (Append Only)
# ============================================================
bronze_output_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/{BRONZE_PATH}/sales/"

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(bronze_output_path)

log_message(f"Bronze write complete. Records written: {get_row_count(df_bronze)}")

# ============================================================
# REGISTER AS DELTA TABLE
# ============================================================
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze_sales
    USING DELTA
    LOCATION '{bronze_output_path}'
""")

log_message("Bronze table registered in metastore. Ingestion complete.")
