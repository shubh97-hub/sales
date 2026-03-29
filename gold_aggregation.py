# Databricks Notebook - Gold Layer Aggregation
# Dataset : Buy Anything Sales (1,000 orders, Jan–Dec 2024)
# Layer   : Gold (Business-Ready Tables for Power BI)
# Description: Creates 4 aggregated Gold tables from Silver layer.

# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, round, current_timestamp
)
from scripts.config import STORAGE_ACCOUNT, GOLD_PATH
from scripts.utils import log_message, get_row_count

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("BuyAnything_Gold_Aggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

log_message("Gold aggregation started.")

# ============================================================
# READ FROM SILVER
# ============================================================
df = spark.read.format("delta").table("silver_sales")
log_message(f"Silver records read: {get_row_count(df)}")

# ============================================================
# HELPER: Write Gold Table
# ============================================================
def write_gold(df_gold, table_name):
    path = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/{GOLD_PATH}/{table_name}/"
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA LOCATION '{path}'
    """)
    log_message(f"Written: {table_name} ({get_row_count(df_gold)} rows)")

# ============================================================
# GOLD TABLE 1: Revenue by Region & Category (Monthly)
# Used for: Regional performance dashboard, category drilldown
# ============================================================
df_revenue = df.groupBy(
    "OrderYear", "OrderQuarter", "OrderMonth", "OrderMonthName",
    "SalesRegion", "ProductCategory"
).agg(
    round(sum("TotalPrice"), 2).alias("TotalRevenue"),
    count("OrderID").alias("TotalOrders"),
    sum("Quantity").alias("TotalUnitsSold"),
    round(avg("TotalPrice"), 2).alias("AvgOrderValue"),
    round(avg("UnitPrice"), 2).alias("AvgUnitPrice")
).withColumn("processed_timestamp", current_timestamp()) \
 .orderBy("OrderYear", "OrderMonth", "SalesRegion")

write_gold(df_revenue, "gold_revenue_by_region")

# ============================================================
# GOLD TABLE 2: Product Performance Ranking
# Used for: Top products KPI card, category-level analysis
# Categories: Electronics, Sports, Clothing, Beauty, Furniture
# ============================================================
df_product = df.groupBy(
    "ProductID", "ProductName", "ProductCategory"
).agg(
    round(sum("TotalPrice"), 2).alias("TotalRevenue"),
    sum("Quantity").alias("TotalUnitsSold"),
    count("OrderID").alias("TotalOrders"),
    round(avg("UnitPrice"), 2).alias("AvgUnitPrice"),
    round(max("UnitPrice"), 2).alias("MaxUnitPrice"),
    round(min("UnitPrice"), 2).alias("MinUnitPrice")
).withColumn("processed_timestamp", current_timestamp()) \
 .orderBy(col("TotalRevenue").desc())

write_gold(df_product, "gold_product_performance")

# ============================================================
# GOLD TABLE 3: Monthly Sales Trend (MoM)
# Used for: Revenue trend line chart, YoY comparison
# ============================================================
df_trend = df.groupBy(
    "OrderYear", "OrderQuarter", "OrderMonth", "OrderMonthName"
).agg(
    round(sum("TotalPrice"), 2).alias("MonthlyRevenue"),
    count("OrderID").alias("MonthlyOrders"),
    sum("Quantity").alias("MonthlyUnitsSold"),
    round(avg("TotalPrice"), 2).alias("AvgOrderValue")
).withColumn("processed_timestamp", current_timestamp()) \
 .orderBy("OrderYear", "OrderMonth")

write_gold(df_trend, "gold_sales_trend")

# ============================================================
# GOLD TABLE 4: Customer Summary
# Used for: Customer value analysis, retention insights
# ============================================================
df_customer = df.groupBy(
    "CustomerID", "CustomerName", "CustomerEmail", "Country", "SalesRegion"
).agg(
    round(sum("TotalPrice"), 2).alias("LifetimeValue"),
    count("OrderID").alias("TotalOrders"),
    sum("Quantity").alias("TotalItemsPurchased"),
    round(avg("TotalPrice"), 2).alias("AvgOrderValue"),
    min("OrderDate").alias("FirstOrderDate"),
    max("OrderDate").alias("LastOrderDate")
).withColumn("processed_timestamp", current_timestamp()) \
 .orderBy(col("LifetimeValue").desc())

write_gold(df_customer, "gold_customer_summary")

log_message("All 4 Gold tables written. Pipeline complete.")
