# =============================================================================
# Gold Layer - Business Aggregations for Power BI
# Sales BI Pipeline | Medallion Architecture
# =============================================================================
# Reads Silver Delta and produces curated Gold Delta tables:
#   gold_sales_summary      — overall KPI summary
#   gold_region_performance — revenue & units by region/quarter
#   gold_item_performance   — product-level ranking & revenue share
#   gold_manager_kpi        — manager scorecard
#   gold_monthly_trend      — monthly revenue trend for time-series visuals
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    round as spark_round, rank, desc, lit, current_timestamp,
    percent_rank, ntile
)
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SalesPipeline_Gold_Aggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = "your_storage_account"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/"
GOLD_BASE   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales"

def gold_path(table: str) -> str:
    return f"{GOLD_BASE}/{table}/"

def write_gold(df, table_name: str):
    path = gold_path(table_name)
    (
        df.withColumn("_aggregated_at", current_timestamp())
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(path)
    )
    logger.info("Written Gold table '%s' → %s  (%d rows)", table_name, path, df.count())

# ---------------------------------------------------------------------------
# Load Silver
# ---------------------------------------------------------------------------
def build_gold():
    logger.info("Loading Silver Delta from: %s", SILVER_PATH)
    df = spark.read.format("delta").load(SILVER_PATH)
    df.cache()

    # ------------------------------------------------------------------
    # 1. Overall KPI Summary
    # ------------------------------------------------------------------
    df_summary = df.agg(
        spark_sum("Sale_amt").alias("Total_Revenue"),
        spark_sum("Units").alias("Total_Units_Sold"),
        count("*").alias("Total_Orders"),
        avg("Sale_amt").alias("Avg_Order_Value"),
        spark_max("Sale_amt").alias("Max_Order_Value"),
        spark_min("Sale_amt").alias("Min_Order_Value"),
    ).withColumn("Avg_Order_Value", spark_round(col("Avg_Order_Value"), 2))

    write_gold(df_summary, "gold_sales_summary")

    # ------------------------------------------------------------------
    # 2. Region Performance by Year & Quarter
    # ------------------------------------------------------------------
    df_region = (
        df.groupBy("Region", "Year", "Quarter")
          .agg(
              spark_sum("Sale_amt").alias("Revenue"),
              spark_sum("Units").alias("Units_Sold"),
              count("*").alias("Orders"),
              avg("Sale_amt").alias("Avg_Sale_amt"),
          )
          .withColumn("Revenue",       spark_round(col("Revenue"), 2))
          .withColumn("Avg_Sale_amt",  spark_round(col("Avg_Sale_amt"), 2))
          .orderBy("Year", "Quarter", desc("Revenue"))
    )

    # Revenue rank within each Year-Quarter
    window_rq = Window.partitionBy("Year", "Quarter").orderBy(desc("Revenue"))
    df_region = df_region.withColumn("Revenue_Rank", rank().over(window_rq))

    write_gold(df_region, "gold_region_performance")

    # ------------------------------------------------------------------
    # 3. Item (Product) Performance
    # ------------------------------------------------------------------
    total_rev = df.agg(spark_sum("Sale_amt")).collect()[0][0]

    df_item = (
        df.groupBy("Item", "Category")
          .agg(
              spark_sum("Sale_amt").alias("Revenue"),
              spark_sum("Units").alias("Units_Sold"),
              count("*").alias("Orders"),
              avg("Unit_price").alias("Avg_Unit_Price"),
          )
          .withColumn("Revenue",        spark_round(col("Revenue"), 2))
          .withColumn("Avg_Unit_Price", spark_round(col("Avg_Unit_Price"), 2))
          .withColumn("Revenue_Share_Pct",
                      spark_round((col("Revenue") / lit(total_rev)) * 100, 2))
          .orderBy(desc("Revenue"))
    )

    window_item = Window.orderBy(desc("Revenue"))
    df_item = df_item.withColumn("Product_Rank", rank().over(window_item))

    write_gold(df_item, "gold_item_performance")

    # ------------------------------------------------------------------
    # 4. Manager KPI Scorecard
    # ------------------------------------------------------------------
    df_manager = (
        df.groupBy("Manager", "Region")
          .agg(
              spark_sum("Sale_amt").alias("Revenue"),
              spark_sum("Units").alias("Units_Sold"),
              count("*").alias("Orders"),
              avg("Sale_amt").alias("Avg_Deal_Size"),
          )
          .withColumn("Revenue",       spark_round(col("Revenue"), 2))
          .withColumn("Avg_Deal_Size", spark_round(col("Avg_Deal_Size"), 2))
          .orderBy(desc("Revenue"))
    )

    window_mgr = Window.orderBy(desc("Revenue"))
    df_manager = df_manager.withColumn("Manager_Rank", rank().over(window_mgr))

    write_gold(df_manager, "gold_manager_kpi")

    # ------------------------------------------------------------------
    # 5. Monthly Revenue Trend (for time-series Power BI visuals)
    # ------------------------------------------------------------------
    df_trend = (
        df.groupBy("Year", "Month", "MonthName", "Quarter")
          .agg(
              spark_sum("Sale_amt").alias("Monthly_Revenue"),
              spark_sum("Units").alias("Monthly_Units"),
              count("*").alias("Monthly_Orders"),
          )
          .withColumn("Monthly_Revenue", spark_round(col("Monthly_Revenue"), 2))
          .orderBy("Year", "Month")
    )

    write_gold(df_trend, "gold_monthly_trend")

    df.unpersist()
    logger.info("All Gold tables written successfully.")

if __name__ == "__main__":
    build_gold()
    print("[Gold] Aggregation complete — all Gold tables ready for Power BI.")
    spark.stop()
