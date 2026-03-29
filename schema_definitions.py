# scripts/schema_definitions.py
# Explicit schema definitions for each Medallion layer.
# Based on: Buy Anything Sales dataset (1,000 orders, Jan–Dec 2024)

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType, TimestampType
)

# ============================================================
# BRONZE SCHEMA
# Raw ingestion — all fields as strings + metadata columns
# Source columns: OrderID, OrderDate, CustomerID, CustomerName,
#                 CustomerEmail, Country, ProductID, ProductCategory,
#                 ProductName, Quantity, UnitPrice, TotalPrice, SalesRegion
# ============================================================
BRONZE_SCHEMA = StructType([
    StructField("OrderID",           StringType(),    nullable=True),
    StructField("OrderDate",         StringType(),    nullable=True),   # Raw MM/dd/yyyy string
    StructField("CustomerID",        StringType(),    nullable=True),
    StructField("CustomerName",      StringType(),    nullable=True),
    StructField("CustomerEmail",     StringType(),    nullable=True),
    StructField("Country",           StringType(),    nullable=True),
    StructField("ProductID",         StringType(),    nullable=True),
    StructField("ProductCategory",   StringType(),    nullable=True),
    StructField("ProductName",       StringType(),    nullable=True),
    StructField("Quantity",          StringType(),    nullable=True),
    StructField("UnitPrice",         StringType(),    nullable=True),
    StructField("TotalPrice",        StringType(),    nullable=True),
    StructField("SalesRegion",       StringType(),    nullable=True),
    # Metadata
    StructField("ingestion_timestamp", TimestampType(), nullable=True),
    StructField("source_file",       StringType(),    nullable=True),
    StructField("layer",             StringType(),    nullable=True),
])

# ============================================================
# SILVER SCHEMA
# Typed, cleaned, validated — ready for analytics
# ============================================================
SILVER_SCHEMA = StructType([
    StructField("OrderID",              IntegerType(),  nullable=False),
    StructField("OrderDate",            DateType(),     nullable=False),
    StructField("CustomerID",           IntegerType(),  nullable=True),
    StructField("CustomerName",         StringType(),   nullable=True),
    StructField("CustomerEmail",        StringType(),   nullable=True),
    StructField("Country",              StringType(),   nullable=True),
    StructField("ProductID",            IntegerType(),  nullable=False),
    StructField("ProductCategory",      StringType(),   nullable=True),  # Electronics, Sports, Clothing, Beauty, Furniture
    StructField("ProductName",          StringType(),   nullable=True),
    StructField("Quantity",             IntegerType(),  nullable=False),
    StructField("UnitPrice",            DoubleType(),   nullable=False),
    StructField("TotalPrice",           DoubleType(),   nullable=False),
    StructField("SalesRegion",          StringType(),   nullable=True),  # North, South, East, West, Central
    # Derived time dimensions
    StructField("OrderYear",            IntegerType(),  nullable=True),
    StructField("OrderMonth",           IntegerType(),  nullable=True),
    StructField("OrderQuarter",         IntegerType(),  nullable=True),
    StructField("OrderMonthName",       StringType(),   nullable=True),
    # Quality flag
    StructField("price_validation_flag", StringType(), nullable=True),  # OK | MISMATCH
    # Metadata
    StructField("processed_timestamp",  TimestampType(), nullable=True),
    StructField("layer",                StringType(),   nullable=True),
])

# ============================================================
# GOLD: Revenue by Region
# ============================================================
GOLD_REVENUE_SCHEMA = StructType([
    StructField("OrderYear",          IntegerType(),  nullable=False),
    StructField("OrderQuarter",       IntegerType(),  nullable=False),
    StructField("OrderMonth",         IntegerType(),  nullable=False),
    StructField("OrderMonthName",     StringType(),   nullable=True),
    StructField("SalesRegion",        StringType(),   nullable=False),
    StructField("ProductCategory",    StringType(),   nullable=False),
    StructField("TotalRevenue",       DoubleType(),   nullable=False),
    StructField("TotalOrders",        IntegerType(),  nullable=False),
    StructField("TotalUnitsSold",     IntegerType(),  nullable=False),
    StructField("AvgOrderValue",      DoubleType(),   nullable=True),
    StructField("AvgUnitPrice",       DoubleType(),   nullable=True),
    StructField("processed_timestamp", TimestampType(), nullable=True),
])

# ============================================================
# GOLD: Product Performance
# ============================================================
GOLD_PRODUCT_SCHEMA = StructType([
    StructField("ProductID",          IntegerType(),  nullable=False),
    StructField("ProductName",        StringType(),   nullable=False),
    StructField("ProductCategory",    StringType(),   nullable=True),
    StructField("TotalRevenue",       DoubleType(),   nullable=False),
    StructField("TotalUnitsSold",     IntegerType(),  nullable=False),
    StructField("TotalOrders",        IntegerType(),  nullable=False),
    StructField("AvgUnitPrice",       DoubleType(),   nullable=True),
    StructField("MaxUnitPrice",       DoubleType(),   nullable=True),
    StructField("MinUnitPrice",       DoubleType(),   nullable=True),
    StructField("processed_timestamp", TimestampType(), nullable=True),
])
