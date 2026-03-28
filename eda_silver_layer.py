# Databricks notebook source
# Sales BI Pipeline — Exploratory Data Analysis
# Run this notebook interactively to explore the Silver layer before building dashboards.

# COMMAND ----------
# %md
# ## Sales Data — Exploratory Data Analysis
# **Dataset**: SaleData.xlsx | **Columns**: OrderDate, Region, Manager, SalesMan, Item, Units, Unit_price, Sale_amt
# **Layer**: Silver (cleaned & enriched)

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

spark = SparkSession.builder.appName("SalesEDA").getOrCreate()

STORAGE_ACCOUNT = "your_storage_account"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/"

df = spark.read.format("delta").load(SILVER_PATH)
df.cache()
print(f"Total rows: {df.count()}")

# COMMAND ----------
# %md ### 1. Schema & Basic Stats
df.printSchema()
display(df.describe())

# COMMAND ----------
# %md ### 2. Null Check per Column
null_counts = df.select([
    count(when(isnull(c), c)).alias(c) for c in df.columns
])
display(null_counts)

# COMMAND ----------
# %md ### 3. Revenue by Region
region_rev = (
    df.groupBy("Region")
      .agg(round(sum("Sale_amt"), 2).alias("Revenue"),
           sum("Units").alias("Units_Sold"),
           count("*").alias("Orders"))
      .orderBy(desc("Revenue"))
)
display(region_rev)

# Matplotlib bar chart
region_pd = region_rev.toPandas()
fig, ax = plt.subplots(figsize=(8, 4))
ax.bar(region_pd["Region"], region_pd["Revenue"], color=["#1f77b4","#ff7f0e","#2ca02c"])
ax.set_title("Total Revenue by Region")
ax.set_xlabel("Region")
ax.set_ylabel("Revenue ($)")
for i, v in enumerate(region_pd["Revenue"]):
    ax.text(i, v + 1000, f"${v:,.0f}", ha="center", fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------
# %md ### 4. Product Performance
item_perf = (
    df.groupBy("Item", "Category")
      .agg(round(sum("Sale_amt"), 2).alias("Revenue"),
           sum("Units").alias("Units_Sold"))
      .orderBy(desc("Revenue"))
)
display(item_perf)

# COMMAND ----------
# %md ### 5. Monthly Revenue Trend
monthly = (
    df.groupBy("Year", "Month", "MonthName")
      .agg(round(sum("Sale_amt"), 2).alias("Monthly_Revenue"))
      .orderBy("Year", "Month")
)
monthly_pd = monthly.toPandas()
monthly_pd["Period"] = monthly_pd["MonthName"] + " " + monthly_pd["Year"].astype(str)

fig, ax = plt.subplots(figsize=(12, 4))
ax.plot(monthly_pd["Period"], monthly_pd["Monthly_Revenue"], marker="o", linewidth=2, color="#1f77b4")
ax.fill_between(monthly_pd["Period"], monthly_pd["Monthly_Revenue"], alpha=0.15)
ax.set_title("Monthly Revenue Trend")
ax.set_xlabel("Month")
ax.set_ylabel("Revenue ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------
# %md ### 6. Manager KPI Comparison
manager_kpi = (
    df.groupBy("Manager", "Region")
      .agg(round(sum("Sale_amt"), 2).alias("Revenue"),
           sum("Units").alias("Units"),
           count("*").alias("Orders"))
      .orderBy(desc("Revenue"))
)
display(manager_kpi)

# COMMAND ----------
# %md ### 7. Revenue Band Distribution
rev_band = (
    df.groupBy("RevenueBand")
      .agg(count("*").alias("Orders"),
           round(sum("Sale_amt"), 2).alias("Revenue"))
      .orderBy(desc("Revenue"))
)
display(rev_band)

# COMMAND ----------
# %md ### 8. Correlation Heatmap (Units vs Sale_amt vs Unit_price)
num_pd = df.select("Units", "Unit_price", "Sale_amt").toPandas()
fig, ax = plt.subplots(figsize=(5, 4))
sns.heatmap(num_pd.corr(), annot=True, fmt=".2f", cmap="Blues", ax=ax)
ax.set_title("Correlation: Numeric Features")
plt.tight_layout()
plt.show()

# COMMAND ----------
df.unpersist()
print("EDA complete.")
