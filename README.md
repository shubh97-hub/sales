Architecture Overview
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE FLOW                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Source Data                                                       │
│       │                                                             │
│       ▼                                                             │
│   ┌─────────────────────────────┐                                   │
│   │   Azure Data Factory (ADF)  │  ← Orchestration & Scheduling    │
│   └─────────────┬───────────────┘                                   │
│                 │                                                   │
│       ┌─────────▼──────────┐                                        │
│       │   BRONZE LAYER     │  ← Raw ingestion, no transformations  │
│       │   (Delta Lake)     │                                        │
│       └─────────┬──────────┘                                        │
│                 │                                                   │
│       ┌─────────▼──────────┐                                        │
│       │   SILVER LAYER     │  ← Cleaned, validated, standardised   │
│       │  (PySpark / DBX)   │                                        │
│       └─────────┬──────────┘                                        │
│                 │                                                   │
│       ┌─────────▼──────────┐                                        │
│       │    GOLD LAYER      │  ← Aggregated KPIs, business-ready    │
│       │   (Delta Lake)     │                                        │
│       └─────────┬──────────┘                                        │
│                 │                                                   │
│       ┌─────────▼──────────┐                                        │
│       │    Power BI        │  ← Interactive Dashboards             │
│       │   Dashboard        │                                        │
│       └────────────────────┘                                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘


Project Structure
sales-bi-pipeline/
│
├── 📂 adf_pipelines/
│   ├── pipeline_ingest_bronze.json       # ADF pipeline — raw ingestion
│   ├── pipeline_transform_silver.json    # ADF pipeline — silver transformation
│   └── pipeline_aggregate_gold.json      # ADF pipeline — gold aggregation
│
├── 📂 databricks_notebooks/
│   ├── 01_bronze_ingestion.py            # Raw data landing
│   ├── 02_silver_cleaning.py             # Data cleaning & validation
│   ├── 03_gold_aggregation.py            # Business-level aggregations
│   └── 04_data_quality_checks.py         # Row counts, schema checks
│
├── 📂 sql_queries/
│   ├── revenue_by_region.sql
│   ├── sales_trend_analysis.sql
│   └── kpi_actual_vs_target.sql
│
├── 📂 powerbi/
│   └── Sales_BI_Dashboard.pbix           # Power BI report file
│
├── 📂 docs/
│   └── architecture_diagram.png
│
└── README.md

📈 Key Results

⚡ Automated ETL pipeline eliminating manual data processing effort
🔒 100% data consistency via Delta Lake ACID transactions
📊 Real-time KPI dashboards auto-refreshed after every pipeline run
🏗️ Medallion Architecture ensuring reusable, high-quality data at every layer
🚀 Scalable PySpark compute on Databricks — handles enterprise-scale datasets
