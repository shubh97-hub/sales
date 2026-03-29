# Architecture Notes

## Overview

This project implements a cloud-native, scalable data pipeline on **Microsoft Azure** following the **Medallion Architecture** pattern. The pipeline transforms raw transactional sales data into analytics-ready Gold layer datasets consumed by Power BI.

---

## Components

### 1. Azure Data Lake Storage Gen2 (ADLS)
- Hosts all data layers in separate containers: `raw`, `bronze`, `silver`, `gold`
- Hierarchical namespace enabled for efficient file operations
- Role-Based Access Control (RBAC) for security

### 2. Azure Data Factory (ADF)
- Orchestrates the full pipeline: trigger → bronze → silver → gold → validation
- Scheduled trigger: daily at 02:00 AM UTC
- Monitors pipeline runs and sends failure alerts
- Pipeline definition stored in `adf_pipelines/pipeline_config.json`

### 3. Azure Databricks + PySpark
- Databricks cluster runs all transformation logic
- PySpark enables distributed processing for large datasets
- Notebooks executed in sequence by ADF

### 4. Delta Lake
- All layers stored as Delta tables for ACID transactions
- Supports time travel (version history) for auditing
- Schema enforcement prevents corrupt data from propagating
- OPTIMIZE and ZORDER applied on Gold tables for BI query speed

### 5. Power BI
- Connects directly to Gold Delta tables via Databricks connector
- Dashboards use DirectQuery for real-time data
- Reports: Revenue Trend, Regional Performance, Product Rankings, Customer Summary

---

## Medallion Architecture

```
RAW (ADLS)
  └── CSV / JSON files from source systems

BRONZE (Delta)
  └── Exact copy of raw + metadata columns (ingestion_ts, source_file)
  └── Append-only; no transformations applied
  └── Acts as audit log / source of truth

SILVER (Delta)
  └── Cleaned, typed, validated, deduplicated
  └── Schema enforcement; nulls on critical fields removed
  └── Derived columns: total_sales, net_sales
  └── Overwrite mode (idempotent)

GOLD (Delta)
  └── Business-level aggregations
  └── One table per analytical use case (revenue, product, trend, customer)
  └── Optimized for Power BI DirectQuery
  └── Overwrite mode
```

---

## Pipeline Run Flow

```
[ADF Trigger]
     │
     ▼
01_bronze_ingestion.py     → Reads raw CSV → Writes to Delta Bronze
     │
     ▼
02_silver_transformation.py → Cleans & casts → Writes to Delta Silver
     │
     ▼
03_gold_aggregation.py      → Aggregates → Writes 4 Gold tables
     │
     ▼
04_data_validation.py       → Quality checks → Logs pass/fail per rule
     │
     ▼
[Power BI] ← Reads Gold tables via DirectQuery
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Medallion Architecture | Separates concerns; enables reprocessing from any layer |
| Delta Lake over Parquet | ACID transactions + schema enforcement + time travel |
| Overwrite on Silver/Gold | Ensures idempotency; re-running pipeline gives same result |
| Append on Bronze | Preserves full history of raw data as an immutable audit log |
| Explicit schemas | Prevents silent type coercion on bad source data |
| Validation notebook | Catches issues before dashboards show bad numbers |
