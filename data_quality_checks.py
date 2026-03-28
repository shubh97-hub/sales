# =============================================================================
# Data Quality & Validation Checks
# Sales BI Pipeline | Medallion Architecture
# =============================================================================
# Runs suite of DQ checks on the Silver layer and logs results.
# ADF calls this notebook before triggering the Gold aggregation step.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, countDistinct
import logging, json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("SalesPipeline_DataQuality") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

STORAGE_ACCOUNT = "your_storage_account"
SILVER_PATH     = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/"
DQ_LOG_PATH     = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/dq_logs/"

THRESHOLDS = {
    "max_null_pct":       5.0,   # alert if >5% nulls in any column
    "min_row_count":     10,     # fail if fewer than 10 rows (guards against empty loads)
    "max_negative_sale":  0,     # zero tolerance for negative Sale_amt
    "max_zero_units":     0,     # zero tolerance for 0-unit rows
}


def run_dq_checks(df) -> list[dict]:
    results = []
    total = df.count()

    def record(check, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        logger.info("[DQ] %-40s %s | %s", check, status, detail)
        results.append({"check": check, "status": status, "detail": detail})

    # 1. Row count gate
    record("min_row_count",
           total >= THRESHOLDS["min_row_count"],
           f"rows={total}, threshold={THRESHOLDS['min_row_count']}")

    # 2. Null / missing checks per column
    critical_cols = ["OrderDate", "Region", "Manager", "SalesMan",
                     "Item", "Units", "Unit_price", "Sale_amt"]
    for c in critical_cols:
        null_count = df.filter(isnull(col(c)) | isnan(col(c)) if df.schema[c].dataType.typeName()
                               in ("double", "float") else isnull(col(c))).count()
        null_pct   = round((null_count / total) * 100, 2) if total else 0
        record(f"null_check:{c}",
               null_pct <= THRESHOLDS["max_null_pct"],
               f"null_pct={null_pct}%, count={null_count}")

    # 3. Negative Sale_amt
    neg_count = df.filter(col("Sale_amt") < 0).count()
    record("no_negative_sale_amt",
           neg_count <= THRESHOLDS["max_negative_sale"],
           f"negative_rows={neg_count}")

    # 4. Zero / negative Units
    bad_units = df.filter(col("Units") <= 0).count()
    record("no_zero_or_negative_units",
           bad_units <= THRESHOLDS["max_zero_units"],
           f"bad_unit_rows={bad_units}")

    # 5. Sale_amt consistency (Units * Unit_price should match Sale_amt ± 1)
    mismatch = df.filter(
        (col("Sale_amt") - col("Units") * col("Unit_price")).between(-1, 1) == False
    ).count()
    record("sale_amt_consistency",
           mismatch == 0,
           f"mismatch_rows={mismatch}")

    # 6. Region domain check
    valid_regions = ["EAST", "CENTRAL", "WEST"]
    invalid_region = df.filter(~col("Region").isin(valid_regions)).count()
    record("region_domain_check",
           invalid_region == 0,
           f"invalid_region_rows={invalid_region}")

    # 7. Date range sanity (OrderDate should be between 2015 and today)
    out_of_range = df.filter(
        (col("OrderDate") < "2015-01-01") | (col("OrderDate") > "2030-12-31")
    ).count()
    record("orderdate_range_check",
           out_of_range == 0,
           f"out_of_range_dates={out_of_range}")

    # 8. Duplicate row check
    dupes = total - df.dropDuplicates(["OrderDate", "Region", "SalesMan", "Item"]).count()
    record("no_duplicate_rows",
           dupes == 0,
           f"duplicate_rows={dupes}")

    return results


def save_dq_log(results: list[dict]):
    payload = {
        "run_timestamp": datetime.utcnow().isoformat(),
        "total_checks":  len(results),
        "passed":        sum(1 for r in results if r["status"] == "PASS"),
        "failed":        sum(1 for r in results if r["status"] == "FAIL"),
        "checks":        results,
    }
    log_df = spark.createDataFrame([payload])
    log_df.write.format("delta").mode("append").save(DQ_LOG_PATH)
    logger.info("DQ log saved to: %s", DQ_LOG_PATH)
    return payload


if __name__ == "__main__":
    df = spark.read.format("delta").load(SILVER_PATH)
    results = run_dq_checks(df)
    summary = save_dq_log(results)

    failed = summary["failed"]
    print(f"\n[DQ] Checks: {summary['total_checks']} | "
          f"Passed: {summary['passed']} | Failed: {failed}")

    if failed > 0:
        print("[DQ] PIPELINE HALTED — data quality failures detected. Review logs.")
        raise SystemExit(1)
    else:
        print("[DQ] All checks passed — proceeding to Gold layer.")

    spark.stop()
