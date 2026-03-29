-- ============================================================
-- Buy Anything Sales — Gold Layer Queries
-- Tables  : gold_revenue_by_region, gold_product_performance,
--           gold_sales_trend, gold_customer_summary
-- Platform: Databricks SQL / Power BI DirectQuery
-- ============================================================


-- ============================================================
-- QUERY 1: KPI Summary Card — Total Revenue, Orders, Customers
-- Source  : gold_revenue_by_region + gold_customer_summary
-- ============================================================
SELECT
    ROUND(SUM(TotalRevenue), 2)        AS GrandTotalRevenue,
    SUM(TotalOrders)                   AS GrandTotalOrders,
    SUM(TotalUnitsSold)                AS GrandTotalUnits,
    ROUND(AVG(AvgOrderValue), 2)       AS OverallAvgOrderValue
FROM gold_revenue_by_region;


-- ============================================================
-- QUERY 2: Revenue Trend — Monthly Line Chart
-- Source  : gold_sales_trend
-- ============================================================
SELECT
    OrderYear,
    OrderMonth,
    OrderMonthName,
    OrderQuarter,
    MonthlyRevenue,
    MonthlyOrders,
    MonthlyUnitsSold,
    AvgOrderValue,
    ROUND(
        MonthlyRevenue - LAG(MonthlyRevenue)
            OVER (ORDER BY OrderYear, OrderMonth), 2
    )                                  AS MoMRevenueDiff,
    ROUND(
        (MonthlyRevenue - LAG(MonthlyRevenue)
            OVER (ORDER BY OrderYear, OrderMonth))
        / NULLIF(LAG(MonthlyRevenue)
            OVER (ORDER BY OrderYear, OrderMonth), 0) * 100, 2
    )                                  AS MoMGrowthPct
FROM gold_sales_trend
ORDER BY OrderYear, OrderMonth;


-- ============================================================
-- QUERY 3: Revenue by Region — Bar Chart / Map
-- Source  : gold_revenue_by_region
-- ============================================================
SELECT
    SalesRegion,
    SUM(TotalRevenue)                  AS TotalRevenue,
    SUM(TotalOrders)                   AS TotalOrders,
    SUM(TotalUnitsSold)                AS TotalUnitsSold,
    ROUND(AVG(AvgOrderValue), 2)       AS AvgOrderValue,
    ROUND(
        SUM(TotalRevenue) * 100.0 /
        SUM(SUM(TotalRevenue)) OVER (), 2
    )                                  AS RevenueSharePct
FROM gold_revenue_by_region
GROUP BY SalesRegion
ORDER BY TotalRevenue DESC;


-- ============================================================
-- QUERY 4: Revenue by Category — Donut / Pie Chart
-- Source  : gold_revenue_by_region
-- ============================================================
SELECT
    ProductCategory,
    ROUND(SUM(TotalRevenue), 2)        AS TotalRevenue,
    SUM(TotalOrders)                   AS TotalOrders,
    SUM(TotalUnitsSold)                AS TotalUnitsSold,
    ROUND(AVG(AvgUnitPrice), 2)        AS AvgUnitPrice,
    ROUND(
        SUM(TotalRevenue) * 100.0 /
        SUM(SUM(TotalRevenue)) OVER (), 2
    )                                  AS RevenueSharePct
FROM gold_revenue_by_region
GROUP BY ProductCategory
ORDER BY TotalRevenue DESC;


-- ============================================================
-- QUERY 5: Region × Category Matrix — Heat Map
-- Source  : gold_revenue_by_region
-- ============================================================
SELECT
    SalesRegion,
    ProductCategory,
    ROUND(SUM(TotalRevenue), 2)        AS TotalRevenue,
    SUM(TotalOrders)                   AS TotalOrders,
    ROUND(AVG(AvgOrderValue), 2)       AS AvgOrderValue
FROM gold_revenue_by_region
GROUP BY SalesRegion, ProductCategory
ORDER BY SalesRegion, TotalRevenue DESC;


-- ============================================================
-- QUERY 6: Top 10 Products by Revenue — Table Visual
-- Source  : gold_product_performance
-- ============================================================
SELECT
    ProductID,
    ProductName,
    ProductCategory,
    TotalRevenue,
    TotalUnitsSold,
    TotalOrders,
    AvgUnitPrice,
    DENSE_RANK() OVER (
        ORDER BY TotalRevenue DESC
    )                                  AS RevenueRank
FROM gold_product_performance
ORDER BY TotalRevenue DESC
LIMIT 10;


-- ============================================================
-- QUERY 7: Best Product per Category — KPI Cards
-- Source  : gold_product_performance
-- ============================================================
SELECT
    ProductCategory,
    ProductName,
    TotalRevenue,
    TotalUnitsSold,
    TotalOrders,
    AvgUnitPrice
FROM (
    SELECT *,
        DENSE_RANK() OVER (
            PARTITION BY ProductCategory
            ORDER BY TotalRevenue DESC
        ) AS RankInCategory
    FROM gold_product_performance
)
WHERE RankInCategory = 1
ORDER BY TotalRevenue DESC;


-- ============================================================
-- QUERY 8: Top 10 Customers by Lifetime Value — Leaderboard
-- Source  : gold_customer_summary
-- ============================================================
SELECT
    CustomerID,
    CustomerName,
    Country,
    SalesRegion,
    LifetimeValue,
    TotalOrders,
    TotalItemsPurchased,
    AvgOrderValue,
    FirstOrderDate,
    LastOrderDate,
    DATEDIFF(LastOrderDate, FirstOrderDate) AS CustomerAgeDays,
    DENSE_RANK() OVER (
        ORDER BY LifetimeValue DESC
    )                                       AS LTV_Rank
FROM gold_customer_summary
ORDER BY LifetimeValue DESC
LIMIT 10;


-- ============================================================
-- QUERY 9: Customer Distribution by Region & Country (Top 15 Countries)
-- Source  : gold_customer_summary
-- ============================================================
SELECT
    Country,
    SalesRegion,
    COUNT(DISTINCT CustomerID)          AS UniqueCustomers,
    ROUND(SUM(LifetimeValue), 2)        AS TotalRevenue,
    ROUND(AVG(AvgOrderValue), 2)        AS AvgOrderValue
FROM gold_customer_summary
GROUP BY Country, SalesRegion
ORDER BY TotalRevenue DESC
LIMIT 15;


-- ============================================================
-- QUERY 10: Quarterly Revenue Summary with QoQ Growth
-- Source  : gold_sales_trend
-- ============================================================
SELECT
    OrderYear,
    OrderQuarter,
    ROUND(SUM(MonthlyRevenue), 2)       AS QuarterlyRevenue,
    SUM(MonthlyOrders)                  AS QuarterlyOrders,
    SUM(MonthlyUnitsSold)               AS QuarterlyUnitsSold,
    ROUND(
        SUM(MonthlyRevenue) - LAG(SUM(MonthlyRevenue))
            OVER (ORDER BY OrderYear, OrderQuarter), 2
    )                                   AS QoQRevenueDiff,
    ROUND(
        (SUM(MonthlyRevenue) - LAG(SUM(MonthlyRevenue))
            OVER (ORDER BY OrderYear, OrderQuarter))
        / NULLIF(LAG(SUM(MonthlyRevenue))
            OVER (ORDER BY OrderYear, OrderQuarter), 0) * 100, 2
    )                                   AS QoQGrowthPct
FROM gold_sales_trend
GROUP BY OrderYear, OrderQuarter
ORDER BY OrderYear, OrderQuarter;
