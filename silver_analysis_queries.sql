-- ============================================================
-- Buy Anything Sales — Silver Layer Analysis Queries
-- Table   : silver_sales
-- Platform: Databricks SQL / Azure Synapse Analytics
-- Dataset : 1,000 orders | Jan–Dec 2024
-- Columns : OrderID, OrderDate, CustomerID, CustomerName,
--           CustomerEmail, Country, ProductID, ProductCategory,
--           ProductName, Quantity, UnitPrice, TotalPrice,
--           SalesRegion, OrderYear, OrderMonth, OrderQuarter,
--           OrderMonthName, price_validation_flag
-- ============================================================


-- ============================================================
-- QUERY 1: Total Revenue, Orders & Units Sold (Overall KPIs)
-- ============================================================
SELECT
    COUNT(DISTINCT OrderID)        AS TotalOrders,
    COUNT(DISTINCT CustomerID)     AS TotalCustomers,
    SUM(Quantity)                  AS TotalUnitsSold,
    ROUND(SUM(TotalPrice), 2)      AS TotalRevenue,
    ROUND(AVG(TotalPrice), 2)      AS AvgOrderValue,
    ROUND(MIN(TotalPrice), 2)      AS MinOrderValue,
    ROUND(MAX(TotalPrice), 2)      AS MaxOrderValue
FROM silver_sales;


-- ============================================================
-- QUERY 2: Monthly Revenue Trend (Jan–Dec 2024)
-- ============================================================
SELECT
    OrderYear,
    OrderMonth,
    OrderMonthName,
    OrderQuarter,
    ROUND(SUM(TotalPrice), 2)          AS MonthlyRevenue,
    COUNT(DISTINCT OrderID)            AS MonthlyOrders,
    SUM(Quantity)                      AS MonthlyUnitsSold,
    ROUND(AVG(TotalPrice), 2)          AS AvgOrderValue,
    ROUND(
        SUM(TotalPrice) - LAG(SUM(TotalPrice))
            OVER (ORDER BY OrderYear, OrderMonth), 2
    )                                  AS MoMRevenueDiff,
    ROUND(
        (SUM(TotalPrice) - LAG(SUM(TotalPrice))
            OVER (ORDER BY OrderYear, OrderMonth))
        / NULLIF(LAG(SUM(TotalPrice))
            OVER (ORDER BY OrderYear, OrderMonth), 0) * 100, 2
    )                                  AS MoMGrowthPct
FROM silver_sales
GROUP BY OrderYear, OrderMonth, OrderMonthName, OrderQuarter
ORDER BY OrderYear, OrderMonth;


-- ============================================================
-- QUERY 3: Revenue by Sales Region
-- ============================================================
SELECT
    SalesRegion,
    COUNT(DISTINCT OrderID)            AS TotalOrders,
    COUNT(DISTINCT CustomerID)         AS UniqueCustomers,
    SUM(Quantity)                      AS TotalUnitsSold,
    ROUND(SUM(TotalPrice), 2)          AS TotalRevenue,
    ROUND(AVG(TotalPrice), 2)          AS AvgOrderValue,
    ROUND(
        SUM(TotalPrice) * 100.0 /
        SUM(SUM(TotalPrice)) OVER (), 2
    )                                  AS RevenueSharePct
FROM silver_sales
GROUP BY SalesRegion
ORDER BY TotalRevenue DESC;


-- ============================================================
-- QUERY 4: Revenue by Product Category
-- ============================================================
SELECT
    ProductCategory,
    COUNT(DISTINCT OrderID)            AS TotalOrders,
    SUM(Quantity)                      AS TotalUnitsSold,
    ROUND(SUM(TotalPrice), 2)          AS TotalRevenue,
    ROUND(AVG(UnitPrice), 2)           AS AvgUnitPrice,
    ROUND(AVG(TotalPrice), 2)          AS AvgOrderValue,
    ROUND(
        SUM(TotalPrice) * 100.0 /
        SUM(SUM(TotalPrice)) OVER (), 2
    )                                  AS RevenueSharePct
FROM silver_sales
GROUP BY ProductCategory
ORDER BY TotalRevenue DESC;


-- ============================================================
-- QUERY 5: Revenue by Region AND Category (Cross-Tab)
-- ============================================================
SELECT
    SalesRegion,
    ProductCategory,
    COUNT(DISTINCT OrderID)            AS TotalOrders,
    SUM(Quantity)                      AS TotalUnitsSold,
    ROUND(SUM(TotalPrice), 2)          AS TotalRevenue,
    ROUND(AVG(TotalPrice), 2)          AS AvgOrderValue
FROM silver_sales
GROUP BY SalesRegion, ProductCategory
ORDER BY SalesRegion, TotalRevenue DESC;


-- ============================================================
-- QUERY 6: Top 10 Products by Revenue
-- ============================================================
SELECT
    ProductID,
    ProductName,
    ProductCategory,
    COUNT(DISTINCT OrderID)            AS TotalOrders,
    SUM(Quantity)                      AS TotalUnitsSold,
    ROUND(SUM(TotalPrice), 2)          AS TotalRevenue,
    ROUND(AVG(UnitPrice), 2)           AS AvgUnitPrice,
    DENSE_RANK() OVER (
        ORDER BY SUM(TotalPrice) DESC
    )                                  AS RevenueRank
FROM silver_sales
GROUP BY ProductID, ProductName, ProductCategory
ORDER BY TotalRevenue DESC
LIMIT 10;


-- ============================================================
-- QUERY 7: Top 10 Products by Category (Rank within Category)
-- ============================================================
SELECT
    ProductCategory,
    ProductName,
    ROUND(SUM(TotalPrice), 2)          AS TotalRevenue,
    SUM(Quantity)                      AS TotalUnitsSold,
    COUNT(DISTINCT OrderID)            AS TotalOrders,
    DENSE_RANK() OVER (
        PARTITION BY ProductCategory
        ORDER BY SUM(TotalPrice) DESC
    )                                  AS RankInCategory
FROM silver_sales
GROUP BY ProductCategory, ProductName
QUALIFY RankInCategory <= 5
ORDER BY ProductCategory, RankInCategory;


-- ============================================================
-- QUERY 8: Customer Segmentation — RFM Analysis
-- (Recency, Frequency, Monetary)
-- Reference date: 2024-12-31 (last date in dataset)
-- ============================================================
WITH customer_rfm AS (
    SELECT
        CustomerID,
        CustomerName,
        SalesRegion,
        Country,
        DATEDIFF('2024-12-31', MAX(OrderDate))  AS Recency_Days,
        COUNT(DISTINCT OrderID)                  AS Frequency,
        ROUND(SUM(TotalPrice), 2)                AS Monetary
    FROM silver_sales
    GROUP BY CustomerID, CustomerName, SalesRegion, Country
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY Recency_Days ASC)  AS R_Score,   -- lower recency = better
        NTILE(5) OVER (ORDER BY Frequency DESC)     AS F_Score,
        NTILE(5) OVER (ORDER BY Monetary DESC)      AS M_Score
    FROM customer_rfm
)
SELECT
    CustomerID,
    CustomerName,
    SalesRegion,
    Country,
    Recency_Days,
    Frequency,
    Monetary,
    R_Score,
    F_Score,
    M_Score,
    (R_Score + F_Score + M_Score)      AS RFM_TotalScore,
    CASE
        WHEN (R_Score + F_Score + M_Score) >= 13 THEN 'Champions'
        WHEN (R_Score + F_Score + M_Score) >= 10 THEN 'Loyal Customers'
        WHEN (R_Score + F_Score + M_Score) >= 7  THEN 'Potential Loyalists'
        WHEN R_Score >= 4 AND (F_Score + M_Score) < 6 THEN 'New Customers'
        WHEN R_Score <= 2 AND F_Score >= 3        THEN 'At Risk'
        ELSE 'Needs Attention'
    END                                AS CustomerSegment
FROM rfm_scored
ORDER BY RFM_TotalScore DESC;


-- ============================================================
-- QUERY 9: Quarterly Revenue Summary with QoQ Growth
-- ============================================================
SELECT
    OrderYear,
    OrderQuarter,
    ROUND(SUM(TotalPrice), 2)          AS QuarterlyRevenue,
    COUNT(DISTINCT OrderID)            AS TotalOrders,
    SUM(Quantity)                      AS TotalUnitsSold,
    ROUND(
        SUM(TotalPrice) - LAG(SUM(TotalPrice))
            OVER (ORDER BY OrderYear, OrderQuarter), 2
    )                                  AS QoQRevenueDiff,
    ROUND(
        (SUM(TotalPrice) - LAG(SUM(TotalPrice))
            OVER (ORDER BY OrderYear, OrderQuarter))
        / NULLIF(LAG(SUM(TotalPrice))
            OVER (ORDER BY OrderYear, OrderQuarter), 0) * 100, 2
    )                                  AS QoQGrowthPct
FROM silver_sales
GROUP BY OrderYear, OrderQuarter
ORDER BY OrderYear, OrderQuarter;


-- ============================================================
-- QUERY 10: Top 10 Customers by Revenue (Lifetime Value)
-- ============================================================
SELECT
    CustomerID,
    CustomerName,
    CustomerEmail,
    Country,
    SalesRegion,
    COUNT(DISTINCT OrderID)             AS TotalOrders,
    SUM(Quantity)                       AS TotalItemsPurchased,
    ROUND(SUM(TotalPrice), 2)           AS LifetimeValue,
    ROUND(AVG(TotalPrice), 2)           AS AvgOrderValue,
    MIN(OrderDate)                      AS FirstOrderDate,
    MAX(OrderDate)                      AS LastOrderDate,
    DATEDIFF(MAX(OrderDate), MIN(OrderDate)) AS CustomerAgeDays,
    DENSE_RANK() OVER (
        ORDER BY SUM(TotalPrice) DESC
    )                                   AS LTV_Rank
FROM silver_sales
GROUP BY CustomerID, CustomerName, CustomerEmail, Country, SalesRegion
ORDER BY LifetimeValue DESC
LIMIT 10;


-- ============================================================
-- QUERY 11: Country-Level Revenue Analysis
-- ============================================================
SELECT
    Country,
    SalesRegion,
    COUNT(DISTINCT CustomerID)          AS UniqueCustomers,
    COUNT(DISTINCT OrderID)             AS TotalOrders,
    SUM(Quantity)                       AS TotalUnitsSold,
    ROUND(SUM(TotalPrice), 2)           AS TotalRevenue,
    ROUND(AVG(TotalPrice), 2)           AS AvgOrderValue
FROM silver_sales
GROUP BY Country, SalesRegion
ORDER BY TotalRevenue DESC
LIMIT 20;


-- ============================================================
-- QUERY 12: Data Quality Check — Price Validation Summary
-- (TotalPrice vs Quantity * UnitPrice discrepancy)
-- ============================================================
SELECT
    price_validation_flag,
    COUNT(*)                            AS RecordCount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS Pct
FROM silver_sales
GROUP BY price_validation_flag;
