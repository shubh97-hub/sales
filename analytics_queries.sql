-- =============================================================================
-- Sales BI Pipeline — Analytics SQL Queries
-- Run against Gold layer Delta tables via Databricks SQL Warehouse
-- or imported into Power BI as DirectQuery / imported tables.
-- =============================================================================


-- =============================================================================
-- 1. Total Revenue KPIs
-- =============================================================================
SELECT
    ROUND(SUM(Sale_amt), 2)                        AS Total_Revenue,
    SUM(Units)                                      AS Total_Units_Sold,
    COUNT(*)                                        AS Total_Orders,
    ROUND(AVG(Sale_amt), 2)                        AS Avg_Order_Value,
    ROUND(MAX(Sale_amt), 2)                        AS Max_Order_Value,
    ROUND(MIN(Sale_amt), 2)                        AS Min_Order_Value
FROM silver_sales;


-- =============================================================================
-- 2. Revenue by Region
-- =============================================================================
SELECT
    Region,
    ROUND(SUM(Sale_amt), 2)                        AS Total_Revenue,
    SUM(Units)                                      AS Total_Units,
    COUNT(*)                                        AS Orders,
    ROUND(AVG(Sale_amt), 2)                        AS Avg_Sale,
    ROUND(SUM(Sale_amt) * 100.0 /
          SUM(SUM(Sale_amt)) OVER (), 2)            AS Revenue_Share_Pct
FROM silver_sales
GROUP BY Region
ORDER BY Total_Revenue DESC;


-- =============================================================================
-- 3. Revenue by Region and Quarter (for quarterly trend analysis)
-- =============================================================================
SELECT
    Region,
    Year,
    Quarter,
    ROUND(SUM(Sale_amt), 2)                        AS Quarterly_Revenue,
    SUM(Units)                                      AS Quarterly_Units,
    COUNT(*)                                        AS Orders,
    RANK() OVER (PARTITION BY Year, Quarter
                 ORDER BY SUM(Sale_amt) DESC)       AS Revenue_Rank
FROM silver_sales
GROUP BY Region, Year, Quarter
ORDER BY Year, Quarter, Revenue_Rank;


-- =============================================================================
-- 4. Product Performance Ranking
-- =============================================================================
SELECT
    Item,
    Category,
    ROUND(SUM(Sale_amt), 2)                        AS Total_Revenue,
    SUM(Units)                                      AS Total_Units_Sold,
    COUNT(*)                                        AS Orders,
    ROUND(AVG(Unit_price), 2)                      AS Avg_Unit_Price,
    ROUND(SUM(Sale_amt) * 100.0 /
          SUM(SUM(Sale_amt)) OVER (), 2)            AS Revenue_Share_Pct,
    RANK() OVER (ORDER BY SUM(Sale_amt) DESC)      AS Product_Rank
FROM silver_sales
GROUP BY Item, Category
ORDER BY Product_Rank;


-- =============================================================================
-- 5. Manager Scorecard
-- =============================================================================
SELECT
    Manager,
    Region,
    ROUND(SUM(Sale_amt), 2)                        AS Total_Revenue,
    SUM(Units)                                      AS Total_Units,
    COUNT(*)                                        AS Total_Orders,
    ROUND(AVG(Sale_amt), 2)                        AS Avg_Deal_Size,
    ROUND(SUM(Sale_amt) * 100.0 /
          SUM(SUM(Sale_amt)) OVER (), 2)            AS Revenue_Share_Pct,
    RANK() OVER (ORDER BY SUM(Sale_amt) DESC)      AS Manager_Rank
FROM silver_sales
GROUP BY Manager, Region
ORDER BY Total_Revenue DESC;


-- =============================================================================
-- 6. Salesperson Performance
-- =============================================================================
SELECT
    SalesMan,
    Manager,
    Region,
    ROUND(SUM(Sale_amt), 2)                        AS Total_Revenue,
    SUM(Units)                                      AS Total_Units,
    COUNT(*)                                        AS Orders,
    ROUND(AVG(Sale_amt), 2)                        AS Avg_Sale,
    RANK() OVER (ORDER BY SUM(Sale_amt) DESC)      AS Sales_Rank
FROM silver_sales
GROUP BY SalesMan, Manager, Region
ORDER BY Total_Revenue DESC;


-- =============================================================================
-- 7. Monthly Revenue Trend
-- =============================================================================
SELECT
    Year,
    Month,
    MonthName,
    Quarter,
    ROUND(SUM(Sale_amt), 2)                        AS Monthly_Revenue,
    SUM(Units)                                      AS Monthly_Units,
    COUNT(*)                                        AS Monthly_Orders,
    -- Month-over-Month Growth
    ROUND(
        (SUM(Sale_amt) - LAG(SUM(Sale_amt)) OVER (ORDER BY Year, Month))
        * 100.0 /
        NULLIF(LAG(SUM(Sale_amt)) OVER (ORDER BY Year, Month), 0)
    , 2)                                            AS MoM_Growth_Pct
FROM silver_sales
GROUP BY Year, Month, MonthName, Quarter
ORDER BY Year, Month;


-- =============================================================================
-- 8. Revenue Band Distribution
-- =============================================================================
SELECT
    RevenueBand,
    COUNT(*)                                        AS Orders,
    ROUND(SUM(Sale_amt), 2)                        AS Total_Revenue,
    ROUND(AVG(Sale_amt), 2)                        AS Avg_Sale,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS Order_Share_Pct
FROM silver_sales
GROUP BY RevenueBand
ORDER BY Total_Revenue DESC;


-- =============================================================================
-- 9. Item Sales by Region (Cross-tab insight)
-- =============================================================================
SELECT
    Item,
    Region,
    ROUND(SUM(Sale_amt), 2)                        AS Revenue,
    SUM(Units)                                      AS Units_Sold,
    COUNT(*)                                        AS Orders
FROM silver_sales
GROUP BY Item, Region
ORDER BY Item, Revenue DESC;


-- =============================================================================
-- 10. Top 5 Orders by Sale Amount
-- =============================================================================
SELECT
    OrderDate,
    Region,
    Manager,
    SalesMan,
    Item,
    Units,
    Unit_price,
    Sale_amt,
    RANK() OVER (ORDER BY Sale_amt DESC)           AS Overall_Rank
FROM silver_sales
ORDER BY Sale_amt DESC
LIMIT 5;
