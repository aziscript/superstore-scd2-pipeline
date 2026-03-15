-- =============================================================================
-- SuperStore Warehouse Analysis Queries
-- Database: superstore_warehouse
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Total Sales and Profit by Year
-- -----------------------------------------------------------------------------
SELECT
    d.year,
    SUM(fo.sales)                                   AS total_sales,
    SUM(fo.profit)                                  AS total_profit,
    ROUND(SUM(fo.profit) / SUM(fo.sales) * 100, 2) AS profit_margin_pct,
    COUNT(DISTINCT fo.order_id)                     AS total_orders
FROM fact_orders    fo
JOIN dim_date       d  ON fo.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;


-- -----------------------------------------------------------------------------
-- 2. Top 10 Products by Sales (Current SCD2 Versions Only)
-- -----------------------------------------------------------------------------
SELECT
    p.product_name,
    p.category,
    p.sub_category,
    SUM(fo.sales)                                   AS total_sales,
    SUM(fo.profit)                                  AS total_profit,
    SUM(fo.quantity)                                AS units_sold,
    ROUND(SUM(fo.profit) / SUM(fo.sales) * 100, 2) AS profit_margin_pct
FROM fact_orders        fo
JOIN dim_product_scd2   p  ON fo.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.category, p.sub_category
ORDER BY total_sales DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 3. Sales by Category and Sub-Category
-- -----------------------------------------------------------------------------
SELECT
    p.category,
    p.sub_category,
    SUM(fo.sales)                                           AS total_sales,
    SUM(fo.profit)                                          AS total_profit,
    SUM(fo.quantity)                                        AS units_sold,
    ROUND(AVG(fo.discount) * 100, 2)                        AS avg_discount_pct,
    ROUND(SUM(fo.profit) / SUM(fo.sales) * 100, 2)          AS profit_margin_pct
FROM fact_orders        fo
JOIN dim_product_scd2   p  ON fo.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.category, p.sub_category
ORDER BY p.category, total_sales DESC;


-- -----------------------------------------------------------------------------
-- 4. SCD2 — Products That Changed Attributes Over Time
--    Identifies product_ids with more than one historical version.
-- -----------------------------------------------------------------------------
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.start_date,
    p.end_date,
    p.is_current,
    COUNT(*) OVER (PARTITION BY p.product_id) AS version_count
FROM dim_product_scd2 p
WHERE p.product_id IN (
    SELECT product_id
    FROM dim_product_scd2
    GROUP BY product_id
    HAVING COUNT(*) > 1
)
ORDER BY p.product_id, p.start_date;


-- -----------------------------------------------------------------------------
-- 5. Customer Segment Performance (Sales, Profit, Discount)
-- -----------------------------------------------------------------------------
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_key)                  AS customer_count,
    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    SUM(fo.sales)                                   AS total_sales,
    SUM(fo.profit)                                  AS total_profit,
    ROUND(AVG(fo.discount) * 100, 2)                AS avg_discount_pct,
    ROUND(SUM(fo.profit) / SUM(fo.sales) * 100, 2) AS profit_margin_pct,
    ROUND(SUM(fo.sales) / COUNT(DISTINCT fo.order_id), 2) AS avg_order_value
FROM fact_orders    fo
JOIN dim_customer   c  ON fo.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY total_sales DESC;


-- -----------------------------------------------------------------------------
-- 6. Monthly Sales Trend
-- -----------------------------------------------------------------------------
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(fo.sales)                   AS total_sales,
    SUM(fo.profit)                  AS total_profit,
    COUNT(DISTINCT fo.order_id)     AS total_orders,
    -- Month-over-month sales change
    SUM(fo.sales) - LAG(SUM(fo.sales)) OVER (ORDER BY d.year, d.month) AS mom_sales_change
FROM fact_orders    fo
JOIN dim_date       d  ON fo.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- -----------------------------------------------------------------------------
-- 7. Top 10 Most Profitable Customers
-- -----------------------------------------------------------------------------
SELECT
    c.customer_name,
    c.segment,
    c.state,
    c.market,
    c.region,
    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    SUM(fo.sales)                                   AS total_sales,
    SUM(fo.profit)                                  AS total_profit,
    ROUND(SUM(fo.profit) / SUM(fo.sales) * 100, 2) AS profit_margin_pct
FROM fact_orders    fo
JOIN dim_customer   c  ON fo.customer_key = c.customer_key
GROUP BY
    c.customer_name, c.segment, c.state, c.market, c.region
ORDER BY total_profit DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 8. Markets and Regions by Profit Margin
-- -----------------------------------------------------------------------------
SELECT
    c.market,
    c.region,
    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    SUM(fo.sales)                                   AS total_sales,
    SUM(fo.profit)                                  AS total_profit,
    SUM(fo.shipping_cost)                           AS total_shipping_cost,
    ROUND(AVG(fo.discount) * 100, 2)                AS avg_discount_pct,
    ROUND(SUM(fo.profit) / SUM(fo.sales) * 100, 2) AS profit_margin_pct
FROM fact_orders    fo
JOIN dim_customer   c  ON fo.customer_key = c.customer_key
GROUP BY c.market, c.region
ORDER BY profit_margin_pct DESC;


-- -----------------------------------------------------------------------------
-- 9. Shipping Mode Analysis (Cost vs Sales)
-- -----------------------------------------------------------------------------
SELECT
    fo.ship_mode,
    COUNT(DISTINCT fo.order_id)                             AS total_orders,
    SUM(fo.sales)                                           AS total_sales,
    SUM(fo.shipping_cost)                                   AS total_shipping_cost,
    ROUND(SUM(fo.shipping_cost) / SUM(fo.sales) * 100, 2)  AS shipping_cost_pct_of_sales,
    SUM(fo.profit)                                          AS total_profit,
    ROUND(AVG(fo.shipping_cost), 2)                         AS avg_shipping_cost_per_order,
    -- Average days to ship (if dates are stored as DATE types)
    ROUND(AVG(DATEDIFF(fo.ship_date, d.full_date)), 1)      AS avg_days_to_ship
FROM fact_orders    fo
JOIN dim_date       d  ON fo.date_key = d.date_key
GROUP BY fo.ship_mode
ORDER BY total_orders DESC;


-- -----------------------------------------------------------------------------
-- 10. Loss-Making Products (Negative Profit)
--     Shows current product versions where total profit across all orders < 0.
-- -----------------------------------------------------------------------------
SELECT
    p.product_name,
    p.category,
    p.sub_category,
    COUNT(DISTINCT fo.order_id)     AS total_orders,
    SUM(fo.sales)                   AS total_sales,
    SUM(fo.quantity)                AS units_sold,
    ROUND(AVG(fo.discount) * 100, 2) AS avg_discount_pct,
    SUM(fo.profit)                  AS total_profit
FROM fact_orders        fo
JOIN dim_product_scd2   p  ON fo.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.category, p.sub_category
HAVING SUM(fo.profit) < 0
ORDER BY total_profit ASC;
