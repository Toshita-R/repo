-- ============================================================
-- Q1. What is monthly GMV, order volume, and repeat-purchase rate?
-- ============================================================
SELECT
  year_month,
  orders,
  customers,
  gmv,
  avg_order_value,
  repeat_customer_pct
FROM ecom_dev.gold.mart_monthly_revenue
WHERE orders > 100          -- exclude the sparse 2016 pilot months
ORDER BY year_month;


-- ============================================================
-- Q2. Which sellers and states drive late deliveries?
-- ============================================================
SELECT
  seller_state,
  COUNT(DISTINCT seller_id)      AS sellers,
  SUM(line_items)                AS line_items,
  ROUND(SUM(gmv), 2)             AS gmv,
  ROUND(AVG(avg_delivery_days), 1) AS avg_delivery_days,
  ROUND(AVG(late_pct), 2)        AS late_pct,
  ROUND(AVG(avg_review_score), 2) AS avg_review_score
FROM ecom_dev.gold.mart_seller_scorecard
GROUP BY seller_state
HAVING SUM(line_items) >= 100
ORDER BY late_pct DESC;


-- The 20 worst-performing sellers by volume
SELECT
  seller_id, seller_city, seller_state,
  line_items, orders, gmv,
  avg_delivery_days, late_pct, avg_review_score
FROM ecom_dev.gold.mart_seller_scorecard
WHERE line_items >= 100
ORDER BY late_pct DESC
LIMIT 20;


-- ============================================================
-- Q3. Does delivery delay cause bad reviews, and by how much?
-- ============================================================
SELECT
  CASE WHEN is_late_delivery THEN 'Late' ELSE 'On time' END AS delivery_status,
  COUNT(*)                                    AS reviews,
  ROUND(AVG(review_score), 2)                 AS avg_score,
  ROUND(100.0 * AVG(CASE WHEN is_negative THEN 1.0 ELSE 0.0 END), 1) AS pct_1_or_2_star,
  ROUND(100.0 * AVG(CASE WHEN is_positive THEN 1.0 ELSE 0.0 END), 1) AS pct_4_or_5_star
FROM ecom_dev.gold.fct_reviews
WHERE is_late_delivery IS NOT NULL
GROUP BY is_late_delivery;


-- Score decay by severity of lateness
SELECT
  CASE
    WHEN f.delivery_delay_days <= -7 THEN '7+ days early'
    WHEN f.delivery_delay_days <  0  THEN 'Early'
    WHEN f.delivery_delay_days =  0  THEN 'On the day'
    WHEN f.delivery_delay_days <= 7  THEN '1-7 days late'
    WHEN f.delivery_delay_days <= 14 THEN '8-14 days late'
    ELSE '15+ days late'
  END                          AS delay_bucket,
  COUNT(DISTINCT f.order_id)   AS orders,
  ROUND(AVG(r.review_score),2) AS avg_score
FROM ecom_dev.gold.fct_order_items f
JOIN ecom_dev.gold.fct_reviews r ON f.order_id = r.order_id
WHERE f.delivery_delay_days IS NOT NULL
GROUP BY 1
ORDER BY MIN(f.delivery_delay_days);


-- ============================================================
-- Q4. How did product categorisation change over time?
-- ============================================================
SELECT
  product_id,
  product_category_name,
  __START_AT AS valid_from,
  __END_AT   AS valid_to,
  CASE WHEN __END_AT IS NULL THEN 'current' ELSE 'superseded' END AS version_status
FROM ecom_dev.silver.sv_products_history
WHERE product_id IN (
  SELECT product_id FROM ecom_dev.silver.sv_products_history
  GROUP BY product_id HAVING COUNT(*) > 1
)
ORDER BY product_id, __START_AT
LIMIT 40;


-- ============================================================
-- Supporting: data quality summary
-- ============================================================
SELECT layer, dataset, expectation, total_passed, total_failed,
  ROUND(100.0 * total_failed / NULLIF(total_passed + total_failed, 0), 3) AS failure_pct
FROM ecom_dev.ops.dq_expectation_report
ORDER BY total_failed DESC;


-- Supporting: geographic revenue
SELECT
  c.customer_state,
  COUNT(DISTINCT f.order_id)     AS orders,
  ROUND(SUM(f.gross_revenue), 2) AS gmv,
  ROUND(AVG(f.delivery_days), 1) AS avg_delivery_days
FROM ecom_dev.gold.fct_order_items f
JOIN ecom_dev.gold.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_state
ORDER BY gmv DESC;