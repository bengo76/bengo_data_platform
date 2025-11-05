{{
  config(
    materialized='view'
  )
}}

WITH order_items AS (
  SELECT 
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at
  FROM {{ ref('raw_order_items') }}
),

price_tiers AS (
  SELECT * FROM {{ ref('seed_price_tier_mapping') }}
),

enhanced_order_items AS (
  SELECT 
    oi.*,
    oi.quantity * oi.unit_price AS line_total,
    ROW_NUMBER() OVER (PARTITION BY oi.order_id ORDER BY oi.created_at) AS item_sequence,
    COALESCE(pt.price_tier, 'Budget') AS price_tier
  FROM order_items oi
  LEFT JOIN price_tiers pt ON oi.unit_price >= pt.min_price AND oi.unit_price <= pt.max_price
)

SELECT 
  id,
  order_id,
  product_id,
  quantity,
  unit_price,
  line_total,
  created_at,
  EXTRACT(YEAR FROM created_at) AS created_year,
  EXTRACT(MONTH FROM created_at) AS created_month,
  EXTRACT(DAY FROM created_at) AS created_day,
  EXTRACT(DOW FROM created_at) AS created_day_of_week,
  EXTRACT(HOUR FROM created_at) AS created_hour,
  item_sequence,
  price_tier,
  CASE 
    WHEN quantity = 1 THEN 'Single Item'
    WHEN quantity = 2 THEN 'Pair'
    WHEN quantity >= 3 THEN 'Multiple Items'
  END AS quantity_category,
  CASE 
    WHEN line_total < 50 THEN 'Low Value Line'
    WHEN line_total < 200 THEN 'Medium Value Line'
    WHEN line_total < 500 THEN 'High Value Line'
    ELSE 'Premium Value Line'
  END AS line_value_tier

FROM enhanced_order_items