{{
  config(
    materialized='view'
  )
}}

WITH orders AS (
  SELECT 
    record_id,
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    created_at
  FROM {{ ref('raw_orders') }}
)

SELECT 
  record_id,
  order_id,
  customer_id,
  order_date,
  status,
  total_amount,
  created_at,
  ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at) AS status_sequence,
  CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC) = 1
    THEN TRUE 
    ELSE FALSE 
  END AS is_current_status,
  EXTRACT(YEAR FROM created_at) AS created_year,
  EXTRACT(MONTH FROM created_at) AS created_month,
  EXTRACT(DOW FROM created_at) AS created_day_of_week

FROM orders