{{
  config(
    materialized='view'
  )
}}

WITH customers AS (
  SELECT 
    id,
    name,
    email,
    signup_date,
    country,
    created_at
  FROM {{ ref('raw_customers') }}
),

country_regions AS (
  SELECT * FROM {{ ref('seed_country_region_mapping') }}
)

SELECT 
  c.id,
  c.name,
  c.email,
  c.signup_date,
  c.country,
  c.created_at,
  EXTRACT(YEAR  FROM c.signup_date) AS signup_year,
  EXTRACT(MONTH FROM c.signup_date) AS signup_month,
  EXTRACT(DOW   FROM c.signup_date) AS signup_day_of_week,
  
  -- Customer tenure calculation
  (CURRENT_DATE - c.signup_date) AS customer_tenure_days,
  
  -- Regional grouping using seed data
  COALESCE(cr.region, 'other') AS region

FROM customers c
LEFT JOIN country_regions cr ON LOWER(c.country) = cr.country