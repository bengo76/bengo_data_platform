{{
  config(
    materialized='view'
  )
}}

WITH products AS (
  SELECT 
    id,
    name,
    category,
    price,
    created_at
  FROM {{ ref('raw_products') }}
),

price_tiers AS (
  SELECT * FROM {{ ref('seed_price_tier_mapping') }}
),

category_groups AS (
  SELECT * FROM {{ ref('seed_category_group_mapping') }}
),

seasonality_keywords AS (
  SELECT * FROM {{ ref('seed_seasonality_keywords') }}
),

collection_keywords AS (
  SELECT * FROM {{ ref('seed_collection_type_keywords') }}
)

SELECT 
  p.id,
  p.name,
  p.category,
  p.price,
  p.created_at,
  EXTRACT(YEAR FROM p.created_at) AS product_year,
  EXTRACT(MONTH FROM p.created_at) AS product_month,
  
  -- Price tier analysis using seed data
  COALESCE(pt.price_tier, 'Budget') AS price_tier,
  
  -- Detailed price tier based on actual price ranges
  CASE 
    WHEN p.price >= 500 THEN 'Ultra-Premium'
    WHEN p.price >= 400 THEN 'Luxury'
    WHEN p.price >= 150 THEN 'Premium'
    WHEN p.price >= 50 THEN 'Mid-Range'
    ELSE 'Budget'
  END AS detailed_price_tier,
  
  -- Category grouping using seed data
  COALESCE(cg.category_group, 'Other') AS category_group,
  
  -- Category hierarchy sorting
  CASE 
    WHEN COALESCE(cg.category_group, 'Other') = 'Clothing' THEN 1
    WHEN COALESCE(cg.category_group, 'Other') = 'Accessories' THEN 2
    WHEN COALESCE(cg.category_group, 'Other') = 'Footwear' THEN 3
    WHEN COALESCE(cg.category_group, 'Other') = 'Intimates' THEN 4
    WHEN COALESCE(cg.category_group, 'Other') = 'Activewear' THEN 5
    ELSE 6
  END AS category_sort_order,
  
  -- Season detection using seed data (take first match to avoid duplicates)
  COALESCE(
    (SELECT seasonality 
     FROM seasonality_keywords sk 
     WHERE LOWER(p.name) LIKE '%' || LOWER(sk.keyword) || '%' 
     LIMIT 1), 
    'Year-Round'
  ) AS seasonality,
  
  -- Collection type using seed data (take first match to avoid duplicates)
  COALESCE(
    (SELECT collection_type 
     FROM collection_keywords ck 
     WHERE LOWER(p.name) LIKE '%' || LOWER(ck.keyword) || '%' 
     LIMIT 1), 
    'Regular'
  ) AS collection_type,
  
  -- Product name analysis
  LENGTH(p.name) AS name_length,
  
  -- Product line classifications based on name keywords
  CASE 
    WHEN LOWER(p.name) LIKE '%limited%' OR LOWER(p.name) LIKE '%exclusive%' THEN TRUE
    ELSE FALSE
  END AS is_limited_edition,
  
  CASE 
    WHEN LOWER(p.name) LIKE '%premium%' OR LOWER(p.name) LIKE '%signature%' THEN TRUE
    ELSE FALSE
  END AS is_premium_line,
  
  CASE 
    WHEN LOWER(p.name) LIKE '%classic%' OR LOWER(p.name) LIKE '%essential%' THEN TRUE
    ELSE FALSE
  END AS is_classic_line,
  
  -- Business status flags
  TRUE AS is_active,
  FALSE AS is_discontinued

FROM products p
LEFT JOIN price_tiers pt ON p.price >= pt.min_price AND p.price <= pt.max_price
LEFT JOIN category_groups cg ON p.category = cg.category