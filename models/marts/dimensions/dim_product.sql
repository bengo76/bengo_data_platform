{{
  config(
    materialized='incremental',
    unique_key='product_key',
    indexes=[
      {'columns': ['product_key'], 'unique': true},
      {'columns': ['product_id'], 'unique': true},
      {'columns': ['category']},
      {'columns': ['category_group']},
      {'columns': ['price_tier']},
      {'columns': ['is_active']},
    ],
    on_schema_change='fail',
    post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_product_name_gin ON {{ this }} USING gin(to_tsvector('english', product_name))",
    tags=['dimension', 'incremental']
  )
}}

WITH product_base AS (
  SELECT 
    -- Surrogate key generation
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS product_key,
    
    -- Natural key and attributes from staging (all calculations already done)
    id AS product_id,
    name AS product_name,
    category,
    price,
    created_at,
    product_year,
    product_month,
    
    -- Enhanced attributes from staging
    price_tier,
    detailed_price_tier,
    category_group,
    category_sort_order,
    seasonality,
    collection_type,
    
    -- Product analysis from staging
    name_length,
    is_limited_edition,
    is_premium_line,
    is_classic_line,
    
    -- Business flags from staging
    is_active,
    is_discontinued,
    
    -- Audit fields
    CURRENT_TIMESTAMP AS dim_created_at,
    CURRENT_TIMESTAMP AS dim_updated_at,
    1 AS dim_version
    
  FROM {{ ref('stg_products') }}
  {% if is_incremental() %}
    WHERE created_at >= current_date - interval '3 days'
  {% endif %}
),

-- Add analytics in separate step for window functions
product_analytics AS (
  SELECT 
    *,
    -- Price analytics
    NTILE(10) OVER (ORDER BY price) AS price_decile,
    NTILE(4) OVER (ORDER BY price) AS price_quartile,
    
    -- Ranking within category by price
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank_in_category
  FROM product_base
),

-- Final transformations
final_products AS (
  SELECT 
    *,
    -- Category position based on price quartile within category
    CASE 
      WHEN NTILE(4) OVER (PARTITION BY category ORDER BY price) = 4 THEN 'Category Premium'
      WHEN NTILE(4) OVER (PARTITION BY category ORDER BY price) = 1 THEN 'Category Budget'
      ELSE 'Category Mid-Market'
    END AS category_position,
    
    -- Overall price position
    CASE 
      WHEN price_decile >= 9 THEN 'Top 10%'
      WHEN price_quartile = 4 THEN 'Top 25%'
      WHEN price_quartile >= 3 THEN 'Above Median'
      WHEN price_quartile = 2 THEN 'Below Median'
      ELSE 'Bottom 25%'
    END AS overall_price_position
    
  FROM product_analytics
)

SELECT * FROM final_products
ORDER BY category_sort_order, price DESC