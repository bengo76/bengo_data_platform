{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    indexes=[
      {'columns': ['order_id']},
      {'columns': ['product_id']},
      {'columns': ['created_at']},
      {'columns': ['order_id', 'product_id']},
    ]
  )
}}

  SELECT 
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at
  FROM {{ source('data', 'source_order_items') }}
  
  {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
  {% endif %}