{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    indexes=[
      {'columns': ['category']},
      {'columns': ['price']}, 
      {'columns': ['created_at']},
      {'columns': ['name']},
    ],
    on_schema_change='fail'
  )
}}

  SELECT 
    id,
    name,
    category,
    price,
    created_at
  FROM {{ source('data', 'source_products') }}

  {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
  {% endif %}
