{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    indexes=[
      {'columns': ['email'], 'unique': true},
      {'columns': ['signup_date']},
      {'columns': ['country']},
      {'columns': ['created_at']},
    ]
  )
}}

  SELECT 
    id,
    name,
    email,
    signup_date,
    country,
    created_at
  FROM {{ source('data', 'source_customers') }}

  {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
  {% endif %}
