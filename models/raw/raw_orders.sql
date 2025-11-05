{{
  config(
    materialized='incremental',
    unique_key='record_id',
    incremental_strategy='merge',
    indexes=[
      {'columns': ['order_id']},
      {'columns': ['customer_id']},
      {'columns': ['status']},
      {'columns': ['order_date']},
      {'columns': ['total_amount']},
      {'columns': ['order_id', 'created_at']},
    ],
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_created_at_month ON {{ this }} (DATE_TRUNC('month', created_at))",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_created_at_day ON {{ this }} (DATE_TRUNC('day', created_at))"
    ]
  )
}}

  SELECT 
    record_id,
    id AS order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    created_at
  FROM {{ source('data', 'source_orders') }}
  
  {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
  {% endif %}
