{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['order_line_key'], 'unique': true},
            {'columns': ['order_line_id'], 'unique': true},
            {'columns': ['order_key']},
            {'columns': ['product_key']},
            {'columns': ['customer_key']},
            {'columns': ['order_date_key']},
            {'columns': ['order_id']},
            {'columns': ['product_id']},
        ],
        on_schema_change='fail',
        tags=['fact']
    )
}}

with order_items_base as (
    select 
        id as order_line_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        created_at,
        item_sequence,
        quantity_category,
        price_tier,
        line_value_tier
    from {{ ref('stg_order_items') }}
),

order_context as (
    select 
        order_id,
        customer_id,
        order_date,
        status as order_status,
        total_amount as order_total_amount,
        is_current_status
    from {{ ref('stg_orders') }}
    where is_current_status = true  -- Only latest status per order
),

order_items_with_context as (
    select 
        oi.*,
        oc.customer_id,
        oc.order_date,
        oc.order_status,
        oc.order_total_amount
    from order_items_base oi
    inner join order_context oc on oi.order_id = oc.order_id
),

-- Join with dimension tables to get surrogate keys
order_items_with_dims as (
    select 
        oiwc.*,
        dc.customer_key,
        dp.product_key
    from order_items_with_context oiwc
    inner join {{ ref('dim_customer') }} dc on dc.customer_id = oiwc.customer_id
    inner join {{ ref('dim_product') }} dp on dp.product_id = oiwc.product_id
),

final as (
    select
        -- Surrogate key for order line
        {{ dbt_utils.generate_surrogate_key(['order_line_id']) }} as order_line_key,
        
        -- Natural key
        order_line_id,
        
        -- Parent fact table foreign key
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,
        
        -- Dimension foreign keys (from joins)
        customer_key,
        product_key,
        cast(to_char(order_date, 'YYYYMMDD') as integer) as order_date_key,
        
        -- Natural keys for reference
        order_id,
        product_id,
        customer_id,
        
        -- Line-level facts/measures
        quantity,
        unit_price,
        line_total,
        
        -- Line characteristics
        item_sequence,
        quantity_category,
        price_tier as line_price_tier,
        line_value_tier,
        
        -- Calculated line metrics
        case 
            when quantity > 1 then unit_price * (quantity - 1)
            else 0
        end as additional_quantity_value,
        
        case 
            when item_sequence = 1 then true
            else false
        end as is_first_line_item,
        
        -- Order context (denormalized for analysis convenience)
        order_date,
        order_status,
        order_total_amount,
        
        -- Line contribution to order
        case 
            when order_total_amount > 0 then (line_total / order_total_amount) * 100
            else 0
        end as line_percentage_of_order,
        
        case 
            when line_total = order_total_amount then 'Single Line Order'
            when (line_total / order_total_amount) >= 0.5 then 'Dominant Line'
            when (line_total / order_total_amount) >= 0.25 then 'Major Line'
            else 'Minor Line'
        end as line_importance,
        
        -- Product analysis at line level
        case 
            when quantity = 1 then 'Single Item'
            when quantity = 2 then 'Pair'
            when quantity >= 3 and quantity <= 5 then 'Small Batch'
            else 'Large Batch'
        end as quantity_category_detailed,
        
        -- Pricing analysis
        case 
            when unit_price < 50 then 'Budget Line'
            when unit_price < 150 then 'Mid-Range Line'  
            when unit_price < 400 then 'Premium Line'
            else 'Luxury Line'
        end as unit_price_category,
        
        -- Time analysis
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        extract(quarter from order_date) as order_quarter,
        extract(dow from order_date) as order_day_of_week,
        
        -- Business flags
        case when order_status = 'completed' then true else false end as is_from_completed_order,
        case when order_status = 'cancelled' then true else false end as is_from_cancelled_order,
        case when order_status = 'refunded' then true else false end as is_from_refunded_order,
        case when order_status = 'pending' then true else false end as is_from_pending_order,
        
        -- Revenue recognition flags (for financial reporting)
        case 
            when order_status = 'completed' then line_total
            else 0
        end as recognized_revenue,
        
        case 
            when order_status = 'refunded' then line_total
            else 0
        end as refunded_amount,
        
        case 
            when order_status in ('completed', 'refunded') then line_total
            else 0
        end as historical_revenue,
        
        -- Audit fields
        created_at as line_created_at,
        current_timestamp as fact_created_at,
        current_timestamp as fact_updated_at
        
    from order_items_with_dims
    where customer_id is not null 
      and product_id is not null  -- Ensure we have valid foreign key references
)

select * from final
order by order_date desc, order_id, item_sequence