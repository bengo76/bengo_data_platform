{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['order_key'], 'unique': true},
            {'columns': ['order_id'], 'unique': true},
            {'columns': ['customer_key']},
            {'columns': ['order_date_key']},
            {'columns': ['order_status']},
            {'columns': ['order_date']},
        ],
        on_schema_change='fail',
        tags=['fact']
    )
}}

with order_base as (
    select 
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        created_at,
        is_current_status
    from {{ ref('stg_orders') }}
    where is_current_status = true  -- Only latest status per order
),

order_items_agg as (
    select 
        order_id,
        count(*) as total_line_items,
        sum(quantity) as total_quantity,
        sum(line_total) as calculated_total_amount,
        avg(unit_price) as avg_unit_price,
        min(unit_price) as min_unit_price,
        max(unit_price) as max_unit_price,
        count(distinct product_id) as unique_products_count
    from {{ ref('stg_order_items') }}
    group by order_id
),

order_with_items as (
    select 
        o.*,
        coalesce(oi.total_line_items, 0) as total_line_items,
        coalesce(oi.total_quantity, 0) as total_quantity,
        coalesce(oi.calculated_total_amount, 0) as calculated_total_amount,
        oi.avg_unit_price,
        oi.min_unit_price,
        oi.max_unit_price,
        coalesce(oi.unique_products_count, 0) as unique_products_count
    from order_base o
    left join order_items_agg oi on o.order_id = oi.order_id
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,
        
        -- Natural key
        order_id,
        
        -- Dimension foreign keys (using lookups to get surrogate keys)
        (select customer_key from {{ ref('dim_customer') }} dc where dc.customer_id = order_with_items.customer_id) as customer_key,
        cast(to_char(order_date, 'YYYYMMDD') as integer) as order_date_key,
        
        -- Order facts/measures
        total_amount as order_amount,
        calculated_total_amount,
        total_line_items,
        total_quantity,
        unique_products_count,
        
        -- Pricing metrics
        avg_unit_price,
        min_unit_price,
        max_unit_price,
        
        -- Order characteristics
        case 
            when total_line_items = 1 then 'Single Item'
            when total_line_items <= 3 then 'Small Order'
            when total_line_items <= 5 then 'Medium Order'
            else 'Large Order'
        end as order_size_category,
        
        case 
            when unique_products_count = total_line_items then 'All Unique'
            when unique_products_count = 1 then 'Single Product'
            else 'Mixed Products'
        end as product_variety,
        
        -- Status information
        status as order_status,
        
        -- Timing metrics
        order_date,
        created_at as status_updated_at,
        
        -- Business flags
        case when status = 'completed' then true else false end as is_completed,
        case when status = 'cancelled' then true else false end as is_cancelled,
        case when status = 'refunded' then true else false end as is_refunded,
        case when status = 'pending' then true else false end as is_pending,
        
        -- Order timing analysis
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        extract(quarter from order_date) as order_quarter,
        extract(dow from order_date) as order_day_of_week,
        extract(hour from order_date) as order_hour,
        
        case 
            when extract(dow from order_date) in (0, 6) then 'Weekend'
            else 'Weekday'
        end as order_timing,
        
        case 
            when extract(hour from order_date) between 6 and 11 then 'Morning'
            when extract(hour from order_date) between 12 and 17 then 'Afternoon'
            when extract(hour from order_date) between 18 and 22 then 'Evening'
            else 'Night'
        end as order_time_of_day,
        
        -- Calculated metrics
        case 
            when total_line_items > 0 then total_amount / total_line_items
            else 0
        end as avg_amount_per_line,
        
        case 
            when total_quantity > 0 then total_amount / total_quantity
            else 0
        end as avg_amount_per_item,
        
        -- Audit fields
        current_timestamp as fact_created_at,
        current_timestamp as fact_updated_at
        
    from order_with_items
    where customer_id is not null  -- Ensure we have valid customer references
)

select * from final
order by order_date desc, order_id