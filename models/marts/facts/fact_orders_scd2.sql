{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'status_change_sequence'],
        indexes=[
            {'columns': ['order_key'], 'unique': true},
            {'columns': ['order_id']},
            {'columns': ['customer_key']},
            {'columns': ['order_date_key']},
            {'columns': ['order_status']},
            {'columns': ['is_current_status']},
            {'columns': ['status_effective_date']},
            {'columns': ['status_end_date']},
        ],
        on_schema_change='fail',
        tags=['fact', 'scd2', 'incremental']
    )
}}

-- SCD2 Fact Table: Tracks all status changes for orders over time
with order_status_history as (
    select 
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        created_at as status_change_date,
        is_current_status,
        
        -- Create sequence number for each status change per order
        row_number() over (partition by order_id order by created_at) as status_change_sequence,
        
        -- Calculate effective dates for each status
        created_at as status_effective_date,
        lead(created_at) over (partition by order_id order by created_at) as status_end_date
        
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where created_at >= current_date - interval '3 days'
    {% endif %}
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
        osh.*,
        coalesce(oi.total_line_items, 0) as total_line_items,
        coalesce(oi.total_quantity, 0) as total_quantity,
        coalesce(oi.calculated_total_amount, 0) as calculated_total_amount,
        oi.avg_unit_price,
        oi.min_unit_price,
        oi.max_unit_price,
        coalesce(oi.unique_products_count, 0) as unique_products_count
    from order_status_history osh
    left join order_items_agg oi on osh.order_id = oi.order_id
),

final as (
    select
        -- Surrogate key (unique per status change)
        {{ dbt_utils.generate_surrogate_key(['order_id', 'status_change_sequence']) }} as order_key,
        
        -- Natural keys
        order_id,
        status_change_sequence,
        
        -- Dimension foreign keys
        (select customer_key from {{ ref('dim_customer') }} dc where dc.customer_id = order_with_items.customer_id) as customer_key,
        cast(to_char(order_date, 'YYYYMMDD') as integer) as order_date_key,
        cast(to_char(status_effective_date, 'YYYYMMDD') as integer) as status_effective_date_key,
        
        -- SCD2 Status tracking
        status as order_status,
        status_effective_date,
        status_end_date,
        is_current_status,
        
        -- Calculate status duration in hours (NULL for current status)
        case 
            when status_end_date is not null then 
                extract(epoch from (status_end_date - status_effective_date)) / 3600.0
            else null
        end as status_duration_hours,
        
        -- Order facts/measures (as of this status change)
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
        
        -- Timing metrics
        order_date,
        
        -- Business flags for current status
        case when status = 'completed' then true else false end as is_completed,
        case when status = 'cancelled' then true else false end as is_cancelled,
        case when status = 'refunded' then true else false end as is_refunded,
        case when status = 'pending' then true else false end as is_pending,
        case when status = 'items added' then true else false end as is_items_added,
        
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
        
        -- Status change timing analysis
        extract(year from status_effective_date) as status_change_year,
        extract(month from status_effective_date) as status_change_month,
        extract(hour from status_effective_date) as status_change_hour,
        
        case 
            when extract(dow from status_effective_date) in (0, 6) then 'Weekend'
            else 'Weekday'
        end as status_change_timing,
        
        -- Calculated metrics
        case 
            when total_line_items > 0 then total_amount / total_line_items
            else 0
        end as avg_amount_per_line,
        
        case 
            when total_quantity > 0 then total_amount / total_quantity
            else 0
        end as avg_amount_per_item,
        
        -- SCD2 metrics: order lifecycle stage tracking
        case 
            when status_change_sequence = 1 then 'Initial'
            when is_current_status = true then 'Current'
            else 'Historical'
        end as status_lifecycle_stage,
        
        -- Business metrics for status transitions
        case 
            when status = 'completed' and status_change_sequence > 1 then 
                extract(epoch from (status_effective_date - order_date)) / 3600.0
            else null
        end as hours_to_completion,
        
        case 
            when status = 'cancelled' and status_change_sequence > 1 then 
                extract(epoch from (status_effective_date - order_date)) / 3600.0
            else null
        end as hours_to_cancellation,
        
        -- Audit fields
        current_timestamp as fact_created_at,
        current_timestamp as fact_updated_at
        
    from order_with_items
    where customer_id is not null  -- Ensure we have valid customer references
)

select * from final
order by order_id, status_change_sequence