{{
    config(
        materialized='incremental',
        unique_key='date_key',
        indexes=[
            {'columns': ['date_key'], 'unique': true},
            {'columns': ['order_date']},
            {'columns': ['year_month']},
        ],
        on_schema_change='fail',
        tags=['metric', 'daily', 'incremental']
    )
}}

-- Using SCD2 with date spine to get point-in-time order metrics
with date_spine as (
    select 
        calendar_date as metric_date,
        date_key as metric_date_key
    from {{ ref('dim_date') }}
    {% if is_incremental() %}
        where calendar_date >= current_date - interval '3 days'
    {% endif %}
),

-- Get the effective order status for each date
orders_point_in_time as (
    select 
        ds.metric_date,
        ds.metric_date_key,
        fo.order_id,
        fo.order_date,
        fo.order_date_key,
        fo.customer_key,
        fo.order_status,
        fo.order_amount,
        fo.total_line_items,
        fo.total_quantity,
        fo.unique_products_count,
        fo.avg_unit_price,
        fo.min_unit_price,
        fo.max_unit_price,
        fo.order_size_category,
        fo.product_variety,
        fo.order_timing,
        fo.order_time_of_day,
        fo.status_effective_date,
        fo.status_end_date,
        fo.is_current_status
    from date_spine ds
    inner join {{ ref('fact_orders_scd2') }} fo
        on ds.metric_date >= date(fo.status_effective_date)
        and (fo.status_end_date is null or ds.metric_date < date(fo.status_end_date))
    {% if is_incremental() %}
        where fo.status_effective_date >= current_date - interval '7 days'
    {% endif %}
),

daily_orders_metrics as (
    select 
        metric_date as order_date,
        metric_date_key as order_date_key,
        extract(year from metric_date) as order_year,
        extract(month from metric_date) as order_month,
        extract(quarter from metric_date) as order_quarter,
        extract(dow from metric_date) as day_of_week,
        to_char(metric_date, 'YYYY-MM') as year_month,
        to_char(metric_date, 'Day') as day_name,
        case 
            when extract(dow from metric_date) in (0, 6) then 'Weekend'
            else 'Weekday'
        end as day_type,
        
        -- Order volume metrics (point-in-time counts)
        count(distinct order_id) as total_orders,
        count(distinct case when order_status = 'completed' then order_id end) as completed_orders,
        count(distinct case when order_status = 'cancelled' then order_id end) as cancelled_orders,
        count(distinct case when order_status = 'pending' then order_id end) as pending_orders,
        count(distinct case when order_status = 'refunded' then order_id end) as refunded_orders,
        count(distinct case when order_status = 'items added' then order_id end) as items_added_orders,
        
        -- Revenue metrics (point-in-time totals)
        sum(case when order_status in ('completed', 'refunded') then order_amount else 0 end) as total_revenue,
        sum(case when order_status = 'completed' then order_amount else 0 end) as completed_revenue,
        sum(case when order_status = 'refunded' then order_amount else 0 end) as refunded_revenue,
        avg(case when order_status in ('completed', 'refunded') then order_amount end) as avg_order_value,
        percentile_cont(0.5) within group (order by case when order_status in ('completed', 'refunded') then order_amount end) as median_order_value,
        min(case when order_status in ('completed', 'refunded') then order_amount end) as min_order_value,
        max(case when order_status in ('completed', 'refunded') then order_amount end) as max_order_value,
        
        -- Customer metrics (point-in-time)
        count(distinct customer_key) as unique_customers,
        count(distinct case when order_status = 'completed' then customer_key end) as customers_with_completed_orders,
        
        -- Product metrics (point-in-time)
        sum(total_line_items) as total_line_items,
        sum(total_quantity) as total_quantity_sold,
        sum(unique_products_count) as total_unique_products_ordered,
        avg(total_line_items) as avg_line_items_per_order,
        avg(total_quantity) as avg_quantity_per_order,
        
        -- Order characteristics (point-in-time)
        count(distinct case when order_size_category = 'Single Item' then order_id end) as single_item_orders,
        count(distinct case when order_size_category = 'Small Order' then order_id end) as small_orders,
        count(distinct case when order_size_category = 'Medium Order' then order_id end) as medium_orders,
        count(distinct case when order_size_category = 'Large Order' then order_id end) as large_orders,
        
        count(distinct case when product_variety = 'All Unique' then order_id end) as all_unique_product_orders,
        count(distinct case when product_variety = 'Single Product' then order_id end) as single_product_orders,
        count(distinct case when product_variety = 'Mixed Products' then order_id end) as mixed_product_orders,
        
        -- Timing analysis (point-in-time)
        count(distinct case when order_timing = 'Weekend' then order_id end) as weekend_orders,
        count(distinct case when order_timing = 'Weekday' then order_id end) as weekday_orders,
        
        count(distinct case when order_time_of_day = 'Morning' then order_id end) as morning_orders,
        count(distinct case when order_time_of_day = 'Afternoon' then order_id end) as afternoon_orders,
        count(distinct case when order_time_of_day = 'Evening' then order_id end) as evening_orders,
        count(distinct case when order_time_of_day = 'Night' then order_id end) as night_orders,
        
        -- SCD2-specific metrics: Daily status transitions
        count(distinct case when date(status_effective_date) = metric_date then order_id end) as daily_status_changes,
        count(distinct case when date(status_effective_date) = metric_date and order_status = 'completed' then order_id end) as daily_completions,
        count(distinct case when date(status_effective_date) = metric_date and order_status = 'cancelled' then order_id end) as daily_cancellations,
        count(distinct case when date(status_effective_date) = metric_date and order_status = 'items added' then order_id end) as daily_items_added
        
    from orders_point_in_time
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

enhanced_metrics as (
    select 
        *,
        
        -- Calculated ratios (point-in-time)
        case 
            when total_orders > 0 then 
                round((completed_orders::numeric / total_orders::numeric) * 100, 2)
            else 0 
        end as completion_rate_pct,
        
        case 
            when total_orders > 0 then 
                round((cancelled_orders::numeric / total_orders::numeric) * 100, 2)
            else 0 
        end as cancellation_rate_pct,
        
        case 
            when total_orders > 0 then 
                round((refunded_orders::numeric / total_orders::numeric) * 100, 2)
            else 0 
        end as refund_rate_pct,
        
        case 
            when total_orders > 0 then 
                round((items_added_orders::numeric / total_orders::numeric) * 100, 2)
            else 0 
        end as items_added_rate_pct,
        
        case 
            when completed_revenue > 0 then 
                round((refunded_revenue / completed_revenue) * 100, 2)
            else 0 
        end as revenue_refund_rate_pct,
        
        case 
            when total_orders > 0 then 
                round(unique_customers::numeric / total_orders::numeric, 2)
            else 0 
        end as orders_per_customer_ratio,
        
        case 
            when total_orders > 0 then 
                round((weekend_orders::numeric / total_orders::numeric) * 100, 2)
            else 0 
        end as weekend_order_pct,
        
        -- SCD2-specific ratios: Daily activity rates
        case 
            when total_orders > 0 then 
                round((daily_status_changes::numeric / total_orders::numeric) * 100, 2)
            else 0 
        end as daily_activity_rate_pct,
        
        case 
            when daily_status_changes > 0 then 
                round((daily_completions::numeric / daily_status_changes::numeric) * 100, 2)
            else 0 
        end as completion_conversion_rate_pct,
        
        case 
            when daily_status_changes > 0 then 
                round((daily_cancellations::numeric / daily_status_changes::numeric) * 100, 2)
            else 0 
        end as cancellation_conversion_rate_pct
        
    from daily_orders_metrics
),

final_with_trends as (
    select 
        *,
        
        -- 7-day moving averages (point-in-time aware)
        round(avg(total_orders) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_total_orders,
        
        round(avg(completed_orders) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_completed_orders,
        
        round(avg(total_revenue) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_total_revenue,
        
        round(avg(completed_revenue) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_completed_revenue,
        
        round(avg(completion_rate_pct) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_completion_rate_pct,
        
        round(avg(cancellation_rate_pct) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_cancellation_rate_pct,
        
        -- SCD2-specific 7-day moving averages
        round(avg(daily_status_changes) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_status_changes,
        
        round(avg(daily_activity_rate_pct) over (
            order by order_date 
            rows between 6 preceding and current row
        ), 2) as avg_7d_activity_rate_pct,
        
        -- Day-over-day changes (point-in-time aware)
        coalesce(
            total_orders - lag(total_orders) over (order by order_date), 
            0
        ) as total_orders_change_1d,
        
        coalesce(
            completed_orders - lag(completed_orders) over (order by order_date), 
            0
        ) as completed_orders_change_1d,
        
        coalesce(
            round(
                total_revenue - lag(total_revenue) over (order by order_date), 
                2
            ), 
            0
        ) as total_revenue_change_1d,
        
        coalesce(
            round(
                completion_rate_pct - lag(completion_rate_pct) over (order by order_date), 
                2
            ), 
            0
        ) as completion_rate_change_1d,
        
        -- SCD2-specific day-over-day changes
        coalesce(
            daily_status_changes - lag(daily_status_changes) over (order by order_date), 
            0
        ) as status_changes_change_1d,
        
        current_timestamp as metrics_updated_at
        
    from enhanced_metrics
)

select 
    order_date_key as date_key,
    order_date,
    order_year as year,
    order_month as month,
    order_quarter as quarter,
    day_of_week,
    year_month,
    day_name,
    day_type,
    
    -- Core metrics (point-in-time counts)
    total_orders,
    completed_orders,
    pending_orders,
    cancelled_orders,
    refunded_orders,
    items_added_orders,
    
    -- Revenue metrics (point-in-time sums)
    total_revenue,
    completed_revenue,
    0 as pending_revenue,  -- Not tracked separately in SCD2 approach
    0 as cancelled_revenue,  -- Not tracked separately in SCD2 approach 
    refunded_revenue,
    avg_order_value,
    median_order_value,
    min_order_value,
    max_order_value,
    
    -- Customer metrics (point-in-time distinct counts)
    unique_customers,
    customers_with_completed_orders,
    
    -- Product metrics (point-in-time aggregates)
    total_line_items,
    total_quantity_sold,
    total_unique_products_ordered,
    avg_line_items_per_order,
    avg_quantity_per_order,
    
    -- Order characteristics (point-in-time)
    single_item_orders,
    small_orders,
    medium_orders,
    large_orders,
    all_unique_product_orders,
    single_product_orders,
    mixed_product_orders,
    
    -- Timing analysis (point-in-time)
    weekend_orders,
    weekday_orders,
    morning_orders,
    afternoon_orders,
    evening_orders,
    night_orders,
    
    -- SCD2-specific metrics (daily activity tracking)
    daily_status_changes,
    daily_completions,
    daily_cancellations,
    daily_items_added,
    
    -- Calculated ratios
    completion_rate_pct,
    cancellation_rate_pct,
    refund_rate_pct,
    items_added_rate_pct,
    revenue_refund_rate_pct,
    orders_per_customer_ratio,
    weekend_order_pct,
    
    -- SCD2-specific ratios
    daily_activity_rate_pct,
    completion_conversion_rate_pct,
    cancellation_conversion_rate_pct,
    
    -- 7-day moving averages
    avg_7d_total_orders,
    avg_7d_completed_orders,
    avg_7d_total_revenue,
    avg_7d_completed_revenue,
    avg_7d_completion_rate_pct,
    avg_7d_cancellation_rate_pct,
    avg_7d_status_changes,
    avg_7d_activity_rate_pct,
    
    -- Day-over-day changes
    total_orders_change_1d,
    completed_orders_change_1d,
    total_revenue_change_1d,
    completion_rate_change_1d,
    status_changes_change_1d,
    
    metrics_updated_at

from final_with_trends
order by order_date desc