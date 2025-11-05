{{
    config(
        materialized='incremental',
        unique_key='trend_date',
        indexes=[
            {'columns': ['trend_date'], 'unique': true},
            {'columns': ['trend_period']},
            {'columns': ['seasonality_pattern']},
            {'columns': ['analysis_updated_at']},
        ],
        on_schema_change='fail',
        tags=['analysis', 'trends', 'seasonality', 'incremental']
    )
}}

with daily_order_trends as (
    select 
        date(fo.order_date) as trend_date,
        extract(year from fo.order_date) as trend_year,
        extract(month from fo.order_date) as trend_month,
        extract(quarter from fo.order_date) as trend_quarter,
        extract(dow from fo.order_date) as day_of_week,
        to_char(fo.order_date, 'YYYY-MM') as year_month,
        to_char(fo.order_date, 'YYYY-Q') as year_quarter,
        
        -- Order volume and value
        count(*) as total_orders,
        count(case when fo.order_status = 'completed' then 1 end) as completed_orders,
        sum(fo.order_amount) as total_revenue,
        sum(case when fo.order_status = 'completed' then fo.order_amount else 0 end) as completed_revenue,
        avg(fo.order_amount) as avg_order_value,
        
        -- Customer metrics
        count(distinct fo.customer_key) as unique_customers,
        
        -- Product metrics
        sum(fo.total_line_items) as total_line_items,
        avg(fo.total_line_items) as avg_items_per_order,
        
        -- Completion rate
        round(
            count(case when fo.order_status = 'completed' then 1 end)::numeric / 
            count(*)::numeric * 100, 
            2
        ) as completion_rate_pct
        
    from {{ ref('fact_orders') }} fo
    {% if is_incremental() %}
        where fo.order_date >= current_date - interval '3 days'
    {% endif %}
    group by 
        date(fo.order_date),
        extract(year from fo.order_date),
        extract(month from fo.order_date), 
        extract(quarter from fo.order_date),
        extract(dow from fo.order_date),
        to_char(fo.order_date, 'YYYY-MM'),
        to_char(fo.order_date, 'YYYY-Q')
),

trend_analysis as (
    select 
        *,
        
        -- Moving averages (7-day and 30-day)
        avg(total_orders) over (
            order by trend_date 
            rows between 6 preceding and current row
        ) as orders_7d_avg,
        
        avg(completed_revenue) over (
            order by trend_date 
            rows between 6 preceding and current row
        ) as revenue_7d_avg,
        
        avg(total_orders) over (
            order by trend_date 
            rows between 29 preceding and current row
        ) as orders_30d_avg,
        
        avg(completed_revenue) over (
            order by trend_date 
            rows between 29 preceding and current row
        ) as revenue_30d_avg,
        
        -- Period-over-period comparisons
        lag(total_orders, 1) over (order by trend_date) as prev_day_orders,
        lag(total_orders, 7) over (order by trend_date) as prev_week_orders,
        lag(total_orders, 30) over (order by trend_date) as prev_month_orders,
        
        lag(completed_revenue, 1) over (order by trend_date) as prev_day_revenue,
        lag(completed_revenue, 7) over (order by trend_date) as prev_week_revenue,
        lag(completed_revenue, 30) over (order by trend_date) as prev_month_revenue
        
    from daily_order_trends
),

seasonality_patterns as (
    select 
        ta.*,
        
        -- Growth rates
        case 
            when prev_day_orders > 0 then 
                round((total_orders - prev_day_orders)::numeric / prev_day_orders::numeric * 100, 2)
            else 0 
        end as daily_growth_pct,
        
        case 
            when prev_week_orders > 0 then 
                round((total_orders - prev_week_orders)::numeric / prev_week_orders::numeric * 100, 2)
            else 0 
        end as weekly_growth_pct,
        
        case 
            when prev_month_orders > 0 then 
                round((total_orders - prev_month_orders)::numeric / prev_month_orders::numeric * 100, 2)
            else 0 
        end as monthly_growth_pct,
        
        -- Seasonality indicators
        case 
            when trend_month in (12, 1, 2) then 'Winter'
            when trend_month in (3, 4, 5) then 'Spring'
            when trend_month in (6, 7, 8) then 'Summer'
            when trend_month in (9, 10, 11) then 'Fall'
        end as season,
        
        case 
            when day_of_week = 0 then 'Sunday'
            when day_of_week = 1 then 'Monday'
            when day_of_week = 2 then 'Tuesday'
            when day_of_week = 3 then 'Wednesday'
            when day_of_week = 4 then 'Thursday'
            when day_of_week = 5 then 'Friday'
            when day_of_week = 6 then 'Saturday'
        end as day_name,
        
        case 
            when day_of_week in (0, 6) then 'Weekend'
            else 'Weekday'
        end as day_type,
        
        -- Holiday/special period detection (basic)
        case 
            when trend_month = 12 and extract(day from trend_date) >= 20 then 'Holiday Season'
            when trend_month = 11 and extract(day from trend_date) >= 20 then 'Black Friday Period'
            when trend_month = 2 and extract(day from trend_date) between 10 and 20 then 'Valentines Period'
            else 'Regular Period'
        end as special_period
        
    from trend_analysis ta
),

performance_classification as (
    select 
        *,
        
        -- Performance vs. moving averages
        case 
            when total_orders >= orders_7d_avg * 1.2 then 'Above 7d Average'
            when total_orders >= orders_7d_avg * 0.8 then 'Near 7d Average'
            else 'Below 7d Average'
        end as performance_vs_7d,
        
        case 
            when completed_revenue >= revenue_30d_avg * 1.2 then 'High Revenue Day'
            when completed_revenue >= revenue_30d_avg * 0.8 then 'Average Revenue Day'
            else 'Low Revenue Day'
        end as revenue_performance,
        
        -- Trend classification
        case 
            when daily_growth_pct >= 10 then 'Strong Growth'
            when daily_growth_pct >= 5 then 'Moderate Growth'
            when daily_growth_pct >= -5 then 'Stable'
            when daily_growth_pct >= -15 then 'Moderate Decline'
            else 'Strong Decline'
        end as daily_trend_pattern,
        
        -- Volume classification
        case 
            when total_orders >= 200 then 'Very High Volume'
            when total_orders >= 150 then 'High Volume'
            when total_orders >= 100 then 'Medium Volume'
            when total_orders >= 50 then 'Low Volume'
            else 'Very Low Volume'
        end as volume_tier,
        
        current_timestamp as analysis_updated_at
        
    from seasonality_patterns
),

-- Monthly aggregation for longer-term trends
monthly_trends as (
    select 
        year_month as trend_period,
        'Monthly' as period_type,
        sum(total_orders) as period_orders,
        sum(completed_revenue) as period_revenue,
        avg(avg_order_value) as period_avg_order_value,
        sum(unique_customers) as period_unique_customers,
        avg(completion_rate_pct) as period_completion_rate,
        
        -- Month-over-month growth
        lag(sum(total_orders)) over (order by year_month) as prev_period_orders,
        case 
            when lag(sum(total_orders)) over (order by year_month) > 0 then
                round((sum(total_orders) - lag(sum(total_orders)) over (order by year_month))::numeric / 
                      lag(sum(total_orders)) over (order by year_month)::numeric * 100, 2)
            else 0
        end as period_growth_pct
        
    from performance_classification
    group by year_month
),

final_trends as (
    select 
        trend_date,
        'Daily' as period_type,
        trend_date::text as trend_period,
        total_orders as period_orders,
        completed_revenue as period_revenue,
        avg_order_value as period_avg_order_value,
        unique_customers as period_unique_customers,
        completion_rate_pct as period_completion_rate,
        daily_growth_pct as period_growth_pct,
        season,
        day_name,
        day_type,
        special_period,
        performance_vs_7d,
        revenue_performance,
        daily_trend_pattern,
        volume_tier,
        season as seasonality_pattern,
        analysis_updated_at
    from performance_classification
    
    union all
    
    select 
        null as trend_date,
        period_type,
        trend_period,
        period_orders,
        period_revenue,
        period_avg_order_value,
        period_unique_customers,
        period_completion_rate,
        period_growth_pct,
        null as season,
        null as day_name,
        null as day_type,
        null as special_period,
        null as performance_vs_7d,
        null as revenue_performance,
        null as daily_trend_pattern,
        null as volume_tier,
        'Monthly' as seasonality_pattern,
        current_timestamp as analysis_updated_at
    from monthly_trends
)

select * from final_trends
order by period_type, trend_period