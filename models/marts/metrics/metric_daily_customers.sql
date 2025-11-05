{{
    config(
        materialized='incremental',
        unique_key='date_key',
        indexes=[
            {'columns': ['date_key'], 'unique': true},
            {'columns': ['metric_date']},
            {'columns': ['year_month']},
            {'columns': ['metrics_updated_at']},
        ],
        on_schema_change='fail',
        tags=['metric', 'daily', 'incremental']
    )
}}

with daily_customer_activity as (
    select 
        date(fo.order_date) as metric_date,
        fo.order_date_key as date_key,
        extract(year from fo.order_date) as metric_year,
        extract(month from fo.order_date) as metric_month,
        to_char(fo.order_date, 'YYYY-MM') as year_month,
        
        -- Active customers (placed orders)
        count(distinct fo.customer_key) as active_customers,
        count(distinct case when fo.order_status = 'completed' then fo.customer_key end) as customers_with_completed_orders,
        count(distinct case when fo.order_status = 'cancelled' then fo.customer_key end) as customers_with_cancelled_orders,
        
        -- Customer value metrics
        sum(fo.order_amount) as total_customer_spend,
        avg(fo.order_amount) as avg_spend_per_customer,
        max(fo.order_amount) as highest_customer_spend,
        
        -- Customer behavior
        avg(fo.total_line_items) as avg_items_per_customer,
        avg(fo.total_quantity) as avg_quantity_per_customer,
        count(*) as total_orders_by_customers,
        
        -- Customer segments activity
        count(distinct case when dc.customer_segment = 'Premium Loyal' then fo.customer_key end) as premium_loyal_customers,
        count(distinct case when dc.customer_segment = 'Core Customer' then fo.customer_key end) as core_customers,
        count(distinct case when dc.customer_segment = 'Growing Customer' then fo.customer_key end) as growing_customers,
        count(distinct case when dc.customer_segment = 'New Customer' then fo.customer_key end) as new_customers,
        
        -- Regional activity
        count(distinct case when dc.region = 'North America' then fo.customer_key end) as na_customers,
        count(distinct case when dc.region = 'Europe' then fo.customer_key end) as eu_customers,
        count(distinct case when dc.region = 'Asia Pacific' then fo.customer_key end) as apac_customers,
        count(distinct case when dc.region = 'Other' then fo.customer_key end) as other_region_customers
        
    from {{ ref('fact_orders') }} fo
    inner join {{ ref('dim_customer') }} dc on fo.customer_key = dc.customer_key
    {% if is_incremental() %}
        where fo.order_date >= current_date - interval '3 days'
    {% endif %}
    group by date(fo.order_date), fo.order_date_key, extract(year from fo.order_date), extract(month from fo.order_date), to_char(fo.order_date, 'YYYY-MM')
),

daily_customer_acquisition as (
    select 
        dc.signup_date as metric_date,
        cast(to_char(dc.signup_date, 'YYYYMMDD') as integer) as date_key,
        
        -- New customer acquisition
        count(*) as new_customers_acquired,
        count(case when dc.region = 'North America' then 1 end) as new_na_customers,
        count(case when dc.region = 'Europe' then 1 end) as new_eu_customers,
        count(case when dc.region = 'Asia Pacific' then 1 end) as new_apac_customers,
        count(case when dc.region = 'Other' then 1 end) as new_other_customers,
        
        -- Customer acquisition by timing
        count(case when extract(dow from dc.signup_date) in (0, 6) then 1 end) as weekend_signups,
        count(case when extract(dow from dc.signup_date) not in (0, 6) then 1 end) as weekday_signups
        
    from {{ ref('dim_customer') }} dc
    group by dc.signup_date
),

combined_daily_metrics as (
    select 
        coalesce(dca.metric_date, dca_acq.metric_date) as metric_date,
        coalesce(dca.date_key, dca_acq.date_key) as date_key,
        coalesce(dca.metric_year, extract(year from dca_acq.metric_date)) as metric_year,
        coalesce(dca.metric_month, extract(month from dca_acq.metric_date)) as metric_month,
        coalesce(dca.year_month, to_char(dca_acq.metric_date, 'YYYY-MM')) as year_month,
        
        -- Activity metrics (0 if no activity)
        coalesce(dca.active_customers, 0) as active_customers,
        coalesce(dca.customers_with_completed_orders, 0) as customers_with_completed_orders,
        coalesce(dca.customers_with_cancelled_orders, 0) as customers_with_cancelled_orders,
        coalesce(dca.total_customer_spend, 0) as total_customer_spend,
        coalesce(dca.avg_spend_per_customer, 0) as avg_spend_per_customer,
        coalesce(dca.highest_customer_spend, 0) as highest_customer_spend,
        coalesce(dca.avg_items_per_customer, 0) as avg_items_per_customer,
        coalesce(dca.avg_quantity_per_customer, 0) as avg_quantity_per_customer,
        coalesce(dca.total_orders_by_customers, 0) as total_orders_by_customers,
        
        -- Segment activity
        coalesce(dca.premium_loyal_customers, 0) as premium_loyal_customers,
        coalesce(dca.core_customers, 0) as core_customers,
        coalesce(dca.growing_customers, 0) as growing_customers,
        coalesce(dca.new_customers, 0) as new_customers,
        
        -- Regional activity  
        coalesce(dca.na_customers, 0) as na_customers,
        coalesce(dca.eu_customers, 0) as eu_customers,
        coalesce(dca.apac_customers, 0) as apac_customers,
        coalesce(dca.other_region_customers, 0) as other_region_customers,
        
        -- Acquisition metrics (0 if no acquisitions)
        coalesce(dca_acq.new_customers_acquired, 0) as new_customers_acquired,
        coalesce(dca_acq.new_na_customers, 0) as new_na_customers,
        coalesce(dca_acq.new_eu_customers, 0) as new_eu_customers,
        coalesce(dca_acq.new_apac_customers, 0) as new_apac_customers,
        coalesce(dca_acq.new_other_customers, 0) as new_other_customers,
        coalesce(dca_acq.weekend_signups, 0) as weekend_signups,
        coalesce(dca_acq.weekday_signups, 0) as weekday_signups
        
    from daily_customer_activity dca
    full outer join daily_customer_acquisition dca_acq 
        on dca.date_key = dca_acq.date_key
),

enriched_metrics as (
    select 
        *,
        
        -- Calculate ratios and percentages
        case 
            when active_customers > 0 then 
                round((customers_with_completed_orders::numeric / active_customers::numeric) * 100, 2)
            else 0 
        end as completion_rate_by_customers_pct,
        
        case 
            when active_customers > 0 then 
                round(total_orders_by_customers::numeric / active_customers::numeric, 2)
            else 0 
        end as avg_orders_per_active_customer,
        
        case 
            when new_customers_acquired > 0 then 
                round((weekend_signups::numeric / new_customers_acquired::numeric) * 100, 2)
            else 0 
        end as weekend_signup_pct,
        
        -- Running totals for month-to-date
        sum(new_customers_acquired) over (
            partition by year_month 
            order by metric_date 
            rows unbounded preceding
        ) as mtd_new_customers,
        
        sum(active_customers) over (
            partition by year_month 
            order by metric_date 
            rows unbounded preceding
        ) as mtd_total_active_customers,
        
        -- 7-day rolling averages
        avg(active_customers) over (
            order by metric_date 
            rows between 6 preceding and current row
        ) as avg_active_customers_7d,
        
        avg(new_customers_acquired) over (
            order by metric_date 
            rows between 6 preceding and current row
        ) as avg_new_customers_7d,
        
        avg(total_customer_spend) over (
            order by metric_date 
            rows between 6 preceding and current row
        ) as avg_customer_spend_7d,
        
        -- Prior day comparison
        lag(active_customers, 1) over (order by metric_date) as prev_day_active_customers,
        lag(new_customers_acquired, 1) over (order by metric_date) as prev_day_new_customers,
        
        current_timestamp as metrics_updated_at
        
    from combined_daily_metrics
)

select * from enriched_metrics
where metric_date is not null
order by metric_date desc