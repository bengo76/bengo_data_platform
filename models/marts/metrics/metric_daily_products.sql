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

with daily_product_sales as (
    select 
        date(fol.order_date) as metric_date,
        fol.order_date_key as date_key,
        extract(year from fol.order_date) as metric_year,
        extract(month from fol.order_date) as metric_month,
        to_char(fol.order_date, 'YYYY-MM') as year_month,
        
        -- Product volume metrics
        count(distinct fol.product_key) as unique_products_sold,
        count(*) as total_line_items,
        sum(fol.quantity) as total_quantity_sold,
        sum(fol.line_total) as total_product_revenue,
        sum(case when fol.order_status = 'completed' then fol.line_total else 0 end) as completed_product_revenue,
        sum(case when fol.order_status = 'refunded' then fol.line_total else 0 end) as refunded_product_revenue,
        
        -- Product pricing metrics
        avg(fol.unit_price) as avg_unit_price,
        avg(fol.line_total) as avg_line_total,
        percentile_cont(0.5) within group (order by fol.unit_price) as median_unit_price,
        min(fol.unit_price) as min_unit_price,
        max(fol.unit_price) as max_unit_price,
        
        -- Category performance
        count(distinct case when dp.category_group = 'Clothing' then fol.product_key end) as clothing_products_sold,
        count(distinct case when dp.category_group = 'Accessories' then fol.product_key end) as accessories_products_sold,
        count(distinct case when dp.category_group = 'Footwear' then fol.product_key end) as footwear_products_sold,
        count(distinct case when dp.category_group = 'Intimates' then fol.product_key end) as intimates_products_sold,
        count(distinct case when dp.category_group = 'Activewear' then fol.product_key end) as activewear_products_sold,
        
        -- Revenue by category
        sum(case when dp.category_group = 'Clothing' then fol.line_total else 0 end) as clothing_revenue,
        sum(case when dp.category_group = 'Accessories' then fol.line_total else 0 end) as accessories_revenue,
        sum(case when dp.category_group = 'Footwear' then fol.line_total else 0 end) as footwear_revenue,
        sum(case when dp.category_group = 'Intimates' then fol.line_total else 0 end) as intimates_revenue,
        sum(case when dp.category_group = 'Activewear' then fol.line_total else 0 end) as activewear_revenue,
        
        -- Price tier performance
        count(case when dp.price_tier = 'Budget' then 1 end) as budget_items_sold,
        count(case when dp.price_tier = 'Mid-Range' then 1 end) as midrange_items_sold,
        count(case when dp.price_tier = 'Premium' then 1 end) as premium_items_sold,
        count(case when dp.price_tier = 'Luxury' then 1 end) as luxury_items_sold,
        
        -- Revenue by price tier
        sum(case when dp.price_tier = 'Budget' then fol.line_total else 0 end) as budget_revenue,
        sum(case when dp.price_tier = 'Mid-Range' then fol.line_total else 0 end) as midrange_revenue,
        sum(case when dp.price_tier = 'Premium' then fol.line_total else 0 end) as premium_revenue,
        sum(case when dp.price_tier = 'Luxury' then fol.line_total else 0 end) as luxury_revenue,
        
        -- Order line characteristics
        count(case when fol.quantity_category = 'Single Item' then 1 end) as single_item_lines,
        count(case when fol.quantity_category = 'Pair' then 1 end) as pair_lines,
        count(case when fol.quantity_category = 'Multiple Items' then 1 end) as multiple_item_lines,
        
        -- Line importance distribution
        count(case when fol.line_importance = 'Single Line Order' then 1 end) as single_line_orders,
        count(case when fol.line_importance = 'Dominant Line' then 1 end) as dominant_lines,
        count(case when fol.line_importance = 'Major Line' then 1 end) as major_lines,
        count(case when fol.line_importance = 'Minor Line' then 1 end) as minor_lines
        
    from {{ ref('fact_order_line') }} fol
    inner join {{ ref('dim_product') }} dp on fol.product_key = dp.product_key
    {% if is_incremental() %}
        where fol.order_date >= current_date - interval '3 days'
    {% endif %}
    group by date(fol.order_date), fol.order_date_key, extract(year from fol.order_date), extract(month from fol.order_date), to_char(fol.order_date, 'YYYY-MM')
),

enriched_product_metrics as (
    select 
        *,
        
        -- Calculate percentages and ratios
        case 
            when total_line_items > 0 then 
                round((unique_products_sold::numeric / total_line_items::numeric) * 100, 2)
            else 0 
        end as product_variety_pct,
        
        case 
            when total_product_revenue > 0 then 
                round((completed_product_revenue / total_product_revenue) * 100, 2)
            else 0 
        end as revenue_completion_rate_pct,
        
        case 
            when completed_product_revenue > 0 then 
                round((refunded_product_revenue / completed_product_revenue) * 100, 2)
            else 0 
        end as revenue_refund_rate_pct,
        
        case 
            when total_line_items > 0 then 
                round(total_quantity_sold::numeric / total_line_items::numeric, 2)
            else 0 
        end as avg_quantity_per_line,
        
        -- Category revenue percentages
        case 
            when total_product_revenue > 0 then 
                round((clothing_revenue / total_product_revenue) * 100, 2)
            else 0 
        end as clothing_revenue_pct,
        
        case 
            when total_product_revenue > 0 then 
                round((accessories_revenue / total_product_revenue) * 100, 2)
            else 0 
        end as accessories_revenue_pct,
        
        case 
            when total_product_revenue > 0 then 
                round((footwear_revenue / total_product_revenue) * 100, 2)
            else 0 
        end as footwear_revenue_pct,
        
        -- Price tier distribution
        case 
            when total_line_items > 0 then 
                round((premium_items_sold + luxury_items_sold)::numeric / total_line_items::numeric * 100, 2)
            else 0 
        end as premium_luxury_items_pct,
        
        case 
            when total_product_revenue > 0 then 
                round((premium_revenue + luxury_revenue) / total_product_revenue * 100, 2)
            else 0 
        end as premium_luxury_revenue_pct,
        
        -- Running totals for month-to-date
        sum(total_product_revenue) over (
            partition by year_month 
            order by metric_date 
            rows unbounded preceding
        ) as mtd_product_revenue,
        
        sum(total_quantity_sold) over (
            partition by year_month 
            order by metric_date 
            rows unbounded preceding
        ) as mtd_quantity_sold,
        
        -- 7-day rolling averages
        avg(total_product_revenue) over (
            order by metric_date 
            rows between 6 preceding and current row
        ) as avg_product_revenue_7d,
        
        avg(unique_products_sold) over (
            order by metric_date 
            rows between 6 preceding and current row
        ) as avg_unique_products_7d,
        
        avg(total_quantity_sold) over (
            order by metric_date 
            rows between 6 preceding and current row
        ) as avg_quantity_sold_7d,
        
        -- Prior day comparison
        lag(total_product_revenue, 1) over (order by metric_date) as prev_day_revenue,
        lag(unique_products_sold, 1) over (order by metric_date) as prev_day_unique_products,
        lag(total_quantity_sold, 1) over (order by metric_date) as prev_day_quantity,
        
        -- Week-over-week comparison
        lag(total_product_revenue, 7) over (order by metric_date) as wow_revenue,
        lag(unique_products_sold, 7) over (order by metric_date) as wow_unique_products,
        
        current_timestamp as metrics_updated_at
        
    from daily_product_sales
)

select * from enriched_product_metrics
order by metric_date desc