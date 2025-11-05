{{
    config(
        materialized='incremental',
        unique_key='product_key',
        indexes=[
            {'columns': ['product_key'], 'unique': true},
            {'columns': ['product_id'], 'unique': true},
            {'columns': ['category_group']},
            {'columns': ['price_tier']},
            {'columns': ['performance_tier']},
            {'columns': ['analysis_updated_at']},
        ],
        on_schema_change='fail',
        tags=['analysis', 'product', 'incremental']
    )
}}

with product_sales_summary as (
    select 
        fol.product_key,
        fol.product_id,
        
        -- Sales volume metrics
        count(*) as total_order_lines,
        count(distinct fol.order_key) as unique_orders,
        count(distinct fol.customer_key) as unique_customers,
        sum(fol.quantity) as total_quantity_sold,
        
        -- Revenue metrics
        sum(fol.line_total) as total_revenue,
        sum(case when fol.order_status = 'completed' then fol.line_total else 0 end) as completed_revenue,
        sum(case when fol.order_status = 'refunded' then fol.line_total else 0 end) as refunded_revenue,
        sum(case when fol.order_status = 'cancelled' then fol.line_total else 0 end) as cancelled_revenue,
        avg(fol.line_total) as avg_line_value,
        avg(fol.unit_price) as avg_selling_price,
        
        -- Order line characteristics
        avg(fol.quantity) as avg_quantity_per_line,
        avg(fol.line_percentage_of_order) as avg_order_contribution_pct,
        count(case when fol.line_importance = 'Single Line Order' then 1 end) as single_line_orders,
        count(case when fol.line_importance = 'Dominant Line' then 1 end) as dominant_lines,
        count(case when fol.line_importance = 'Major Line' then 1 end) as major_lines,
        count(case when fol.line_importance = 'Minor Line' then 1 end) as minor_lines,
        
        -- Timing metrics
        min(fol.order_date) as first_sale_date,
        max(fol.order_date) as last_sale_date,
        count(distinct fol.order_date_key) as selling_days,
        
        -- Customer behavior
        count(case when fol.is_first_line_item then 1 end) as times_first_in_order,
        avg(fol.item_sequence) as avg_position_in_order
        
    from {{ ref('fact_order_line') }} fol
    {% if is_incremental() %}
        where fol.order_date >= current_date - interval '3 days'
    {% endif %}
    group by fol.product_key, fol.product_id
),

product_with_details as (
    select 
        pss.*,
        dp.product_name,
        dp.category,
        dp.category_group,
        dp.price as catalog_price,
        dp.price_tier,
        dp.detailed_price_tier,
        dp.price_decile,
        dp.price_quartile,
        dp.category_position,
        dp.is_active,
        dp.is_limited_edition,
        dp.is_premium_line,
        dp.is_classic_line,
        dp.created_at as product_created_at,
        
        -- Calculate performance metrics
        case 
            when pss.completed_revenue > 0 then 
                round((pss.refunded_revenue / pss.completed_revenue) * 100, 2)
            else 0 
        end as refund_rate_pct,
        
        case 
            when pss.total_revenue > 0 then 
                round((pss.completed_revenue / pss.total_revenue) * 100, 2)
            else 0 
        end as completion_rate_pct,
        
        case 
            when dp.price > 0 then 
                round((pss.avg_selling_price / dp.price) * 100, 2)
            else 0 
        end as price_realization_pct,
        
        -- Calculate days since launch
        current_date - dp.created_at::date as days_since_launch,
        
        -- Revenue per day since launch
        case 
            when (current_date - dp.created_at::date) > 0 then 
                pss.completed_revenue / (current_date - dp.created_at::date)
            else 0 
        end as revenue_per_day_since_launch
        
    from product_sales_summary pss
    inner join {{ ref('dim_product') }} dp on pss.product_key = dp.product_key
),

product_rankings as (
    select 
        *,
        
        -- Revenue rankings
        row_number() over (order by completed_revenue desc) as revenue_rank_overall,
        row_number() over (partition by category_group order by completed_revenue desc) as revenue_rank_in_category,
        row_number() over (partition by price_tier order by completed_revenue desc) as revenue_rank_in_price_tier,
        
        -- Volume rankings
        row_number() over (order by total_quantity_sold desc) as volume_rank_overall,
        row_number() over (partition by category_group order by total_quantity_sold desc) as volume_rank_in_category,
        
        -- Customer reach rankings
        row_number() over (order by unique_customers desc) as customer_reach_rank,
        row_number() over (partition by category_group order by unique_customers desc) as customer_reach_rank_in_category,
        
        -- Performance percentiles
        ntile(10) over (order by completed_revenue) as revenue_decile,
        ntile(4) over (order by completed_revenue) as revenue_quartile,
        ntile(10) over (order by total_quantity_sold) as volume_decile,
        ntile(4) over (order by unique_customers) as customer_reach_quartile
        
    from product_with_details
),

final_product_analysis as (
    select 
        *,
        
        -- Performance tier classification
        case 
            when revenue_quartile = 4 and customer_reach_quartile = 4 then 'Star Performer'
            when revenue_quartile = 4 then 'High Revenue'
            when customer_reach_quartile = 4 then 'Wide Appeal'
            when revenue_quartile >= 3 or customer_reach_quartile >= 3 then 'Good Performer'
            when revenue_quartile = 2 then 'Average Performer'
            else 'Low Performer'
        end as performance_tier,
        
        -- Revenue contribution
        case 
            when revenue_decile >= 9 then 'Top 10%'
            when revenue_quartile = 4 then 'Top 25%'
            when revenue_quartile >= 3 then 'Above Average'
            when revenue_quartile = 2 then 'Below Average'
            else 'Bottom 25%'
        end as revenue_contribution_tier,
        
        -- Customer penetration
        case 
            when customer_reach_quartile = 4 then 'High Penetration'
            when customer_reach_quartile >= 3 then 'Medium Penetration'
            when customer_reach_quartile = 2 then 'Low Penetration'
            else 'Very Low Penetration'
        end as customer_penetration_tier,
        
        -- Product lifecycle stage
        case 
            when days_since_launch <= 30 then 'New Launch'
            when days_since_launch <= 90 then 'Early Stage'
            when days_since_launch <= 180 then 'Growth Stage'
            when days_since_launch <= 365 then 'Mature Stage'
            else 'Established'
        end as lifecycle_stage,
        
        -- Price positioning analysis
        case 
            when price_realization_pct >= 95 then 'Full Price'
            when price_realization_pct >= 85 then 'Minimal Discount'
            when price_realization_pct >= 75 then 'Moderate Discount'
            when price_realization_pct >= 60 then 'Heavy Discount'
            else 'Deep Discount'
        end as pricing_strategy,
        
        -- Order behavior analysis
        case 
            when (single_line_orders::numeric / total_order_lines::numeric) >= 0.5 then 'Standalone Purchase'
            when (dominant_lines::numeric / total_order_lines::numeric) >= 0.3 then 'Primary Item'
            when (major_lines::numeric / total_order_lines::numeric) >= 0.3 then 'Complementary Item'
            else 'Add-On Item'
        end as purchase_behavior,
        
        current_timestamp as analysis_updated_at
        
    from product_rankings
)

select * from final_product_analysis
order by revenue_rank_overall