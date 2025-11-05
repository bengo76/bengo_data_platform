{{
    config(
        materialized='incremental',
        unique_key='metric_date',
        indexes=[
            {'columns': ['metric_date'], 'unique': true},
            {'columns': ['year_month']},
            {'columns': ['metric_date', 'business_health_score']},
            {'columns': ['analysis_updated_at']},
        ],
        on_schema_change='fail',
        tags=['analysis', 'dashboard', 'metrics', 'incremental']
    )
}}

/*
Cross-functional metrics analysis combining customer, order, and product metrics
to provide comprehensive business insights and health scoring.
*/

with combined_metrics as (
    select 
        coalesce(mdc.metric_date, mdo.order_date, mdp.metric_date) as metric_date,
        coalesce(mdc.date_key, mdo.date_key, mdp.date_key) as date_key,
        coalesce(mdc.year_month, mdo.year_month, mdp.year_month) as year_month,
        coalesce(mdc.metric_year, mdo.year, mdp.metric_year) as metric_year,
        coalesce(mdc.metric_month, mdo.month, mdp.metric_month) as metric_month,
        
        -- Customer metrics
        coalesce(mdc.active_customers, 0) as active_customers,
        coalesce(mdc.new_customers_acquired, 0) as new_customers_acquired,
        coalesce(mdc.total_customer_spend, 0) as total_customer_spend,
        coalesce(mdc.avg_spend_per_customer, 0) as avg_spend_per_customer,
        coalesce(mdc.completion_rate_by_customers_pct, 0) as customer_completion_rate_pct,
        coalesce(mdc.avg_orders_per_active_customer, 0) as avg_orders_per_active_customer,
        coalesce(mdc.premium_loyal_customers, 0) as premium_loyal_customers,
        coalesce(mdc.core_customers, 0) as core_customers,
        coalesce(mdc.growing_customers, 0) as growing_customers,
        coalesce(mdc.new_customers, 0) as new_customers_active,
        coalesce(mdc.avg_active_customers_7d, 0) as avg_active_customers_7d,
        coalesce(mdc.avg_new_customers_7d, 0) as avg_new_customers_7d,
        
        -- Order metrics
        coalesce(mdo.total_orders, 0) as total_orders,
        coalesce(mdo.completed_orders, 0) as completed_orders,
        coalesce(mdo.cancelled_orders, 0) as cancelled_orders,
        coalesce(mdo.refunded_orders, 0) as refunded_orders,
        coalesce(mdo.total_revenue, 0) as total_revenue,
        coalesce(mdo.completed_revenue, 0) as completed_revenue,
        coalesce(mdo.avg_order_value, 0) as avg_order_value,
        coalesce(mdo.completion_rate_pct, 0) as order_completion_rate_pct,
        coalesce(mdo.cancellation_rate_pct, 0) as order_cancellation_rate_pct,
        coalesce(mdo.refund_rate_pct, 0) as order_refund_rate_pct,
        coalesce(mdo.unique_customers, 0) as ordering_customers,
        coalesce(mdo.avg_line_items_per_order, 0) as avg_line_items_per_order,
        coalesce(mdo.weekend_order_pct, 0) as weekend_order_pct,
        coalesce(mdo.avg_7d_total_orders, 0) as avg_orders_7d,
        coalesce(mdo.avg_7d_total_revenue, 0) as avg_revenue_7d,
        
        -- Product metrics
        coalesce(mdp.unique_products_sold, 0) as unique_products_sold,
        coalesce(mdp.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(mdp.total_product_revenue, 0) as total_product_revenue,
        coalesce(mdp.avg_unit_price, 0) as avg_unit_price,
        coalesce(mdp.product_variety_pct, 0) as product_variety_pct,
        coalesce(mdp.premium_luxury_revenue_pct, 0) as premium_luxury_revenue_pct,
        coalesce(mdp.clothing_revenue_pct, 0) as clothing_revenue_pct,
        coalesce(mdp.accessories_revenue_pct, 0) as accessories_revenue_pct,
        coalesce(mdp.footwear_revenue_pct, 0) as footwear_revenue_pct,
        coalesce(mdp.avg_product_revenue_7d, 0) as avg_product_revenue_7d,
        coalesce(mdp.avg_unique_products_7d, 0) as avg_unique_products_7d
        
    from {{ ref('metric_daily_customers') }} mdc
    full outer join {{ ref('metric_daily_orders') }} mdo 
        on mdc.date_key = mdo.date_key
    full outer join {{ ref('metric_daily_products') }} mdp 
        on coalesce(mdc.date_key, mdo.date_key) = mdp.date_key
    
    where coalesce(mdc.metric_date, mdo.order_date, mdp.metric_date) is not null
    {% if is_incremental() %}
        and coalesce(mdc.metric_date, mdo.order_date, mdp.metric_date) >= current_date - interval '3 days'
    {% endif %}
),

business_intelligence as (
    select 
        *,
        
        -- === GROWTH INDICATORS ===
        case 
            when active_customers > 0 and new_customers_acquired > 0 then
                round((new_customers_acquired::numeric / active_customers::numeric) * 100, 2)
            else 0 
        end as customer_acquisition_rate_pct,
        
        case 
            when active_customers > 0 then
                round(total_revenue / active_customers, 2)
            else 0
        end as revenue_per_active_customer,
        
        case 
            when ordering_customers > 0 then
                round(completed_revenue / ordering_customers, 2)
            else 0
        end as completed_revenue_per_ordering_customer,
        
        -- === EFFICIENCY INDICATORS ===
        case 
            when total_orders > 0 then
                round(unique_products_sold::numeric / total_orders::numeric, 2)
            else 0
        end as products_per_order_ratio,
        
        case 
            when unique_products_sold > 0 then
                round(total_quantity_sold::numeric / unique_products_sold::numeric, 2)
            else 0
        end as avg_quantity_per_product,
        
        case 
            when active_customers > 0 and ordering_customers > 0 then
                round((ordering_customers::numeric / active_customers::numeric) * 100, 2)
            else 0
        end as customer_order_conversion_pct,
        
        -- === PREMIUM INDICATORS ===
        case 
            when active_customers > 0 then
                round((premium_loyal_customers + core_customers)::numeric / active_customers::numeric * 100, 2)
            else 0
        end as premium_core_customer_pct,
        
        case 
            when avg_order_value > 0 and avg_unit_price > 0 then
                round(avg_order_value / avg_unit_price, 2)
            else 0
        end as items_per_order_value_ratio,
        
        -- === HEALTH INDICATORS ===
        case 
            when total_revenue > 0 then
                round(((completed_revenue - coalesce(total_revenue - completed_revenue, 0)) / total_revenue) * 100, 2)
            else 0
        end as net_revenue_success_pct,
        
        case 
            when total_orders > 0 then
                round((completed_orders::numeric / total_orders::numeric) * 100, 2)
            else 0
        end as overall_fulfillment_rate_pct,
        
        -- === TREND INDICATORS ===
        case 
            when avg_active_customers_7d > 0 then
                round(((active_customers - avg_active_customers_7d) / avg_active_customers_7d) * 100, 2)
            else 0
        end as customer_growth_vs_7d_avg_pct,
        
        case 
            when avg_revenue_7d > 0 then
                round(((total_revenue - avg_revenue_7d) / avg_revenue_7d) * 100, 2)
            else 0
        end as revenue_growth_vs_7d_avg_pct,
        
        case 
            when avg_orders_7d > 0 then
                round(((total_orders - avg_orders_7d) / avg_orders_7d) * 100, 2)
            else 0
        end as order_growth_vs_7d_avg_pct
        
    from combined_metrics
),

business_health_scoring as (
    select 
        *,
        
        -- === BUSINESS HEALTH SCORE COMPONENTS ===
        -- Customer Health (0-25 points)
        least(25, greatest(0, 
            case 
                when customer_acquisition_rate_pct >= 10 then 10
                when customer_acquisition_rate_pct >= 5 then 7
                when customer_acquisition_rate_pct >= 2 then 5
                when customer_acquisition_rate_pct >= 1 then 3
                else 1
            end +
            case 
                when customer_completion_rate_pct >= 90 then 8
                when customer_completion_rate_pct >= 80 then 6
                when customer_completion_rate_pct >= 70 then 4
                when customer_completion_rate_pct >= 60 then 2
                else 0
            end +
            case 
                when premium_core_customer_pct >= 40 then 7
                when premium_core_customer_pct >= 30 then 5
                when premium_core_customer_pct >= 20 then 3
                when premium_core_customer_pct >= 10 then 1
                else 0
            end
        )) as customer_health_score,
        
        -- Order Health (0-25 points)
        least(25, greatest(0,
            case 
                when overall_fulfillment_rate_pct >= 95 then 10
                when overall_fulfillment_rate_pct >= 85 then 8
                when overall_fulfillment_rate_pct >= 75 then 6
                when overall_fulfillment_rate_pct >= 65 then 4
                when overall_fulfillment_rate_pct >= 50 then 2
                else 0
            end +
            case 
                when avg_order_value >= 150 then 8
                when avg_order_value >= 100 then 6
                when avg_order_value >= 75 then 4
                when avg_order_value >= 50 then 2
                else 1
            end +
            case 
                when order_cancellation_rate_pct <= 5 then 7
                when order_cancellation_rate_pct <= 10 then 5
                when order_cancellation_rate_pct <= 15 then 3
                when order_cancellation_rate_pct <= 20 then 1
                else 0
            end
        )) as order_health_score,
        
        -- Revenue Health (0-25 points)
        least(25, greatest(0,
            case 
                when net_revenue_success_pct >= 95 then 10
                when net_revenue_success_pct >= 90 then 8
                when net_revenue_success_pct >= 85 then 6
                when net_revenue_success_pct >= 80 then 4
                when net_revenue_success_pct >= 75 then 2
                else 0
            end +
            case 
                when revenue_per_active_customer >= 200 then 8
                when revenue_per_active_customer >= 150 then 6
                when revenue_per_active_customer >= 100 then 4
                when revenue_per_active_customer >= 50 then 2
                else 1
            end +
            case 
                when premium_luxury_revenue_pct >= 30 then 7
                when premium_luxury_revenue_pct >= 20 then 5
                when premium_luxury_revenue_pct >= 15 then 3
                when premium_luxury_revenue_pct >= 10 then 1
                else 0
            end
        )) as revenue_health_score,
        
        -- Growth Health (0-25 points)
        least(25, greatest(0,
            case 
                when customer_growth_vs_7d_avg_pct >= 10 then 10
                when customer_growth_vs_7d_avg_pct >= 5 then 8
                when customer_growth_vs_7d_avg_pct >= 0 then 6
                when customer_growth_vs_7d_avg_pct >= -5 then 4
                when customer_growth_vs_7d_avg_pct >= -10 then 2
                else 0
            end +
            case 
                when revenue_growth_vs_7d_avg_pct >= 10 then 8
                when revenue_growth_vs_7d_avg_pct >= 5 then 6
                when revenue_growth_vs_7d_avg_pct >= 0 then 4
                when revenue_growth_vs_7d_avg_pct >= -5 then 2
                else 0
            end +
            case 
                when order_growth_vs_7d_avg_pct >= 10 then 7
                when order_growth_vs_7d_avg_pct >= 5 then 5
                when order_growth_vs_7d_avg_pct >= 0 then 3
                when order_growth_vs_7d_avg_pct >= -5 then 1
                else 0
            end
        )) as growth_health_score
        
    from business_intelligence
),

final_analysis as (
    select 
        *,
        
        -- === OVERALL BUSINESS HEALTH SCORE ===
        customer_health_score + order_health_score + revenue_health_score + growth_health_score as business_health_score,
        
        -- === HEALTH GRADE ===
        case 
            when (customer_health_score + order_health_score + revenue_health_score + growth_health_score) >= 90 then 'A+ Excellent'
            when (customer_health_score + order_health_score + revenue_health_score + growth_health_score) >= 80 then 'A Good'
            when (customer_health_score + order_health_score + revenue_health_score + growth_health_score) >= 70 then 'B+ Above Average'
            when (customer_health_score + order_health_score + revenue_health_score + growth_health_score) >= 60 then 'B Average'
            when (customer_health_score + order_health_score + revenue_health_score + growth_health_score) >= 50 then 'C+ Below Average'
            when (customer_health_score + order_health_score + revenue_health_score + growth_health_score) >= 40 then 'C Poor'
            else 'D Critical'
        end as business_health_grade,
        
        -- === KEY ALERTS ===
        case 
            when order_cancellation_rate_pct > 25 then 'HIGH_CANCELLATION'
            when customer_acquisition_rate_pct < 1 then 'LOW_ACQUISITION' 
            when overall_fulfillment_rate_pct < 70 then 'LOW_FULFILLMENT'
            when revenue_growth_vs_7d_avg_pct < -15 then 'REVENUE_DECLINE'
            when customer_growth_vs_7d_avg_pct < -15 then 'CUSTOMER_DECLINE'
            else null
        end as primary_alert,
        
        -- === OPPORTUNITIES ===
        case 
            when premium_luxury_revenue_pct < 15 and avg_order_value < 100 then 'UPSELL_OPPORTUNITY'
            when customer_order_conversion_pct < 80 then 'CONVERSION_OPPORTUNITY'
            when product_variety_pct < 50 then 'CROSS_SELL_OPPORTUNITY'
            when weekend_order_pct < 25 then 'WEEKEND_GROWTH_OPPORTUNITY'
            else null
        end as primary_opportunity,
        
        current_timestamp as analysis_updated_at
        
    from business_health_scoring
)

select 
    metric_date,
    date_key,
    year_month,
    metric_year,
    metric_month,
    
    -- === CORE BUSINESS METRICS ===
    active_customers,
    new_customers_acquired,
    total_orders,
    completed_orders,
    total_revenue,
    completed_revenue,
    avg_order_value,
    unique_products_sold,
    
    -- === CUSTOMER INSIGHTS ===
    customer_acquisition_rate_pct,
    customer_completion_rate_pct,
    revenue_per_active_customer,
    avg_orders_per_active_customer,
    premium_core_customer_pct,
    customer_order_conversion_pct,
    
    -- === ORDER & PRODUCT INSIGHTS ===
    order_completion_rate_pct,
    order_cancellation_rate_pct,
    order_refund_rate_pct,
    overall_fulfillment_rate_pct,
    products_per_order_ratio,
    product_variety_pct,
    premium_luxury_revenue_pct,
    weekend_order_pct,
    
    -- === REVENUE INSIGHTS ===
    net_revenue_success_pct,
    completed_revenue_per_ordering_customer,
    avg_unit_price,
    clothing_revenue_pct,
    accessories_revenue_pct,
    footwear_revenue_pct,
    
    -- === GROWTH & TRENDS ===
    customer_growth_vs_7d_avg_pct,
    revenue_growth_vs_7d_avg_pct,
    order_growth_vs_7d_avg_pct,
    avg_active_customers_7d,
    avg_revenue_7d,
    avg_orders_7d,
    
    -- === HEALTH SCORING ===
    customer_health_score,
    order_health_score,
    revenue_health_score,
    growth_health_score,
    business_health_score,
    business_health_grade,
    
    -- === ALERTS & OPPORTUNITIES ===
    primary_alert,
    primary_opportunity,
    
    analysis_updated_at

from final_analysis
order by metric_date desc