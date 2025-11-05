{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        indexes=[
            {'columns': ['customer_key'], 'unique': true},
            {'columns': ['behavioral_segment']},
            {'columns': ['purchase_pattern']},
            {'columns': ['engagement_level']},
            {'columns': ['analysis_updated_at']},
        ],
        on_schema_change='fail',
        tags=['analysis', 'customer', 'segmentation', 'behavior', 'incremental']
    )
}}

with customer_purchase_behavior as (
    select 
        fo.customer_key,
        dc.customer_id,
        
        -- Purchase frequency
        count(distinct fo.order_key) as total_orders,
        count(distinct case when fo.order_status = 'completed' then fo.order_key end) as completed_orders,
        count(distinct date_trunc('month', fo.order_date)) as active_months,
        
        -- Purchase timing
        min(fo.order_date) as first_purchase_date,
        max(fo.order_date) as last_purchase_date,
        extract(days from (max(fo.order_date) - min(fo.order_date))) as customer_lifespan_days,
        extract(days from (current_date - max(fo.order_date))) as days_since_last_purchase,
        
        -- Purchase values
        sum(fo.order_amount) as total_spent,
        sum(case when fo.order_status = 'completed' then fo.order_amount else 0 end) as total_completed_spent,
        avg(fo.order_amount) as avg_order_value,
        min(fo.order_amount) as min_order_value,
        max(fo.order_amount) as max_order_value,
        
        -- Purchase patterns
        sum(fo.total_line_items) as total_items_purchased,
        avg(fo.total_line_items) as avg_items_per_order,
        
        -- Day of week preferences
        count(case when extract(dow from fo.order_date) in (0, 6) then 1 end) as weekend_orders,
        count(case when extract(dow from fo.order_date) between 1 and 5 then 1 end) as weekday_orders,
        
        -- Seasonal patterns
        count(case when extract(month from fo.order_date) in (12, 1, 2) then 1 end) as winter_orders,
        count(case when extract(month from fo.order_date) in (3, 4, 5) then 1 end) as spring_orders,
        count(case when extract(month from fo.order_date) in (6, 7, 8) then 1 end) as summer_orders,
        count(case when extract(month from fo.order_date) in (9, 10, 11) then 1 end) as fall_orders
        
    from {{ ref('fact_orders') }} fo
    inner join {{ ref('dim_customer') }} dc on fo.customer_key = dc.customer_key
    {% if is_incremental() %}
        where fo.order_date >= current_date - interval '3 days'
    {% endif %}
    group by fo.customer_key, dc.customer_id
),

customer_product_preferences as (
    select 
        fol.customer_key,
        
        -- Product diversity
        count(distinct fol.product_key) as unique_products_purchased,
        count(distinct dp.category) as unique_categories_purchased,
        count(distinct dp.category_group) as unique_category_groups_purchased,
        
        -- Price preferences
        avg(fol.unit_price) as avg_price_preference,
        min(fol.unit_price) as min_price_purchased,
        max(fol.unit_price) as max_price_purchased,
        
        -- Category preferences (top category by spend)
        mode() within group (order by dp.category_group) as preferred_category_group,
        
        -- Premium preferences
        count(case when dp.price_tier in ('Premium', 'Luxury') then 1 end) as premium_purchases,
        count(case when dp.is_limited_edition then 1 end) as limited_edition_purchases,
        
        -- Purchase behaviors
        sum(fol.quantity) as total_quantity_purchased,
        avg(fol.quantity) as avg_quantity_per_line,
        
        -- Line importance patterns
        count(case when fol.line_importance = 'Single Line Order' then 1 end) as single_line_orders,
        count(case when fol.line_importance = 'Dominant Line' then 1 end) as dominant_line_purchases
        
    from {{ ref('fact_order_line') }} fol
    inner join {{ ref('dim_product') }} dp on fol.product_key = dp.product_key
    where fol.order_status = 'completed'
    {% if is_incremental() %}
        and fol.order_date >= current_date - interval '3 days'
    {% endif %}
    group by fol.customer_key
),

customer_behavioral_metrics as (
    select 
        cpb.*,
        cpp.unique_products_purchased,
        cpp.unique_categories_purchased,
        cpp.unique_category_groups_purchased,
        cpp.avg_price_preference,
        cpp.preferred_category_group,
        cpp.premium_purchases,
        cpp.limited_edition_purchases,
        cpp.total_quantity_purchased,
        cpp.avg_quantity_per_line,
        cpp.single_line_orders,
        cpp.dominant_line_purchases,
        
        -- Calculate behavioral scores
        case 
            when cpb.customer_lifespan_days > 0 then 
                round(cpb.total_orders::numeric / (cpb.customer_lifespan_days::numeric / 30.0), 2)
            else cpb.total_orders 
        end as purchase_frequency_monthly,
        
        case 
            when cpb.total_orders > 0 then 
                round(cpp.unique_products_purchased::numeric / cpb.total_orders::numeric, 2)
            else 0 
        end as product_exploration_ratio,
        
        case 
            when cpb.total_orders > 0 then 
                round(cpb.weekend_orders::numeric / cpb.total_orders::numeric * 100, 2)
            else 0 
        end as weekend_preference_pct,
        
        case 
            when cpp.unique_products_purchased > 0 then 
                round(cpp.premium_purchases::numeric / cpp.unique_products_purchased::numeric * 100, 2)
            else 0 
        end as premium_preference_pct
        
    from customer_purchase_behavior cpb
    left join customer_product_preferences cpp on cpb.customer_key = cpp.customer_key
),

customer_segmentation as (
    select 
        *,
        
        -- RFM-style behavioral segmentation
        case 
            when days_since_last_purchase <= 30 and purchase_frequency_monthly >= 2 and avg_order_value >= 150 then 'VIP Active'
            when days_since_last_purchase <= 60 and purchase_frequency_monthly >= 1 and avg_order_value >= 100 then 'High Value Active'
            when days_since_last_purchase <= 90 and total_orders >= 5 then 'Regular Active'
            when days_since_last_purchase <= 180 and total_orders >= 3 then 'Moderate Active'
            when days_since_last_purchase <= 365 and total_orders >= 2 then 'Low Active'
            when total_orders = 1 then 'One-Time Customer'
            else 'Inactive'
        end as behavioral_segment,
        
        -- Purchase pattern classification
        case 
            when product_exploration_ratio >= 0.8 then 'Explorer'
            when product_exploration_ratio >= 0.5 then 'Variety Seeker'
            when product_exploration_ratio >= 0.3 then 'Moderate Repeater'
            when product_exploration_ratio >= 0.2 then 'Brand Loyal'
            else 'Highly Loyal'
        end as purchase_pattern,
        
        -- Engagement level
        case 
            when active_months >= 12 and purchase_frequency_monthly >= 1 then 'Highly Engaged'
            when active_months >= 6 and purchase_frequency_monthly >= 0.5 then 'Engaged'
            when active_months >= 3 and purchase_frequency_monthly >= 0.25 then 'Moderately Engaged'
            when active_months >= 1 then 'Low Engagement'
            else 'Single Purchase'
        end as engagement_level,
        
        -- Value tier
        case 
            when total_completed_spent >= 2000 then 'Platinum'
            when total_completed_spent >= 1000 then 'Gold'
            when total_completed_spent >= 500 then 'Silver'
            when total_completed_spent >= 100 then 'Bronze'
            else 'Entry'
        end as value_tier,
        
        -- Shopping preferences
        case 
            when weekend_preference_pct >= 60 then 'Weekend Shopper'
            when weekend_preference_pct >= 40 then 'Mixed Shopper'
            else 'Weekday Shopper'
        end as timing_preference,
        
        case 
            when premium_preference_pct >= 50 then 'Premium Buyer'
            when premium_preference_pct >= 25 then 'Occasional Premium'
            when premium_preference_pct >= 10 then 'Budget Conscious'
            else 'Value Seeker'
        end as price_sensitivity,
        
        -- Seasonal preferences
        case 
            when fall_orders = greatest(winter_orders, spring_orders, summer_orders, fall_orders) then 'Fall Shopper'
            when winter_orders = greatest(winter_orders, spring_orders, summer_orders, fall_orders) then 'Winter Shopper'
            when spring_orders = greatest(winter_orders, spring_orders, summer_orders, fall_orders) then 'Spring Shopper'
            when summer_orders = greatest(winter_orders, spring_orders, summer_orders, fall_orders) then 'Summer Shopper'
            else 'Year-Round Shopper'
        end as seasonal_preference
        
    from customer_behavioral_metrics
),

final_customer_analysis as (
    select 
        cs.*,
        dc.customer_name,
        dc.region,
        dc.tenure_segment,
        dc.customer_segment as demographic_segment,
        
        -- Marketing personas
        case 
            when cs.behavioral_segment in ('VIP Active', 'High Value Active') and cs.premium_preference_pct >= 30 then 'Premium Enthusiast'
            when cs.behavioral_segment in ('VIP Active', 'High Value Active') and cs.product_exploration_ratio >= 0.6 then 'Engaged Explorer'
            when cs.behavioral_segment = 'Regular Active' and cs.purchase_pattern = 'Brand Loyal' then 'Loyal Regular'
            when cs.behavioral_segment = 'Regular Active' and cs.purchase_pattern in ('Explorer', 'Variety Seeker') then 'Active Browser'
            when cs.behavioral_segment in ('Moderate Active', 'Low Active') and cs.total_completed_spent >= 500 then 'Sleeping Giant'
            when cs.behavioral_segment = 'One-Time Customer' and cs.avg_order_value >= 100 then 'High-Value Trial'
            when cs.behavioral_segment = 'One-Time Customer' then 'Trial Customer'
            when cs.behavioral_segment = 'Inactive' and cs.total_completed_spent >= 300 then 'Win-Back Target'
            else 'Standard Customer'
        end as marketing_persona,
        
        -- Risk assessment
        case 
            when cs.days_since_last_purchase <= 60 then 'Low Risk'
            when cs.days_since_last_purchase <= 120 and cs.total_orders >= 3 then 'Medium Risk'
            when cs.days_since_last_purchase <= 180 and cs.total_completed_spent >= 200 then 'High Risk'
            else 'Lost Customer'
        end as churn_risk,
        
        current_timestamp as analysis_updated_at
        
    from customer_segmentation cs
    inner join {{ ref('dim_customer') }} dc on cs.customer_key = dc.customer_key
)

select * from final_customer_analysis
order by total_completed_spent desc