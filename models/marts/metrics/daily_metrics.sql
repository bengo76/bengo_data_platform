{{
    config(
        materialized='incremental',
        unique_key=['date_key', 'metric_name', 'dimension_type', 'dimension_value'],
        indexes=[
            {'columns': ['date_key', 'metric_name']},
            {'columns': ['metric_date']},
            {'columns': ['metric_name']},
            {'columns': ['dimension_type', 'dimension_value']},
            {'columns': ['year_month']},
        ],
        on_schema_change='fail',
        tags=['metric', 'daily', 'unified', 'incremental']
    )
}}

-- Sustainable unified daily metrics using configuration-driven approach
{% set orders_metrics = [
    {'name': 'total_orders', 'dimension_type': 'base', 'dimension_value': 'all'},
    {'name': 'completed_orders', 'dimension_type': 'status', 'dimension_value': 'completed'},
    {'name': 'cancelled_orders', 'dimension_type': 'status', 'dimension_value': 'cancelled'},
    {'name': 'pending_orders', 'dimension_type': 'status', 'dimension_value': 'pending'},
    {'name': 'refunded_orders', 'dimension_type': 'status', 'dimension_value': 'refunded'},
    {'name': 'items_added_orders', 'dimension_type': 'status', 'dimension_value': 'items_added'},
    {'name': 'total_revenue', 'dimension_type': 'revenue_type', 'dimension_value': 'total'},
    {'name': 'completed_revenue', 'dimension_type': 'revenue_type', 'dimension_value': 'completed'},
    {'name': 'refunded_revenue', 'dimension_type': 'revenue_type', 'dimension_value': 'refunded'},
    {'name': 'avg_order_value', 'dimension_type': 'revenue_type', 'dimension_value': 'average'},
    {'name': 'unique_customers', 'dimension_type': 'customer_type', 'dimension_value': 'all'},
    {'name': 'customers_with_completed_orders', 'dimension_type': 'customer_type', 'dimension_value': 'active'},
    {'name': 'completion_rate_pct', 'dimension_type': 'rate_type', 'dimension_value': 'completion'},
    {'name': 'cancellation_rate_pct', 'dimension_type': 'rate_type', 'dimension_value': 'cancellation'},
    {'name': 'single_item_orders', 'dimension_type': 'order_size', 'dimension_value': 'single_item'},
    {'name': 'small_orders', 'dimension_type': 'order_size', 'dimension_value': 'small'},
    {'name': 'medium_orders', 'dimension_type': 'order_size', 'dimension_value': 'medium'},
    {'name': 'large_orders', 'dimension_type': 'order_size', 'dimension_value': 'large'},
    {'name': 'weekend_orders', 'dimension_type': 'timing', 'dimension_value': 'weekend'},
    {'name': 'weekday_orders', 'dimension_type': 'timing', 'dimension_value': 'weekday'},
    {'name': 'morning_orders', 'dimension_type': 'time_of_day', 'dimension_value': 'morning'},
    {'name': 'afternoon_orders', 'dimension_type': 'time_of_day', 'dimension_value': 'afternoon'},
    {'name': 'evening_orders', 'dimension_type': 'time_of_day', 'dimension_value': 'evening'},
    {'name': 'night_orders', 'dimension_type': 'time_of_day', 'dimension_value': 'night'}
] %}

{% set products_metrics = [
    {'name': 'unique_products_sold', 'dimension_type': 'product_type', 'dimension_value': 'all'},
    {'name': 'total_line_items', 'dimension_type': 'product_type', 'dimension_value': 'all'},
    {'name': 'total_quantity_sold', 'dimension_type': 'product_type', 'dimension_value': 'all'},
    {'name': 'total_product_revenue', 'dimension_type': 'product_type', 'dimension_value': 'all'},
    {'name': 'clothing_revenue', 'dimension_type': 'category', 'dimension_value': 'clothing'},
    {'name': 'accessories_revenue', 'dimension_type': 'category', 'dimension_value': 'accessories'},
    {'name': 'footwear_revenue', 'dimension_type': 'category', 'dimension_value': 'footwear'},
    {'name': 'intimates_revenue', 'dimension_type': 'category', 'dimension_value': 'intimates'},
    {'name': 'activewear_revenue', 'dimension_type': 'category', 'dimension_value': 'activewear'},
    {'name': 'clothing_products_sold', 'dimension_type': 'category', 'dimension_value': 'clothing'},
    {'name': 'accessories_products_sold', 'dimension_type': 'category', 'dimension_value': 'accessories'},
    {'name': 'footwear_products_sold', 'dimension_type': 'category', 'dimension_value': 'footwear'},
    {'name': 'intimates_products_sold', 'dimension_type': 'category', 'dimension_value': 'intimates'},
    {'name': 'activewear_products_sold', 'dimension_type': 'category', 'dimension_value': 'activewear'}
] %}

{% set customers_metrics = [
    {'name': 'active_customers', 'dimension_type': 'customer_activity', 'dimension_value': 'active'},
    {'name': 'customers_with_completed_orders', 'dimension_type': 'customer_activity', 'dimension_value': 'completed_orders'},
    {'name': 'new_customers', 'dimension_type': 'customer_segment', 'dimension_value': 'new_customer'},
    {'name': 'premium_loyal_customers', 'dimension_type': 'customer_segment', 'dimension_value': 'premium_loyal'},
    {'name': 'core_customers', 'dimension_type': 'customer_segment', 'dimension_value': 'core_customer'},
    {'name': 'growing_customers', 'dimension_type': 'customer_segment', 'dimension_value': 'growing_customer'},
    {'name': 'total_customer_spend', 'dimension_type': 'spend_type', 'dimension_value': 'total'},
    {'name': 'avg_spend_per_customer', 'dimension_type': 'spend_type', 'dimension_value': 'average'},
    {'name': 'highest_customer_spend', 'dimension_type': 'spend_type', 'dimension_value': 'maximum'},
    {'name': 'na_customers', 'dimension_type': 'region', 'dimension_value': 'north_america'},
    {'name': 'eu_customers', 'dimension_type': 'region', 'dimension_value': 'europe'},
    {'name': 'apac_customers', 'dimension_type': 'region', 'dimension_value': 'asia_pacific'}
] %}

with orders_base as (
    select 
        date_key,
        order_date as metric_date,
        year_month,
        {% for metric in orders_metrics %}
        {{ metric.name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('metric_daily_orders') }}
    {% if is_incremental() %}
        where order_date >= current_date - interval '3 days'
    {% endif %}
),

products_base as (
    select 
        date_key,
        metric_date,
        year_month,
        {% for metric in products_metrics %}
        {{ metric.name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('metric_daily_products') }}
    {% if is_incremental() %}
        where metric_date >= current_date - interval '3 days'
    {% endif %}
),

customers_base as (
    select 
        date_key,
        metric_date,
        year_month,
        {% for metric in customers_metrics %}
        {{ metric.name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('metric_daily_customers') }}
    {% if is_incremental() %}
        where metric_date >= current_date - interval '3 days'
    {% endif %}
),

all_metrics as (
    -- Orders metrics
    {% for metric in orders_metrics %}
    select 
        'orders' as metric_domain,
        date_key,
        metric_date,
        year_month,
        '{{ metric.name }}' as metric_name,
        {{ metric.name }} as metric_value,
        '{{ metric.dimension_type }}' as dimension_type,
        '{{ metric.dimension_value }}' as dimension_value
    from orders_base
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
    
    union all
    
    -- Products metrics  
    {% for metric in products_metrics %}
    select 
        'products' as metric_domain,
        date_key,
        metric_date,
        year_month,
        '{{ metric.name }}' as metric_name,
        {{ metric.name }} as metric_value,
        '{{ metric.dimension_type }}' as dimension_type,
        '{{ metric.dimension_value }}' as dimension_value
    from products_base
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
    
    union all
    
    -- Customers metrics
    {% for metric in customers_metrics %}
    select 
        'customers' as metric_domain,
        date_key,
        metric_date,
        year_month,
        '{{ metric.name }}' as metric_name,
        {{ metric.name }} as metric_value,
        '{{ metric.dimension_type }}' as dimension_type,
        '{{ metric.dimension_value }}' as dimension_value
    from customers_base
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
),

unified_metrics as (
    select * from all_metrics
    where metric_value is not null
)

select 
    date_key,
    metric_date,
    extract(year from metric_date) as year,
    extract(month from metric_date) as month,
    extract(quarter from metric_date) as quarter,
    year_month,
    metric_domain,
    metric_name,
    metric_value,
    dimension_type,
    dimension_value,
    current_timestamp as created_at

from unified_metrics

order by metric_date desc, metric_domain, metric_name, dimension_type, dimension_value