{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        indexes=[
            {'columns': ['customer_key'], 'unique': true},
            {'columns': ['customer_id'], 'unique': true},
            {'columns': ['email'], 'unique': true},
            {'columns': ['customer_segment']},
            {'columns': ['region']},
            {'columns': ['tenure_segment']},
        ],
        on_schema_change='fail',
        tags=['dimension', 'incremental']
    )
}}

with customer_base as (
    select 
        id as customer_id,
        name,
        email,
        signup_date,
        country,
        created_at,
        signup_year,
        signup_month,
        signup_day_of_week,
        customer_tenure_days,
        region
    from {{ ref('stg_customers') }}
    {% if is_incremental() %}
        where created_at >= current_date - interval '3 days'
    {% endif %}
),

customer_with_analytics as (
    select
        customer_id,
        name,
        email,
        signup_date,
        country,
        created_at,
        signup_year,
        signup_month,
        signup_day_of_week,
        customer_tenure_days,
        region as original_region,
        
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        
        -- Tenure analysis based on actual days
        case 
            when customer_tenure_days >= 365 * 2 then 'Loyal (2+ years)'
            when customer_tenure_days >= 365 then 'Established (1-2 years)'
            when customer_tenure_days >= 180 then 'Regular (6-12 months)'
            when customer_tenure_days >= 90 then 'Growing (3-6 months)'
            when customer_tenure_days >= 30 then 'New (1-3 months)'
            else 'Fresh (< 1 month)'
        end as tenure_segment,
        
        -- Quartile calculation for tenure
        case 
            when customer_tenure_days >= 547 then 4  -- Approximate 75th percentile
            when customer_tenure_days >= 365 then 3  -- Approximate 50th percentile  
            when customer_tenure_days >= 182 then 2  -- Approximate 25th percentile
            else 1
        end as tenure_quartile,
        
        -- Geographic analysis with proper case and consolidation
        case 
            when region = 'north america' then 'North America'
            when region = 'europe' then 'Europe'
            when region = 'asia pacific' then 'Asia Pacific'
            else 'Other'  -- Consolidate south america, africa, and other into 'Other'
        end as region,
        
        case 
            when region = 'north america' then 'NA'
            when region = 'europe' then 'EU'
            when region = 'asia pacific' then 'APAC'
            else 'Other'
        end as region_code,
        
        -- Signup timing analysis
        case 
            when signup_day_of_week in (1, 7) then 'Weekend'
            else 'Weekday'
        end as signup_timing,
        
        case 
            when signup_month in (12, 1, 2) then 'Winter'
            when signup_month in (3, 4, 5) then 'Spring'
            when signup_month in (6, 7, 8) then 'Summer'
            when signup_month in (9, 10, 11) then 'Fall'
        end as signup_season,
        
        -- Customer lifecycle status
        case 
            when customer_tenure_days >= 0 then true
            else false
        end as is_current_customer,
        
        -- Risk assessment
        case 
            when customer_tenure_days < 30 then 'High Risk'
            when customer_tenure_days < 90 then 'Medium Risk'
            else 'Low Risk'
        end as churn_risk_segment
    from customer_base
),

final as (
    select
        customer_key,
        customer_id,
        name as customer_name,
        email,
        signup_date,
        country,
        region,
        region_code,
        created_at,
        
        -- Tenure metrics
        customer_tenure_days,
        tenure_segment,
        tenure_quartile,
        
        -- Signup analysis
        signup_year,
        signup_month,
        signup_day_of_week,
        signup_timing,
        signup_season,
        
        -- Segmentation
        case 
            when tenure_quartile = 4 and region in ('North America', 'Europe') then 'Premium Loyal'
            when tenure_quartile >= 3 then 'Core Customer'
            when tenure_quartile = 2 then 'Growing Customer'
            else 'New Customer'
        end as customer_segment,
        
        -- Status flags
        is_current_customer,
        churn_risk_segment,
        
        -- Metadata
        current_timestamp as created_at_dim,
        current_timestamp as updated_at_dim
        
    from customer_with_analytics
)

select * from final