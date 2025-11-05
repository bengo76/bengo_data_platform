-- Examples of how to query the SCD2 fact_orders table

-- 1. Current status of all orders (latest snapshot)
select 
    order_id,
    order_status,
    status_effective_date,
    order_amount,
    total_line_items
from {{ ref('fact_orders_scd2') }}
where is_current_status = true
order by order_id;

-- 2. Complete status history for a specific order
select 
    order_id,
    status_change_sequence,
    order_status,
    status_effective_date,
    status_end_date,
    status_duration_hours,
    status_lifecycle_stage
from {{ ref('fact_orders_scd2') }}
where order_id = 'ORD-001'  -- Replace with actual order ID
order by status_change_sequence;

-- 3. Average time to completion by order size category
select 
    order_size_category,
    count(*) as completed_orders,
    avg(hours_to_completion) as avg_hours_to_completion,
    min(hours_to_completion) as min_hours_to_completion,
    max(hours_to_completion) as max_hours_to_completion
from {{ ref('fact_orders_scd2') }}
where order_status = 'completed'
    and hours_to_completion is not null
group by order_size_category
order by avg_hours_to_completion;

-- 4. Status transition analysis - how many orders go through each status
select 
    order_status,
    count(distinct order_id) as unique_orders,
    count(*) as total_status_instances,
    avg(status_duration_hours) as avg_duration_hours
from {{ ref('fact_orders_scd2') }}
where status_end_date is not null  -- Exclude current status
group by order_status
order by unique_orders desc;

-- 5. Daily order status changes (status change activity)
select 
    status_effective_date::date as change_date,
    order_status,
    count(*) as status_changes,
    count(distinct order_id) as unique_orders_affected
from {{ ref('fact_orders_scd2') }}
where status_effective_date >= current_date - interval '30 days'
group by status_effective_date::date, order_status
order by change_date desc, status_changes desc;

-- 6. Find orders that were cancelled after items were added
select 
    order_id,
    count(*) as total_status_changes,
    string_agg(order_status, ' → ' order by status_change_sequence) as status_journey
from {{ ref('fact_orders_scd2') }}
group by order_id
having string_agg(order_status, ' → ' order by status_change_sequence) like '%items added%cancelled%'
order by order_id;

-- 7. Performance metrics: Orders completing quickly vs slowly
with completion_analysis as (
    select 
        order_id,
        order_amount,
        hours_to_completion,
        case 
            when hours_to_completion <= 24 then 'Fast (≤24h)'
            when hours_to_completion <= 72 then 'Medium (24-72h)'
            else 'Slow (>72h)'
        end as completion_speed
    from {{ ref('fact_orders_scd2') }}
    where order_status = 'completed'
        and hours_to_completion is not null
)
select 
    completion_speed,
    count(*) as order_count,
    avg(order_amount) as avg_order_value,
    sum(order_amount) as total_revenue
from completion_analysis
group by completion_speed
order by 
    case completion_speed 
        when 'Fast (≤24h)' then 1
        when 'Medium (24-72h)' then 2
        when 'Slow (>72h)' then 3
    end;