{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['date_key'], 'unique': true},
      {'columns': ['calendar_date'], 'unique': true}
    ],
    on_schema_change='fail',
    tags=['dimension']
  )
}}

WITH date_spine AS (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2020-01-01' as date)",
      end_date="cast('2027-12-31' as date)"
   )
  }}
)

SELECT 
  -- Primary key: YYYYMMDD format
  CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
  
  -- Calendar date
  date_day AS calendar_date,
  
  -- Basic date parts
  EXTRACT(YEAR FROM date_day) AS year,
  EXTRACT(QUARTER FROM date_day) AS quarter,
  EXTRACT(MONTH FROM date_day) AS month,
  EXTRACT(DAY FROM date_day) AS day,
  EXTRACT(DOW FROM date_day) AS day_of_week,
  EXTRACT(DOY FROM date_day) AS day_of_year,
  EXTRACT(WEEK FROM date_day) AS week_of_year,
  
  -- Text representations
  CAST(EXTRACT(YEAR FROM date_day) AS TEXT) AS year_text,
  TO_CHAR(date_day, 'YYYY-MM') AS year_month,
  TO_CHAR(date_day, 'YYYY-Q') || EXTRACT(QUARTER FROM date_day) AS year_quarter,
  TO_CHAR(date_day, 'Month') AS month_name,
  TO_CHAR(date_day, 'Mon') AS month_short_name,
  TO_CHAR(date_day, 'Day') AS day_name,
  TO_CHAR(date_day, 'Dy') AS day_short_name,
  
  -- Boolean flags
  CASE WHEN EXTRACT(DOW FROM date_day) BETWEEN 1 AND 5 THEN true ELSE false END AS is_weekday,
  CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN true ELSE false END AS is_weekend,
  CASE WHEN EXTRACT(DAY FROM date_day) = 1 THEN true ELSE false END AS is_first_day_of_month,
  CASE WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::date THEN true ELSE false END AS is_last_day_of_month,
  CASE WHEN date_day = DATE_TRUNC('quarter', date_day)::date THEN true ELSE false END AS is_first_day_of_quarter,
  CASE WHEN date_day = (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months' - INTERVAL '1 day')::date THEN true ELSE false END AS is_last_day_of_quarter,
  CASE WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1 THEN true ELSE false END AS is_first_day_of_year,
  CASE WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 31 THEN true ELSE false END AS is_last_day_of_year,
  
  -- Relative period classification
  CASE 
    WHEN date_day = CURRENT_DATE THEN 'Today'
    WHEN date_day = CURRENT_DATE - INTERVAL '1 day' THEN 'Yesterday'
    WHEN date_day >= CURRENT_DATE - INTERVAL '7 days' AND date_day < CURRENT_DATE THEN 'Last 7 Days'
    WHEN date_day >= CURRENT_DATE - INTERVAL '30 days' AND date_day < CURRENT_DATE THEN 'Last 30 Days'
    WHEN DATE_TRUNC('month', date_day) = DATE_TRUNC('month', CURRENT_DATE) THEN 'This Month'
    WHEN DATE_TRUNC('month', date_day) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' THEN 'Last Month'
    WHEN EXTRACT(YEAR FROM date_day) = EXTRACT(YEAR FROM CURRENT_DATE) THEN 'This Year'
    WHEN EXTRACT(YEAR FROM date_day) = EXTRACT(YEAR FROM CURRENT_DATE) - 1 THEN 'Last Year'
    ELSE 'Other'
  END AS relative_period,
  
  -- Season classification (Northern Hemisphere)
  CASE 
    WHEN EXTRACT(MONTH FROM date_day) IN (12, 1, 2) THEN 'Winter'
    WHEN EXTRACT(MONTH FROM date_day) IN (3, 4, 5) THEN 'Spring'
    WHEN EXTRACT(MONTH FROM date_day) IN (6, 7, 8) THEN 'Summer'
    WHEN EXTRACT(MONTH FROM date_day) IN (9, 10, 11) THEN 'Fall'
  END AS season
  
FROM date_spine
ORDER BY calendar_date