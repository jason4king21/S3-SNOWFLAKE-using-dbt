-- models/gold/weekly_sales_breakdown.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

SELECT
    EXTRACT(YEAR FROM date) AS sales_year,
    TO_CHAR(date, 'MMMM') AS SALES_MONTH,
    EXTRACT(MONTH FROM date) AS month_num,
    EXTRACT(DAY FROM date) AS sales_day,
    SUM(weekly_sales) AS weekly_sales
FROM {{ ref('walmart_fact_table') }} as fact
GROUP BY 1, 2, 3, 4
