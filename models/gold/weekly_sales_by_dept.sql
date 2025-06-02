-- models/gold/weekly_sales_by_dept.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

SELECT
    DEPT_ID,
    SUM(weekly_sales) AS WEEKLY_SALES
FROM {{ ref('walmart_fact_table') }} as fact
WHERE DEPT_ID IS NOT NULL
GROUP BY DEPT_ID