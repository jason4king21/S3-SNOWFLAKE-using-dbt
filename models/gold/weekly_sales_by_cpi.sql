-- models/gold/weekly_sales_by_cpi.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

WITH sales_data AS (
    SELECT
        TRY_TO_NUMBER(CPI) AS CPI, 
        SUM(weekly_sales) AS weekly_sales
    FROM {{ ref('walmart_fact_table') }}
    GROUP BY cpi
)

SELECT * FROM sales_data