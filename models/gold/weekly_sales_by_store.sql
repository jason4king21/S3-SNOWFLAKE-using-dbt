-- models/gold/weekly_sales_by_store.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

WITH sales_data AS (
    SELECT
        store_id,
        isholiday,
        SUM(weekly_sales) AS weekly_sales
    FROM {{ ref('walmart_fact_table') }}
    GROUP BY store_id, isholiday
)

SELECT * FROM sales_data