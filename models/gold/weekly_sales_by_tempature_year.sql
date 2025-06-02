-- models/gold/weekly_sales_by_tempature_year.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

WITH sales_data AS (
    SELECT
        store_temperature,
        year(date) AS year,
        SUM(weekly_sales) AS weekly_sales
    FROM {{ ref('walmart_fact_table') }}
    GROUP BY store_temperature, year
)

SELECT * FROM sales_data