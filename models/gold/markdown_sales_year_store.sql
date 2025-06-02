-- models/gold/markdown_sales_year_store.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

WITH sales_data AS (
    SELECT
        store_id,
        year(date) as year,
        SUM(markdown1) AS markdown1,
        SUM(markdown2) AS markdown2,
        SUM(markdown3) AS markdown3,
        SUM(markdown4) AS markdown4,
        SUM(markdown5) AS markdown5
    FROM {{ ref('walmart_fact_table') }} as fact
    GROUP BY store_id, year
)

SELECT * FROM sales_data