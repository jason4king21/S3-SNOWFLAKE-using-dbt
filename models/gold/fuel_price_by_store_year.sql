-- models/gold/fuel_price_store_year.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

WITH sales_data AS (
    SELECT
        fact.store_id as store_id,
        year(date) as year,
        SUM(fuel_price) AS fuel_price
    FROM {{ ref('walmart_fact_table') }} as fact
    LEFT JOIN {{ ref('Walmart_store_dim') }} as store
        on fact.store_id = store.store_id
    GROUP BY fact.store_id, year
)

SELECT * FROM sales_data