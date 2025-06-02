-- models/gold/weekly_sales_by_store_type_month.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

WITH sales_data AS (
    SELECT
        type,
        month(date) as month,
        SUM(weekly_sales) AS weekly_sales
    FROM {{ ref('walmart_fact_table') }} as fact
    LEFT JOIN {{ ref('Walmart_store_dim') }} as store
        on fact.store_id = store.store_id
    GROUP BY type, month
)

SELECT * FROM sales_data