-- models/gold/weekly_sales_by_store_type.sql

{{ config(
    materialized='view',
    schema='GOLD'
) }}

WITH sales_data AS (
    SELECT
        fact.store_id as store_id,
        store.type as type,
        SUM(weekly_sales) AS weekly_sales
    FROM {{ ref('walmart_fact_table') }} as fact
    LEFT JOIN {{ ref('Walmart_store_dim') }} as store
        on fact.store_id = store.store_id
    GROUP BY fact.store_id, type
)

SELECT * FROM sales_data