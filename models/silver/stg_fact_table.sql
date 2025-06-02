{{ config(
    materialized='incremental',
    unique_key=['store_id','dept_id','date'],
    incremental_strategy='merge',
    merge_update_columns=[
        'date_id', 
        'weekly_sales',
        'Fuel_price',
        'Store_temperature',
        'unemployment',
        'cpi',
        'Markdown1',
        'Markdown2',
        'Markdown3',
        'Markdown4',
        'Markdown5',
        'isholiday', 
        'update_date',
        ],
    schema='SILVER'
) }}

WITH source AS (
    SELECT
        fact.store as store_id,
        deptfact.dept_id as dept_id,
        fact.date,
        deptfact.weekly_sales as weekly_sales,
        fact.Fuel_price,
        fact.temperature as Store_temperature,
        fact.unemployment,
        fact.CPI,
        CASE WHEN fact.Markdown1 = 'NA' THEN 0 ELSE CAST(fact.Markdown1 AS FLOAT) END AS MARKDOWN1,
        CASE WHEN fact.Markdown2 = 'NA' THEN 0 ELSE CAST(fact.Markdown2 AS FLOAT) END AS MARKDOWN2,
        CASE WHEN fact.Markdown3 = 'NA' THEN 0 ELSE CAST(fact.Markdown3 AS FLOAT) END AS MARKDOWN3,
        CASE WHEN fact.Markdown4 = 'NA' THEN 0 ELSE CAST(fact.Markdown4 AS FLOAT) END AS MARKDOWN4,
        CASE WHEN fact.Markdown5 = 'NA' THEN 0 ELSE CAST(fact.Markdown5 AS FLOAT) END AS MARKDOWN5,
        fact.isholiday,
        fact.INGESTION_TIMESTAMP
        
    FROM {{ ref('Walmart_department_fact') }} AS deptfact
    LEFT JOIN {{ source('source', 'FACT') }} as fact
    ON fact.store = deptfact.store_id and fact.date = deptfact.date
)

SELECT
    source.store_id,
    source.dept_id,
    source.date,
    datedim.date_id,
    source.weekly_sales,
    source.Fuel_price,
    source.Store_temperature,
    source.unemployment,
    source.CPI,
    source.Markdown1,
    source.Markdown2,
    source.Markdown3,
    source.Markdown4,
    source.Markdown5,
    source.isholiday,
    CURRENT_TIMESTAMP AS update_date,
    {% if is_incremental() %}
        COALESCE(target.insert_date, CURRENT_TIMESTAMP) AS insert_date
    {% else %}
        CURRENT_TIMESTAMP AS insert_date
    {% endif %}
FROM source
LEFT JOIN {{ ref('Walmart_date_dim') }} AS datedim
    ON source.store_id = datedim.store_id and source.date = datedim.store_date

{% if is_incremental() %}
LEFT JOIN {{ this }} AS target
    ON source.store_id = target.store_id
    WHERE source.INGESTION_TIMESTAMP > (SELECT MAX(update_date) FROM {{ this }})
{% endif %}