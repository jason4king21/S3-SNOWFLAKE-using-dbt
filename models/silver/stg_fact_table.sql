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
        'Markdown1',
        'Markdown1',
        'Markdown1',
        'Markdown1',
        'isholiday', 
        'updated_at',
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
        fact.Markdown1,
        fact.Markdown2,
        fact.Markdown3,
        fact.Markdown4,
        fact.Markdown5,
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