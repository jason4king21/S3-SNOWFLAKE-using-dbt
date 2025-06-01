{{ config(
    materialized='incremental',
    unique_key=['store_id','dept_id','date'],
    incremental_strategy='merge',
    merge_update_columns=['weekly_sales', 'isholiday', 'update_date'],
    schema='SILVER'
) }}


SELECT
    source.store as store_id,
    source.dept as dept_id,
    source.date,
    source.weekly_sales,
    source.isholiday,
    CURRENT_TIMESTAMP AS update_date,
    {% if is_incremental() %}
        COALESCE(target.insert_date, CURRENT_TIMESTAMP) AS insert_date
    {% else %}
        CURRENT_TIMESTAMP AS insert_date
    {% endif %}
FROM {{ source('source', 'DEPARTMENT') }} as source
{% if is_incremental() %}
LEFT JOIN {{ this }} AS target
    ON source.store = target.store_id 
    and source.dept = target.dept_id
    and source.date = target.date
    WHERE source.INGESTION_TIMESTAMP > (SELECT MAX(update_date) FROM {{ this }})
{% endif %}
