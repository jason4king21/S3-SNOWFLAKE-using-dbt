{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge',
    merge_update_columns=['type', 'size', 'update_date'],
    schema='SILVER'
) }}

SELECT
    source.STORE as store_id,
    dept.dept_id,
    source.TYPE,
    source.SIZE,
    CURRENT_TIMESTAMP AS update_date,
    {% if is_incremental() %}
        COALESCE(target.insert_date, CURRENT_TIMESTAMP) AS insert_date
    {% else %}
        CURRENT_TIMESTAMP AS insert_date
    {% endif %}
FROM {{ source('source', 'STORES') }} AS source
LEFT JOIN {{ ref('Walmart_department_dim') }} AS dept
    ON source.store = dept.store_id
{% if is_incremental() %}
LEFT JOIN {{ this }} AS target
    ON source.store = target.store_id
    WHERE source.INGESTION_TIMESTAMP > (SELECT MAX(update_date) FROM {{ this }})
{% endif %}