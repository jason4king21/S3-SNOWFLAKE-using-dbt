{{ config(
    materialized='incremental',
    unique_key='store',
    incremental_strategy='merge',
    merge_update_columns=['type', 'size', 'updated_at'],
    schema='SILVER'
) }}

SELECT
    source.STORE,
    dept.DEPT,
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
    ON source.STORE = dept.STORE
{% if is_incremental() %}
LEFT JOIN {{ this }} AS target
    ON source.STORE = target.STORE
    WHERE source.INGESTION_TIMESTAMP > (SELECT MAX(update_date) FROM {{ this }})
{% endif %}