{{ config(
    materialized='incremental',
    unique_key=['store_id','dept_id'],
    incremental_strategy='merge',
    schema='SILVER'
) }}


WITH source AS (
    SELECT
        store as store_id,
        dept as dept_id,
        INGESTION_TIMESTAMP
        
    FROM {{ source('source', 'DEPARTMENT') }}
    GROUP BY store, dept, INGESTION_TIMESTAMP
    ORDER BY store, dept
)

SELECT
    source.store_id,
    source.dept_id,
    CURRENT_TIMESTAMP AS update_date,
    {% if is_incremental() %}
        COALESCE(target.insert_date, CURRENT_TIMESTAMP) AS insert_date
    {% else %}
        CURRENT_TIMESTAMP AS insert_date
    {% endif %}
FROM source
{% if is_incremental() %}
LEFT JOIN {{ this }} AS target
    ON source.store_id = target.store_id and source.dept_id = target.dept_id
    WHERE source.INGESTION_TIMESTAMP > (SELECT MAX(update_date) FROM {{ this }})
{% endif %}
