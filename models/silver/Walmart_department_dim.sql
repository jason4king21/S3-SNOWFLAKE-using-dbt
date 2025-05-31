{{ config(
    materialized='incremental',
    unique_key=['store','dept'],
    incremental_strategy='merge',
    schema='SILVER'
) }}

SELECT
    source.STORE,
    source.dept,
    CURRENT_TIMESTAMP AS update_date,
    {% if is_incremental() %}
        COALESCE(target.insert_date, CURRENT_TIMESTAMP) AS insert_date
    {% else %}
        CURRENT_TIMESTAMP AS insert_date
    {% endif %}
FROM {{ source('source', 'DEPARTMENT') }} AS source
{% if is_incremental() %}
LEFT JOIN {{ this }} AS target
    ON source.STORE = target.STORE and source.dept = target.dept
    WHERE source.INGESTION_TIMESTAMP > (SELECT MAX(update_date) FROM {{ this }})
{% endif %}
