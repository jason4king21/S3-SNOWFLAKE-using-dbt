{{ config(
    materialized='incremental',
    unique_key='date_id',
    incremental_strategy='merge',
    merge_update_columns=['isholiday'],
    schema='SILVER'
) }}

WITH source AS (
    SELECT
        store as store_id,
        date as store_date,
        INGESTION_TIMESTAMP,
        MAX(isholiday) AS isholiday
        
    FROM {{ source('source', 'DEPARTMENT') }}
    GROUP BY store, date, INGESTION_TIMESTAMP
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source.store_id', 'source.store_date']) }} AS date_id,
    source.store_id,
    source.store_date,
    source.isholiday,
    CURRENT_TIMESTAMP AS update_date,
    {% if is_incremental() %}
        COALESCE(target.insert_date, CURRENT_TIMESTAMP) AS insert_date
    {% else %}
        CURRENT_TIMESTAMP AS insert_date
    {% endif %}
FROM source
{% if is_incremental() %}
LEFT JOIN {{ this }} AS target
    ON source.store_id = target.store_id and source.store_date = target.store_date
    WHERE source.INGESTION_TIMESTAMP > (SELECT MAX(update_date) FROM {{ this }})
{% endif %}