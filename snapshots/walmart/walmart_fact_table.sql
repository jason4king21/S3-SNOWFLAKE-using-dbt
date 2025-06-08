{% snapshot walmart_fact_table %}

{{
    config(
        target_schema='SILVER',
        target_database='WALMART',
        unique_key="concat(DATE_ID, '-', STORE_ID, '-', DEPT_ID)",
        strategy='check',
        check_cols=[
            'WEEKLY_SALES',
            'FUEL_PRICE',
            'STORE_TEMPERATURE',
            'UNEMPLOYMENT',
            'CPI',
            'MARKDOWN1',
            'MARKDOWN2',
            'MARKDOWN3',
            'MARKDOWN4',
            'MARKDOWN5',
            'ISHOLIDAY'
        ]
    )
}}

SELECT
    DATE_ID,
    STORE_ID,
    DATE,
    DEPT_ID,
    WEEKLY_SALES,
    FUEL_PRICE,
    STORE_TEMPERATURE,
    UNEMPLOYMENT,
    CPI,
    MARKDOWN1,
    MARKDOWN2,
    MARKDOWN3,
    MARKDOWN4,
    MARKDOWN5,
    ISHOLIDAY
FROM {{ ref('stg_fact_table') }}

{% endsnapshot %}