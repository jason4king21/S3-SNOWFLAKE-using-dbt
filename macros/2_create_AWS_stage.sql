{% macro create_AWS_stage() %}
  {% do run_query("""
    CREATE OR REPLACE STAGE my_stage
    URL = 's3://jk-walmart-data-input/data/'
    FILE_FORMAT = my_csv_format
    STORAGE_INTEGRATION = my_s3_integration;
  """) %}
{% endmacro %}