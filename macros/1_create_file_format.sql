{% macro create_file_format() %}
  {% do run_query("""
    CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    PARSE_HEADER = TRUE;
  """) %}
{% endmacro %}