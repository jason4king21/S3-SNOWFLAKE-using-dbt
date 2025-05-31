{% macro create_control_table() %}
  {% do run_query("""
    CREATE OR REPLACE TABLE control_table (
      file_name STRING,
      table_name STRING,
      md5 STRING
    );
  """) %}
{% endmacro %}