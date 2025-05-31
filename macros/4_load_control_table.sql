{% macro load_control_table() %}
  {% do run_query("LIST @my_stage") %}
  {% do run_query("""
    INSERT INTO control_table (file_name, table_name, md5)
    SELECT
      SPLIT_PART(\"name\", '/', -1) AS file_name,
      SPLIT_PART(SPLIT_PART(\"name\", '/', -1), '.', 1) AS table_name,
      \"md5\"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  """) %}
{% endmacro %}