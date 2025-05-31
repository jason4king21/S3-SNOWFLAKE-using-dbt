{% macro create_tracking_table() %}
  {% do run_query("""
    CREATE TABLE IF NOT EXISTS imported_files (
    file_name STRING, 
    md5 STRING PRIMARY KEY,
    import_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
  """) %}
{% endmacro %}