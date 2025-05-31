{% macro call_load_walmart_data() %}
  {% do run_query("CALL load_walmart_data();") %}
{% endmacro %}