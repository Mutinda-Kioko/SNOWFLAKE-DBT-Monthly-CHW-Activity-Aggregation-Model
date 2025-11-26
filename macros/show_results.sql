{% macro show_results() %}
  {% if execute and target.name == 'dev' %}
    {% set sql %}
      select *
      from {{ this }}
      order by chv_id, report_month
      limit 50
    {% endset %}
    {% set results = run_query(sql) %}
    {% do results.print_table() %}
  {% endif %}
{% endmacro %}