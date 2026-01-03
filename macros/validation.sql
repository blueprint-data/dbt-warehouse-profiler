{% macro validate_source(schema, table) %}
  {{ return(adapter.dispatch('validate_source', 'dbt_warehouse_profiler')(schema, table)) }}
{% endmacro %}


{% macro validate_dataset_sources(schema) %}
  {{ return(adapter.dispatch('validate_dataset_sources', 'dbt_warehouse_profiler')(schema)) }}
{% endmacro %}
