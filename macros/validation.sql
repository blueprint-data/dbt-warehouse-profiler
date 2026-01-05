{% macro validate_source(schema, table, database=none) %}
  {{ return(adapter.dispatch('validate_source', 'dbt_warehouse_profiler')(schema, table, database)) }}
{% endmacro %}


{% macro validate_dataset_sources(schema, database=none) %}
  {{ return(adapter.dispatch('validate_dataset_sources', 'dbt_warehouse_profiler')(schema, database)) }}
{% endmacro %}
