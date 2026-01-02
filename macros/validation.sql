{% macro validate_source(schema, table) %}
  {{ return(adapter.dispatch('validate_source', 'warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% macro validate_dataset_sources(schema) %}
  {{ return(adapter.dispatch('validate_dataset_sources', 'warehouse_profiler')(schema)) }}
{% endmacro %}
