{% macro validate_source(schema, table, database=none, output_format='text') %}
  {{ return(adapter.dispatch('validate_source', 'dbt_warehouse_profiler')(schema, table, database, output_format)) }}
{% endmacro %}


{% macro validate_dataset_sources(schema, database=none, output_format='text') %}
  {{ return(adapter.dispatch('validate_dataset_sources', 'dbt_warehouse_profiler')(schema, database, output_format)) }}
{% endmacro %}
