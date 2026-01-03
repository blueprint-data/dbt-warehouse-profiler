{% macro list_schemas(exclude_schemas=[]) %}
  {{ return(adapter.dispatch('list_schemas', 'dbt_warehouse_profiler')(exclude_schemas)) }}
{% endmacro %}

{% macro list_tables(schema) %}
  {{ return(adapter.dispatch('list_tables', 'dbt_warehouse_profiler')(schema)) }}
{% endmacro %}

{% macro list_columns(schema, table) %}
  {{ return(adapter.dispatch('list_columns', 'dbt_warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% macro profile_table(schema, table) %}
  {{ return(adapter.dispatch('profile_table', 'dbt_warehouse_profiler')(schema, table)) }}
{% endmacro %}

