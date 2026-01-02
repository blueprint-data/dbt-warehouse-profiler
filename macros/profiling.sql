{% macro list_schemas(exclude_schemas=[]) %}
  {{ return(adapter.dispatch('list_schemas', 'warehouse_profiler')(exclude_schemas)) }}
{% endmacro %}

{% macro list_tables(schema) %}
  {{ return(adapter.dispatch('list_tables', 'warehouse_profiler')(schema)) }}
{% endmacro %}

{% macro list_columns(schema, table) %}
  {{ return(adapter.dispatch('list_columns', 'warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% macro profile_table(schema, table) %}
  {{ return(adapter.dispatch('profile_table', 'warehouse_profiler')(schema, table)) }}
{% endmacro %}
