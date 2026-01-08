{% macro list_databases() %}
  {{ return(adapter.dispatch('list_databases', 'dbt_warehouse_profiler')()) }}
{% endmacro %}

{% macro list_database_schemas(database=none, exclude_schemas=[]) %}
  {{ return(adapter.dispatch('list_database_schemas', 'dbt_warehouse_profiler')(database, exclude_schemas)) }}
{% endmacro %}

{% macro list_tables(schema, database=none) %}
  {{ return(adapter.dispatch('list_tables', 'dbt_warehouse_profiler')(schema, database)) }}
{% endmacro %}

{% macro list_columns(schema, table, database=none) %}
  {{ return(adapter.dispatch('list_columns', 'dbt_warehouse_profiler')(schema, table, database)) }}
{% endmacro %}

{% macro profile_table(schema, table, database=none) %}
  {{ return(adapter.dispatch('profile_table', 'dbt_warehouse_profiler')(schema, table, database)) }}
{% endmacro %}

{% macro profile_columns(schema, table, database=none) %}
  {{ return(adapter.dispatch('profile_columns', 'dbt_warehouse_profiler')(schema, table, database)) }}
{% endmacro %}

