{% macro list_databases(output_format='text') %}
  {{ return(adapter.dispatch('list_databases', 'dbt_warehouse_profiler')(output_format)) }}
{% endmacro %}

{% macro list_database_schemas(database=none, exclude_schemas=[], output_format='text') %}
  {{ return(adapter.dispatch('list_database_schemas', 'dbt_warehouse_profiler')(database, exclude_schemas, output_format)) }}
{% endmacro %}

{% macro list_tables(schema, database=none, output_format='text') %}
  {{ return(adapter.dispatch('list_tables', 'dbt_warehouse_profiler')(schema, database, output_format)) }}
{% endmacro %}

{% macro list_columns(schema, table, database=none, output_format='text') %}
  {{ return(adapter.dispatch('list_columns', 'dbt_warehouse_profiler')(schema, table, database, output_format)) }}
{% endmacro %}

{% macro profile_table(schema, table, database=none, output_format='text') %}
  {{ return(adapter.dispatch('profile_table', 'dbt_warehouse_profiler')(schema, table, database, output_format)) }}
{% endmacro %}

{% macro profile_columns(schema, table, database=none, output_format='text') %}
  {{ return(adapter.dispatch('profile_columns', 'dbt_warehouse_profiler')(schema, table, database, output_format)) }}
{% endmacro %}

{% macro execute_raw_query(query, output_format='text') %}
  {{ return(adapter.dispatch('execute_raw_query', 'dbt_warehouse_profiler')(query, output_format)) }}
{% endmacro %}
