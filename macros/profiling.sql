{% macro list_schemas(exclude_schemas=[]) %}
  {{ return(adapter.dispatch('list_schemas', 'dbt_warehouse_profiler')(exclude_schemas)) }}
{% endmacro %}

{% docs list_schemas %}
Lists all schemas (datasets) in your data warehouse project. Supports excluding specific schemas from the results.

**Arguments:**
- `exclude_schemas` (optional, list): List of schema names to exclude from results

**Example:**
```bash
dbt run-operation dbt_warehouse_profiler.list_schemas
dbt run-operation dbt_warehouse_profiler.list_schemas --args '{exclude_schemas: ["temp_schema", "test_schema"]}'
```
{% enddocs %}

{% macro list_tables(schema) %}
  {{ return(adapter.dispatch('list_tables', 'dbt_warehouse_profiler')(schema)) }}
{% endmacro %}

{% docs list_tables %}
Lists all tables and views in a specific schema. Returns table names along with their types (BASE TABLE, VIEW, etc.)

**Arguments:**
- `schema` (required, string): Name of the schema to list tables from

**Example:**
```bash
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "your_schema"}'
```
{% enddocs %}

{% macro list_columns(schema, table) %}
  {{ return(adapter.dispatch('list_columns', 'dbt_warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% docs list_columns %}
Lists all columns in a specific table with their data types and nullability. Results are ordered by the column's ordinal position in the table.

**Arguments:**
- `schema` (required, string): Name of the schema
- `table` (required, string): Name of the table

**Example:**
```bash
dbt run-operation dbt_warehouse_profiler.list_columns --args '{schema: "your_schema", table: "your_table"}'
```
{% enddocs %}

{% macro profile_table(schema, table) %}
  {{ return(adapter.dispatch('profile_table', 'dbt_warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% docs profile_table %}
Provides comprehensive profiling information about a table including row count, table size in bytes, last modified timestamp, preview of first N rows (configurable), partitioning status, and clustering status.

**Arguments:**
- `schema` (required, string): Name of the schema
- `table` (required, string): Name of the table

**Configuration:**
The number of preview rows can be configured via `dbt_project.yml`:
```yaml
vars:
  dbt_warehouse_profiler:
    bigquery:
      max_preview_rows: 10
```

**Example:**
```bash
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "your_schema", table: "your_table"}'
```
{% enddocs %}
