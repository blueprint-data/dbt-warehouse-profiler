{% macro validate_source(schema, table) %}
  {{ return(adapter.dispatch('validate_source', 'dbt_warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% docs validate_source %}
Checks if a specific table is declared as a dbt source and has documentation. Returns the source name and documentation status if found.

**Arguments:**
- `schema` (required, string): Name of the schema
- `table` (required, string): Name of the table

**Output:**
- Whether table is declared as a source
- Source name if found
- Whether documentation exists
- Preview of description

**Example:**
```bash
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "your_schema", table: "your_table"}'
```
{% enddocs %}

{% macro validate_dataset_sources(schema) %}
  {{ return(adapter.dispatch('validate_dataset_sources', 'dbt_warehouse_profiler')(schema)) }}
{% endmacro %}

{% docs validate_dataset_sources %}
Scans all tables in a dataset and validates their source declarations. Provides summary statistics showing the percentage of tables that are declared as sources and have documentation.

**Arguments:**
- `schema` (required, string): Name of the schema to validate

**Output:**
- List of all tables with their documentation status
- Summary statistics showing percentage of documented sources

**Example:**
```bash
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "your_schema"}'
```
{% enddocs %}
