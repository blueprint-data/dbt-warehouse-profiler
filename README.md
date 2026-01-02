# warehouse-profiler

A dbt package for profiling and documenting data warehouse datasets. This package provides macros to explore your data warehouse, validate source documentation, and understand table structures.

## Supported Data Warehouses

- **BigQuery** (fully supported)
- **Snowflake** (planned)
- **PostgreSQL** (planned)

The package uses dbt's adapter dispatch pattern, making it extensible for additional databases.

## Installation

Add the following to your `packages.yml`:

```yaml
packages:
  - local: /path/to/warehouse-profiler
```

Then run:

```bash
dbt deps
```

## Configuration

Add the following to your `dbt_project.yml` to configure the package:

```yaml
vars:
  dbt_warehouse_profiler:
    bigquery:
      default_region: 'us'
      max_preview_rows: 10
      exclude_schemas: []
```

**Configuration Options:**

- `default_region`: Default BigQuery region for cross-dataset queries (default: 'us')
- `max_preview_rows`: Maximum number of preview rows to show in profile_table (default: 10)
- `exclude_schemas`: Schemas to exclude from validation by default (default: [])

## Available Macros

### 1. `list_schemas`

List all schemas (datasets) in your BigQuery project.

**Usage:**
```bash
dbt run-operation dbt_warehouse_profiler.list_schemas
```

**With exclusions:**
```bash
dbt run-operation dbt_warehouse_profiler.list_schemas --args '{exclude_schemas: ["temp_schema", "test_schema"]}'
```

**Parameters:**
- `exclude_schemas` (optional): List of schema names to exclude from results

---

### 2. `list_tables`

List all tables and views in a specific schema.

**Usage:**
```bash
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "your_schema"}'
```

**Parameters:**
- `schema` (required): Name of the schema to list tables from

**Output:**
- Table name and type (BASE TABLE, VIEW, etc.)

---

### 3. `list_columns`

List all columns in a specific table with their data types.

**Usage:**
```bash
dbt run-operation dbt_warehouse_profiler.list_columns --args '{schema: "your_schema", table: "your_table"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table

**Output:**
- Column name, data type, and nullability

---

### 4. `profile_table`

Get comprehensive profiling information about a table.

**Usage:**
```bash
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "your_schema", table: "your_table"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table

**Output:**
- Row count
- Table size in bytes
- Last modified timestamp
- Preview of first 10 rows
- Partitioning status
- Clustering status

---

### 5. `validate_source`

Check if a specific table is declared as a dbt source and has documentation.

**Usage:**
```bash
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "your_schema", table: "your_table"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table

**Output:**
- Whether table is declared as a source
- Source name if found
- Whether documentation exists
- Preview of description

---

### 6. `validate_dataset_sources`

Scan all tables in a dataset and validate their source declarations.

**Usage:**
```bash
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "your_schema"}'
```

**Parameters:**
- `schema` (required): Name of the schema to validate

**Output:**
- List of all tables with their documentation status
- Summary statistics showing percentage of documented sources

## Examples

### Quick Dataset Exploration

```bash
# List all datasets
dbt run-operation dbt_warehouse_profiler.list_schemas

# Explore a specific dataset
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "prod_data"}'

# Profile a specific table
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "prod_data", table: "users"}'
```

### Documentation Validation

```bash
# Validate all sources in a dataset
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "raw_data"}'

# Check a specific table
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "raw_data", table: "events"}'
```

## Requirements

- dbt Core >= 1.0.0
- BigQuery adapter
- Appropriate BigQuery permissions to query:
  - `INFORMATION_SCHEMA.TABLES`
  - `INFORMATION_SCHEMA.COLUMNS`
  - `__TABLES__` metadata

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

### Version 1.0.0
- Initial release
- BigQuery profiling macros
- Source validation macros
- Dataset exploration utilities
- Uses dbt adapter dispatch pattern for extensibility
