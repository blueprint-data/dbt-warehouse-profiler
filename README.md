# dbt-warehouse-profiler

A dbt package for profiling and documenting data warehouse datasets. This package provides macros to explore your data warehouse, validate source documentation, and understand table structures.

[![CI](https://github.com/blueprintdata/dbt-warehouse-profiler/workflows/CI/badge.svg)](https://github.com/blueprintdata/dbt-warehouse-profiler/actions/workflows/ci.yml)
[![Documentation](https://github.com/blueprintdata/dbt-warehouse-profiler/workflows/Deploy%20Documentation/badge.svg)](https://blueprintdata.github.io/dbt-warehouse-profiler/)

## Supported Data Warehouses

- **BigQuery** (fully supported)
- **Snowflake** (fully supported)
- **PostgreSQL** (planned)

The package uses dbt's adapter dispatch pattern, making it extensible for additional databases.

## Installation

### Option 1: Install from dbt Hub (recommended)

Add the following to your `packages.yml`:

```yaml
packages:
  - package: blueprintdata/dbt_warehouse_profiler
    version: [">=0.1.0"]
```

Then run:

```bash
dbt deps
```

### Option 2: Local Development Setup

To set up the package for local development:

```bash
./scripts/setup-local.sh
```

This script will:
- Create a Python virtual environment
- Install dbt-core and your chosen database adapter (BigQuery, Snowflake, or PostgreSQL)
- Set up `profiles.yml` template
- Install package dependencies
- Validate the package

After setup, activate the virtual environment:
```bash
source venv/bin/activate
```

See [pyproject.toml](pyproject.toml) for dependency details.

## Configuration

Add the following to your `dbt_project.yml` to configure the package:

```yaml
vars:
  dbt_warehouse_profiler:
    bigquery:
      default_region: 'us'
      max_preview_rows: 10
      exclude_schemas: []
    snowflake:
      max_preview_rows: 10
      exclude_schemas: []
```

**Configuration Options:**

### BigQuery
- `default_region`: Default BigQuery region for cross-dataset queries (default: 'us')
- `max_preview_rows`: Maximum number of preview rows to show in profile_table (default: 10)
- `max_sample_values`: Maximum number of sample values to show in profile_columns (default: 5)
- `exclude_schemas`: Schemas to exclude from validation by default (default: [])

### Snowflake
- `max_preview_rows`: Maximum number of preview rows to show in profile_table (default: 10)
- `max_sample_values`: Maximum number of sample values to show in profile_columns (default: 5)
- `exclude_schemas`: Schemas to exclude from validation by default (default: [])

## JSON Output Format

All macros support an optional `output_format` parameter for programmatic consumption. By default, macros output human-readable text. Set `output_format: "json"` to get structured JSON output instead.

**Usage:**
```bash
# Human-readable output (default)
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "prod_data"}'

# JSON output for programmatic use
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "prod_data", output_format: "json"}'
```

**Example JSON Output:**
```json
{"tables": [{"name": "users", "type": "BASE TABLE"}, {"name": "orders", "type": "VIEW"}]}
```

This is useful for:
- Integrating with CI/CD pipelines
- Building automation scripts
- Feeding data into other tools and services
- Parsing output programmatically

## Available Macros

### 1. `list_databases`

List all databases accessible in your data warehouse.

**Usage:**
```bash
# Human-readable output
dbt run-operation dbt_warehouse_profiler.list_databases

# JSON output
dbt run-operation dbt_warehouse_profiler.list_databases --args '{output_format: "json"}'
```

**Parameters:**
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
- For **Snowflake**: Lists all accessible databases in the account
- For **BigQuery**: Returns the current project only

**JSON Schema:**
```json
{"databases": ["my_project"]}
```

**Note:**
- This macro is particularly useful for Snowflake where you may want to explore multiple databases to use as sources.
- **BigQuery limitation**: Due to BigQuery's INFORMATION_SCHEMA being project-scoped, this macro can only return the current project from your dbt profile. Listing all accessible projects requires API access beyond what dbt macros support. To explore data sources within your current project, use `list_database_schemas` to see all available datasets.

---

### 2. `list_database_schemas`

List all schemas (datasets) in your data warehouse.

**For Snowflake**, you can optionally specify which database to explore:

**Usage:**
```bash
# List schemas in current/default database
dbt run-operation dbt_warehouse_profiler.list_database_schemas

# Snowflake: List schemas in a specific database
dbt run-operation dbt_warehouse_profiler.list_database_schemas --args '{database: "RAW_DATA"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.list_database_schemas --args '{output_format: "json"}'
```

**With exclusions:**
```bash
dbt run-operation dbt_warehouse_profiler.list_database_schemas --args '{exclude_schemas: ["temp_schema", "test_schema"]}'
```

**Parameters:**
- `database` (optional, Snowflake only): Name of the database to explore. Defaults to target database.
- `exclude_schemas` (optional): List of schema names to exclude from results
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**JSON Schema:**
```json
{"schemas": ["public", "raw_data", "analytics"]}
```

---

### 3. `list_tables`

List all tables and views in a specific schema.

**Usage:**
```bash
# List tables in a schema
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "your_schema"}'

# Snowflake: List tables in a specific database and schema
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "PUBLIC", database: "RAW_DATA"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "your_schema", output_format: "json"}'
```

**Parameters:**
- `schema` (required): Name of the schema to list tables from
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
- Table name and type (BASE TABLE, VIEW, etc.)

**JSON Schema:**
```json
{"tables": [{"name": "users", "type": "BASE TABLE"}, {"name": "orders_view", "type": "VIEW"}]}
```

---

### 4. `list_columns`

List all columns in a specific table with their data types.

**Usage:**
```bash
# List columns in a table
dbt run-operation dbt_warehouse_profiler.list_columns --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: List columns in a specific database
dbt run-operation dbt_warehouse_profiler.list_columns --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.list_columns --args '{schema: "your_schema", table: "your_table", output_format: "json"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
- Column name, data type, and nullability

**JSON Schema:**
```json
{"columns": [{"name": "id", "data_type": "INT64", "is_nullable": "NO"}, {"name": "email", "data_type": "STRING", "is_nullable": "YES"}]}
```

---

### 5. `profile_table`

Get comprehensive profiling information about a table.

**Usage:**
```bash
# Profile a table
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: Profile a table in a specific database
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "your_schema", table: "your_table", output_format: "json"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
- Row count
- Table size in bytes
- Last modified timestamp
- Preview of first 10 rows
- Partitioning status (BigQuery)
- Clustering status (BigQuery and Snowflake)

**JSON Schema:**
```json
{
  "schema": "prod_data",
  "table": "users",
  "row_count": 1234567,
  "size_bytes": 56789012,
  "last_modified": "2024-01-15 10:30:00+00:00",
  "partitioned": true,
  "clustered": false,
  "preview": [{"id": "1", "name": "John", "email": "john@example.com"}]
}
```

---

### 6. `profile_columns`

Get detailed column-level statistics for a table.

**Usage:**
```bash
# Profile columns in a table
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: Profile columns in a specific database
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "your_schema", table: "your_table", output_format: "json"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
For each column, the macro outputs statistics based on column type:

- **Numeric columns**: Null count/%, distinct count, min, max, avg
- **Date/Time columns**: Null count/%, distinct count, min, max
- **String columns**: Null count/%, distinct count, sample values
- **Boolean columns**: Null count/%, distinct count, true/false counts
- **Complex types (ARRAY, STRUCT, JSON)**: Null count/% only

**Example Text Output:**
```
=== Column Profile for prod_data.users ===

Total rows: 1234567

Column: user_id (INT64, NOT NULL)
  Nulls: 0 (0.0%)
  Distinct: 1234567
  Min: 1 | Max: 1234567 | Avg: 617284.0

Column: email (STRING, NULLABLE)
  Nulls: 1234 (0.1%)
  Distinct: 1233333
  Samples: ['user@example.com', 'test@test.org', 'admin@company.io']

Column: is_active (BOOL, NOT NULL)
  Nulls: 0 (0.0%)
  Distinct: 2
  True: 1100000 (89.1%) | False: 134567 (10.9%)

=== Summary ===
Columns analyzed: 3
Columns with nulls: 1
```

**JSON Schema:**
```json
{
  "schema": "prod_data",
  "table": "users",
  "total_rows": 1234567,
  "columns": [
    {
      "name": "user_id",
      "data_type": "INT64",
      "is_nullable": false,
      "null_count": 0,
      "null_percentage": 0.0,
      "distinct_count": 1234567,
      "min": 1,
      "max": 1234567,
      "avg": 617284.0,
      "type_category": "numeric"
    },
    {
      "name": "email",
      "data_type": "STRING",
      "is_nullable": true,
      "null_count": 1234,
      "null_percentage": 0.1,
      "distinct_count": 1233333,
      "samples": ["user@example.com", "test@test.org"],
      "type_category": "string"
    }
  ],
  "summary": {
    "columns_analyzed": 3,
    "columns_with_nulls": 1
  }
}
```

---

### 7. `validate_source`

Check if a specific table is declared as a dbt source and has documentation.

**Usage:**
```bash
# Validate a source
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: Validate a source in a specific database
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "your_schema", table: "your_table", output_format: "json"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
- Whether table is declared as a source
- Source name if found
- Whether documentation exists
- Preview of description

**JSON Schema:**
```json
{
  "schema": "raw_data",
  "table": "events",
  "source_declared": true,
  "source_name": "raw_events",
  "has_documentation": true,
  "description": "Raw event data from the application..."
}
```

---

### 8. `validate_dataset_sources`

Scan all tables in a dataset and validate their source declarations.

**Usage:**
```bash
# Validate all sources in a schema
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "your_schema"}'

# Snowflake: Validate sources in a specific database
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "PUBLIC", database: "RAW_DATA"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "your_schema", output_format: "json"}'
```

**Parameters:**
- `schema` (required): Name of the schema to validate
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
- List of all tables with their documentation status
- Summary statistics showing percentage of documented sources

**JSON Schema:**
```json
{
  "schema": "raw_data",
  "tables": [
    {"name": "users", "source_declared": true, "source_name": "raw", "has_documentation": true, "status": "documented"},
    {"name": "events", "source_declared": true, "source_name": "raw", "has_documentation": false, "status": "undocumented"},
    {"name": "temp_data", "source_declared": false, "source_name": null, "has_documentation": false, "status": "not_declared"}
  ],
  "summary": {
    "total_tables": 3,
    "declared_sources": 2,
    "declared_percentage": 66.7,
    "documented_sources": 1,
    "documented_percentage": 33.3
  }
}
```

---

### 9. `execute_raw_query`

Execute arbitrary SQL queries against your data warehouse for ad-hoc analysis.

**Usage:**
```bash
# Execute a simple SELECT query
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT * FROM my_schema.my_table LIMIT 10"}'

# Execute an aggregation query
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT status, COUNT(*) as count FROM orders GROUP BY status"}'

# JSON output
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT * FROM my_schema.my_table LIMIT 10", output_format: "json"}'

# Execute a JOIN query (BigQuery)
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT u.name, o.total FROM `project.dataset.users` u JOIN `project.dataset.orders` o ON u.id = o.user_id LIMIT 100"}'

# Execute a query with complex filters (Snowflake)
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT * FROM ANALYTICS.PUBLIC.SALES WHERE sale_date >= '\''2024-01-01'\'' AND amount > 1000 ORDER BY amount DESC LIMIT 50"}'
```

**Parameters:**
- `query` (required): The SQL query to execute
- `output_format` (optional): Output format - `"text"` (default) or `"json"`

**Output:**
- Number of rows and columns returned
- Column headers
- All result rows

**JSON Schema:**
```json
{
  "query": "SELECT * FROM users LIMIT 2",
  "success": true,
  "row_count": 2,
  "column_count": 3,
  "columns": ["id", "name", "email"],
  "rows": [
    {"id": "1", "name": "John", "email": "john@example.com"},
    {"id": "2", "name": "Jane", "email": "jane@example.com"}
  ]
}
```

**Security Notes:**
- Uses your existing dbt profile credentials
- Subject to your warehouse user permissions
- No additional authentication required
- Be cautious with queries that return large result sets (use LIMIT)

**Tips:**
- Always use LIMIT to control output size for large tables
- For BigQuery, remember to use backticks for fully qualified table names
- For Snowflake, use proper quoting for case-sensitive identifiers
- Single quotes in queries need to be escaped as `'\''` in bash arguments

## Examples

### Quick Dataset Exploration (BigQuery)

```bash
# List the current project
dbt run-operation dbt_warehouse_profiler.list_databases

# List all datasets
dbt run-operation dbt_warehouse_profiler.list_database_schemas

# Explore a specific dataset
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "prod_data"}'

# Profile a specific table
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "prod_data", table: "users"}'

# Get detailed column statistics
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "prod_data", table: "users"}'
```

### Quick Database Exploration (Snowflake)

```bash
# List all accessible databases
dbt run-operation dbt_warehouse_profiler.list_databases

# List schemas in a specific database
dbt run-operation dbt_warehouse_profiler.list_database_schemas --args '{database: "RAW_DATA"}'

# Explore tables in a specific database and schema
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "PUBLIC", database: "RAW_DATA"}'

# Profile a specific table
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "PUBLIC", table: "CUSTOMERS", database: "RAW_DATA"}'

# Get detailed column statistics
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "PUBLIC", table: "CUSTOMERS", database: "RAW_DATA"}'
```

### Documentation Validation

```bash
# BigQuery: Validate all sources in a dataset
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "raw_data"}'

# Snowflake: Validate all sources in a database schema
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "PUBLIC", database: "RAW_DATA"}'

# Check a specific table
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "raw_data", table: "events"}'
```

### Ad-Hoc Query Execution

```bash
# Execute a simple query
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT COUNT(*) FROM my_schema.users"}'

# BigQuery: Analyze data with aggregations
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT DATE(created_at) as date, COUNT(*) as signups FROM `project.dataset.users` WHERE created_at >= '\''2024-01-01'\'' GROUP BY date ORDER BY date DESC LIMIT 30"}'

# Snowflake: Join multiple tables
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT c.customer_name, COUNT(o.order_id) as order_count, SUM(o.total) as total_spent FROM CUSTOMERS c LEFT JOIN ORDERS o ON c.id = o.customer_id GROUP BY c.customer_name ORDER BY total_spent DESC LIMIT 20"}'
```

### JSON Output for Automation

```bash
# Get table list as JSON for scripting
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "prod_data", output_format: "json"}' 2>&1 | grep '^{' | jq .

# Profile columns and parse with jq
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "prod_data", table: "users", output_format: "json"}' 2>&1 | grep '^{' | jq '.columns[] | select(.null_percentage > 10)'

# Validate sources and check coverage
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "raw_data", output_format: "json"}' 2>&1 | grep '^{' | jq '.summary.documented_percentage'
```

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

### Release Process

This project uses semantic-release for automated versioning. When contributing:

- Use conventional commit messages: `feat:`, `fix:`, `docs:`, etc.
- Pushing to `main` will automatically trigger a release if needed
- See [RELEASE.md](RELEASE.md) for detailed release workflow

### Running Tests

To run integration tests locally:

```bash
cd integration_tests
dbt deps
dbt seed
dbt run
dbt test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a detailed history of changes.
