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

## Available Macros

### 1. `list_databases`

List all databases accessible in your data warehouse.

**Usage:**
```bash
dbt run-operation dbt_warehouse_profiler.list_databases
```

**Output:**
- For **Snowflake**: Lists all accessible databases in the account
- For **BigQuery**: Returns the current project only

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
```

**With exclusions:**
```bash
dbt run-operation dbt_warehouse_profiler.list_database_schemas --args '{exclude_schemas: ["temp_schema", "test_schema"]}'
```

**Parameters:**
- `database` (optional, Snowflake only): Name of the database to explore. Defaults to target database.
- `exclude_schemas` (optional): List of schema names to exclude from results

---

### 3. `list_tables`

List all tables and views in a specific schema.

**Usage:**
```bash
# List tables in a schema
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "your_schema"}'

# Snowflake: List tables in a specific database and schema
dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "PUBLIC", database: "RAW_DATA"}'
```

**Parameters:**
- `schema` (required): Name of the schema to list tables from
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.

**Output:**
- Table name and type (BASE TABLE, VIEW, etc.)

---

### 4. `list_columns`

List all columns in a specific table with their data types.

**Usage:**
```bash
# List columns in a table
dbt run-operation dbt_warehouse_profiler.list_columns --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: List columns in a specific database
dbt run-operation dbt_warehouse_profiler.list_columns --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.

**Output:**
- Column name, data type, and nullability

---

### 5. `profile_table`

Get comprehensive profiling information about a table.

**Usage:**
```bash
# Profile a table
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: Profile a table in a specific database
dbt run-operation dbt_warehouse_profiler.profile_table --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.

**Output:**
- Row count
- Table size in bytes
- Last modified timestamp
- Preview of first 10 rows
- Partitioning status (BigQuery)
- Clustering status (BigQuery and Snowflake)

---

### 6. `profile_columns`

Get detailed column-level statistics for a table.

**Usage:**
```bash
# Profile columns in a table
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: Profile columns in a specific database
dbt run-operation dbt_warehouse_profiler.profile_columns --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.

**Output:**
For each column, the macro outputs statistics based on column type:

- **Numeric columns**: Null count/%, distinct count, min, max, avg
- **Date/Time columns**: Null count/%, distinct count, min, max
- **String columns**: Null count/%, distinct count, sample values
- **Boolean columns**: Null count/%, distinct count, true/false counts
- **Complex types (ARRAY, STRUCT, JSON)**: Null count/% only

**Example Output:**
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

---

### 7. `validate_source`

Check if a specific table is declared as a dbt source and has documentation.

**Usage:**
```bash
# Validate a source
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "your_schema", table: "your_table"}'

# Snowflake: Validate a source in a specific database
dbt run-operation dbt_warehouse_profiler.validate_source --args '{schema: "PUBLIC", table: "users", database: "RAW_DATA"}'
```

**Parameters:**
- `schema` (required): Name of the schema
- `table` (required): Name of the table
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.

**Output:**
- Whether table is declared as a source
- Source name if found
- Whether documentation exists
- Preview of description

---

### 8. `validate_dataset_sources`

Scan all tables in a dataset and validate their source declarations.

**Usage:**
```bash
# Validate all sources in a schema
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "your_schema"}'

# Snowflake: Validate sources in a specific database
dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "PUBLIC", database: "RAW_DATA"}'
```

**Parameters:**
- `schema` (required): Name of the schema to validate
- `database` (optional, Snowflake only): Name of the database. Defaults to target database.

**Output:**
- List of all tables with their documentation status
- Summary statistics showing percentage of documented sources

---

### 9. `execute_raw_query`

Execute arbitrary SQL queries against your data warehouse for ad-hoc analysis.

**Usage:**
```bash
# Execute a simple SELECT query
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT * FROM my_schema.my_table LIMIT 10"}'

# Execute an aggregation query
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT status, COUNT(*) as count FROM orders GROUP BY status"}'

# Execute a JOIN query (BigQuery)
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT u.name, o.total FROM `project.dataset.users` u JOIN `project.dataset.orders` o ON u.id = o.user_id LIMIT 100"}'

# Execute a query with complex filters (Snowflake)
dbt run-operation dbt_warehouse_profiler.execute_raw_query --args '{query: "SELECT * FROM ANALYTICS.PUBLIC.SALES WHERE sale_date >= '\''2024-01-01'\'' AND amount > 1000 ORDER BY amount DESC LIMIT 50"}'
```

**Parameters:**
- `query` (required): The SQL query to execute

**Output:**
- Number of rows and columns returned
- Column headers
- All result rows

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
