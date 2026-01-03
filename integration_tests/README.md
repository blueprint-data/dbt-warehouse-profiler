# Integration Tests

This directory contains integration tests for dbt-warehouse-profiler.

## Running Tests

1. From the integration_tests directory, run:
   ```bash
   dbt deps
   dbt seed
   dbt run
   dbt test
   ```

2. Test the macros:
   ```bash
   dbt run-operation dbt_warehouse_profiler.list_schemas
   dbt run-operation dbt_warehouse_profiler.list_tables --args '{schema: "test_data"}'
   dbt run-operation dbt_warehouse_profiler.validate_dataset_sources --args '{schema: "test_data"}'
   ```

## Test Data

Add test data as seeds in the `seeds/` directory to validate package functionality.
