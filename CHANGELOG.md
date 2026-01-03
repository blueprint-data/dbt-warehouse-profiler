# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-03

### Added
- Initial release of dbt-warehouse-profiler
- BigQuery profiling macros (`list_schemas`, `list_tables`, `list_columns`, `profile_table`)
- Source validation macros (`validate_source`, `validate_dataset_sources`)
- Dataset exploration utilities
- Cross-database compatibility using dbt adapter dispatch pattern
- Configurable variables for BigQuery region, preview rows, and schema exclusions
- Comprehensive README with usage examples
- Integration tests framework
- GitHub templates for issues and pull requests
- Proper package structure following dbt best practices

### Supported Data Warehouses
- BigQuery (fully supported)
- Snowflake (planned)
- PostgreSQL (planned)

### Added
- Initial release of dbt-warehouse-profiler
- BigQuery profiling macros (`list_schemas`, `list_tables`, `list_columns`, `profile_table`)
- Source validation macros (`validate_source`, `validate_dataset_sources`)
- Dataset exploration utilities
- Cross-database compatibility using dbt adapter dispatch pattern
- Configurable variables for BigQuery region, preview rows, and schema exclusions
- Comprehensive README with usage examples

### Supported Data Warehouses
- BigQuery (fully supported)
- Snowflake (planned)
- PostgreSQL (planned)

[Unreleased]: https://github.com/blueprintdata/dbt-warehouse-profiler/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/blueprintdata/dbt-warehouse-profiler/releases/tag/v0.1.0
