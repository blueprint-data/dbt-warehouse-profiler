## [1.1.3](https://github.com/blueprint-data/dbt-warehouse-profiler/compare/v1.1.2...v1.1.3) (2026-01-05)


### Bug Fixes

* change list_schemas macro name to avoid conflicts ([59ebb79](https://github.com/blueprint-data/dbt-warehouse-profiler/commit/59ebb799a6e34ccbeb2dde07d51506414474ce06))
* parse issue ([ff3eedd](https://github.com/blueprint-data/dbt-warehouse-profiler/commit/ff3eedd859718a42652ed1c5e9d89feb2e13a713))

## [1.1.2](https://github.com/blueprint-data/dbt-warehouse-profiler/compare/v1.1.1...v1.1.2) (2026-01-05)


### Bug Fixes

* add snowflake support ([986c3d3](https://github.com/blueprint-data/dbt-warehouse-profiler/commit/986c3d3bb48a07009222bee1e41cf1c5d0e539d1))

## [1.1.1](https://github.com/blueprint-data/dbt-warehouse-profiler/compare/v1.1.0...v1.1.1) (2026-01-03)


### Bug Fixes

* release workflow ([666d486](https://github.com/blueprint-data/dbt-warehouse-profiler/commit/666d486660c3aeba92c135fe9d348aa97dbd4a25))

# [1.1.0](https://github.com/blueprint-data/dbt-warehouse-profiler/compare/v1.0.0...v1.1.0) (2026-01-03)


### Features

* publish first release ([514dcfa](https://github.com/blueprint-data/dbt-warehouse-profiler/commit/514dcfae85e7d5c5c45f5b0a6570ed617b80b348))

# 1.0.0 (2026-01-03)


### Bug Fixes

* readme ([3973bb1](https://github.com/blueprint-data/dbt-warehouse-profiler/commit/3973bb1790ba84ba61a791676c0ad6295feacf81))


### Features

* add dbt bigquery adapter for warehouse profiler ([56c6b89](https://github.com/blueprint-data/dbt-warehouse-profiler/commit/56c6b895992ab139cb4448a3f9f0e36d15ed802e))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-03

### Added
- Initial release of dbt-warehouse-profiler
- BigQuery profiling macros (`list_database_schemas`, `list_tables`, `list_columns`, `profile_table`)
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
- BigQuery profiling macros (`list_database_schemas`, `list_tables`, `list_columns`, `profile_table`)
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
