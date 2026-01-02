# Plan: Make warehouse-profiler Adapter-Agnostic

## Overview
Refactor the warehouse-profiler package to use dbt's adapter dispatch pattern, making it extensible for future database adapters while currently only implementing BigQuery support.

## Goals
1. Create dispatcher macros for all 6 public macros
2. Rename current implementations with `bigquery__` prefix
3. Scope configuration variables to BigQuery namespace
4. Update documentation to reflect adapter-agnostic architecture
5. Maintain backward compatibility where possible

## Implementation Steps

### Step 1: Update dbt_project.yml Configuration
**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/dbt_project.yml`

**Changes:**
- Move variables from `warehouse_profiler:` to `warehouse_profiler:bigquery:`
- Update dispatch configuration to properly route to bigquery implementations

**Before:**
```yaml
vars:
  warehouse_profiler:
    default_region: 'us'
    exclude_schemas: []
    max_preview_rows: 10
```

**After:**
```yaml
vars:
  warehouse_profiler:
    bigquery:
      default_region: 'us'
      exclude_schemas: []
      max_preview_rows: 10

dispatch:
  - macro_namespace: warehouse_profiler
    search_order: ['warehouse_profiler', 'dbt']
```

### Step 2: Reorganize Macro Files
**Directory structure:**
```
macros/
├── profiling.sql          (dispatcher macros - public interface)
├── validation.sql         (dispatcher macros - public interface)
└── bigquery/
    ├── profiling.sql      (bigquery__ implementations)
    └── validation.sql     (bigquery__ implementations)
```

### Step 3: Create Dispatcher Macros in profiling.sql
**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/profiling.sql`

**Replace entire file with dispatchers:**

```sql
{% macro list_schemas(exclude_schemas=[]) %}
  {{ return(adapter.dispatch('list_schemas', 'warehouse_profiler')(exclude_schemas)) }}
{% endmacro %}

{% macro list_tables(schema) %}
  {{ return(adapter.dispatch('list_tables', 'warehouse_profiler')(schema)) }}
{% endmacro %}

{% macro list_columns(schema, table) %}
  {{ return(adapter.dispatch('list_columns', 'warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% macro profile_table(schema, table) %}
  {{ return(adapter.dispatch('profile_table', 'warehouse_profiler')(schema, table)) }}
{% endmacro %}
```

### Step 4: Create BigQuery Implementations
**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/bigquery/profiling.sql`

**Create with `bigquery__` prefixed versions:**
- `bigquery__list_schemas(exclude_schemas=[])`
- `bigquery__list_tables(schema)`
- `bigquery__list_columns(schema, table)`
- `bigquery__profile_table(schema, table)`

**Update variable references:**
- Change `var('warehouse_profiler:default_region', 'us')`
- To: `var('warehouse_profiler:bigquery:default_region', 'us')`
- Change `var('warehouse_profiler:max_preview_rows', 10)`
- To: `var('warehouse_profiler:bigquery:max_preview_rows', 10)`
- Change `var('warehouse_profiler:exclude_schemas', [])`
- To: `var('warehouse_profiler:bigquery:exclude_schemas', [])`

### Step 5: Update Validation Macros
**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/validation.sql`

**Replace with dispatcher:**
```sql
{% macro validate_source(schema, table) %}
  {{ return(adapter.dispatch('validate_source', 'warehouse_profiler')(schema, table)) }}
{% endmacro %}

{% macro validate_dataset_sources(schema) %}
  {{ return(adapter.dispatch('validate_dataset_sources', 'warehouse_profiler')(schema)) }}
{% endmacro %}
```

**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/bigquery/validation.sql`

**Create BigQuery implementations:**
- `bigquery__validate_source(schema, table)` - Keep as-is (database-agnostic)
- `bigquery__validate_dataset_sources(schema)` - Update INFORMATION_SCHEMA query

### Step 6: Update README.md
**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/README.md`

**Changes:**
1. Update "Supported Data Warehouses" section:
```markdown
## Supported Data Warehouses

- **BigQuery** (fully supported)
- **Snowflake** (planned)
- **PostgreSQL** (planned)

The package uses dbt's adapter dispatch pattern, making it extensible for additional databases.
```

2. Update configuration example:
```yaml
vars:
  warehouse_profiler:
    bigquery:
      default_region: 'us'
      max_preview_rows: 10
      exclude_schemas: []
```

3. Add note about adapter-agnostic design

### Step 7: Update Integration Tests
**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/integration_tests/dbt_project.yml`

**Update variable scoping:**
```yaml
vars:
  warehouse_profiler:
    bigquery:
      default_region: 'us'
      max_preview_rows: 5
      exclude_schemas: ['temp', 'scratch']
```

**File:** `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/integration_tests/README.md`

**Update configuration examples to use new variable structure**

## Critical Files to Modify

1. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/dbt_project.yml`
2. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/profiling.sql`
3. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/validation.sql`
4. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/bigquery/profiling.sql` (new)
5. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/macros/bigquery/validation.sql` (new)
6. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/README.md`
7. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/integration_tests/dbt_project.yml`
8. `/Users/manuel/Documents/projects/blueprintdata/warehouse-profiler/integration_tests/README.md`

## Testing Strategy

After implementation:
1. Run `dbt deps` in integration_tests/
2. Test each macro operation:
   - `dbt run-operation warehouse_profiler.list_schemas`
   - `dbt run-operation warehouse_profiler.list_tables --args '{schema: "test"}'`
   - `dbt run-operation warehouse_profiler.profile_table --args '{schema: "test", table: "table"}'`
   - `dbt run-operation warehouse_profiler.validate_dataset_sources --args '{schema: "test"}'`
3. Verify variables are read correctly from new namespace

## Migration Notes for Users

Users will need to update their `dbt_project.yml`:

**Old:**
```yaml
vars:
  warehouse_profiler:
    default_region: 'us'
```

**New:**
```yaml
vars:
  warehouse_profiler:
    bigquery:
      default_region: 'us'
```

This is a **breaking change** but necessary for proper adapter scoping. Include in CHANGELOG and README.

## Benefits

1. **Extensibility**: Easy to add Snowflake, PostgreSQL, etc. in the future
2. **Clear separation**: BigQuery-specific code is clearly isolated
3. **Standard pattern**: Follows dbt community best practices
4. **Maintainability**: Each adapter implementation is independent
5. **Future-proof**: Ready for multi-database support without refactoring

## Next Steps After Implementation

1. Update version to 2.0.0 (breaking change)
2. Update CHANGELOG.md
3. Consider creating default__ implementations for common databases
4. Add contribution guide for new adapters
